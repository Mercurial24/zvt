# -*- coding: utf-8 -*-
import os
import re

import pandas as pd
from sqlalchemy import Column, String, Float, func
from sqlalchemy.orm import declarative_base

from zvt import zvt_env
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.contract.register import register_schema
from zvt.contract import Mixin
from zvt.domain import Stock
from zvt.contract.api import df_to_db
from zvt.recorders.xysz.xysz_api import get_xysz_client
from zvt.utils.time_utils import now_pd_timestamp

# AmazingData 库即使 is_local=False 也会拼接 local_path + 子目录，
# 必须提供有效可写路径，否则触发 NoneType + str 错误。
# 与其它 xysz recorder 一致使用 data_path/xysz/cache。若环境变量为 Windows 路径（如 D://...），
# 在 Linux 下 AmazingData 库可能误在当前目录建 "D:" 目录，故仅当为非 Windows 风格时才用环境变量。
def _get_adj_factor_cache_path():
    default = os.path.join(zvt_env["data_path"], "xysz", "cache") + os.sep
    env_path = os.environ.get("XYSZ_LOCAL_CACHE_PATH", "").strip()
    if not env_path or re.match(r"^[A-Za-z]:[/\\]", env_path):
        return default
    path = os.path.abspath(env_path.rstrip("/\\")) + os.sep
    return path


_XYSZ_LOCAL_CACHE_PATH = _get_adj_factor_cache_path()
os.makedirs(_XYSZ_LOCAL_CACHE_PATH, exist_ok=True)

# 每批次拉取的股票数量，可根据网络和服务器性能调整
# AmazingData 服务端能承载几千只股票一起拉取（amazing_data_down 里是一次拉全市场）
# 这里设置为 3000 一口拉完 A 股一半
BATCH_SIZE = 3000

# ---------------------------------------------------------------------------
# Schema 定义
# ---------------------------------------------------------------------------
StockAdjFactorBase = declarative_base()


class StockAdjFactor(StockAdjFactorBase, Mixin):
    __tablename__ = "stock_adj_factor"

    provider = Column(String(length=32))
    code = Column(String(length=32))

    # 累积后复权因子
    hfq_factor = Column(Float)
    # 单次除权因子（暂未使用）
    qfq_factor = Column(Float)


register_schema(
    providers=["xysz"],
    db_name="stock_adj_factor",
    schema_base=StockAdjFactorBase,
    entity_type="stock",
)


# ---------------------------------------------------------------------------
# Recorder
# ---------------------------------------------------------------------------
class xyszStockAdjFactorRecorder(FixedCycleDataRecorder):
    provider = "xysz"
    entity_provider = "xysz"
    entity_schema = Stock
    data_schema = StockAdjFactor

    def __init__(self, force_update=False, sleeping_time=0, **kwargs):
        self.client = get_xysz_client()
        super().__init__(force_update, sleeping_time, **kwargs)

    # ------------------------------------------------------------------
    # 内部工具方法
    # ------------------------------------------------------------------

    @staticmethod
    def _get_xysz_code(entity) -> str:
        """从 entity 构造星耀数智股票代码（如 000001.SZ）。"""
        exchange = entity.exchange
        if not exchange:
            # entity.id 格式为 "stock_sz_000001"，第二段是交易所
            parts = entity.id.split("_")
            exchange = parts[1] if len(parts) >= 2 else ""
        return f"{entity.code}.{exchange.upper()}"

    def _batch_query_latest_timestamps(self) -> dict:
        """
        一次 SQL 查询获取所有 entity 的最新数据时间戳，
        替代逐个 entity 查询，大幅减少数据库交互次数。

        Returns:
            {entity_id: latest_timestamp (datetime)}
        """
        try:
            rows = (
                self.session.query(
                    self.data_schema.entity_id,
                    func.max(self.data_schema.timestamp),
                )
                .group_by(self.data_schema.entity_id)
                .all()
            )
            return {row[0]: row[1] for row in rows}
        except Exception as e:
            self.logger.warning(
                f"批量查询最新时间戳失败，将降级为逐个查询: {e}"
            )
            return {}

    # ------------------------------------------------------------------
    # 核心：重写 run() 实现批量拉取
    # ------------------------------------------------------------------

    def run(self):
        """
        重写基类 run()，改为批量拉取复权因子：

        原始方案：5486 次单独 API 调用，每次 ~15s → 总计约 26 小时
        批量方案：~55 次批量 API 调用（每批 100 只）→ 总计约 1 小时

        增量更新时（每日已有数据），绝大多数 entity 会被快速跳过（size=0）。
        """
        now = now_pd_timestamp()
        # 考虑到收盘后数据就绪时间，如果当前时间小于 16:00，则逻辑上的“今天”应当是昨天。
        # 这样避免在数据未就绪时重复拉取今天可能不完整或还未产生的数据。
        if now.hour < 16:
            logical_today = now.date() - pd.Timedelta(days=1)
        else:
            logical_today = now.date()

        # Step 1: 批量查询已有数据的最新时间戳
        self.logger.info(
            f"批量查询 {len(self.entities)} 只股票的已有数据时间戳... (逻辑更新目标: {logical_today})"
        )
        latest_ts_map = self._batch_query_latest_timestamps()

        # Step 2: 筛选出需要更新的 entity
        tasks = []  # [(entity, xysz_code, start_timestamp), ...]
        skipped = 0

        for entity in self.entities:
            # 未上市，跳过
            if entity.timestamp and entity.timestamp.date() > logical_today:
                skipped += 1
                continue

            latest_saved_ts = latest_ts_map.get(entity.id, entity.timestamp)

            # 已有目标日期（或更新）的数据，跳过
            if latest_saved_ts is not None:
                try:
                    if pd.Timestamp(latest_saved_ts).date() >= logical_today:
                        skipped += 1
                        continue
                except Exception:
                    pass

            xysz_code = self._get_xysz_code(entity)
            tasks.append((entity, xysz_code, latest_saved_ts))

        self.logger.info(
            f"待更新: {len(tasks)} 只 | 已是最新/跳过: {skipped} 只"
        )

        if not tasks:
            self.logger.info("所有股票数据已是最新，无需更新。")
            self.on_finish()
            return

        # Step 3: 分批拉取
        total_batches = (len(tasks) + BATCH_SIZE - 1) // BATCH_SIZE
        saved_entity_count = 0
        error_count = 0

        for batch_start in range(0, len(tasks), BATCH_SIZE):
            batch = tasks[batch_start : batch_start + BATCH_SIZE]
            current_batch_num = batch_start // BATCH_SIZE + 1
            code_list = [xysz_code for _, xysz_code, _ in batch]

            self.logger.info(
                f"Batch {current_batch_num}/{total_batches}: "
                f"拉取 {len(code_list)} 只股票的后复权因子..."
            )

            # 批量拉取 API
            try:
                df_back = self.client.get_backward_factor(
                    code_list=code_list,
                    local_path=_XYSZ_LOCAL_CACHE_PATH,
                    is_local=False,
                )
            except Exception as e:
                self.logger.error(f"Batch {current_batch_num} 拉取失败: {e}")
                error_count += len(batch)
                continue

            if df_back is None or df_back.empty:
                self.logger.warning(f"Batch {current_batch_num} 返回空数据，跳过")
                continue

            # 整理日期列：reset_index 后第一列为交易日期
            df_back = df_back.reset_index()
            date_col = df_back.columns[0]
            if date_col == "index":
                df_back = df_back.rename(columns={"index": "timestamp"})
                date_col = "timestamp"

            # Parse date robustly
            if pd.api.types.is_integer_dtype(df_back[date_col]) or pd.api.types.is_float_dtype(df_back[date_col]):
                # Filter out invalid small integers (like 0)
                df_back = df_back[df_back[date_col] > 19000101]
                if not df_back.empty:
                    df_back[date_col] = pd.to_datetime(df_back[date_col].astype(int).astype(str), format="%Y%m%d")
            else:
                df_back[date_col] = pd.to_datetime(df_back[date_col], errors='coerce')
            
            if df_back.empty:
                continue

            # Step 4: 逐 entity 切片并收集，每 50 只批量写入一次数据库
            import tqdm
            dfs_to_save = []
            
            for entity, xysz_code, start_ts in tqdm.tqdm(batch, desc=f"Batch {current_batch_num}/{total_batches} 整理&写入"):
                if xysz_code not in df_back.columns:
                    self.logger.warning(f"{xysz_code} 不在响应列中，跳过")
                    continue

                entity_df = df_back[[date_col, xysz_code]].copy()
                entity_df.columns = ["timestamp", "hfq_factor"]

                # 增量过滤：只保留 start_ts 之后（含）且在逻辑更新目标日期之前的记录
                if start_ts is not None:
                    entity_df = entity_df[
                        (entity_df["timestamp"] >= pd.Timestamp(start_ts))
                        & (entity_df["timestamp"].dt.date <= logical_today)
                    ]
                else:
                    entity_df = entity_df[entity_df["timestamp"].dt.date <= logical_today]

                # 过滤无效值
                entity_df = entity_df.dropna(subset=["hfq_factor"])

                if entity_df.empty:
                    continue

                # 补全必要字段
                entity_df["entity_id"] = entity.id
                entity_df["code"] = entity.code
                entity_df["provider"] = self.provider
                # id = entity_id_YYYY-MM-DD（向量化，效率高）
                entity_df["id"] = entity_df["timestamp"].dt.strftime(
                    entity.id + "_%Y-%m-%d"
                )
                
                dfs_to_save.append(entity_df)
                
                # 累积到 50 只股票的数据（约几十万行）做一次批量入库，降低 df_to_db 函数开销
                if len(dfs_to_save) >= 50:
                    combined_df = pd.concat(dfs_to_save, ignore_index=True)
                    try:
                        df_to_db(
                            df=combined_df,
                            data_schema=self.data_schema,
                            provider=self.provider,
                            force_update=self.force_update,
                        )
                        saved_entity_count += len(dfs_to_save)
                    except Exception as e:
                        self.logger.error(f"批量写入 {len(dfs_to_save)} 只股票失败: {e}")
                        error_count += len(dfs_to_save)
                    dfs_to_save.clear()

            # 处理剩余的数据
            if dfs_to_save:
                combined_df = pd.concat(dfs_to_save, ignore_index=True)
                try:
                    df_to_db(
                        df=combined_df,
                        data_schema=self.data_schema,
                        provider=self.provider,
                        force_update=self.force_update,
                    )
                    saved_entity_count += len(dfs_to_save)
                except Exception as e:
                    self.logger.error(f"最后批量写入股票失败: {e}")
                    error_count += len(dfs_to_save)
                dfs_to_save.clear()

            self.logger.info(
                f"Batch {current_batch_num}/{total_batches} 完成，"
                f"累计已保存: {saved_entity_count} 只"
            )

        self.logger.info(
            f"全部完成。成功保存: {saved_entity_count} 只，失败: {error_count} 只"
        )
        self.session.expire_all()
        self.on_finish()

    def record(self, entity, start, end, size, timestamps):
        """
        此方法在批量模式下不被调用（run() 已完全重写）。
        保留此方法仅为满足基类抽象接口要求。
        """
        pass


if __name__ == "__main__":
    pass
