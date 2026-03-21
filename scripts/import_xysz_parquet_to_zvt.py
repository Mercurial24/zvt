# -*- coding: utf-8 -*-
"""
将星河数智 my_quant_begin 下载的 parquet 数据（/data/stock_data/xysz_data/base_data）
分批导入到 zvt 的 SQLite，避免一次性加载大文件导致内存不足。

使用方式（在 conda quant 环境下）：
  cd /data/code/zvt && conda run -n quant python scripts/import_xysz_parquet_to_zvt.py --base-dir /data/stock_data/xysz_data/base_data

源数据路径说明:
  数据由 amazing_data_down.py 下载并由 daily_update.py 更新，存放在:
  /data/stock_data/xysz_data/base_data

主要导入逻辑说明:
  目前脚本【会导入】的文件（与阶段1 daily_update 下载的 parquet 对应）:
    - klines_daily_dir 或 klines_daily.parquet (日线行情)
    - balance_sheet.parquet (资产负债表)
    - income.parquet (利润表)
    - cash_flow.parquet (现金流量表)
    - holder_num.parquet (股东人数)
    - share_holder.parquet (前十大股东)
    - dividend.parquet (分红送配 → DividendDetail)
    - right_issue.parquet (配股 → RightsIssueDetail)
    - backward_factor.parquet (后复权因子 → stock_adj_factor)
    - industry_base_info.parquet (行业基础信息 → Block)
    - industry_constituent.parquet (行业成分股 → BlockStock)

  目前脚本【不会导入】的文件（ZVT 无对应 schema 或未实现）:
    - equity_structure.parquet (股本结构)
    - equity_pledge_freeze.parquet (股权质押/冻结)
    - equity_restricted.parquet (限售解禁)
    - profit_express.parquet (业绩快报)
    - profit_notice.parquet (业绩预告)
    - adj_factor.parquet (单次复权因子，可选；当前仅用后复权)

  其他参数:
    --only klines_daily,holder_num (只导入某几类)
    --max-rows 10000 (限制行数用于测试)
    --count-rows (klines_daily 为目录时预扫总行数以显示百分比进度条，会多读一遍数据)
    --skip-dup-check (首次全量导入建议加此项以加速，但重复运行会报唯一约束错误)
    --batch-size 100000 (内存充足可调大以提速)
    --fast-unsafe (加此项执行 PRAGMA synchronous=OFF，进一步提速但有丢数据风险)

注意:
- 需先有 xysz 的 Stock 元数据（或本脚本会从 parquet 中出现的 code 推导 entity_id，写入时仍可入库，
  但 get_entities(Stock) 若没有 xysz 则需先跑 xyszStockMetaRecorder）。
- 财务/股东表与 zvt 的 BalanceSheet/IncomeStatement/CashFlowStatement/HolderNum/TopTenHolder 等
  列名已做映射，与 xysz recorder 一致。
"""

from __future__ import annotations

import argparse
import gc
import logging
import os
import sys
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
from tqdm import tqdm

# 确保能 import zvt（zvt 包在 src 下）
if __name__ == "__main__":
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    _root = os.path.abspath(os.path.join(_script_dir, ".."))
    _src = os.path.join(_root, "src")
    for _p in (_src, _root):
        if _p not in sys.path:
            sys.path.insert(0, _p)

import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 每批处理行数，控制单次内存
DEFAULT_ROW_BATCH = 10000


def xysz_code_to_entity_id(market_code: str) -> Tuple[str, str, str]:
    """600104.SH -> (entity_id, exchange, code)."""
    if not isinstance(market_code, str) or "." not in market_code:
        return f"stock_cn_{market_code}", "cn", str(market_code)
    code, exchange = market_code.strip().upper().rsplit(".", 1)
    ex = "sh" if exchange == "SH" else ("sz" if exchange == "SZ" else "bj")
    return f"stock_{ex}_{code}", ex, code


def _normalize_adj_factor_column_to_market_code(col: str) -> str:
    """
    Parquet 宽表列名可能是 "000001.SZ" 或 "000001"。
    若仅有代码无交易所，按 A 股规则补全为 CODE.EXCHANGE，使 entity_id 与 Stock 一致（stock_sz_000001），
    否则导入会写成 stock_cn_000001，Recorder 查 stock_sz_000001 无记录会重复拉取。
    """
    s = str(col).strip()
    if "." in s:
        return s
    if not s or not s.isdigit():
        return s
    first = s[0]
    if first == "6":
        return f"{s}.SH"
    if first in ("0", "3"):
        return f"{s}.SZ"
    if first in ("4", "8"):
        return f"{s}.BJ"
    return f"{s}.SZ"


def _rename_and_select(
    df: pd.DataFrame,
    column_map: Dict[str, str],
    schema_columns: List[str],
) -> pd.DataFrame:
    cols_upper = {str(c).upper(): c for c in df.columns}
    rename_dict = {}
    for k, v in column_map.items():
        if k.upper() in cols_upper:
            if v in df.columns and cols_upper[k.upper()] != v:
                continue
            rename_dict[cols_upper[k.upper()]] = v
    df = df.rename(columns=rename_dict)
    df = df.loc[:, ~df.columns.duplicated()]
    out_cols = [c for c in schema_columns if c in df.columns]
    return df[out_cols]


def _count_parquet_rows(path: str, max_rows: Optional[int] = None) -> Optional[int]:
    """统计 parquet 行数，不把整份数据读入内存。单文件用元数据，目录用流式 batch.num_rows。"""
    if not os.path.exists(path):
        return None
    try:
        if os.path.isdir(path):
            import pyarrow.dataset as ds
            dataset = ds.dataset(path, format="parquet", partitioning="hive")
            total = 0
            for batch in dataset.to_batches(batch_size=50000):
                total += batch.num_rows
                if max_rows is not None and total >= max_rows:
                    return max_rows
                del batch
            return total
        else:
            pf = pq.ParquetFile(path)
            total = sum(
                pf.metadata.row_group(i).num_rows
                for i in range(pf.metadata.num_row_groups)
            )
            return min(total, max_rows) if max_rows is not None else total
    except Exception as e:
        logger.debug("统计行数失败 %s: %s", path, e)
        return None


def _read_parquet_in_batches(
    path: str,
    max_rows: Optional[int],
    batch_size: int,
):
    """按 batch_size 行分批读取，支持单文件和分区目录，避免 OOM。"""
    total_read = 0
    buffer_dfs = []
    buffer_len = 0

    def yield_buffer():
        if not buffer_dfs:
            return None
        return pd.concat(buffer_dfs, ignore_index=True)

    if os.path.isdir(path):
        # 分区目录：使用 pyarrow.dataset 流式扫描
        import pyarrow.dataset as ds
        dataset = ds.dataset(path, format="parquet", partitioning="hive")
        for batch in dataset.to_batches(batch_size=batch_size):
            if max_rows is not None and total_read >= max_rows:
                break
            df = batch.to_pandas()
            if df.index.names and any(n for n in df.index.names):
                df = df.reset_index()
            if max_rows is not None and total_read + len(df) > max_rows:
                df = df.iloc[:max_rows - total_read]
            total_read += len(df)
            
            buffer_dfs.append(df)
            buffer_len += len(df)
            
            if buffer_len >= batch_size:
                yield yield_buffer()
                buffer_dfs = []
                buffer_len = 0
                gc.collect()
                
            del batch
            
        if buffer_dfs:
            yield yield_buffer()
            buffer_dfs = []
            gc.collect()
    else:
        # 单文件：按 row_group 分批读取
        pf = pq.ParquetFile(path)
        for i in range(pf.metadata.num_row_groups):
            if max_rows is not None and total_read >= max_rows:
                break
            table = pf.read_row_group(i)
            n_rows = table.num_rows
            for start in range(0, n_rows, batch_size):
                if max_rows is not None and total_read >= max_rows:
                    break
                length = min(batch_size, n_rows - start)
                if max_rows is not None and total_read + length > max_rows:
                    length = max_rows - total_read
                if length <= 0:
                    break
                chunk = table.slice(start, length)
                df = chunk.to_pandas()
                if df.index.names and any(n for n in df.index.names):
                    df = df.reset_index()
                total_read += len(df)
                
                buffer_dfs.append(df)
                buffer_len += len(df)
                
                if buffer_len >= batch_size:
                    yield yield_buffer()
                    buffer_dfs = []
                    buffer_len = 0
                    gc.collect()
                    
                del chunk
            del table
            gc.collect()
            
        if buffer_dfs:
            yield yield_buffer()
            buffer_dfs = []
            gc.collect()


def import_klines_daily(
    base_dir: str,
    provider: str,
    max_rows: Optional[int],
    batch_size: int,
    df_to_db: Callable,
    count_rows_for_progress: bool = False,
) -> int:
    # 优先使用分区目录，其次兼容旧的单文件
    path = os.path.join(base_dir, "klines_daily_dir")
    if not os.path.isdir(path):
        path = os.path.join(base_dir, "klines_daily.parquet")
    if not os.path.exists(path):
        logger.warning("不存在 %s", path)
        return 0
    from zvt.contract import IntervalLevel, AdjustType
    from zvt.api.kdata import get_kdata_schema

    data_schema = get_kdata_schema(entity_type="stock", level=IntervalLevel.LEVEL_1DAY, adjust_type=AdjustType.qfq)
    schema_cols = list(data_schema.__table__.columns.keys())
    # 单文件可直接从元数据取总数；目录需 --count-rows 才预扫（避免双倍 I/O）
    if os.path.isfile(path) or count_rows_for_progress:
        if count_rows_for_progress and os.path.isdir(path):
            logger.info("klines_daily 预扫行数（目录会多读一遍）…")
        total_rows_hint = _count_parquet_rows(path, max_rows)
        if total_rows_hint is not None:
            logger.info("klines_daily 待导入约 %d 行", total_rows_hint)
    else:
        total_rows_hint = None
    total = 0
    if total_rows_hint is not None and total_rows_hint > 0:
        pbar = tqdm(total=total_rows_hint, unit="行", desc="klines_daily", dynamic_ncols=True)
    else:
        pbar = tqdm(unit="行", desc="klines_daily", dynamic_ncols=True)
    for batch_df in _read_parquet_in_batches(path, max_rows, batch_size):
        if batch_df.empty:
            continue
        # 索引可能是 (code, date) 或列里有 code, date
        if batch_df.index.names and "code" in (batch_df.index.names or []):
            batch_df = batch_df.reset_index()
        df = batch_df.copy()
        # 列名: kline_time, open, high, low, close, volume, amount, code, date（只取一个作为 timestamp，避免重复列）
        if "amount" in df.columns:
            df = df.rename(columns={"amount": "turnover"})
        ts_col = None
        for c in ("date", "kline_time", "timestamp"):
            if c in df.columns:
                ts_col = c
                break
        if ts_col and ts_col != "timestamp":
            df = df.rename(columns={ts_col: "timestamp"})
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"].astype(str).str.replace("-", ""), format="%Y%m%d", errors="coerce")
        if "code" not in df.columns and "entity_id" not in df.columns:
            logger.warning("klines_daily 缺少 code 列，跳过本批")
            pbar.update(len(df))
            continue
        # 构建 entity_id / code（向量化，避免逐行 apply 导致越跑越慢）
        if "entity_id" not in df.columns or "code" in df.columns:
            _code_str = df["code"].astype(str).str.strip().str.upper()
            _parts = _code_str.str.rsplit(pat=".", n=1, expand=True)
            if _parts.shape[1] == 2:
                _code_part = _parts[0]
                _ex = _parts[1].map({"SH": "sh", "SZ": "sz"}).fillna("bj")
                df["entity_id"] = "stock_" + _ex + "_" + _code_part
                df["code"] = _code_part
            else:
                df["entity_id"] = "stock_cn_" + _code_str
                df["code"] = _code_str
        if "provider" not in df.columns:
            df["provider"] = provider
        df["name"] = df.get("name", df.get("code", ""))
        df["level"] = "1d"
        df["change_pct"] = df.get("change_pct")
        df["turnover_rate"] = df.get("turnover_rate")
        df["is_limit_up"] = df.get("is_limit_up")
        df["is_limit_down"] = df.get("is_limit_down")
        # 向量化 id，避免逐行 apply
        df["id"] = df["entity_id"] + "_" + df["timestamp"].dt.strftime("%Y-%m-%d")
        out_cols = [c for c in schema_cols if c in df.columns]
        attempted = len(df)
        saved = df_to_db(df=df[out_cols], data_schema=data_schema, provider=provider, force_update=False)
        total += saved
        pbar.update(attempted)
        pbar.set_postfix(写入=saved, 累计=total)
        logger.debug("klines_daily 本批尝试 %d 行，实际写入 %d 行，累计写入 %d", attempted, saved, total)
        del df, batch_df
        gc.collect()
    pbar.close()
    return total


def _import_finance_table(
    base_dir: str,
    filename: str,
    provider: str,
    data_schema: Any,
    column_map: Dict[str, str],
    max_rows: Optional[int],
    batch_size: int,
    df_to_db: Callable,
) -> int:
    path = os.path.join(base_dir, filename)
    if not os.path.isfile(path):
        logger.warning("不存在 %s", path)
        return 0
    schema_cols = list(data_schema.__table__.columns.keys())
    total = 0
    for batch_df in _read_parquet_in_batches(path, max_rows, batch_size):
        if batch_df.empty:
            continue
        df = batch_df.copy()
        if df.index.names and "code" in (df.index.names or []):
            df = df.reset_index()
        code_col = "MARKET_CODE" if "MARKET_CODE" in df.columns else "code"
        if code_col not in df.columns:
            logger.warning("%s 缺少 %s，跳过本批", filename, code_col)
            continue
        df["entity_id"] = df[code_col].apply(lambda c: xysz_code_to_entity_id(c)[0])
        df["code"] = df[code_col].apply(lambda c: xysz_code_to_entity_id(c)[2] if isinstance(c, str) and "." in c else c)
        df["provider"] = provider
        df = _rename_and_select(df, column_map, schema_cols)
        
        if "report_date" in df.columns:
            if pd.api.types.is_numeric_dtype(df["report_date"]):
                df["report_date"] = pd.to_datetime(df["report_date"].astype(int).astype(str), format="%Y%m%d", errors="coerce")
            else:
                df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
            
            # 统一为 YYYY-MM-DD，与 Recorder 一致
            df["report_period"] = df["report_date"].dt.strftime("%Y-%m-%d")

        if "timestamp" in df.columns:
            if pd.api.types.is_numeric_dtype(df["timestamp"]):
                df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int).astype(str), format="%Y%m%d", errors="coerce")
            else:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        if "report_period" in df.columns and "entity_id" in df.columns:
            # 直接赋值覆盖可能来自 parquet 的错误格式 id
            df["id"] = df["entity_id"] + "_" + df["report_period"].astype(str)
            
        out_cols = [c for c in schema_cols if c in df.columns]
        if not out_cols:
            continue
        attempted = len(df)
        saved = df_to_db(df=df[out_cols], data_schema=data_schema, provider=provider, force_update=False)
        total += saved
        logger.info("%s 本批尝试 %d 行，实际写入 %d 行，累计写入 %d", filename, attempted, saved, total)
        del df, batch_df
        gc.collect()
    return total


def import_balance_sheet(base_dir: str, provider: str, max_rows: Optional[int], batch_size: int, df_to_db: Callable) -> int:
    from zvt.domain import BalanceSheet
    from zvt.recorders.xysz.finance.xysz_finance_recorder import xyszBalanceSheetRecorder
    return _import_finance_table(
        base_dir, "balance_sheet.parquet", provider,
        BalanceSheet, xyszBalanceSheetRecorder._get_column_map(None),
        max_rows, batch_size, df_to_db,
    )


def import_income(base_dir: str, provider: str, max_rows: Optional[int], batch_size: int, df_to_db: Callable) -> int:
    from zvt.domain import IncomeStatement
    from zvt.recorders.xysz.finance.xysz_finance_recorder import xyszIncomeStatementRecorder
    return _import_finance_table(
        base_dir, "income.parquet", provider,
        IncomeStatement, xyszIncomeStatementRecorder._get_column_map(None),
        max_rows, batch_size, df_to_db,
    )


def import_cash_flow(base_dir: str, provider: str, max_rows: Optional[int], batch_size: int, df_to_db: Callable) -> int:
    from zvt.domain import CashFlowStatement
    from zvt.recorders.xysz.finance.xysz_finance_recorder import xyszCashFlowRecorder
    return _import_finance_table(
        base_dir, "cash_flow.parquet", provider,
        CashFlowStatement, xyszCashFlowRecorder._get_column_map(None),
        max_rows, batch_size, df_to_db,
    )


def import_holder_num(base_dir: str, provider: str, max_rows: Optional[int], batch_size: int, df_to_db: Callable) -> int:
    from zvt.domain import HolderNum
    from zvt.recorders.xysz.holder.xysz_holder_recorder import xyszHolderNumRecorder
    path = os.path.join(base_dir, "holder_num.parquet")
    if not os.path.isfile(path):
        logger.warning("不存在 %s", path)
        return 0
    schema_cols = list(HolderNum.__table__.columns.keys())
    column_map = xyszHolderNumRecorder._get_column_map(None)
    total = 0
    for batch_df in _read_parquet_in_batches(path, max_rows, batch_size):
        if batch_df.empty:
            continue
        df = batch_df.copy()
        if "MARKET_CODE" not in df.columns and "code" not in df.columns:
            continue
        code_col = "MARKET_CODE" if "MARKET_CODE" in df.columns else "code"
        df["entity_id"] = df[code_col].apply(lambda c: xysz_code_to_entity_id(c)[0])
        df["code"] = df[code_col].apply(lambda c: xysz_code_to_entity_id(c)[2] if isinstance(c, str) and "." in c else c)
        df["provider"] = provider
        df = _rename_and_select(df, column_map, schema_cols)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y%m%d", errors="coerce")
        if "report_date" in df.columns:
            df["report_date"] = pd.to_datetime(df["report_date"], format="%Y%m%d", errors="coerce")
        if "report_period" not in df.columns and "report_date" in df.columns:
            df["report_period"] = df["report_date"].dt.strftime("%Y-%m-%d")
        if "id" not in df.columns and "report_period" in df.columns and "entity_id" in df.columns:
            df["id"] = df["entity_id"] + "_" + df["report_period"].astype(str)
        out_cols = [c for c in schema_cols if c in df.columns]
        if out_cols:
            attempted = len(df)
            saved = df_to_db(df=df[out_cols], data_schema=HolderNum, provider=provider, force_update=False)
            total += saved
            logger.info("holder_num 本批尝试 %d 行，实际写入 %d 行，累计写入 %d", attempted, saved, total)
        del df, batch_df
        gc.collect()
    return total


def import_share_holder(base_dir: str, provider: str, max_rows: Optional[int], batch_size: int, df_to_db: Callable) -> int:
    """导入十大股东（TopTenHolder）。仅 HOLDER_TYPE==10。"""
    from zvt.domain import TopTenHolder
    from zvt.recorders.xysz.holder.xysz_holder_recorder import xyszTopTenHolderRecorder
    path = os.path.join(base_dir, "share_holder.parquet")
    if not os.path.isfile(path):
        logger.warning("不存在 %s", path)
        return 0
    schema_cols = list(TopTenHolder.__table__.columns.keys())
    column_map = xyszTopTenHolderRecorder._get_column_map(None)
    total = 0
    for batch_df in _read_parquet_in_batches(path, max_rows, batch_size):
        if batch_df.empty:
            continue
        df = batch_df.copy()
        if "HOLDER_TYPE" in df.columns:
            df = df[df["HOLDER_TYPE"] == 10]
        if df.empty:
            continue
        code_col = "MARKET_CODE" if "MARKET_CODE" in df.columns else "code"
        df["entity_id"] = df[code_col].apply(lambda c: xysz_code_to_entity_id(c)[0])
        df["code"] = df[code_col].apply(lambda c: xysz_code_to_entity_id(c)[2] if isinstance(c, str) and "." in c else c)
        df["provider"] = provider
        df = _rename_and_select(df, column_map, schema_cols)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y%m%d", errors="coerce")
        if "report_date" in df.columns:
            df["report_date"] = pd.to_datetime(df["report_date"], format="%Y%m%d", errors="coerce")
        if "report_period" not in df.columns and "report_date" in df.columns:
            df["report_period"] = df["report_date"].dt.strftime("%Y-%m-%d")
        if "holder_name" in df.columns and "report_period" in df.columns and "entity_id" in df.columns:
            df["id"] = df["entity_id"] + "_" + df["report_period"].astype(str) + "_" + df["holder_name"].astype(str)
        out_cols = [c for c in schema_cols if c in df.columns]
        if out_cols:
            attempted = len(df)
            saved = df_to_db(df=df[out_cols], data_schema=TopTenHolder, provider=provider, force_update=False)
            total += saved
            logger.info("share_holder(TopTenHolder) 本批尝试 %d 行，实际写入 %d 行，累计写入 %d", attempted, saved, total)
        del df, batch_df
        gc.collect()
    return total


def import_dividend(base_dir: str, provider: str, max_rows: Optional[int], batch_size: int, df_to_db: Callable) -> int:
    """导入分红送配 → DividendDetail（阶段1 写入 dividend.parquet）。"""
    from zvt.domain import DividendDetail
    from zvt.recorders.xysz.dividend_financing.xysz_dividend_financing_recorder import xyszDividendDetailRecorder
    path = os.path.join(base_dir, "dividend.parquet")
    if not os.path.isfile(path):
        logger.warning("不存在 %s", path)
        return 0
    column_map = xyszDividendDetailRecorder._get_column_map(None)
    schema_cols = list(DividendDetail.__table__.columns.keys())
    total = 0
    for batch_df in _read_parquet_in_batches(path, max_rows, batch_size):
        if batch_df.empty:
            continue
        df = batch_df.copy()
        if df.index.names and "code" in (df.index.names or []):
            df = df.reset_index()
        code_col = "MARKET_CODE" if "MARKET_CODE" in df.columns else "code"
        if code_col not in df.columns:
            continue
        df["entity_id"] = df[code_col].apply(lambda c: xysz_code_to_entity_id(c)[0])
        df["code"] = df[code_col].apply(lambda c: xysz_code_to_entity_id(c)[2] if isinstance(c, str) and "." in c else c)
        df["provider"] = provider
        if "ANN_DATE" in df.columns:
            df["timestamp"] = pd.to_datetime(df["ANN_DATE"].astype(str), format="%Y%m%d", errors="coerce")
            df["announce_date"] = df["timestamp"]
        df = _rename_and_select(df, column_map, schema_cols)
        if "record_date" in df.columns:
            df["record_date"] = pd.to_datetime(df["record_date"], errors="coerce")
        if "dividend_date" in df.columns:
            df["dividend_date"] = pd.to_datetime(df["dividend_date"], errors="coerce")
        if "id" not in df.columns and "entity_id" in df.columns and "timestamp" in df.columns:
            df["id"] = df["entity_id"] + "_" + df["timestamp"].dt.strftime("%Y-%m-%d")
            if "dividend_date" in df.columns:
                d = df["dividend_date"].fillna(pd.Timestamp("1970-01-01")).dt.strftime("%Y-%m-%d")
                df["id"] = df["id"] + "_" + d
        out_cols = [c for c in schema_cols if c in df.columns]
        if out_cols:
            attempted = len(df)
            saved = df_to_db(df=df[out_cols], data_schema=DividendDetail, provider=provider, force_update=False)
            total += saved
            logger.info("dividend 本批尝试 %d 行，实际写入 %d 行，累计写入 %d", attempted, saved, total)
        del df, batch_df
        gc.collect()
    return total


def import_right_issue(base_dir: str, provider: str, max_rows: Optional[int], batch_size: int, df_to_db: Callable) -> int:
    """导入配股 → RightsIssueDetail（阶段1 写入 right_issue.parquet）。"""
    from zvt.domain import RightsIssueDetail
    from zvt.recorders.xysz.dividend_financing.xysz_dividend_financing_recorder import xyszRightsIssueDetailRecorder
    path = os.path.join(base_dir, "right_issue.parquet")
    if not os.path.isfile(path):
        logger.warning("不存在 %s", path)
        return 0
    column_map = xyszRightsIssueDetailRecorder._get_column_map(None)
    schema_cols = list(RightsIssueDetail.__table__.columns.keys())
    total = 0
    for batch_df in _read_parquet_in_batches(path, max_rows, batch_size):
        if batch_df.empty:
            continue
        df = batch_df.copy()
        if df.index.names and "code" in (df.index.names or []):
            df = df.reset_index()
        code_col = "MARKET_CODE" if "MARKET_CODE" in df.columns else "code"
        if code_col not in df.columns:
            continue
        df["entity_id"] = df[code_col].apply(lambda c: xysz_code_to_entity_id(c)[0])
        df["code"] = df[code_col].apply(lambda c: xysz_code_to_entity_id(c)[2] if isinstance(c, str) and "." in c else c)
        df["provider"] = provider
        df = _rename_and_select(df, column_map, schema_cols)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        if "id" not in df.columns and "entity_id" in df.columns and "timestamp" in df.columns:
            df["id"] = df["entity_id"] + "_" + df["timestamp"].dt.strftime("%Y-%m-%d")
        out_cols = [c for c in schema_cols if c in df.columns]
        if out_cols:
            attempted = len(df)
            saved = df_to_db(df=df[out_cols], data_schema=RightsIssueDetail, provider=provider, force_update=False)
            total += saved
            logger.info("right_issue 本批尝试 %d 行，实际写入 %d 行，累计写入 %d", attempted, saved, total)
        del df, batch_df
        gc.collect()
    return total


def import_stock_meta(base_dir: str, provider: str, max_rows: Optional[int], batch_size: int, df_to_db: Callable) -> int:
    """导入股票基础信息 → Stock（阶段1 写入 stock_meta.parquet）。"""
    from zvt.domain import Stock

    path = os.path.join(base_dir, "stock_meta.parquet")
    if not os.path.isfile(path):
        logger.warning("不存在 %s", path)
        return 0
    schema_cols = list(Stock.__table__.columns.keys())
    total = 0
    for batch_df in _read_parquet_in_batches(path, max_rows, batch_size):
        if batch_df.empty:
            continue
        df = batch_df.copy()
        
        # 字段映射，保持数据湖原始名与落库模型隔离
        rename_dict = {
            "SECURITY_NAME": "name",
            "LISTDATE": "list_date",
            "DELISTDATE": "end_date",
            "FLOAT_CAP": "float_cap",
            "TOTAL_CAP": "total_cap",
        }
        df = df.rename(columns={k: v for k, v in rename_dict.items() if k in df.columns})

        if "entity_id" not in df.columns and "MARKET_CODE" in df.columns:
            df["entity_id"] = df["MARKET_CODE"].apply(lambda c: xysz_code_to_entity_id(c)[0])
            df["exchange"] = df["MARKET_CODE"].apply(lambda c: xysz_code_to_entity_id(c)[1])
            df["code"] = df["MARKET_CODE"].apply(lambda c: xysz_code_to_entity_id(c)[2])
        elif "code" not in df.columns and "entity_id" in df.columns:
            df["code"] = df["entity_id"].str.replace(r"^stock_[a-z]+_", "", regex=True)
            df["exchange"] = df["entity_id"].apply(lambda x: x.split("_")[1] if len(str(x).split("_")) > 1 else "cn")
            
        if "id" not in df.columns:
            df["id"] = df["entity_id"]
        df["entity_type"] = "stock"
        df["provider"] = provider
        
        for col in ["list_date", "end_date"]:
            if col in df.columns and df[col].dtype == object:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                
        if "timestamp" not in df.columns and "list_date" in df.columns:
            df["timestamp"] = df["list_date"]

        out_cols = [c for c in schema_cols if c in df.columns]
        if not out_cols:
            continue
        attempted = len(df)
        saved = df_to_db(df=df[out_cols], data_schema=Stock, provider=provider, force_update=True)
        total += saved
        logger.info("stock_meta 本批尝试 %d 行，实际写入 %d 行，累计写入 %d", attempted, saved, total)
        del df, batch_df
        gc.collect()
    return total


def import_industry_base_info(base_dir: str, provider: str, max_rows: Optional[int], batch_size: int, df_to_db: Callable) -> int:
    """导入行业基础信息 → Block（阶段1 写入 industry_base_info.parquet）。"""
    from zvt.domain import Block
    from zvt.domain import BlockCategory

    path = os.path.join(base_dir, "industry_base_info.parquet")
    if not os.path.isfile(path):
        logger.warning("不存在 %s", path)
        return 0
    schema_cols = list(Block.__table__.columns.keys())
    total = 0
    for batch_df in _read_parquet_in_batches(path, max_rows, batch_size):
        if batch_df.empty:
            continue
        df = batch_df.copy()
        if df.index.names and "INDEX_CODE" in (df.index.names or []):
            df = df.reset_index()
        if "INDEX_CODE" not in df.columns:
            logger.warning("industry_base_info 缺少 INDEX_CODE，跳过本批")
            continue
        df["entity_id"] = "block_cn_" + df["INDEX_CODE"].astype(str).str.strip()
        df["id"] = df["entity_id"]
        df["entity_type"] = "block"
        df["exchange"] = "cn"
        df["code"] = df["INDEX_CODE"].astype(str).str.strip()
        
        # 优先根据 LEVEL_TYPE 选取对应的名称
        df["name"] = df["code"] # default
        for level in [1, 2, 3]:
            name_col = f"LEVEL{level}_NAME"
            if name_col in df.columns:
                mask = (df["LEVEL_TYPE"] == level)
                df.loc[mask, "name"] = df.loc[mask, name_col].astype(str).str.strip()
        
        # 备选补齐
        for level_col in ["LEVEL3_NAME", "LEVEL2_NAME", "LEVEL1_NAME", "INDUSTRY_CODE"]:
            if level_col in df.columns:
                df["name"] = df["name"].fillna(df[level_col].astype(str).str.strip())
                df.loc[df["name"] == "", "name"] = df.loc[df["name"] == "", level_col].astype(str).str.strip()

        df["category"] = BlockCategory.industry.value
        df["timestamp"] = pd.Timestamp.now()
        df = df.drop_duplicates(subset=["id"], keep="last")
        out_cols = [c for c in schema_cols if c in df.columns]
        if out_cols:
            attempted = len(df)
            saved = df_to_db(df=df[out_cols], data_schema=Block, provider=provider, force_update=True)
            total += saved
            logger.info("industry_base_info 本批尝试 %d 行，实际写入 %d 行，累计写入 %d", attempted, saved, total)
        del df, batch_df
        gc.collect()
    return total


def import_industry_constituent(base_dir: str, provider: str, max_rows: Optional[int], batch_size: int, df_to_db: Callable) -> int:
    """导入行业成分股 → BlockStock（阶段1 写入 industry_constituent.parquet）。"""
    from zvt.domain import BlockStock

    path = os.path.join(base_dir, "industry_constituent.parquet")
    if not os.path.isfile(path):
        logger.warning("不存在 %s", path)
        return 0
    schema_cols = list(BlockStock.__table__.columns.keys())
    total = 0
    for batch_df in _read_parquet_in_batches(path, max_rows, batch_size):
        if batch_df.empty:
            continue
        df = batch_df.copy()
        if df.index.names and "INDEX_CODE" in (df.index.names or []):
            df = df.reset_index()
        if "INDEX_CODE" not in df.columns or "CON_CODE" not in df.columns:
            logger.warning("industry_constituent 缺少 INDEX_CODE/CON_CODE，跳过本批")
            continue
        df["entity_id"] = "block_cn_" + df["INDEX_CODE"].astype(str).str.strip()
        con = df["CON_CODE"].astype(str).str.strip()
        if con.str.contains(r"\.", na=False).any():
            df["stock_id"] = con.apply(lambda c: xysz_code_to_entity_id(c)[0])
            df["stock_code"] = con.apply(lambda c: xysz_code_to_entity_id(c)[2] if isinstance(c, str) and "." in c else c)
        else:
            df["stock_id"] = "stock_cn_" + con
            df["stock_code"] = con
        df["id"] = df["entity_id"] + "_" + df["stock_id"]
        df["entity_type"] = "block"
        df["exchange"] = "cn"
        df["code"] = df["INDEX_CODE"].astype(str).str.strip()
        df["name"] = df["INDEX_NAME"].astype(str).str.strip() if "INDEX_NAME" in df.columns else df["code"]
        df["stock_name"] = ""
        df["timestamp"] = pd.Timestamp.now()
        df = df.drop_duplicates(subset=["id"], keep="last")
        out_cols = [c for c in schema_cols if c in df.columns]
        if out_cols:
            attempted = len(df)
            saved = df_to_db(df=df[out_cols], data_schema=BlockStock, provider=provider, force_update=True)
            total += saved
            logger.info("industry_constituent 本批尝试 %d 行，实际写入 %d 行，累计写入 %d", attempted, saved, total)
        del df, batch_df
        gc.collect()
    return total


def import_backward_factor(base_dir: str, provider: str, max_rows: Optional[int], batch_size: int, df_to_db: Callable) -> int:
    """导入后复权因子 → stock_adj_factor。

    支持两种格式：
    1. 新格式（per-stock）: backward_factor/ 目录，每只股票一个 CODE.parquet 文件
    2. 旧格式（宽表）: backward_factor.parquet 单文件
    """
    from zvt.recorders.xysz.quotes.xysz_stock_adj_factor_recorder import StockAdjFactor

    dir_path = os.path.join(base_dir, "backward_factor")
    legacy_path = os.path.join(base_dir, "backward_factor.parquet")

    use_per_stock = os.path.isdir(dir_path) and len(os.listdir(dir_path)) > 0

    if use_per_stock:
        return _import_backward_factor_per_stock(dir_path, provider, max_rows, batch_size, df_to_db, StockAdjFactor)
    elif os.path.isfile(legacy_path):
        return _import_backward_factor_legacy(legacy_path, provider, max_rows, batch_size, df_to_db, StockAdjFactor)
    else:
        logger.warning("不存在 backward_factor 目录或 backward_factor.parquet")
        return 0


def _import_backward_factor_per_stock(
    dir_path: str, provider: str, max_rows: Optional[int], batch_size: int, df_to_db: Callable, StockAdjFactor
) -> int:
    """从 per-stock parquet 目录导入复权因子。"""
    schema_cols = list(StockAdjFactor.__table__.columns.keys())
    files = sorted(f for f in os.listdir(dir_path) if f.endswith(".parquet"))
    total = 0
    pbar = tqdm(files, desc="backward_factor (per-stock)", unit="file", dynamic_ncols=True)
    for fname in pbar:
        if max_rows is not None and total >= max_rows:
            break
        fpath = os.path.join(dir_path, fname)
        market_code_raw = fname.replace(".parquet", "")
        market_code = _normalize_adj_factor_column_to_market_code(market_code_raw)
        entity_id, exchange, code = xysz_code_to_entity_id(market_code)

        try:
            df = pd.read_parquet(fpath)
        except Exception as e:
            logger.warning("读取 %s 失败: %s", fpath, e)
            continue
        if df.empty:
            continue

        date_col = "date" if "date" in df.columns else df.columns[0]
        factor_col = "factor" if "factor" in df.columns else df.columns[-1]
        df = df.rename(columns={date_col: "timestamp", factor_col: "hfq_factor"})

        if pd.api.types.is_numeric_dtype(df["timestamp"]):
            df = df[df["timestamp"] > 19000101]
            df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int).astype(str), format="%Y%m%d", errors="coerce")
        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["hfq_factor"])
        if df.empty:
            continue

        df["entity_id"] = entity_id
        df["code"] = code
        df["provider"] = provider
        df["id"] = entity_id + "_" + df["timestamp"].dt.strftime("%Y-%m-%d")

        out_cols = [c for c in schema_cols if c in df.columns]
        for start in range(0, len(df), batch_size):
            if max_rows is not None and total >= max_rows:
                break
            chunk = df.iloc[start : start + batch_size]
            if max_rows is not None and total + len(chunk) > max_rows:
                chunk = chunk.iloc[: max_rows - total]
            saved = df_to_db(df=chunk[out_cols], data_schema=StockAdjFactor, provider=provider, force_update=False)
            total += saved
        pbar.set_postfix_str(f"累计={total}")
        del df
        gc.collect()
    pbar.close()
    return total


def _import_backward_factor_legacy(
    path: str, provider: str, max_rows: Optional[int], batch_size: int, df_to_db: Callable, StockAdjFactor
) -> int:
    """从旧宽表 parquet 文件导入复权因子（兼容）。"""
    schema_cols = list(StockAdjFactor.__table__.columns.keys())
    total = 0
    wide_row_batch = min(500, batch_size)
    out_cols_cache = None
    pf = pq.ParquetFile(path)
    n_wide_rows = sum(pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups))
    n_code_cols = max(0, len(pf.schema.names) - 1)
    total_batches_estimate = (n_wide_rows * n_code_cols) // batch_size
    if max_rows is not None:
        total_batches_estimate = min(total_batches_estimate, max(1, max_rows // batch_size))
    pbar = tqdm(total=total_batches_estimate, desc="backward_factor (legacy)", unit="batch", dynamic_ncols=True)
    for i in range(pf.metadata.num_row_groups):
        if max_rows is not None and total >= max_rows:
            break
        table = pf.read_row_group(i)
        rg_df = table.to_pandas()
        del table
        if rg_df.empty:
            continue
        rg_df = rg_df.reset_index()
        date_col = rg_df.columns[0]
        if date_col == "index":
            rg_df = rg_df.rename(columns={"index": "timestamp"})
            date_col = "timestamp"
        if pd.api.types.is_integer_dtype(rg_df[date_col]) or pd.api.types.is_float_dtype(rg_df[date_col]):
            rg_df = rg_df[rg_df[date_col] > 19000101]
            if rg_df.empty:
                del rg_df
                gc.collect()
                continue
            rg_df[date_col] = pd.to_datetime(rg_df[date_col].astype(int).astype(str), format="%Y%m%d", errors="coerce")
        else:
            rg_df[date_col] = pd.to_datetime(rg_df[date_col], errors="coerce")
        for row_start in range(0, len(rg_df), wide_row_batch):
            if max_rows is not None and total >= max_rows:
                break
            batch_df = rg_df.iloc[row_start : row_start + wide_row_batch].copy()
            code_cols = [c for c in batch_df.columns if c != date_col]
            if not code_cols:
                del batch_df
                gc.collect()
                continue
            long_df = batch_df.melt(id_vars=[date_col], value_vars=code_cols, var_name="code", value_name="hfq_factor")
            del batch_df
            gc.collect()
            long_df = long_df.rename(columns={date_col: "timestamp"})
            long_df = long_df.dropna(subset=["hfq_factor"])
            if long_df.empty:
                gc.collect()
                continue
            market_code = long_df["code"].apply(lambda c: _normalize_adj_factor_column_to_market_code(str(c)))
            long_df["entity_id"] = market_code.apply(lambda c: xysz_code_to_entity_id(c)[0])
            long_df["code"] = market_code.apply(lambda c: xysz_code_to_entity_id(c)[2])
            long_df["provider"] = provider
            long_df["id"] = long_df["entity_id"] + "_" + long_df["timestamp"].dt.strftime("%Y-%m-%d")
            if out_cols_cache is None:
                out_cols_cache = [c for c in schema_cols if c in long_df.columns]
            for start in range(0, len(long_df), batch_size):
                if max_rows is not None and total >= max_rows:
                    break
                chunk = long_df.iloc[start : start + batch_size]
                if max_rows is not None and total + len(chunk) > max_rows:
                    chunk = chunk.iloc[: max_rows - total]
                chunk_len = len(chunk)
                saved = df_to_db(df=chunk[out_cols_cache], data_schema=StockAdjFactor, provider=provider, force_update=False)
                total += saved
                pbar.update(1)
                pbar.set_postfix_str(f"本批尝试={chunk_len} 写入={saved} 累计={total}")
            del long_df
            gc.collect()
        del rg_df
        gc.collect()
    pbar.close()
    return total


def main():
    parser = argparse.ArgumentParser(description="将 xysz base_data 下 parquet 分批导入 zvt")
    parser.add_argument("--base-dir", default="/mnt/point/stock_data/xysz_data/base_data", help="parquet 所在目录")
    parser.add_argument("--only", default="", help="只导入的表，逗号分隔，如 stock_meta,klines_daily,balance_sheet,income,cash_flow,holder_num,share_holder,dividend,right_issue,backward_factor,industry_base_info,industry_constituent。空表示全部")
    parser.add_argument("--max-rows", type=int, default=None, help="每种表最多导入行数（用于测试）")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_ROW_BATCH, help="每批行数")
    parser.add_argument("--provider", default="xysz", help="写入 zvt 的 provider")
    parser.add_argument(
        "--skip-dup-check",
        action="store_true",
        help="跳过「先查库再写入」的重复检查，适合首次全量导入，可明显提速；重复运行可能报唯一约束错误",
    )
    parser.add_argument("--count-only", action="store_true", help="仅统计 parquet 行数并打印，不导入、不连库")
    parser.add_argument(
        "--count-rows",
        action="store_true",
        help="klines_daily 为目录时也预扫总行数以显示百分比进度条（会多读一遍目录数据，耗时会增加）",
    )
    parser.add_argument(
        "--fast-unsafe",
        action="store_true",
        help="与 --skip-dup-check 同用：PRAGMA synchronous=OFF，写入更快但断电有丢数据风险",
    )
    args = parser.parse_args()

    if args.count_only:
        only = [x.strip() for x in args.only.split(",") if x.strip()]
        tables = [
            ("stock_meta", None, os.path.join(args.base_dir, "stock_meta.parquet")),
            ("klines_daily", os.path.join(args.base_dir, "klines_daily_dir"), os.path.join(args.base_dir, "klines_daily.parquet")),
            ("balance_sheet", None, os.path.join(args.base_dir, "balance_sheet.parquet")),
            ("income", None, os.path.join(args.base_dir, "income.parquet")),
            ("cash_flow", None, os.path.join(args.base_dir, "cash_flow.parquet")),
            ("holder_num", None, os.path.join(args.base_dir, "holder_num.parquet")),
            ("share_holder", None, os.path.join(args.base_dir, "share_holder.parquet")),
            ("dividend", None, os.path.join(args.base_dir, "dividend.parquet")),
            ("right_issue", None, os.path.join(args.base_dir, "right_issue.parquet")),
            ("backward_factor", None, os.path.join(args.base_dir, "backward_factor.parquet")),
            ("industry_base_info", None, os.path.join(args.base_dir, "industry_base_info.parquet")),
            ("industry_constituent", None, os.path.join(args.base_dir, "industry_constituent.parquet")),
        ]
        for name, path_dir, path_file in tables:
            if only and name not in only:
                continue
            path = path_dir if path_dir and os.path.isdir(path_dir) else path_file
            n = _count_parquet_rows(path, args.max_rows)
            if n is not None:
                logger.info("%s: 共 %d 行", name, n)
            else:
                logger.info("%s: 未找到或无法统计", name)
        return

    from zvt.contract.api import df_to_db

    effective_batch_size = args.batch_size
    # --skip-dup-check 时：sub_size 与 batch 一致，每批一次 commit
    if args.skip_dup_check:
        _sub_size = effective_batch_size
        _session_cache = {}
        _fast_unsafe = getattr(args, "fast_unsafe", False)

        def _df_to_db(df, data_schema, provider, force_update=False):
            from sqlalchemy.sql.expression import text
            from zvt.contract.api import get_db_session
            key = (provider, data_schema.__tablename__)
            if key not in _session_cache:
                session = get_db_session(provider=provider, data_schema=data_schema)
                conn = session.connection()
                conn.execute(text("PRAGMA journal_mode = WAL"))
                conn.execute(text("PRAGMA synchronous = NORMAL"))
                if _fast_unsafe:
                    conn.execute(text("PRAGMA synchronous = OFF"))
                conn.execute(text("PRAGMA cache_size = -65536"))
                conn.execute(text("PRAGMA temp_store = MEMORY"))
                _session_cache[key] = session
            return df_to_db(
                df=df,
                data_schema=data_schema,
                provider=provider,
                force_update=force_update,
                need_check=False,
                sub_size=_sub_size,
                session=_session_cache[key],
            )
        if _fast_unsafe:
            logger.warning("--fast-unsafe: PRAGMA synchronous=OFF，断电可能损库，仅临时导入可用")
    else:
        _df_to_db = df_to_db

    only = [x.strip() for x in args.only.split(",") if x.strip()]
    runners = []
    if not only or "stock_meta" in only:
        runners.append(("stock_meta", lambda: import_stock_meta(args.base_dir, args.provider, args.max_rows, effective_batch_size, _df_to_db)))
    if not only or "klines_daily" in only:
        runners.append(("klines_daily", lambda: import_klines_daily(args.base_dir, args.provider, args.max_rows, effective_batch_size, _df_to_db, args.count_rows)))
    if not only or "balance_sheet" in only:
        runners.append(("balance_sheet", lambda: import_balance_sheet(args.base_dir, args.provider, args.max_rows, effective_batch_size, _df_to_db)))
    if not only or "income" in only:
        runners.append(("income", lambda: import_income(args.base_dir, args.provider, args.max_rows, effective_batch_size, _df_to_db)))
    if not only or "cash_flow" in only:
        runners.append(("cash_flow", lambda: import_cash_flow(args.base_dir, args.provider, args.max_rows, effective_batch_size, _df_to_db)))
    if not only or "holder_num" in only:
        runners.append(("holder_num", lambda: import_holder_num(args.base_dir, args.provider, args.max_rows, effective_batch_size, _df_to_db)))
    if not only or "share_holder" in only:
        runners.append(("share_holder", lambda: import_share_holder(args.base_dir, args.provider, args.max_rows, effective_batch_size, _df_to_db)))
    if not only or "dividend" in only:
        runners.append(("dividend", lambda: import_dividend(args.base_dir, args.provider, args.max_rows, effective_batch_size, _df_to_db)))
    if not only or "right_issue" in only:
        runners.append(("right_issue", lambda: import_right_issue(args.base_dir, args.provider, args.max_rows, effective_batch_size, _df_to_db)))
    if not only or "backward_factor" in only:
        runners.append(("backward_factor", lambda: import_backward_factor(args.base_dir, args.provider, args.max_rows, effective_batch_size, _df_to_db)))
    if not only or "industry_base_info" in only:
        runners.append(("industry_base_info", lambda: import_industry_base_info(args.base_dir, args.provider, args.max_rows, effective_batch_size, _df_to_db)))
    if not only or "industry_constituent" in only:
        runners.append(("industry_constituent", lambda: import_industry_constituent(args.base_dir, args.provider, args.max_rows, effective_batch_size, _df_to_db)))

    for name, run in runners:
        logger.info("开始导入: %s", name)
        n = run()
        logger.info("%s 导入完成，共 %d 行", name, n)
        # 每类数据导入完后主动回收内存，避免下一类（如 income）时 OOM 被 Killed
        gc.collect()


if __name__ == "__main__":
    main()
