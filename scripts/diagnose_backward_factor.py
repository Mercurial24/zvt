#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
诊断：为什么数据湖导入 backward_factor 后，Recorder 仍会拉取复权因子。
检查 1) parquet 列名格式 -> 导入后的 entity_id  2) ZVT 表内 entity_id  3) Stock 的 entity.id 是否一致。

运行方式（需 conda 环境 quant，含 pyarrow/zvt 依赖）：
  conda activate quant
  cd /data/code/zvt && python scripts/diagnose_backward_factor.py
  或：bash scripts/run_diagnose_backward_factor.sh
"""
import os
import sys

# 确保能 import zvt
_script_dir = os.path.dirname(os.path.abspath(__file__))
_root = os.path.abspath(os.path.join(_script_dir, ".."))
_src = os.path.join(_root, "src")
for _p in (_src, _root):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pandas as pd

BASE_DIR = os.environ.get("XYSZ_PARQUET_BASE_DIR", "/data/stock_data/xysz_data/base_data")
PARQUET_PATH = os.path.join(BASE_DIR, "backward_factor.parquet")


def _read_parquet_columns():
    """读取 parquet 列名，不依赖 pyarrow 直接读文件头"""
    try:
        df = pd.read_parquet(PARQUET_PATH, columns=None)
        return list(df.columns)
    except Exception as e:
        try:
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(PARQUET_PATH)
            return pf.schema.names
        except Exception:
            pass
        raise e


def xysz_code_to_entity_id(market_code: str):
    """与 import_xysz_parquet_to_zvt 一致"""
    if not isinstance(market_code, str) or "." not in market_code:
        return f"stock_cn_{market_code}", "cn", str(market_code)
    code, exchange = market_code.strip().upper().rsplit(".", 1)
    ex = "sh" if exchange == "SH" else ("sz" if exchange == "SZ" else "bj")
    return f"stock_{ex}_{code}", ex, code


def main():
    print("=" * 60)
    print("1. 检查 backward_factor.parquet 是否存在及列名格式")
    print("=" * 60)
    if not os.path.isfile(PARQUET_PATH):
        print(f"不存在: {PARQUET_PATH}")
        print("结论: 数据湖没有该文件，导入会跳过，Recorder 会拉取。")
        return
    print(f"文件: {PARQUET_PATH}")
    cols = _read_parquet_columns()
    print(f"列数: {len(cols)}")
    # 第一列通常是日期索引（读成 index 或列名）
    date_like = [c for c in cols if c in ("index", "date", "timestamp", "timestamp_date") or "date" in c.lower()]
    code_cols = [c for c in cols if c not in date_like][:30]
    print(f"前 30 个「股票列」: {code_cols}")
    # 推断导入后的 entity_id
    sample_entity_ids = []
    no_dot = 0
    for c in code_cols:
        eid, _, _ = xysz_code_to_entity_id(str(c))
        sample_entity_ids.append((c, eid))
        if "." not in str(c):
            no_dot += 1
    print(f"导入后 entity_id 示例（前 10）: {sample_entity_ids[:10]}")
    if no_dot > 0:
        print(f"⚠️ 有 {no_dot} 个列名不含 '.'，会变成 stock_cn_*，与 Stock 的 stock_*_* 不一致！")
    else:
        print("列名均含交易所后缀，导入 entity_id 应与 Stock 一致。")

    print()
    print("2. 检查 ZVT stock_adj_factor 表 (provider=xysz)")
    print("=" * 60)
    try:
        from zvt.recorders.xysz.quotes.xysz_stock_adj_factor_recorder import StockAdjFactor
        from zvt.contract.api import get_db_session
        from sqlalchemy import func

        session = get_db_session(provider="xysz", data_schema=StockAdjFactor)
        # 总行数
        from sqlalchemy import text
        r = session.execute(text(f"SELECT COUNT(*) FROM {StockAdjFactor.__tablename__}")).scalar()
        print(f"表 {StockAdjFactor.__tablename__} 总行数: {r}")
        if r == 0:
            print("结论: 表为空，导入未写入或从未成功导入。Recorder 会拉取。")
            return
        # 不同 entity_id 数量
        r2 = session.execute(
            text(f"SELECT COUNT(DISTINCT entity_id) FROM {StockAdjFactor.__tablename__}")
        ).scalar()
        print(f"不同 entity_id 数: {r2}")
        # 最新时间戳
        rows = (
            session.query(StockAdjFactor.entity_id, func.max(StockAdjFactor.timestamp))
            .group_by(StockAdjFactor.entity_id)
            .limit(10)
            .all()
        )
        print(f"部分 entity 及最新 timestamp: {[(r[0], str(r[1])) for r in rows]}")
        # 是否有 stock_cn_ 开头（错误格式）
        from sqlalchemy import distinct
        cn_count = session.query(StockAdjFactor.entity_id).filter(
            StockAdjFactor.entity_id.like("stock_cn_%")
        ).distinct().count()
        if cn_count > 0:
            print(f"⚠️ 表中有 {cn_count} 个 entity_id 为 stock_cn_*，与 Recorder 用的 stock_*_* 不一致，会被视为无数据。")
        session.close()
    except Exception as e:
        print(f"查询 ZVT 失败: {e}")
        import traceback
        traceback.print_exc()
        return

    print()
    print("3. 检查 Stock 表 (xysz) 的 entity.id 格式")
    print("=" * 60)
    try:
        from zvt.domain import Stock
        from zvt.contract.api import get_entities

        entities = get_entities(entity_schema=Stock, provider="xysz", return_type="df", limit=10)
        if entities is None or entities.empty:
            print("未查到 Stock 实体（provider=xysz）")
        else:
            ids = entities["id"].tolist()
            print(f"Stock 的 entity id 示例: {ids}")
        print()
    except Exception as e:
        print(f"查询 Stock 失败: {e}")

    print("4. 结论与建议")
    print("=" * 60)
    print("若 parquet 列名为 000001.SZ 等形式 -> 导入 entity_id=stock_sz_000001，与 Recorder 一致，导入后应有最新日期则 Recorder 会跳过。")
    print("若 parquet 列名为 000001 无后缀 -> 导入 entity_id=stock_cn_000001，Recorder 查 stock_sz_000001 无记录，会拉取。")
    print("若表总行数为 0 -> 导入未执行或写入 0 行，需检查导入日志是否包含 backward_factor 及写入行数。")


if __name__ == "__main__":
    main()
