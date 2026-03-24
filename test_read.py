import os
import sys

# 确保能找到 src 目录
sys.path.insert(0, "/data/code/zvt/src")

from zvt.domain import Stock1dKdata, Stock1dHfqKdata, HolderNum, StockValuation
from zvt.contract.api import get_data


def _xysz_entity_id_from_market_code(market_code: str) -> str:
    """例如 000793.SZ -> stock_sz_000793"""
    parts = market_code.upper().strip().split(".")
    if len(parts) != 2:
        raise ValueError(f"需要 代码.交易所 格式，例如 000793.SZ，当前: {market_code!r}")
    num, ex = parts
    return f"stock_{ex.lower()}_{num}"

if __name__ == "__main__":
    code = "000793.SZ"
    start_ts = "2025-01-01"
    
    # 1. 测试读取 不复权(BFQ) K线
    df_bfq = get_data(
        data_schema=Stock1dKdata,
        code=code,
        provider="qmt",
        start_timestamp=start_ts
    )
    
    # 2. 测试读取 后复权(HFQ) K线
    # ZVT 的 ParquetReader 会自动定位到 base_data/klines_yearly_hfq 目录
    df_hfq = get_data(
        data_schema=Stock1dHfqKdata,
        code=code,
        provider="qmt",
        start_timestamp=start_ts
    )
    
    if df_bfq is not None and not df_bfq.empty:
        print(f"✅ 成功读取不复权(BFQ) K线，数据量: {len(df_bfq)}")
        print(df_bfq.tail(3))
        
    # 2. 读取 后复权(HFQ) K线
    df_hfq = get_data(
        data_schema=Stock1dHfqKdata,
        code=code,
        provider="qmt",
        start_timestamp=start_ts
    )
    if df_hfq is not None and not df_hfq.empty:
        print(f"\n✅ 成功读取后复权(HFQ) K线，数据量: {len(df_hfq)}")
        print(df_hfq.tail(3))
    else:
        print("\n❌ 未能读取到后复权数据，请确认 full_update 是否同步了后复权。")
    
    # 3. 测试读取股东人数 (从 Parquet 直接读原生文件)
    import pandas as pd
    
    # 因为 QMT 的财务类数据湖原本没有转化为 ZVT 自己的 schema（列名还是原始的），
    # 最靠谱的去读原始 Parquet 会是直接用 pandas 读取
    parquet_path = "/mnt/point/stock_data/qmt_data/base_data/holder_num.parquet"
    if os.path.exists(parquet_path):
        df_holder = pd.read_parquet(parquet_path)
        # 筛选单只股票
        df_stock = df_holder[df_holder['code'] == code]
        print(f"\n✅ 成功从原始 Parquet 读取股东人数数据，该股记录数: {len(df_stock)}")
        # QMT 原始的列名是 endDate, declareDate, shareholder (总户数) 等
        print(df_stock[['endDate', 'declareDate', 'code', 'shareholderA', 'shareholder']].tail(5))
    else:
        print(f"\n❌ 未找 Parquet 原始数据文件: {parquet_path}")

    # 4. 测试读取股东人数 (从 SQLite)
    df_holder_sqlite = get_data(
        data_schema=HolderNum,
        entity_id="stock_sz_000793",
        provider="qmt",
        start_timestamp="2020-01-01"
    )
    if df_holder_sqlite is not None and not df_holder_sqlite.empty:
        print(f"\n✅ 成功从 SQLite 读取股东人数数据 (HolderNum)，该股记录数: {len(df_holder_sqlite)}")
        print(df_holder_sqlite[['code', 'report_period', 'report_date', 'holder_num']].tail(5))
    else:
        print("\n❌ 未能从 SQLite 读取到股东人数数据。")

    # 5. 测试读取 xysz 估值表中的股息率（SQLite: xysz_valuation.db）
    try:
        entity_id_xysz = _xysz_entity_id_from_market_code(code)
    except ValueError as e:
        entity_id_xysz = None
        print(f"\n⚠️ 无法解析 xysz entity_id: {e}")

    if entity_id_xysz:
        df_val = get_data(
            data_schema=StockValuation,
            entity_id=entity_id_xysz,
            provider="xysz",
            start_timestamp=start_ts,
            columns=[
                "timestamp",
                "code",
                "dividend_ps_ttm",
                "dividend_yield_ttm",
                "pe_ttm",
                "pb",
            ],
        )
        if df_val is not None and not df_val.empty:
            show_cols = [
                c
                for c in [
                    "timestamp",
                    "code",
                    "dividend_ps_ttm",
                    "dividend_yield_ttm",
                    "pe_ttm",
                    "pb",
                ]
                if c in df_val.columns
            ]
            print(f"\n✅ 成功从 SQLite 读取 xysz 估值（含股息率），记录数: {len(df_val)}")
            print(df_val[show_cols].tail(8))
        else:
            print(
                "\n❌ 未能读取 xysz StockValuation（含 dividend_yield_ttm）。"
                "请先运行 scripts/run_xysz_dividend_backfill_once.sh 或 zvt_daily_job 中相关步骤。"
            )
