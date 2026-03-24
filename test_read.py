import os
import sys

# 确保能找到 src 目录
sys.path.insert(0, "/data/code/zvt/src")

from zvt.domain import Stock1dKdata, Stock1dHfqKdata, HolderNum
from zvt.contract.api import get_data

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
