import pyarrow.dataset as ds
import os

path = '/data/stock_data/xysz_data/base_data/klines_daily_dir'
if not os.path.exists(path):
    path = '/data/stock_data/xysz_data/base_data/klines_daily.parquet'

print('Total rows:', ds.dataset(path, format='parquet').count_rows())
