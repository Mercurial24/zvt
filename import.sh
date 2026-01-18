cd /data/code/zvt
export ZVT_HOME=/data/code/zvt/zvt-home   # 或你用的目录
# 基础导入命令
# python scripts/import_xysz_parquet_to_zvt.py --base-dir /data/stock_data/xysz_data/base_data
# 加速版本（适合首次全量导入）
python scripts/import_xysz_parquet_to_zvt.py --base-dir /data/stock_data/xysz_data/base_data --skip-dup-check --batch-size 50000 --fast-unsafe

# 若 klines_daily 已导入完、只需补财务/股东表时，可改用下面这行（省内存）：
# python scripts/import_xysz_parquet_to_zvt.py --base-dir /data/stock_data/xysz_data/base_data --only balance_sheet,income,cash_flow,holder_num,share_holder
# 财务/股东表加速版本：
# python scripts/import_xysz_parquet_to_zvt.py --base-dir /data/stock_data/xysz_data/base_data --only balance_sheet,income,cash_flow,holder_num,share_holder --skip-dup-check --batch-size 100000 --fast-unsafe