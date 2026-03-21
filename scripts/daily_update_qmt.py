# -*- coding: utf-8 -*-
"""
从 Windows 端通过 QMT 转发服务同步已经下载的数据到本地 Ubuntu。
保存格式为 parquet，路径和文件结构参考 xysz 的 daily_update.py 设计。
已根据 zvt_daily_job.py 补全了逻辑，包含指数行情同步与后复权数据。
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from client_linux import get_xtdata_proxy

# ------------------------------- 配置 -------------------------------
# QMT 转发服务地址
BASE_URL = os.environ.get("QMT_FORWARD_URL", "http://192.168.48.207:8000")

# 路径配置
QMT_BASE_DIR = "/mnt/point/stock_data/qmt_data/base_data"
KLINE_YEARLY_PATH = os.path.join(QMT_BASE_DIR, "klines_yearly")
KLINE_YEARLY_HFQ_PATH = os.path.join(QMT_BASE_DIR, "klines_yearly_hfq")
FINANCIAL_DIR = QMT_BASE_DIR

KLINE_LAST_TRADING_DAYS = 15 # 稍微多同步几天，确保增量覆盖
BATCH_SIZE = 200

# ZVT 中定义的重点指数
IMPORTANT_INDEX_CODES = [
    "000001.SH", "000016.SH", "000300.SH", "000905.SH", "000852.SH", "000688.SH",
    "399001.SZ", "399006.SZ", "399370.SZ", "399371.SZ", "399379.SZ", "399380.SZ",
]

def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path) if not path.endswith('/') else path
    if d:
        os.makedirs(d, exist_ok=True)

def _get_qmt_stock_list(xt) -> list:
    """获取 Windows 端已下载数据的股票列表 (沪深A股 + 京市A股)"""
    sh = xt.get_stock_list_in_sector("沪深A股")
    bj = xt.get_stock_list_in_sector("京市A股")
    sh = sh if sh else []
    bj = bj if bj else []
    return sh + bj

def _update_yearly_kline_file(year: int, new_df: pd.DataFrame, target_path: str, label: str) -> None:
    """
    将新的 K 线数据合并写入对应年度的 Parquet 文件。
    - 文件内按 code 排序，保证 Row Group 统计有意义（支持跳过）
    - 以 (code, date) 为唯一键，新数据覆盖旧数据
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    os.makedirs(target_path, exist_ok=True)
    dst_path = os.path.join(target_path, f"year={year}.parquet")
    tmp_path = dst_path + ".tmp"

    if os.path.exists(dst_path):
        try:
            old_df = pq.read_table(dst_path).to_pandas()
            combined = pd.concat([old_df, new_df], axis=0, ignore_index=True)
            del old_df
        except Exception as e:
            print(f"  [{label}][警告] 读取旧文件失败({e})，仅保留新数据")
            combined = new_df
    else:
        combined = new_df

    combined = combined.drop_duplicates(subset=["code", "date"], keep="last")
    
    # 按 code 排序是关键：使得 Parquet 行组统计(min/max code)有效
    sort_cols = ["code"]
    if "time" in combined.columns:
        sort_cols.append("time")
    elif "date" in combined.columns:
        sort_cols.append("date")

    combined = combined.sort_values(sort_cols).reset_index(drop=True)

    table = pa.Table.from_pandas(combined, preserve_index=False)
        pq.write_table(table, tmp_path, row_group_size=100_000, compression="snappy")
    os.replace(tmp_path, dst_path)
    print(f"  [{label}] year={year}.parquet 已更新: {len(combined)} 行")
    del combined, table

def _parse_qmt_date(s):
    """
    Robust parsing of QMT time/date formats.
    Handle:
    1. Milliseconds (int64/float): 1772985600000
    2. YYYYMMDD (int/str): 20260309
    3. Datetime objects
    Returns: Series of YYYYMMDD (int64)
    """
    if s is None or (hasattr(s, 'empty') and s.empty):
        return s
    
    # Handle already datetime
    if pd.api.types.is_datetime64_any_dtype(s):
        return s.dt.strftime('%Y%m%d').astype('int64')
    
    # Convert to numeric if possible
    s_numeric = pd.to_numeric(s, errors='coerce')
    
    # Check if they are milliseconds (threshold at Year 1980 ~ 3e11ms)
    if s_numeric.dropna().max() > 1e11:
        dt = pd.to_datetime(s_numeric, unit='ms', errors='coerce')
        # QMT 1d data in ms might be UTC, but usually it's local time interpreted as UTC by pd.to_datetime
        # Let's just grab the date part
        return dt.dt.strftime('%Y%m%d').astype('int64')
    
    # Otherwise treat as YYYYMMDD or similar 8-digit strings
    s_str = s.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    return pd.to_datetime(s_str.str[:8], format='%Y%m%d', errors='coerce').dt.strftime('%Y%m%d').astype('int64')

def update_klines_daily(xt, sync_hfq=False, count=None):
    """
    增量同步日线数据：
    1. 同步个股日线 (bfq 及可选 hfq)
    2. 同步重点指数日线
    
    针对全量同步进行了内存优化：分批落盘，最后由 Polars 合并分区。
    """
    if count is None:
        count = KLINE_LAST_TRADING_DAYS
        
    all_codes = _get_qmt_stock_list(xt)
    # 合并股票与重点指数
    sync_codes = list(set(all_codes + IMPORTANT_INDEX_CODES))
    
    if not sync_codes:
        print("[日线] 无法获取同步代码列表，跳过")
        return

    # 动态调整 BATCH_SIZE：如果是全量同步，减小批次以防单次响应过大
    kline_batch_size = 50 if count > 1000 else BATCH_SIZE
    
    # 临时存放目录：必须放在本地（如 /tmp），不能放在挂载点上，否则 pyarrow 会报 PermissionError
    import tempfile
    temp_dir = tempfile.mkdtemp(prefix="qmt_kline_tmp_")
    
    bfq_temp_files = []
    hfq_temp_files = []

    desc = "下载 QMT 历史日线(含后复权)" if sync_hfq else "下载 QMT 历史日线"
    
    for i in tqdm(range(0, len(sync_codes), kline_batch_size), desc=desc):
        batch_codes = sync_codes[i : i + kline_batch_size]
        try:
            # 1. 不复权数据 (bfq)
            batch_dict = xt.get_market_data_ex([], batch_codes, period="1d", count=count)
            batch_parts = []
            for code, df in batch_dict.items():
                if df is not None and not df.empty:
                    df = df.copy().reset_index(drop=True)
                    if "time" in df.columns:
                        df["date"] = _parse_qmt_date(df["time"])
                        df["code"] = str(code)
                        batch_parts.append(df)
            
            if batch_parts:
                tmp_df = pd.concat(batch_parts, ignore_index=True)
                tmp_path = os.path.join(temp_dir, f"bfq_batch_{i}.parquet")
                _write_parquet_single_thread(tmp_df, tmp_path)
                bfq_temp_files.append(tmp_path)
                del tmp_df, batch_parts
                
            # 2. 后复权数据 (hfq)
            if sync_hfq:
                stocks_in_batch = [c for c in batch_codes if c not in IMPORTANT_INDEX_CODES]
                if stocks_in_batch:
                    batch_dict_hfq = xt.get_market_data_ex([], stocks_in_batch, period="1d", 
                                                         count=count, dividend_type='back')
                    batch_parts_hfq = []
                    for code, df in batch_dict_hfq.items():
                        if df is not None and not df.empty:
                            df = df.copy().reset_index(drop=True)
                            if "time" in df.columns:
                                df["date"] = _parse_qmt_date(df["time"])
                                df["code"] = str(code)
                                batch_parts_hfq.append(df)
                    
                    if batch_parts_hfq:
                        tmp_df_hfq = pd.concat(batch_parts_hfq, ignore_index=True)
                        tmp_path_hfq = os.path.join(temp_dir, f"hfq_batch_{i}.parquet")
                        _write_parquet_single_thread(tmp_df_hfq, tmp_path_hfq)
                        hfq_temp_files.append(tmp_path_hfq)
                        del tmp_df_hfq, batch_parts_hfq
            
            # 每批次处理完清理内存
            import gc
            gc.collect()

        except Exception as e:
            print(f"[日线] 拉取批次失败: {e}")
            raise e

    def finalize_yearly(temp_files, target_path, name):
        if not temp_files:
            return
        print(f"[{name}] 开始最终年度分层落盘(Map-Reduce 流式优化)...")
        _ensure_dir(target_path)
        
        import tempfile
        import shutil
        import polars as pl
        
        # 建立按年份的中间分发站
        local_map_dir = tempfile.mkdtemp(prefix=f"qmt_map_{name}_")
        
        try:
            # ==== Map 阶段 ====
            # 扫描按股票组合的临时文件，将其拆碎投入按年份命名的篮子(目录)里
            for i, f in enumerate(tqdm(temp_files, desc=f"[{name}] 拆分重组(Map)")):
                try:
                    df = pl.read_parquet(f)
                    if df.is_empty():
                        continue
                    # polars 直接计算并分区保存，极速且不耗内存
                    df = df.with_columns((pl.col("date") // 10000).alias("year"))
                    
                    # [FIX] 修复 Polars partition_by 循环覆盖的问题：手动分流并产生唯一文件名
                    # 注意: Polars group_by 返回 (key_tuple, group_df)，单列分组时 key 是 (val,) 而非 val
                    for year_key, group in df.group_by("year"):
                        year_val = year_key[0] if isinstance(year_key, tuple) else year_key
                        year_dir = os.path.join(local_map_dir, f"year={year_val}")
                        os.makedirs(year_dir, exist_ok=True)
                        # 每个 Batch 使用唯一文件名 (batch_{i}.parquet)，防止相互覆盖
                        group.write_parquet(os.path.join(year_dir, f"batch_{i}.parquet"))
                    del df
                except Exception as e:
                    print(f"[{name}] Split 临时文件 {f} 失败: {e}")

            # ==== Reduce 阶段 ====
            # 此时 local_map_dir/ 下已生成 year=1990/, year=1991/ 等分类目录
            # 遍历这些年份，每一年的数据统一次性合并入库！(每年度只发一次磁盘 I/O)
            year_dirs = [d for d in os.listdir(local_map_dir) if d.startswith("year=")]
            years_involved = len(year_dirs)
            
            for y_dir in tqdm(year_dirs, desc=f"[{name}] 一次性归档(Reduce)"):
                try:
                    y_val = int(y_dir.replace("year=", ""))
                    src_year_path = os.path.join(local_map_dir, y_dir)
                    
                    # 捞出这一个年份刚刚拆碎分拨过来的所有散文件
                    parts = []
                    for pf in os.listdir(src_year_path):
                        if pf.endswith(".parquet"):
                            parts.append(pd.read_parquet(os.path.join(src_year_path, pf)))
                    if not parts:
                        continue
                    
                    # 缝合成该年的局部全量增量 df
                    year_df = pd.concat(parts, axis=0, ignore_index=True)
                    if "year" in year_df.columns:
                        year_df = year_df.drop(columns=["year"])
                        
                    # 仅仅开启并覆写一次最终年度文件！
                    _update_yearly_kline_file(y_val, year_df, target_path, name)
                    
                    del parts, year_df
                except Exception as e:
                    print(f"[{name}] 归档 {y_dir} 异常: {e}")
                    
        finally:
            shutil.rmtree(local_map_dir, ignore_errors=True)
            
        print(f"[{name}] 导出完成，共涉及 {years_involved} 个年份")

    finalize_yearly(bfq_temp_files, KLINE_YEARLY_PATH, "BFQ(不复权)")
    finalize_yearly(hfq_temp_files, KLINE_YEARLY_HFQ_PATH, "HFQ(后复权)")

    # 清理临时文件
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)
    
    print(f"[日线] 同步完成。已全部落盘至年度 Parquet 文件。")

def _release_memory() -> None:
    """强制回收内存并归还给 OS（解决 Python 不释放 RSS 的问题）。"""
    import gc
    import ctypes
    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


def _write_parquet_single_thread(df: pd.DataFrame, path: str) -> None:
    """单线程写 parquet，避免线程资源不足时 to_parquet 失败。"""
    import pyarrow as pa
    import pyarrow.parquet as pq

    try:
        table = pa.Table.from_pandas(df, preserve_index=False, nthreads=1)
        pq.write_table(table, path)
    except Exception:
        # 回退到 pandas 路径
        df.to_parquet(path, index=False, engine="pyarrow")

def _sync_financial_table(xt, table_name: str, file_name: str, all_codes: list):
    """
    同步 QMT 单个财务/股权表，使用临时文件串行汇总并覆盖写入。
    """
    import tempfile
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    path = os.path.join(FINANCIAL_DIR, file_name)
    _ensure_dir(path)
    
    # [CRITICAL] 清理 0 字节损坏文件。Polars 扫描 0 字节文件会直接触发 Aborted (Core Dump)
    if os.path.exists(path) and os.path.getsize(path) == 0:
        print(f"[{table_name}] 发现损坏的 0 字节历史文件，正在清理...")
        os.remove(path)
    
    # 临时存放目录：必须放在本地（如 /tmp）
    import tempfile
    temp_dir = tempfile.mkdtemp(prefix=f"qmt_fin_tmp_{table_name}_")
    temp_files = []

    # 第一阶段：分批下载并落地临时文件
    # [OPTIMIZATION] 财务表字段多、历史长，降低批次大小（从 200 降为 50）以彻底解决 MemoryError
    fin_batch_size = 50
    for i in tqdm(range(0, len(all_codes), fin_batch_size), desc=f"同步-[{table_name}]"):
        batch_codes = all_codes[i : i + fin_batch_size]
        try:
            res = xt.get_financial_data(batch_codes, [table_name])
            if not res:
                _release_memory() # 即使没数据也清一下
                continue
            
            batch_parts = []
            for code, tables_dict in res.items():
                if table_name in tables_dict:
                    df = tables_dict[table_name]
                    if df is not None and not df.empty:
                        df = df.copy()
                        df["code"] = str(code)
                        batch_parts.append(df)
            
            # 拿到数据后立即释放 RPC 返回的大字典
            del res
            
            if not batch_parts:
                _release_memory()
                continue
                
            current_batch_df = pd.concat(batch_parts, ignore_index=True)
            dedup_keys = ["code"]
            if "m_anntime" in current_batch_df.columns:
                dedup_keys.append("m_anntime")
            elif "m_timemark" in current_batch_df.columns:
                dedup_keys.append("m_timemark")
            current_batch_df = current_batch_df.drop_duplicates(subset=dedup_keys, keep="last")
            tmp_path = os.path.join(temp_dir, f"batch_{i}.parquet")
            _write_parquet_single_thread(current_batch_df, tmp_path)
            temp_files.append(tmp_path)
            
            # 及时释放内存
            del current_batch_df
            batch_parts.clear()
            del batch_parts
            _release_memory()
            
        except Exception as e:
            print(f"[财务] 获取 {table_name} 批次失败: {e}")
            raise e

    if not temp_files and not os.path.exists(path):
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
        return

    # 第二阶段：串行汇总临时文件并覆盖目标文件（低内存，规避 Rust 分配崩溃）
    print(f"[{table_name}] 开始最终低内存汇总...")
    valid_files = [tf for tf in temp_files if os.path.exists(tf) and os.path.getsize(tf) > 100]
    if valid_files:
        writer = None
        base_schema = None
        tmp_final = os.path.join(temp_dir, f"{table_name}_final.parquet")
        try:
            for tf in valid_files:
                table = pq.read_table(tf)
                if writer is None:
                    base_schema = table.schema
                    writer = pq.ParquetWriter(tmp_final, base_schema)
                    writer.write_table(table)
                    continue

                # schema 对齐：缺失列补 null，按首文件列顺序输出
                aligned_cols = []
                for field in base_schema:
                    if field.name in table.column_names:
                        aligned_cols.append(table[field.name])
                    else:
                        aligned_cols.append(pa.nulls(table.num_rows, type=field.type))
                aligned = pa.Table.from_arrays(aligned_cols, schema=base_schema)
                writer.write_table(aligned)

            import shutil
            shutil.move(tmp_final, path)
            print(f"[同步] {table_name} -> {file_name} 完成")
        except Exception as e:
            print(f"[{table_name}] 汇总过程报错: {e}")
            raise e
        finally:
            if writer is not None:
                writer.close()
            _release_memory()

    # 清除临时目录
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)

def update_financial(xt):
    all_codes = _get_qmt_stock_list(xt)
    if not all_codes: return
    _sync_financial_table(xt, "Balance", "balance_sheet.parquet", all_codes)
    _sync_financial_table(xt, "CashFlow", "cash_flow.parquet", all_codes)
    _sync_financial_table(xt, "Income", "income.parquet", all_codes)

def update_equity(xt):
    all_codes = _get_qmt_stock_list(xt)
    if not all_codes: return
    _sync_financial_table(xt, "Top10holder", "share_holder.parquet", all_codes)
    _sync_financial_table(xt, "Holdernum", "holder_num.parquet", all_codes)
    _sync_financial_table(xt, "Capital", "equity_structure.parquet", all_codes)
    _sync_financial_table(xt, "Dividend", "dividend.parquet", all_codes)

def run_full_update(url: str = None, sync_hfq: bool = True, count: int = None) -> None:
    """执行全部 QMT 数据湖增量同步（日线含后复权、财务、股本股东股息）。可被 zvt_daily_job 等直接调用。"""
    url = url or BASE_URL
    count = count if count is not None else KLINE_LAST_TRADING_DAYS
    print(f"连接 QMT 服务: {url}")
    xt = get_xtdata_proxy(url)
    xt.get_stock_list_in_sector("沪深A股")  # 测试连接
    update_klines_daily(xt, sync_hfq=sync_hfq, count=count)
    update_financial(xt)
    update_equity(xt)
    print("QMT 数据同步任务结束。")


def main():
    parser = argparse.ArgumentParser(description="增量同步 Windows 端的 QMT 日线(含HFQ)与财务等数据")
    parser.add_argument("--klines", action="store_true", help="仅同步日线")
    parser.add_argument("--hfq", action="store_true", default=True, help="同步后复权 K 线 (默认开启)")
    parser.add_argument("--no-hfq", action="store_false", dest="hfq", help="关闭后复权同步")
    parser.add_argument("--financial", action="store_true", help="仅同步财务报表")
    parser.add_argument("--equity", action="store_true", help="仅同步股本股东跟股息")
    parser.add_argument("--url", default=BASE_URL, help="QMT HTTP RPC 地址")
    parser.add_argument("--count", type=int, default=KLINE_LAST_TRADING_DAYS,
                        help=f"同步日线的交易日数目 (默认 {KLINE_LAST_TRADING_DAYS})")
    parser.add_argument("--all", action="store_true", help="同步所有历史日线 (约从1990年开始)")
    args = parser.parse_args()

    do_all = not (args.klines or args.financial or args.equity)
    if do_all:
        sync_count = 10000 if args.all else args.count
        if args.all:
            print("检测到 --all 参数，将尝试同步 10000 个交易日的历史 K 线 (覆盖1990年后)")
        run_full_update(url=args.url, sync_hfq=args.hfq, count=sync_count)
        return

    print(f"连接 QMT 服务: {args.url}")
    try:
        xt = get_xtdata_proxy(args.url)
        xt.get_stock_list_in_sector("沪深A股")
    except Exception as e:
        print(f"连接失败: {e}")
        return
    sync_count = 10000 if args.all else args.count
    if args.klines:
        update_klines_daily(xt, sync_hfq=args.hfq, count=sync_count)
    if args.financial:
        update_financial(xt)
    if args.equity:
        update_equity(xt)
    print("QMT 数据同步任务结束。")

if __name__ == "__main__":
    main()
