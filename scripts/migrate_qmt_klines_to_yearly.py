#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
一次性迁移脚本：将 QMT 现有"按日期分区"的 K 线 Parquet 迁移为"按年文件"格式。

现有结构（不复权 + 后复权）：
  klines_daily_dir/
  ├── date=20250310/xxx.parquet
  └── ...
  klines_daily_hfq_dir/
  ├── date=20250310/xxx.parquet
  └── ...

迁移后结构：
  klines_yearly/
  ├── year=2024.parquet
  └── year=2025.parquet
  klines_yearly_hfq/
  ├── year=2024.parquet
  └── year=2025.parquet

每个年度文件内数据按 code 排序——Parquet 行组统计正确，
读取时 PyArrow/Polars 能跳过不相关的行组，单股查询极快。

使用方式：
  conda run -n quant python scripts/migrate_qmt_klines_to_yearly.py
  conda run -n quant python scripts/migrate_qmt_klines_to_yearly.py --dry-run
  conda run -n quant python scripts/migrate_qmt_klines_to_yearly.py --year 2025
  conda run -n quant python scripts/migrate_qmt_klines_to_yearly.py --only-hfq   # 只迁移后复权
  conda run -n quant python scripts/migrate_qmt_klines_to_yearly.py --only-bfq   # 只迁移不复权
"""
from __future__ import annotations

import argparse
import os
import sys
import time

from tqdm import tqdm

# ============================================================================
#  配置
# ============================================================================

QMT_BASE_DIR = "/mnt/point/stock_data/qmt_data/base_data"

# 不复权
BFQ_SRC_DIR = os.path.join(QMT_BASE_DIR, "klines_daily_dir")
BFQ_DST_DIR = os.path.join(QMT_BASE_DIR, "klines_yearly")

# 后复权
HFQ_SRC_DIR = os.path.join(QMT_BASE_DIR, "klines_daily_hfq_dir")
HFQ_DST_DIR = os.path.join(QMT_BASE_DIR, "klines_yearly_hfq")

# 每次处理多少个日期分区（控制内存）
PARTITION_BATCH_SIZE = int(os.environ.get("MIGRATE_BATCH_SIZE", "100"))

# 写入 Parquet 时每个 Row Group 的行数
ROW_GROUP_SIZE = int(os.environ.get("ROW_GROUP_SIZE", "100000"))

# ============================================================================
#  工具函数
# ============================================================================

def _list_date_partitions(src_dir: str) -> list[str]:
    """列出所有 date=YYYYMMDD 分区目录名，已排序。"""
    entries = []
    for e in os.listdir(src_dir):
        if e.startswith("date="):
            try:
                int(e.split("=", 1)[1])
                entries.append(e)
            except ValueError:
                pass
    return sorted(entries)


def _date_str_to_year(date_entry: str) -> int:
    """'date=20240319' -> 2024"""
    return int(date_entry.split("=", 1)[1][:4])


def _read_partition(partition_path: str) -> "pd.DataFrame | None":
    """读取一个日期分区目录下的所有 parquet 文件，返回合并后的 DataFrame。"""
    import pandas as pd

    parquet_files = [
        os.path.join(partition_path, f)
        for f in os.listdir(partition_path)
        if f.endswith(".parquet")
    ]
    if not parquet_files:
        return None

    parts = []
    for pf_path in parquet_files:
        try:
            df = pd.read_parquet(pf_path)
            parts.append(df)
        except Exception as e:
            print(f"    [警告] 读取 {pf_path} 失败: {e}")
    if not parts:
        return None
    return pd.concat(parts, axis=0, ignore_index=True)


def _write_yearly_parquet(df: "pd.DataFrame", dst_path: str) -> None:
    """
    将 DataFrame 写入年度 Parquet 文件。
    文件内按 code 排序，保证 Row Group 的 min/max 统计有意义。
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    # QMT 日线用 time 列（毫秒时间戳），也有 date 列（int64 YYYYMMDD）
    sort_cols = ["code"]
    if "time" in df.columns:
        sort_cols.append("time")
    elif "date" in df.columns:
        sort_cols.append("date")

    df = df.sort_values(sort_cols).reset_index(drop=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(
        table,
        dst_path,
        row_group_size=ROW_GROUP_SIZE,
        compression="snappy",
    )


def _merge_into_yearly(
    year: int,
    new_df: "pd.DataFrame",
    dst_dir: str,
    dry_run: bool = False,
) -> None:
    """
    将新数据与已有年度文件合并写入。
    策略：以 (code, date) 为唯一键，新数据覆盖旧数据，其余保留。
    """
    import pandas as pd

    dst_path = os.path.join(dst_dir, f"year={year}.parquet")

    if dry_run:
        print(f"    [dry-run] 会写入 {dst_path} ({len(new_df)} 行)")
        return

    if os.path.exists(dst_path):
        try:
            import pyarrow.parquet as pq
            old_df = pq.read_table(dst_path).to_pandas()
            combined = pd.concat([old_df, new_df], axis=0, ignore_index=True)
            key_cols = ["code", "date"]
            combined = combined.drop_duplicates(subset=key_cols, keep="last")
        except Exception as e:
            print(f"    [警告] 读取旧文件失败({e})，将只保留新数据")
            combined = new_df
    else:
        combined = new_df

    tmp_path = dst_path + ".tmp"
    _write_yearly_parquet(combined, tmp_path)
    os.replace(tmp_path, dst_path)
    print(f"    -> {dst_path} (共 {len(combined)} 行)")


# ============================================================================
#  单个目录的迁移逻辑
# ============================================================================

def migrate_one_dir(
    src_dir: str,
    dst_dir: str,
    label: str,
    only_year: int | None = None,
    dry_run: bool = False,
) -> None:
    import pandas as pd

    if not os.path.isdir(src_dir):
        print(f"[{label}] 源目录不存在: {src_dir}，跳过")
        return

    if not dry_run:
        os.makedirs(dst_dir, exist_ok=True)

    all_partitions = _list_date_partitions(src_dir)
    if not all_partitions:
        print(f"[{label}] 未找到任何 date= 分区目录")
        return

    print(f"[{label}] 找到 {len(all_partitions)} 个日期分区")
    print(f"[{label}] 源目录: {src_dir}")
    print(f"[{label}] 目标目录: {dst_dir}")
    if only_year:
        print(f"[{label}] 只迁移年份: {only_year}")
    if dry_run:
        print(f"[{label}] DRY RUN 模式，不写入文件")

    # 按年分组
    year_partition_map: dict[int, list[str]] = {}
    for entry in all_partitions:
        year = _date_str_to_year(entry)
        if only_year and year != only_year:
            continue
        year_partition_map.setdefault(year, []).append(entry)

    total_years = len(year_partition_map)
    print(f"[{label}] 涉及 {total_years} 个年份: {sorted(year_partition_map.keys())}")

    total_rows_written = 0
    t_start = time.time()

    year_bar = tqdm(sorted(year_partition_map.items()), desc=f"{label} 年份", unit="年", total=total_years)
    for year, partitions in year_bar:
        year_bar.set_postfix_str(f"{year} ({len(partitions)} 交易日)")

        n_batches = (len(partitions) + PARTITION_BATCH_SIZE - 1) // PARTITION_BATCH_SIZE
        year_parts: list[pd.DataFrame] = []

        batch_bar = tqdm(range(n_batches), desc=f"  {year} 批次", unit="批", leave=False)
        for batch_idx in batch_bar:
            batch = partitions[batch_idx * PARTITION_BATCH_SIZE : (batch_idx + 1) * PARTITION_BATCH_SIZE]
            batch_dfs = []
            for entry in batch:
                partition_path = os.path.join(src_dir, entry)
                df = _read_partition(partition_path)
                if df is not None and not df.empty:
                    # 确保有 date 列 (QMT 的 date 已经是 int64 YYYYMMDD)
                    if "date" not in df.columns and "time" in df.columns:
                        df["date"] = pd.to_datetime(df["time"], unit="ms").dt.strftime("%Y%m%d").astype("int64")
                    batch_dfs.append(df)

            if batch_dfs:
                batch_df = pd.concat(batch_dfs, axis=0, ignore_index=True)
                year_parts.append(batch_df)
                batch_bar.set_postfix_str(f"{len(batch_df)} 行")
                del batch_dfs, batch_df
        batch_bar.close()

        if not year_parts:
            print(f"  年份 {year}: 无有效数据，跳过")
            continue

        year_df = pd.concat(year_parts, axis=0, ignore_index=True)
        del year_parts
        print(f"  年份 {year}: 合并后共 {len(year_df)} 行，开始写入...")
        _merge_into_yearly(year, year_df, dst_dir, dry_run=dry_run)
        total_rows_written += len(year_df)
        del year_df

    print(f"\n[{label}] 迁移完成，共写入 {total_rows_written} 行，耗时 {time.time()-t_start:.1f}s")


# ============================================================================
#  主入口
# ============================================================================

def migrate(only_year: int | None = None, dry_run: bool = False,
            do_bfq: bool = True, do_hfq: bool = True) -> None:
    if do_bfq:
        migrate_one_dir(BFQ_SRC_DIR, BFQ_DST_DIR, "BFQ(不复权)", only_year=only_year, dry_run=dry_run)

    if do_hfq:
        migrate_one_dir(HFQ_SRC_DIR, HFQ_DST_DIR, "HFQ(后复权)", only_year=only_year, dry_run=dry_run)

    print(f"\n[迁移完成] 目标目录:")
    if do_bfq:
        print(f"  不复权: {BFQ_DST_DIR}")
    if do_hfq:
        print(f"  后复权: {HFQ_DST_DIR}")
    print(f"\n后续步骤：")
    print(f"  1. 验证目标目录数据正确性")
    print(f"  2. ParquetReader 已自动适配年度格式（优先 klines_yearly，回退 klines_daily_dir）")
    print(f"  3. daily_update_qmt.py 已改为年度写入，后续增量直接进入新目录")


# ============================================================================
#  CLI
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="将 QMT 日期分区 K 线 Parquet 迁移为年度文件格式")
    parser.add_argument("--dry-run", action="store_true", help="模拟运行，不写入文件")
    parser.add_argument("--year", type=int, default=None, help="只迁移指定年份")
    parser.add_argument("--only-bfq", action="store_true", help="只迁移不复权数据")
    parser.add_argument("--only-hfq", action="store_true", help="只迁移后复权数据")
    args = parser.parse_args()

    do_bfq = True
    do_hfq = True
    if args.only_bfq:
        do_hfq = False
    if args.only_hfq:
        do_bfq = False

    migrate(only_year=args.year, dry_run=args.dry_run, do_bfq=do_bfq, do_hfq=do_hfq)
