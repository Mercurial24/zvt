#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
一次性迁移脚本：将现有"按日期分区"的 K 线 Parquet 迁移为"按年文件"格式。

现有结构：
  klines_daily_dir/
  ├── date=20130104/xxx.parquet
  ├── date=20130107/xxx.parquet
  └── ...

迁移后结构：
  klines_yearly/
  ├── year=2013.parquet   (~1.26M行, ~30MB)
  ├── year=2014.parquet
  └── year=2025.parquet   (当年，持续追加)

每个年度文件内数据按 code 排序——Parquet 行组统计正确，
读取时 PyArrow/Polars 能跳过不相关的行组，单股查询极快。

使用方式：
  conda run -n quant python scripts/migrate_klines_to_yearly.py
  conda run -n quant python scripts/migrate_klines_to_yearly.py --dry-run
  conda run -n quant python scripts/migrate_klines_to_yearly.py --year 2024   # 只迁移某年
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

SRC_DIR = "/mnt/point/stock_data/xysz_data/base_data/klines_daily_dir"
DST_DIR = "/mnt/point/stock_data/xysz_data/base_data/klines_yearly"

# 每次处理多少个日期分区（控制内存：每个分区约 100KB，100 个约 10MB，安全）
PARTITION_BATCH_SIZE = int(os.environ.get("MIGRATE_BATCH_SIZE", "100"))

# 写入 Parquet 时每个 Row Group 的行数（影响跳过效率）
# 5000 stocks * 22 days / 50 groups ≈ 2200 rows/group → 细粒度好
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
    import pyarrow.parquet as pq

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
            tbl = pq.read_table(pf_path)
            parts.append(tbl.to_pandas())
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

    df = df.sort_values(["code", "kline_time"]).reset_index(drop=True)
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
            # 合并：以 (code, date) 去重，新数据优先
            combined = pd.concat([old_df, new_df], axis=0, ignore_index=True)
            # date 字段是 int64 YYYYMMDD
            key_cols = ["code", "date"] if "date" in combined.columns else ["code", "kline_time"]
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
#  主迁移逻辑
# ============================================================================

def migrate(only_year: int | None = None, dry_run: bool = False) -> None:
    import pandas as pd

    if not os.path.isdir(SRC_DIR):
        print(f"[ERROR] 源目录不存在: {SRC_DIR}")
        sys.exit(1)

    if not dry_run:
        os.makedirs(DST_DIR, exist_ok=True)

    all_partitions = _list_date_partitions(SRC_DIR)
    if not all_partitions:
        print("[ERROR] 未找到任何 date= 分区目录")
        sys.exit(1)

    print(f"[迁移] 找到 {len(all_partitions)} 个日期分区")
    print(f"[迁移] 源目录: {SRC_DIR}")
    print(f"[迁移] 目标目录: {DST_DIR}")
    if only_year:
        print(f"[迁移] 只迁移年份: {only_year}")
    if dry_run:
        print("[迁移] DRY RUN 模式，不写入文件")

    # 按年分组
    year_partition_map: dict[int, list[str]] = {}
    for entry in all_partitions:
        year = _date_str_to_year(entry)
        if only_year and year != only_year:
            continue
        year_partition_map.setdefault(year, []).append(entry)

    total_years = len(year_partition_map)
    print(f"[迁移] 涉及 {total_years} 个年份: {sorted(year_partition_map.keys())}")

    total_rows_written = 0
    t_start = time.time()

    year_bar = tqdm(sorted(year_partition_map.items()), desc="年份", unit="年", total=total_years)
    for year, partitions in year_bar:
        year_bar.set_postfix_str(f"{year} ({len(partitions)} 交易日)")

        n_batches = (len(partitions) + PARTITION_BATCH_SIZE - 1) // PARTITION_BATCH_SIZE
        year_parts: list[pd.DataFrame] = []

        batch_bar = tqdm(range(n_batches), desc=f"  {year} 批次", unit="批", leave=False)
        for batch_idx in batch_bar:
            batch = partitions[batch_idx * PARTITION_BATCH_SIZE : (batch_idx + 1) * PARTITION_BATCH_SIZE]
            batch_dfs = []
            for entry in batch:
                partition_path = os.path.join(SRC_DIR, entry)
                df = _read_partition(partition_path)
                if df is not None and not df.empty:
                    if "date" not in df.columns and "kline_time" in df.columns:
                        df["date"] = pd.to_datetime(df["kline_time"]).dt.strftime("%Y%m%d").astype("int64")
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
        _merge_into_yearly(year, year_df, DST_DIR, dry_run=dry_run)
        total_rows_written += len(year_df)
        del year_df




    print(f"\n[迁移完成] 共写入 {total_rows_written} 行，总耗时 {time.time()-t_start:.1f}s")
    print(f"[迁移完成] 目标目录: {DST_DIR}")
    print(f"\n后续步骤：")
    print(f"  1. 验证目标目录数据正确性")
    print(f"  2. 更新 ParquetReader 指向新目录")
    print(f"  3. 更新 daily_update_xysz.py 使用新的写入方式")


# ============================================================================
#  CLI
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="将日期分区 K 线 Parquet 迁移为年度文件格式")
    parser.add_argument("--dry-run", action="store_true", help="模拟运行，不写入文件")
    parser.add_argument("--year", type=int, default=None, help="只迁移指定年份")
    args = parser.parse_args()

    migrate(only_year=args.year, dry_run=args.dry_run)
