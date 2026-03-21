#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
K 线 Parquet 读取性能基准测试脚本

测试场景覆盖常见的量化分析需求：
  1. 单只股票全历史
  2. 单只股票指定时间段
  3. 少量股票（3 只）全历史
  4. 少量股票（3 只）指定时间段
  5. 中等数量（50 只）最近 1 年
  6. 全市场某一天
  7. 全市场最近 10 个交易日
  8. 全市场全历史（大查询，注意内存）

使用方式：
  conda run -n quant python scripts/klines_read_benchmark.py
  conda run -n quant python scripts/klines_read_benchmark.py --skip-full  # 跳过全市场全历史
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Callable

import polars as pl

# ============================================================================
#  配置
# ============================================================================

YEARLY_DIR = "/mnt/point/stock_data/xysz_data/base_data/klines_yearly"

# 测试用股票代码
SINGLE_CODE   = "000001.SZ"       # 平安银行，从 1991 年开始
FEW_CODES     = ["000001.SZ", "000002.SZ", "600519.SH"]   # 3 只
MEDIUM_CODES  = [f"{str(i).zfill(6)}.SZ" for i in range(1, 51)]  # 50 只（可能不全存在）

DATE_START_1Y  = "2025-01-01"
DATE_END_1Y    = "2025-12-31"
DATE_START_5Y  = "2021-01-01"
DATE_END_5Y    = "2025-12-31"
SINGLE_DATE    = 20250319       # 某一天（int 格式）
RECENT_10_DAYS = 20250306       # 最近 10 个交易日起始（int）

# ============================================================================
#  工具函数
# ============================================================================

def _get_year_files(start_year: int = 1990, end_year: int = 9999) -> list[str]:
    """从 YEARLY_DIR 中筛选指定年份范围的文件。"""
    files = []
    for f in os.listdir(YEARLY_DIR):
        if not f.startswith("year=") or not f.endswith(".parquet"):
            continue
        try:
            y = int(f[5:-8])
        except ValueError:
            continue
        if start_year <= y <= end_year:
            files.append(os.path.join(YEARLY_DIR, f))
    return sorted(files)


def _scan(files: list[str]) -> pl.LazyFrame:
    """扫描指定文件列表，处理 datetime 精度冲突。"""
    try:
        return pl.scan_parquet(
            files,
            cast_options=pl.ScanCastOptions(datetime_cast="nanosecond-downcast"),
        )
    except TypeError:
        return pl.scan_parquet(files)


def _fmt(rows: int, seconds: float) -> str:
    speed = rows / seconds if seconds > 0 else float("inf")
    return f"{rows:>10,} 行  {seconds:6.3f}s  ({speed:,.0f} 行/s)"


def _bench(name: str, fn: Callable[[], pl.DataFrame]) -> None:
    """执行一个查询并打印耗时。"""
    print(f"\n{'='*60}")
    print(f"📊 {name}")
    print(f"{'='*60}")
    t0 = time.perf_counter()
    try:
        df = fn()
        elapsed = time.perf_counter() - t0
        print(f"   结果: {_fmt(len(df), elapsed)}")
        if len(df) > 0:
            # 打印列名和最后几行样本
            print(f"   列名: {df.columns}")
            print(f"   样本(末3行):\n{df.tail(3)}")
    except MemoryError:
        elapsed = time.perf_counter() - t0
        print(f"   ❌ MemoryError（内存不足，建议分批处理）耗时 {elapsed:.3f}s")
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print(f"   ❌ 错误: {e}  耗时 {elapsed:.3f}s")


# ============================================================================
#  各测试场景
# ============================================================================

def bench_single_stock_full():
    """1. 单只股票全历史（读全部年度文件，按 code 行组跳过）"""
    files = _get_year_files()
    return _scan(files).filter(pl.col("code") == SINGLE_CODE).collect()


def bench_single_stock_1y():
    """2. 单只股票最近 1 年"""
    files = _get_year_files(2025, 2025)
    return _scan(files).filter(pl.col("code") == SINGLE_CODE).collect()


def bench_single_stock_5y():
    """3. 单只股票最近 5 年"""
    files = _get_year_files(2021, 2025)
    return _scan(files).filter(pl.col("code") == SINGLE_CODE).collect()


def bench_few_stocks_full():
    """4. 少量股票（3 只）全历史"""
    files = _get_year_files()
    return _scan(files).filter(pl.col("code").is_in(FEW_CODES)).collect()


def bench_few_stocks_5y():
    """5. 少量股票（3 只）最近 5 年"""
    files = _get_year_files(2021, 2025)
    return _scan(files).filter(pl.col("code").is_in(FEW_CODES)).collect()


def bench_medium_stocks_1y():
    """6. 中等股票数量（50 只）最近 1 年"""
    files = _get_year_files(2025, 2025)
    return _scan(files).filter(pl.col("code").is_in(MEDIUM_CODES)).collect()


def bench_medium_stocks_5y():
    """7. 中等股票数量（50 只）最近 5 年"""
    files = _get_year_files(2021, 2025)
    return _scan(files).filter(pl.col("code").is_in(MEDIUM_CODES)).collect()


def bench_one_day_full_market():
    """8. 全市场某一天（最适合按日期分区的查询，年度格式也能应对）"""
    files = _get_year_files(2025, 2025)
    return _scan(files).filter(pl.col("date") == SINGLE_DATE).collect()


def bench_recent_10days_full_market():
    """9. 全市场最近 10 个交易日"""
    files = _get_year_files(2025, 2026)
    return _scan(files).filter(pl.col("date") >= RECENT_10_DAYS).collect()


def bench_full_market_1y():
    """10. 全市场最近 1 年（无 code 过滤，仅时间过滤）"""
    files = _get_year_files(2025, 2025)
    return _scan(files).collect()


def bench_full_market_all(skip: bool = False):
    """11. 全市场全历史（最大查询，可能 OOM）"""
    if skip:
        print("   ⏭️  已跳过（传入 --skip-full 时跳过此项）")
        return pl.DataFrame()
    files = _get_year_files()
    return _scan(files).select(["code", "kline_time", "close", "volume"]).collect()


# ============================================================================
#  主函数
# ============================================================================

def main(skip_full: bool = False):
    if not os.path.isdir(YEARLY_DIR):
        print(f"[ERROR] 年度 Parquet 目录不存在: {YEARLY_DIR}")
        sys.exit(1)

    files_all = _get_year_files()
    print(f"\n📁 数据目录: {YEARLY_DIR}")
    print(f"📅 年度文件: {len(files_all)} 个 ({os.path.basename(files_all[0])} ~ {os.path.basename(files_all[-1])})")
    print(f"💻 测试开始...\n")

    scenarios = [
        ("1. 单只股票 全历史",          bench_single_stock_full),
        ("2. 单只股票 最近 1 年",        bench_single_stock_1y),
        ("3. 单只股票 最近 5 年",        bench_single_stock_5y),
        ("4. 少量股票(3只) 全历史",      bench_few_stocks_full),
        ("5. 少量股票(3只) 最近 5 年",   bench_few_stocks_5y),
        ("6. 中等股票(50只) 最近 1 年",  bench_medium_stocks_1y),
        ("7. 中等股票(50只) 最近 5 年",  bench_medium_stocks_5y),
        ("8. 全市场 某一天",            bench_one_day_full_market),
        ("9. 全市场 最近10交易日",       bench_recent_10days_full_market),
        ("10. 全市场 最近 1 年",         bench_full_market_1y),
        ("11. 全市场 全历史(仅4列)",     lambda: bench_full_market_all(skip=skip_full)),
    ]

    results = []
    for name, fn in scenarios:
        t0 = time.perf_counter()
        try:
            df = fn()
            elapsed = time.perf_counter() - t0
            rows = len(df) if df is not None else 0
            status = "✅"
        except MemoryError:
            elapsed = time.perf_counter() - t0
            rows = -1
            status = "💥 OOM"
        except Exception as e:
            elapsed = time.perf_counter() - t0
            rows = -1
            status = f"❌ {e}"

        results.append((name, status, rows, elapsed))
        row_str = f"{rows:,}" if rows >= 0 else "N/A"
        print(f"  {status}  {name:<35}  {row_str:>12} 行  {elapsed:6.3f}s")

    print(f"\n{'='*70}")
    print(f"📈 汇总报表")
    print(f"{'='*70}")
    print(f"  {'场景':<40} {'行数':>12}  {'耗时':>8}  {'速度'}")
    print(f"  {'-'*70}")
    for name, status, rows, elapsed in results:
        row_str = f"{rows:,}" if rows >= 0 else "N/A"
        speed = f"{rows/elapsed:,.0f} 行/s" if rows > 0 and elapsed > 0 else "-"
        print(f"  {status} {name:<38} {row_str:>12}  {elapsed:6.3f}s  {speed}")
    print(f"{'='*70}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="K 线 Parquet 读取性能基准测试")
    parser.add_argument("--skip-full", action="store_true", help="跳过全市场全历史（内存受限时使用）")
    args = parser.parse_args()
    main(skip_full=args.skip_full)
