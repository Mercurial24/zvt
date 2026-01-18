# -*- coding: utf-8 -*-
"""
遍历 /data/code/zvt/zvt-home/data/xysz 下的数据，做基础健康检查。

用法:
  python scripts/check_xysz_data.py                    # 快速：完整性 + 表列表
  python scripts/check_xysz_data.py --count           # 含各表行数（大表会较慢）
  python scripts/check_xysz_data.py --cache            # 含 cache 目录统计
  python scripts/check_xysz_data.py --data-dir /path   # 指定 xysz 数据目录
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

# 默认 xysz 数据目录（与 ZVT data_path 一致）
DEFAULT_XYSZ_DATA = os.path.join(
    os.environ.get("ZVT_HOME", "/data/code/zvt/zvt-home"),
    "data",
    "xysz",
)


def check_db(path: str, do_count: bool) -> dict:
    """对单个 SQLite 文件做 integrity_check，可选统计表行数。"""
    result = {"path": path, "ok": False, "tables": [], "error": None}
    try:
        conn = sqlite3.connect(path, timeout=10)
        cur = conn.cursor()
        cur.execute("PRAGMA integrity_check")
        check = cur.fetchone()[0]
        if check != "ok":
            result["error"] = f"integrity_check: {check}"
            conn.close()
            return result
        result["ok"] = True
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        tables = [r[0] for r in cur.fetchall()]
        result["tables"] = tables
        if do_count and tables:
            counts = {}
            for t in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM [{t}]")
                    counts[t] = cur.fetchone()[0]
                except Exception as e:
                    counts[t] = str(e)
            result["counts"] = counts
        conn.close()
    except Exception as e:
        result["error"] = str(e)
    return result


def scan_cache(cache_root: str) -> list[dict]:
    """统计 cache 下各子目录文件数和总大小（不读内容，只 stat）。"""
    out = []
    if not os.path.isdir(cache_root):
        return out
    for name in sorted(os.listdir(cache_root)):
        path = os.path.join(cache_root, name)
        if not os.path.isdir(path):
            continue
        nfiles = 0
        total_size = 0
        for _root, _dirs, files in os.walk(path):
            for f in files:
                nfiles += 1
                try:
                    total_size += os.path.getsize(os.path.join(_root, f))
                except OSError:
                    pass
        out.append({"dir": name, "files": nfiles, "size_mb": round(total_size / (1024 * 1024), 2)})
    return out


def main():
    parser = argparse.ArgumentParser(description="检查 xysz 数据目录下的 DB 与 cache")
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_XYSZ_DATA,
        help="xysz 数据目录，默认 ZVT_HOME/data/xysz",
    )
    parser.add_argument("--count", action="store_true", help="统计每张表的行数（大表较慢）")
    parser.add_argument("--cache", action="store_true", help="统计 cache 子目录文件数与大小")
    args = parser.parse_args()

    data_dir = args.data_dir
    if not os.path.isdir(data_dir):
        print(f"目录不存在: {data_dir}")
        sys.exit(1)

    # 1. 遍历所有 .db 文件（含子目录，如 xysz/xysz_finance.db）
    db_files = []
    for root, _dirs, files in os.walk(data_dir):
        for f in files:
            if f.endswith(".db"):
                db_files.append(os.path.join(root, f))
    db_files.sort()

    if not db_files:
        print(f"未找到 .db 文件: {data_dir}")
    else:
        print(f"找到 {len(db_files)} 个 DB 文件\n")

    all_ok = True
    for path in db_files:
        rel = os.path.relpath(path, data_dir)
        res = check_db(path, do_count=args.count)
        if not res["ok"]:
            all_ok = False
            print(f"[FAIL] {rel}")
            print(f"  {res['error']}")
            continue
        print(f"[OK]   {rel}")
        for t in res["tables"]:
            line = f"  - {t}"
            if args.count and "counts" in res and t in res["counts"]:
                c = res["counts"][t]
                line += f"  rows={c}" if isinstance(c, int) else f"  error={c}"
            print(line)
        print()

    if args.cache:
        cache_root = os.path.join(data_dir, "cache")
        cache_stats = scan_cache(cache_root)
        if cache_stats:
            print("cache 目录统计:")
            for s in cache_stats:
                print(f"  {s['dir']}: {s['files']} 文件, {s['size_mb']} MB")
        else:
            print("cache 目录为空或不存在")

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
