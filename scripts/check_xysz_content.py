# -*- coding: utf-8 -*-
"""
检查 xysz 库中数据的常见异常（重复 id、空关键字段、日期/数值异常等）。

用法:
  python scripts/check_xysz_content.py                    # 检查所有库
  python scripts/check_xysz_content.py --db xysz_finance.db
  python scripts/check_xysz_content.py --table balance_sheet
  python scripts/check_xysz_content.py --quick             # 只做轻量检查，大表不扫全表
  python scripts/check_xysz_content.py --show-bad-rows 5   # 展示错误的具体前5条行数据标识
"""
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
from datetime import datetime

DEFAULT_XYSZ_DATA = os.path.join(
    os.environ.get("ZVT_HOME", "/data/code/zvt/zvt-home"),
    "data",
    "xysz",
)

def get_db_and_tables(data_dir: str, db_name: str = None, table_name: str = None):
    """列出要检查的 (db_path, [tables])。"""
    dbs = []
    for root, _dirs, files in os.walk(data_dir):
        for f in files:
            if f.endswith(".db"):
                path = os.path.join(root, f)
                name = os.path.basename(path)
                if db_name and name != db_name:
                    continue
                try:
                    conn = sqlite3.connect(path, timeout=5)
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                    )
                    tables = [r[0] for r in cur.fetchall()]
                    conn.close()
                    if table_name:
                        tables = [t for t in tables if t == table_name]
                    if tables:
                        dbs.append((path, tables))
                except Exception:
                    dbs.append((path, []))
    return dbs

def check_condition(cur, table: str, condition: str, check_name: str, limit: int) -> tuple[str, str]:
    """检查满足某异常情况的数据条数，若大于 0，还可以提取对应的 id 样例"""
    try:
        cur.execute(f"SELECT COUNT(*) FROM [{table}] WHERE {condition}")
        n = cur.fetchone()[0]
        if n == 0:
            return (check_name, "ok")
        
        msg = f"{n} 条"
        if limit > 0:
            try:
                cur.execute(f"PRAGMA table_info([{table}])")
                cols = [r[1] for r in cur.fetchall()]
                if "id" in cols:
                    cur.execute(f"SELECT id FROM [{table}] WHERE {condition} LIMIT {limit}")
                    bad_ids = [str(r[0]) for r in cur.fetchall()]
                elif "entity_id" in cols and "timestamp" in cols:
                    cur.execute(f"SELECT entity_id, timestamp FROM [{table}] WHERE {condition} LIMIT {limit}")
                    bad_ids = [f"({r[0]},{r[1]})" for r in cur.fetchall()]
                else:
                    cur.execute(f"SELECT * FROM [{table}] WHERE {condition} LIMIT {limit}")
                    bad_ids = [str(r[0]) for r in cur.fetchall()] # just print the first column

                if bad_ids:
                    msg += f"\n      [例: {', '.join(bad_ids)}]"
            except Exception:
                pass
        return (check_name, msg)
    except sqlite3.OperationalError as e:
        return (check_name, f"SQL 错误: {e}")

def checks_common(cur, table: str, quick: bool, limit: int) -> list[tuple[str, str]]:
    """所有表通用：重复 id、空 id。"""
    results = []
    cur.execute(f"PRAGMA table_info([{table}])")
    cols = [r[1] for r in cur.fetchall()]
    if "id" not in cols:
        return [("id 列", "表无 id 列，跳过通用检查")]

    try:
        cur.execute(
            f"SELECT COUNT(*) FROM (SELECT id FROM [{table}] GROUP BY id HAVING COUNT(*) > 1)"
        )
        dup = cur.fetchone()[0]
        if dup == 0:
            results.append(("重复 id", "ok"))
        else:
            msg = f"{dup} 个 id 重复"
            if limit > 0:
                cur.execute(f"SELECT id FROM [{table}] GROUP BY id HAVING COUNT(*) > 1 LIMIT {limit}")
                bad_ids = [str(r[0]) for r in cur.fetchall()]
                if bad_ids:
                    msg += f"\n      [例: {', '.join(bad_ids)}]"
            results.append(("重复 id", msg))
    except sqlite3.OperationalError as e:
        results.append(("重复 id", f"SQL 错误: {e}"))

    results.append(check_condition(cur, table, "id IS NULL OR id = ''", "空 id", limit))
    return results

def checks_finance(cur, table: str, quick: bool, limit: int) -> list[tuple[str, str]]:
    """财务报表类：timestamp/report_date 空、日期离谱、id 格式混用、勾稽关系。"""
    results = []
    cur.execute(f"PRAGMA table_info([{table}])")
    cols = {r[1]: r[2] for r in cur.fetchall()}

    if "timestamp" in cols:
        results.append(check_condition(cur, table, "timestamp IS NULL", "timestamp 为空", limit))
    if "report_date" in cols:
        results.append(check_condition(cur, table, "report_date IS NULL", "report_date 为空", limit))

    next_year = datetime.now().year + 1
    if "report_date" in cols:
        results.append(check_condition(cur, table, f"date(report_date) < '1990-01-01' OR date(report_date) > '{next_year}-12-31'", "report_date 超出合理范围", limit))

    if "id" in cols and not quick:
        results.append(check_condition(cur, table, "id GLOB '*_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'", "id 含 YYYYMMDD 格式(建议统一 YYYY-MM-DD)", limit))

    # 财务勾稽关系检查 (以 balance_sheet 为例)
    if table == "balance_sheet" and "total_assets" in cols and "total_liabilities" in cols and "total_equity" in cols:
        # ABS() 判断差额是否过大（例如 10 块钱容差）
        condition = "ABS(COALESCE(total_assets, 0) - (COALESCE(total_liabilities, 0) + COALESCE(total_equity, 0))) > 10.0"
        results.append(check_condition(cur, table, condition, "❗ 资产不等于负债+对应权益(容差10元)", limit))

    return results

def checks_stock_adj_factor(cur, table: str, quick: bool, limit: int) -> list[tuple[str, str]]:
    """复权因子：重复 (entity_id,timestamp)、hfq_factor 空/零/负。"""
    results = []
    cur.execute(f"PRAGMA table_info([{table}])")
    cols = [r[1] for r in cur.fetchall()]

    if "entity_id" in cols and "timestamp" in cols:
        cur.execute(
            f"SELECT COUNT(*) FROM (SELECT entity_id, timestamp FROM [{table}] GROUP BY entity_id, timestamp HAVING COUNT(*) > 1)"
        )
        dup = cur.fetchone()[0]
        if dup == 0:
            results.append(("(entity_id,timestamp) 重复", "ok"))
        else:
            msg = f"{dup} 组"
            if limit > 0:
                cur.execute(f"SELECT entity_id, timestamp FROM [{table}] GROUP BY entity_id, timestamp HAVING COUNT(*) > 1 LIMIT {limit}")
                bad_groups = [f"({r[0]},{r[1]})" for r in cur.fetchall()]
                msg += f"\n      [例: {', '.join(bad_groups)}]"
            results.append(("(entity_id,timestamp) 重复", msg))

    if "hfq_factor" in cols:
        results.append(check_condition(cur, table, "hfq_factor IS NULL", "hfq_factor 为空", limit))
        results.append(check_condition(cur, table, "hfq_factor <= 0", "hfq_factor <= 0", limit))
        
    return results

def checks_kdata(cur, table: str, quick: bool, limit: int) -> list[tuple[str, str]]:
    """K 线：重复 (entity_id,timestamp)、OHLC 空/负、包含关系错乱、涨跌幅极值、无量价差。"""
    results = []
    cur.execute(f"PRAGMA table_info([{table}])")
    cols = [r[1] for r in cur.fetchall()]

    if "entity_id" in cols and "timestamp" in cols:
        cur.execute(
            f"SELECT COUNT(*) FROM (SELECT entity_id, timestamp FROM [{table}] GROUP BY entity_id, timestamp HAVING COUNT(*) > 1)"
        )
        dup = cur.fetchone()[0]
        if dup == 0:
            results.append(("(entity_id,timestamp) 重复", "ok"))
        else:
            msg = f"{dup} 组"
            if limit > 0:
                cur.execute(f"SELECT entity_id, timestamp FROM [{table}] GROUP BY entity_id, timestamp HAVING COUNT(*) > 1 LIMIT {limit}")
                bad_groups = [f"({r[0]},{r[1]})" for r in cur.fetchall()]
                msg += f"\n      [例: {', '.join(bad_groups)}]"
            results.append(("(entity_id,timestamp) 重复", msg))

    for col in ("open", "high", "low", "close"):
        if col in cols:
            results.append(check_condition(cur, table, f"[{col}] IS NULL OR [{col}] < 0", f"{col} 空或负", limit))
            
    if "high" in cols and "low" in cols:
        results.append(check_condition(cur, table, "high < low", "high < low", limit))

    if "high" in cols and "open" in cols and "close" in cols and "low" in cols:
        results.append(check_condition(cur, table, "high < open OR high < close OR low > open OR low > close", "❗ OHLC包含逻辑错误(最高价不够高或最低价不够低)", limit))

    if "change_pct" in cols:
        # 涨跌幅超过 40% (即 0.4) 的日内涨跌幅通常是新股等，若出现次数多可能是有问题
        results.append(check_condition(cur, table, "change_pct > 0.4 OR change_pct < -0.4", "❗ 涨跌幅极端(>40%或<-40%)", limit))

    if "volume" in cols and "open" in cols and "close" in cols:
        results.append(check_condition(cur, table, "volume = 0 AND open != close", "❗ 无成交量却有价差(非停牌态)", limit))

    return results

TABLE_CHECKS = {
    "balance_sheet": checks_finance,
    "income_statement": checks_finance,
    "cash_flow_statement": checks_finance,
    "stock_adj_factor": checks_stock_adj_factor,
}

def kdata_table(table: str) -> bool:
    return "kdata" in table.lower() and "stock" in table.lower()

def check_table(conn, table: str, quick: bool, limit: int) -> list[tuple[str, str]]:
    cur = conn.cursor()
    out = []
    out.extend(checks_common(cur, table, quick, limit))
    if table in TABLE_CHECKS:
        out.extend(TABLE_CHECKS[table](cur, table, quick, limit))
    elif kdata_table(table):
        out.extend(checks_kdata(cur, table, quick, limit))
    return out

def main():
    parser = argparse.ArgumentParser(description="检查 xysz 数据内容异常")
    parser.add_argument("--data-dir", default=DEFAULT_XYSZ_DATA, help="xysz 数据目录")
    parser.add_argument("--db", default=None, help="只检查该 db 文件，如 xysz_finance.db")
    parser.add_argument("--table", default=None, help="只检查该表名")
    parser.add_argument("--quick", action="store_true", help="轻量检查，大表不做全表扫描")
    parser.add_argument("--show-bad-rows", type=int, default=0, help="如果出现异常，展示出问题的前 N 条完整信息(主要打印对应的 id)")
    args = parser.parse_args()

    if not os.path.isdir(args.data_dir):
        print(f"目录不存在: {args.data_dir}")
        sys.exit(1)

    dbs = get_db_and_tables(args.data_dir, args.db, args.table)
    if not dbs:
        print("未找到符合条件的 .db 或表")
        sys.exit(0)

    any_issue = False
    for path, tables in dbs:
        rel = os.path.relpath(path, args.data_dir)
        print(f"\n[{rel}]")
        try:
            conn = sqlite3.connect(path, timeout=30)
        except Exception as e:
            print(f"  无法打开: {e}")
            any_issue = True
            continue
        for table in tables:
            try:
                results = check_table(conn, table, args.quick, args.show_bad_rows)
                issues = [r for r in results if r[1] != "ok"]
                if issues:
                    any_issue = True
                    print(f"  {table}:")
                    for name, msg in results:
                        tag = "⚠ " if msg != "ok" else "  "
                        print(f"    {tag}{name}: {msg}")
                else:
                    print(f"  {table}: 全部通过")
            except Exception as e:
                any_issue = True
                print(f"  {table}: 检查异常 {e}")
        conn.close()

    print()
    sys.exit(1 if any_issue else 0)

if __name__ == "__main__":
    main()
