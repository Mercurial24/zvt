# -*- coding: utf-8 -*-
"""
按指定交易日筛选高股息率股票。

数据源：StockValuation.dividend_yield_ttm（近 12 个月股息率，税前），
在 xysz 估值 recorder 中由分红与收盘价计算得到。

用法（需在项目根目录且 PYTHONPATH 含 src，或使用 conda quant 环境）::

    conda run -n quant python my_strategies/strategy_dividend_yield_screen.py --date 2025-02-24 --min-yield 0.05

在结果表上追加业务侧估算列（与 xysz 估值 recorder 口径一致前提下近似）：

- **cash_dividend_total_ttm**：近 12 个月现金派现总额（元）≈ ``dividend_ps_ttm * capitalization``；
  ``capitalization`` 为财报对齐的总股本（股），与逐次分红时点数可能略有偏差。
- **net_profit_parent_ttm_implied**：归属母公司净利润 TTM（元）≈ ``market_cap / pe_ttm``，
  由当日市值与动态 PE 反推，与 recorder 内 ``pe_ttm = close / (ttm_net_profit / final_cap)`` 一致；
  若 ``capitalization`` 与计算 PE 用的 ``final_cap`` 不一致，支付率会有误差。
- **dividend_payout_ratio_ttm**：股息支付率 ≈ ``cash_dividend_total_ttm / net_profit_parent_ttm_implied``；
  ``pe_ttm`` 缺失、为 0 或净利润为负时置为 NaN（避免误导）。

若当日无估值记录（例如非交易日），请先确认库中该日是否有数据，或改用最近一个有数据的交易日。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
_src = _ROOT / "src"
if _src.is_dir() and str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from zvt.contract.api import get_data
from zvt.domain import StockValuation


def _add_dividend_payout_columns(df: pd.DataFrame) -> pd.DataFrame:
    """在估值截面 DataFrame 上追加派现总额与股息支付率（业务侧近似）。"""
    if df.empty:
        return df
    out = df.copy()
    cap = pd.to_numeric(out.get("capitalization"), errors="coerce")
    dps = pd.to_numeric(out.get("dividend_ps_ttm"), errors="coerce")
    mcap = pd.to_numeric(out.get("market_cap"), errors="coerce")
    pe = pd.to_numeric(out.get("pe_ttm"), errors="coerce")

    out["cash_dividend_total_ttm"] = np.where(
        cap.notna() & dps.notna() & (cap > 0) & (dps >= 0),
        (dps * cap).astype(float),
        np.nan,
    )

    valid_np = (
        mcap.notna()
        & pe.notna()
        & (pe != 0)
        & np.isfinite(mcap.to_numpy(dtype=float))
        & np.isfinite(pe.to_numpy(dtype=float))
    )
    np_ttm = np.where(valid_np.to_numpy(), (mcap / pe).to_numpy(dtype=float), np.nan)
    out["net_profit_parent_ttm_implied"] = np_ttm

    cash = out["cash_dividend_total_ttm"].to_numpy(dtype=float)
    out["dividend_payout_ratio_ttm"] = np.where(
        valid_np.to_numpy()
        & np.isfinite(np_ttm)
        & (np_ttm > 0)
        & np.isfinite(cash)
        & (cash >= 0),
        cash / np_ttm,
        np.nan,
    )
    return out


def screen_dividend_yield(
    trade_date: str | pd.Timestamp,
    min_yield: float = 0.05,
    provider: str = "xysz",
    extra_columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    返回指定交易日 dividend_yield_ttm > min_yield 的股票截面。

    :param trade_date: 交易日，如 '2025-02-24'
    :param min_yield: 股息率下限（与库中字段一致，为小数，如 0.05 表示 5%）
    :param provider: 估值库 provider，含有效 dividend_yield_ttm 时推荐 xysz
    """
    ts = pd.Timestamp(trade_date).normalize()
    end = ts + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)

    cols = [
        "timestamp",
        "entity_id",
        "code",
        "name",
        "dividend_yield_ttm",
        "dividend_ps_ttm",
        "pe_ttm",
        "pb",
        "market_cap",
        "capitalization",
    ]
    if extra_columns:
        for c in extra_columns:
            if c not in cols:
                cols.append(c)

    df = get_data(
        data_schema=StockValuation,
        provider=provider,
        start_timestamp=ts,
        end_timestamp=end,
        columns=cols,
    )
    if df is None or df.empty:
        return pd.DataFrame()

    out = df[df["dividend_yield_ttm"].notna() & (df["dividend_yield_ttm"] > min_yield)].copy()
    out = _add_dividend_payout_columns(out)
    return out.sort_values("dividend_yield_ttm", ascending=False).reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(description="按交易日筛选高股息率股票（dividend_yield_ttm）")
    parser.add_argument(
        "--date",
        default="2025-02-24",
        help="交易日 YYYY-MM-DD（默认 2025-02-24，可按需改为其他年份的 2 月 24 日）",
    )
    parser.add_argument(
        "--min-yield",
        type=float,
        default=0.05,
        help="股息率下限，小数形式（默认 0.05 即 5%%）",
    )
    parser.add_argument(
        "--provider",
        default="xysz",
        help="StockValuation provider（默认 xysz，需已回填 dividend 相关估值）",
    )
    parser.add_argument("--csv", metavar="PATH", help="可选：结果写入 CSV 路径")
    args = parser.parse_args()

    picked = screen_dividend_yield(
        trade_date=args.date,
        min_yield=args.min_yield,
        provider=args.provider,
    )
    n = len(picked)
    print(f"日期 {args.date} | provider={args.provider} | dividend_yield_ttm > {args.min_yield} | 共 {n} 只")
    if n == 0:
        print("无结果：请检查该日是否有估值数据，或先运行 xysz 估值/分红回填脚本。")
        return
    pd.set_option("display.max_rows", 30)
    pd.set_option("display.width", 120)
    print(picked.head(30).to_string(index=False))
    if n > 30:
        print(f"... 另有 {n - 30} 只未显示")
    if args.csv:
        picked.to_csv(args.csv, index=False, encoding="utf-8-sig")
        print(f"已写入 {args.csv}")


if __name__ == "__main__":
    main()
