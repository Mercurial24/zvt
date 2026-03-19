# -*- coding: utf-8 -*-
"""
xysz 估值 Recorder：纯计算，仅读取 ZVT 内已有的 xysz 日 K、利润表、资产负债表、现金流量表与 Stock 实体，
不调用 AmazingData API，避免加载 C++ SDK。
"""
import pandas as pd
import numpy as np
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import (
    Stock,
    StockValuation,
    IncomeStatement,
    Stock1dKdata,
    BalanceSheet,
    CashFlowStatement,
)
from zvt.contract.api import df_to_db
import logging

logger = logging.getLogger(__name__)


class xyszValuationRecorder(FixedCycleDataRecorder):
    provider = "xysz"
    entity_provider = "xysz"
    entity_schema = Stock
    data_schema = StockValuation

    def __init__(self, force_update=True, sleeping_time=0, **kwargs):
        super().__init__(force_update, sleeping_time, **kwargs)

    def record(self, entity, start, end, size, timestamps):
        entity_id = entity.id
        self.logger.info(f"Calculating Valuation for {entity_id} ...")

        # 1. 行情（含换手率）
        kdata_df = Stock1dKdata.query_data(
            entity_id=entity_id,
            provider="xysz",
            start_timestamp=start,
            end_timestamp=end,
            columns=["timestamp", "close", "volume", "turnover_rate"],
        )
        if kdata_df is None or kdata_df.empty:
            self.logger.warning(f"No kdata for {entity_id}")
            return

        # 2. 利润表（净利润、营业收入）
        income_df = IncomeStatement.query_data(
            entity_id=entity_id,
            provider="xysz",
            columns=["timestamp", "report_period", "net_profit_as_parent", "operating_income"],
        )
        if income_df is None or income_df.empty:
            self.logger.warning(f"No income data for {entity_id}")
            return

        # 3. 资产负债表（股本、归属母公司股东权益）
        balance_df = BalanceSheet.query_data(
            entity_id=entity_id,
            provider="xysz",
            columns=["timestamp", "report_period", "capital", "equity"],
        )
        if balance_df is None or balance_df.empty:
            self.logger.warning(f"No balance sheet data for {entity_id}")
            return

        # 4. 现金流量表（经营活动现金流净额）
        cashflow_df = CashFlowStatement.query_data(
            entity_id=entity_id,
            provider="xysz",
            columns=["timestamp", "report_period", "net_op_cash_flows"],
        )
        if cashflow_df is None or cashflow_df.empty:
            cashflow_df = pd.DataFrame(columns=["timestamp", "report_period", "net_op_cash_flows"])

        # --- 预处理与合并 ---
        kdata_df = kdata_df.copy()
        income_df = income_df.copy()
        balance_df = balance_df.copy()
        cashflow_df = cashflow_df.copy()

        # 增加逻辑：只计算今天之前的估值，防止计算不完整的盘中数据
        today = pd.Timestamp.now().normalize()
        if "timestamp" in kdata_df.columns:
            kdata_df = kdata_df[pd.to_datetime(kdata_df["timestamp"]) < today]
        else:
            kdata_df = kdata_df[kdata_df.index.get_level_values("timestamp") < today]

        if kdata_df.empty:
            return

        # 预设 ts_merge
        if "timestamp" in kdata_df.columns:
            kdata_df["ts_merge"] = pd.to_datetime(kdata_df["timestamp"]).dt.normalize()
        else:
            kdata_df["ts_merge"] = pd.to_datetime(kdata_df.index.get_level_values("timestamp")).dt.normalize()

        income_df["ts_merge"] = pd.to_datetime(income_df["timestamp"]).dt.normalize()
        balance_df["ts_merge"] = pd.to_datetime(balance_df["timestamp"]).dt.normalize()
        cashflow_df["ts_merge"] = pd.to_datetime(cashflow_df["timestamp"]).dt.normalize()

        # merge_asof requires no nulls in 'on' column
        kdata_df = kdata_df.dropna(subset=["ts_merge"])
        income_df = income_df.dropna(subset=["ts_merge"])
        balance_df = balance_df.dropna(subset=["ts_merge"])
        cashflow_df = cashflow_df.dropna(subset=["ts_merge"])

        # 核心逻辑：计算 TTM 指标
        def compute_ttm_col(df, col):
            work_df = df.copy().sort_values("report_period")
            work_df["year"] = pd.to_datetime(work_df["report_period"]).dt.year
            work_df["month"] = pd.to_datetime(work_df["report_period"]).dt.month
            # 建立 (年, 月) -> 累计值的查找表
            lookup = work_df.set_index(["year", "month"])[col].to_dict()

            def get_ttm(row):
                y, m = row["year"], row["month"]
                if m == 12: return row[col] # 年报直接就是 TTM
                prev_annual = lookup.get((y - 1, 12))
                prev_ytd = lookup.get((y - 1, m))
                if prev_annual is not None and prev_ytd is not None:
                    return row[col] + prev_annual - prev_ytd
                return np.nan
            return work_df.apply(get_ttm, axis=1)

        # 处理利润表 TTM
        income_df["ttm_net_profit"] = compute_ttm_col(income_df, "net_profit_as_parent")
        income_df["ttm_operating_income"] = compute_ttm_col(income_df, "operating_income")
        
        # 静态 PE 用年报
        income_annual = income_df[pd.to_datetime(income_df["report_period"]).dt.month == 12][
            ["ts_merge", "report_period", "net_profit_as_parent"]
        ].copy()
        income_annual = income_annual.rename(columns={"net_profit_as_parent": "net_profit_annual"})

        # 处理现金流 TTM
        cashflow_df["ttm_net_op_cash_flows"] = compute_ttm_col(cashflow_df, "net_op_cash_flows")

        # --- 对齐：必须按公告日 (timestamp) 对齐股价 ---

        df_merged = pd.merge_asof(kdata_df.sort_values("ts_merge"), 
                                  income_df[["ts_merge", "ttm_net_profit", "ttm_operating_income", "report_period"]].sort_values(["ts_merge", "report_period"]), 
                                  on="ts_merge", direction="backward")
        
        df_merged = pd.merge_asof(df_merged, balance_df[["ts_merge", "capital", "equity"]].sort_values(["ts_merge"]), 
                                  on="ts_merge", direction="backward")
        
        df_merged = pd.merge_asof(df_merged, cashflow_df[["ts_merge", "ttm_net_op_cash_flows"]].sort_values(["ts_merge"]), 
                                  on="ts_merge", direction="backward")

        if not income_annual.empty:
            df_merged = pd.merge_asof(df_merged, income_annual[["ts_merge", "net_profit_annual"]].sort_values("ts_merge"), 
                                      on="ts_merge", direction="backward")

        # 核心：计算估值。优先使用 K 线里的动态总股本 total_cap，取不到再用财报股本 capital
        df_merged["final_cap"] = df_merged.get("total_cap", pd.Series(np.nan, index=df_merged.index)).combine_first(df_merged["capital"])
        df_merged = df_merged.dropna(subset=["final_cap", "close"])
        
        cap = df_merged["final_cap"].values
        close = df_merged["close"].values
        
        # 1. PE 计算 (不再过滤负值)
        df_merged["pe"] = np.round(close / (df_merged["net_profit_annual"] / cap), 2)
        df_merged["pe_ttm"] = np.round(close / (df_merged["ttm_net_profit"] / cap), 2)
        
        # 2. 市值
        df_merged["market_cap"] = close * cap
        
        # 3. PB (时点指标)
        df_merged["pb"] = np.round(df_merged["market_cap"] / df_merged["equity"], 2)
        
        # 4. PS (TTM)
        df_merged["ps"] = np.round(df_merged["market_cap"] / df_merged["ttm_operating_income"], 2)
        
        # 5. PCF (TTM)
        df_merged["pcf"] = np.round(df_merged["market_cap"] / df_merged["ttm_net_op_cash_flows"], 2)

        # 流通股本 / 流通市值：Stock 实体的 float_cap 存的是流通市值（更新时点），反推流通股本 = 流通市值/最新价，再算各日流通市值
        latest_close = float(close[-1]) if len(close) else np.nan
        entity_circ_mv = getattr(entity, "float_cap", None)
        if (
            latest_close is not None
            and np.isfinite(latest_close)
            and latest_close > 0
            and entity_circ_mv is not None
            and np.isfinite(entity_circ_mv)
        ):
            float_share = float(entity_circ_mv) / latest_close
            df_merged["circulating_cap"] = float_share
            df_merged["circulating_market_cap"] = np.where(close > 0, close * float_share, np.nan)
        else:
            df_merged["circulating_market_cap"] = np.nan
            df_merged["circulating_cap"] = np.nan

        # 换手率：直接来自 K 线
        df_merged["turnover_ratio"] = df_merged.get("turnover_rate", np.nan)

        val_list = []
        for _, row in df_merged.iterrows():
            rec = {
                "id": f"{entity_id}_{row['ts_merge'].strftime('%Y-%m-%d')}",
                "entity_id": entity_id,
                "timestamp": row["ts_merge"],
                "code": entity.code,
                "name": entity.name,
                "pe_ttm": row["pe_ttm"],
                "pe": row["pe"],
                "market_cap": row["market_cap"],
                "capitalization": row["capital"],
                "pb": row["pb"] if pd.notna(row.get("pb")) else None,
                "ps": row["ps"] if pd.notna(row.get("ps")) else None,
                "pcf": row["pcf"] if pd.notna(row.get("pcf")) else None,
                "circulating_cap": row["circulating_cap"] if pd.notna(row.get("circulating_cap")) else None,
                "circulating_market_cap": row["circulating_market_cap"] if pd.notna(row.get("circulating_market_cap")) else None,
                "turnover_ratio": row["turnover_ratio"] if pd.notna(row.get("turnover_ratio")) else None,
                "provider": self.provider,
            }
            val_list.append(rec)

        if val_list:
            df = pd.DataFrame(val_list)
            df_to_db(
                df=df,
                data_schema=StockValuation,
                provider=self.provider,
                force_update=self.force_update,
            )
            self.logger.info(f"Calculated {len(val_list)} valuation records for {entity_id}")

if __name__ == "__main__":
    recorder = xyszValuationRecorder(codes=['000001', '000736', '601599'])
    recorder.run()
