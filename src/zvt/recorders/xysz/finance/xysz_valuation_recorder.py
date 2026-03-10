# -*- coding: utf-8 -*-
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
from zvt.recorders.xysz.xysz_api import get_xysz_client
import logging

try:
    import AmazingData as ad
except ImportError:
    ad = None

logger = logging.getLogger(__name__)


class xyszValuationRecorder(FixedCycleDataRecorder):
    provider = "xysz"
    entity_provider = "xysz"
    entity_schema = Stock
    data_schema = StockValuation

    def __init__(self, force_update=True, sleeping_time=0, **kwargs):
        super().__init__(force_update, sleeping_time, **kwargs)
        self.client = get_xysz_client()

    def _query_optional_kline_fields(self, entity, start, end):
        """从 AmazingData 原始日 K 中补充可选字段，避免依赖历史落库完整性。"""
        if ad is None:
            return None

        start_ts = pd.Timestamp(start) if start is not None else pd.Timestamp("2010-01-01")
        end_ts = pd.Timestamp(end) if end is not None else pd.Timestamp.now()
        xysz_code = f"{entity.code}.{entity.exchange.upper()}"

        try:
            kline_dict = self.client.query_kline(
                code_list=[xysz_code],
                begin_date=int(start_ts.strftime("%Y%m%d")),
                end_date=int(end_ts.strftime("%Y%m%d")),
                period=ad.constant.Period.day.value,
            )
        except Exception as e:
            self.logger.warning(f"query optional kline fields failed for {xysz_code}: {e}")
            return None

        raw_df = kline_dict.get(xysz_code) if kline_dict else None
        if raw_df is None or raw_df.empty:
            return None

        raw_df = raw_df.copy()
        if isinstance(raw_df.index, pd.DatetimeIndex):
            raw_df.index.name = "timestamp"
            raw_df = raw_df.reset_index()
        else:
            raw_df = raw_df.reset_index()

        cols_upper = {str(c).upper(): c for c in raw_df.columns}
        timestamp_col = None
        for candidate in ["KLINE_TIME", "TRADE_DATE", "DATE", "TIMESTAMP"]:
            if candidate in cols_upper:
                timestamp_col = cols_upper[candidate]
                break
        if timestamp_col is None:
            return None

        raw_df = raw_df.rename(columns={timestamp_col: "timestamp"})
        if pd.api.types.is_integer_dtype(raw_df["timestamp"]) or pd.api.types.is_float_dtype(raw_df["timestamp"]):
            raw_df["timestamp"] = pd.to_datetime(raw_df["timestamp"].astype(int).astype(str), format="%Y%m%d")
        else:
            raw_df["timestamp"] = pd.to_datetime(raw_df["timestamp"])

        rename_dict = {}
        for raw_name, target_name in {
            "TURNOVER_RATE": "turnover_rate",
            "A_FLOAT_CAP": "float_cap",
            "TOTAL_CAP": "total_cap",
        }.items():
            if raw_name in cols_upper:
                rename_dict[cols_upper[raw_name]] = target_name
        if rename_dict:
            raw_df = raw_df.rename(columns=rename_dict)

        keep_cols = ["timestamp"] + [col for col in ["turnover_rate", "float_cap", "total_cap"] if col in raw_df.columns]
        raw_df = raw_df[keep_cols].copy()
        raw_df["ts_merge"] = raw_df["timestamp"].dt.normalize()
        raw_df = raw_df.sort_values("ts_merge").drop_duplicates(subset=["ts_merge"], keep="last")
        return raw_df

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
            columns=["report_period", "net_profit_as_parent", "operating_income"],
        )
        if income_df is None or income_df.empty:
            self.logger.warning(f"No income data for {entity_id}")
            return

        # 3. 资产负债表（股本、归属母公司股东权益）
        balance_df = BalanceSheet.query_data(
            entity_id=entity_id,
            provider="xysz",
            columns=["report_period", "capital", "equity"],
        )
        if balance_df is None or balance_df.empty:
            self.logger.warning(f"No balance sheet data for {entity_id}")
            return

        # 4. 现金流量表（经营活动现金流净额）
        cashflow_df = CashFlowStatement.query_data(
            entity_id=entity_id,
            provider="xysz",
            columns=["report_period", "net_op_cash_flows"],
        )
        if cashflow_df is None or cashflow_df.empty:
            cashflow_df = pd.DataFrame(columns=["report_period", "net_op_cash_flows"])

        # --- 预处理与合并 ---
        kdata_df = kdata_df.copy()
        income_df = income_df.copy()
        balance_df = balance_df.copy()
        cashflow_df = cashflow_df.copy()

        if "timestamp" in kdata_df.columns:
            kdata_df["ts_merge"] = pd.to_datetime(kdata_df["timestamp"])
        else:
            kdata_df["ts_merge"] = pd.to_datetime(kdata_df.index.get_level_values("timestamp"))
        kdata_df["ts_merge"] = kdata_df["ts_merge"].dt.normalize()

        income_df["ts_merge"] = pd.to_datetime(income_df["report_period"])
        balance_df["ts_merge"] = pd.to_datetime(balance_df["report_period"])
        cashflow_df["ts_merge"] = pd.to_datetime(cashflow_df["report_period"])

        kdata_df = kdata_df.sort_values("ts_merge")
        income_df = income_df.sort_values("ts_merge")
        balance_df = balance_df.sort_values("ts_merge")
        cashflow_df = cashflow_df.sort_values("ts_merge")

        optional_kline_df = self._query_optional_kline_fields(entity=entity, start=start, end=end)
        if optional_kline_df is not None and not optional_kline_df.empty:
            kdata_df = pd.merge(
                kdata_df,
                optional_kline_df.drop(columns=["timestamp"], errors="ignore"),
                on="ts_merge",
                how="left",
                suffixes=("", "_raw"),
            )
            if "turnover_rate_raw" in kdata_df.columns:
                kdata_df["turnover_rate"] = kdata_df["turnover_rate"].combine_first(kdata_df["turnover_rate_raw"])
                kdata_df = kdata_df.drop(columns=["turnover_rate_raw"])

        # 静态 PE：用最近一期年报净利润。只保留年报（report_period 为 12 月）
        income_annual = income_df[
            income_df["ts_merge"].dt.month == 12
        ][["ts_merge", "net_profit_as_parent", "operating_income"]].copy()
        income_annual = income_annual.rename(columns={"net_profit_as_parent": "net_profit_annual"})

        # 动态 PE(TTM)：用过去四个季度净利润之和。按 report_period 排序后滚动 4 期求和
        income_df = income_df.copy()
        income_df["ttm_net_profit"] = (
            income_df["net_profit_as_parent"].rolling(4, min_periods=4).sum()
        )

        # 先合并行情与利润表（TTM 口径，用于动态 PE 和市销率等）
        df_merged = pd.merge_asof(
            kdata_df,
            income_df[["ts_merge", "net_profit_as_parent", "operating_income", "ttm_net_profit"]],
            on="ts_merge",
            direction="backward",
        )
        # 再合并最近一期年报净利润（用于静态 PE）
        if not income_annual.empty:
            df_merged = pd.merge_asof(
                df_merged,
                income_annual[["ts_merge", "net_profit_annual"]],
                on="ts_merge",
                direction="backward",
            )
        else:
            df_merged["net_profit_annual"] = np.nan

        df_merged = pd.merge_asof(
            df_merged,
            balance_df[["ts_merge", "capital", "equity"]],
            on="ts_merge",
            direction="backward",
        )
        df_merged = pd.merge_asof(
            df_merged,
            cashflow_df[["ts_merge", "net_op_cash_flows"]],
            on="ts_merge",
            direction="backward",
        )

        df_merged = df_merged.dropna(subset=["net_profit_as_parent", "capital"])
        if df_merged.empty:
            self.logger.warning(f"Merged valuation data is empty for {entity_id}")
            return

        df_merged = df_merged[df_merged["capital"] > 0]
        cap = df_merged["capital"].values
        close = df_merged["close"].values
        equity = df_merged["equity"].values
        operating_income = df_merged["operating_income"].values
        net_op_cash = df_merged["net_op_cash_flows"].values

        # 静态 PE：股价 / (最近一期年报归属母公司净利润 / 股本)
        net_profit_annual = df_merged["net_profit_annual"].values
        eps_annual = np.where(cap > 0, net_profit_annual / cap, np.nan)
        pe_static = np.where(
            np.isfinite(eps_annual) & (eps_annual > 0), close / eps_annual, np.nan
        )
        df_merged["pe"] = np.round(np.where(np.isfinite(pe_static), pe_static, np.nan), 2)

        # 动态 PE(TTM)：股价 / (过去四季度归属母公司净利润 / 股本)
        ttm_net_profit = df_merged["ttm_net_profit"].values
        eps_ttm = np.where(cap > 0, ttm_net_profit / cap, np.nan)
        pe_ttm = np.where(
            np.isfinite(eps_ttm) & (eps_ttm > 0), close / eps_ttm, np.nan
        )
        df_merged["pe_ttm"] = np.round(np.where(np.isfinite(pe_ttm), pe_ttm, np.nan), 2)

        df_merged["market_cap"] = close * cap

        # 市净率：市值 / 归属母公司股东权益
        df_merged["pb"] = np.round(
            np.where(
                np.isfinite(equity) & (equity > 0),
                (close * cap) / equity,
                np.nan,
            ),
            2,
        )

        # 市销率：市值 / 营业收入
        df_merged["ps"] = np.round(
            np.where(
                np.isfinite(operating_income) & (operating_income > 0),
                (close * cap) / operating_income,
                np.nan,
            ),
            2,
        )

        # 市现率：市值 / 经营活动现金流净额
        df_merged["pcf"] = np.round(
            np.where(
                np.isfinite(net_op_cash) & (net_op_cash > 0),
                (close * cap) / net_op_cash,
                np.nan,
            ),
            2,
        )

        # 流通股本 / 流通市值：来自 Stock 实体的 float_cap（流通市值），反推流通股本
        float_cap_series = df_merged["float_cap"] if "float_cap" in df_merged.columns else np.nan
        if isinstance(float_cap_series, pd.Series):
            float_cap_values = pd.to_numeric(float_cap_series, errors="coerce").values
        else:
            float_cap_values = np.full(len(df_merged), np.nan)

        fallback_float_cap = getattr(entity, "float_cap", None)
        if fallback_float_cap is not None and np.isfinite(fallback_float_cap):
            float_cap_values = np.where(np.isfinite(float_cap_values), float_cap_values, fallback_float_cap)

        if np.isfinite(float_cap_values).any():
            df_merged["circulating_market_cap"] = float_cap_values
            df_merged["circulating_cap"] = np.where(close > 0, float_cap_values / close, np.nan)
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
