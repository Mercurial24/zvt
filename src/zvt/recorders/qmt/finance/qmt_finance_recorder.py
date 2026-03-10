# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import gc
import logging

from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import Stock, BalanceSheet, IncomeStatement, CashFlowStatement, StockValuation, Stock1dKdata
from zvt.contract.api import df_to_db, get_entities
from zvt.broker.qmt import qmt_quote
from zvt.utils.time_utils import now_pd_timestamp, to_pd_timestamp

logger = logging.getLogger(__name__)

class BaseQmtFinanceRecorder(FixedCycleDataRecorder):
    provider = "qmt"
    entity_provider = "qmt"
    entity_schema = Stock
    data_schema = None
    table_name = None  # QMT table name: e.g., 'Balance', 'Income', 'CashFlow'

    def __init__(self, force_update=False, sleeping_time=0.2, **kwargs):
        super().__init__(force_update, sleeping_time, **kwargs)

    def run(self):
        unfinished_items = self.entities
        if not unfinished_items:
            self.logger.info("No entities found for recording financial data.")
            return

        pbar = tqdm(total=len(unfinished_items), desc=f"QMT {self.data_schema.__name__}")
        
        # QMT get_financial_data can handle list, but to be safe and incremental, we can do it by entity
        # or in batches. Let's do batches to be efficient.
        batch_size = 100
        for i in range(0, len(unfinished_items), batch_size):
            batch = unfinished_items[i:i+batch_size]
            qmt_codes = [f"{e.code}.{e.exchange.upper()}" for e in batch]
            try:
                # Use report_time to get sequential reports
                data_dict = qmt_quote.get_financial_data(
                    qmt_codes, 
                    table_list=[self.table_name], 
                    report_type="report_time"
                )
                
                if not data_dict:
                    pbar.update(len(batch))
                    continue
                
                for entity in batch:
                    qmt_code = f"{entity.code}.{entity.exchange.upper()}"
                    stock_data = data_dict.get(qmt_code, {})
                    df = stock_data.get(self.table_name)
                    
                    if df is not None and not df.empty:
                        df = self._transform_df(df, entity)
                        if df is not None and not df.empty:
                            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)
                    pbar.update(1)
            except Exception as e:
                self.logger.error(f"Error fetching {self.table_name} for batch starting with {batch[0].code}: {e}")
                pbar.update(len(batch))
            
            if self.sleeping_time > 0:
                time.sleep(self.sleeping_time)
            gc.collect()
            
        pbar.close()

    @staticmethod
    def _parse_qmt_datetime(values):
        """兼容 QMT 财务接口常见的整数日期/时间格式。"""
        if isinstance(values, pd.Series):
            s = values.copy()
        else:
            s = pd.Series(values)

        if pd.api.types.is_datetime64_any_dtype(s):
            return pd.to_datetime(s, errors="coerce")

        normalized = s.astype(str).str.strip()
        normalized = normalized.replace(
            {
                "": pd.NA,
                "None": pd.NA,
                "none": pd.NA,
                "nan": pd.NA,
                "NaT": pd.NA,
                "<NA>": pd.NA,
            }
        )
        normalized = normalized.str.replace(r"\.0$", "", regex=True)

        result = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

        mask_8 = normalized.str.fullmatch(r"\d{8}", na=False)
        if mask_8.any():
            result.loc[mask_8] = pd.to_datetime(normalized.loc[mask_8], format="%Y%m%d", errors="coerce")

        mask_14 = normalized.str.fullmatch(r"\d{14}", na=False)
        if mask_14.any():
            result.loc[mask_14] = pd.to_datetime(normalized.loc[mask_14], format="%Y%m%d%H%M%S", errors="coerce")

        fallback_mask = result.isna() & normalized.notna()
        if fallback_mask.any():
            result.loc[fallback_mask] = pd.to_datetime(normalized.loc[fallback_mask], errors="coerce")

        return result

    def _transform_df(self, df: pd.DataFrame, entity):
        # QMT 财务表在不同接口版本下可能有两种格式：
        # 1. index 就是报告期
        # 2. 使用 RangeIndex，报告期放在 m_timetag / report_date 列中
        df = df.copy()
        # Normalize column names to lowercase so QMT doc field names match
        df.columns = [str(c).lower().strip() if isinstance(c, str) else c for c in df.columns]

        if "report_date" in df.columns:
            report_date = self._parse_qmt_datetime(df["report_date"])
        elif "m_timetag" in df.columns:
            report_date = self._parse_qmt_datetime(df["m_timetag"])
        elif isinstance(df.index, pd.DatetimeIndex):
            report_date = pd.Series(df.index, index=df.index)
        else:
            report_date = self._parse_qmt_datetime(df.index)

        df = df.reset_index(drop=True)
        df["report_date"] = report_date.reset_index(drop=True)

        # QMT 通常用 m_anntime 表示公告日，取不到时再退回报告期
        if "m_anntime" in df.columns:
            df["timestamp"] = self._parse_qmt_datetime(df["m_anntime"])
        else:
            df["timestamp"] = df["report_date"]

        if "m_timetag" in df.columns:
            timetag_ts = self._parse_qmt_datetime(df["m_timetag"])
            df["timestamp"] = df["timestamp"].fillna(timetag_ts)
        
        df = df.dropna(subset=["timestamp", "report_date"])
        
        df["entity_id"] = entity.id
        df["code"] = entity.code
        df["provider"] = self.provider
        
        # report_period: YYYY-MM-DD
        df["report_period"] = df["report_date"].dt.strftime("%Y-%m-%d")

        # id: entity_id_report_period
        df["id"] = df.apply(lambda row: f"{row['entity_id']}_{row['report_period']}", axis=1)
        df = df.drop_duplicates(subset=["id"], keep="last")

        # Mapping common English names to ZVT names
        mapping = self._get_column_map()
        if mapping:
            df = df.rename(columns=mapping)
            df = self._collapse_duplicate_columns(df)
            
        # Keep only schema-relevant columns
        schema_cols = [c.name for c in self.data_schema.__table__.columns]
        df_cols = [c for c in df.columns if c in schema_cols]
        return df[df_cols]

    def _get_column_map(self):
        return {}

    @staticmethod
    def _collapse_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
        if not df.columns.duplicated().any():
            return df

        merged = {}
        ordered_columns = []
        for column in df.columns:
            if column in merged:
                continue
            dup_df = df.loc[:, df.columns == column]
            if dup_df.shape[1] == 1:
                merged[column] = dup_df.iloc[:, 0]
            else:
                # 多个别名字段映射到同一个 ZVT 字段时，优先取每行第一个非空值
                merged[column] = dup_df.bfill(axis=1).iloc[:, 0]
            ordered_columns.append(column)

        return pd.DataFrame(merged, index=df.index)[ordered_columns]

class QmtBalanceSheetRecorder(BaseQmtFinanceRecorder):
    data_schema = BalanceSheet
    table_name = "Balance"

    def _get_column_map(self):
        # QMT Balance field names (miniQMT doc) -> ZVT BalanceSheet column names
        return {
            "cash_equivalents": "cash_and_cash_equivalents",
            "bill_receivable": "note_receivable",
            "account_receivable": "accounts_receivable",
            "advance_payment": "advances_to_suppliers",
            "other_receivable": "other_receivables",
            "inventories": "inventories",
            "current_assets_one_year": "current_portion_of_non_current_assets",
            "other_current_assets": "other_current_assets",
            "total_current_assets": "total_current_assets",
            "fin_assets_avail_for_sale": "fi_assets_saleable",
            "long_term_receivables": "long_term_receivables",
            "long_term_eqy_invest": "long_term_equity_investment",
            "invest_real_estate": "real_estate_investment",
            "fix_assets": "fixed_assets",
            "constru_in_process": "construction_in_process",
            "intang_assets": "intangible_assets",
            "goodwill": "goodwill",
            "long_deferred_expense": "long_term_prepaid_expenses",
            "deferred_tax_assets": "deferred_tax_assets",
            "other_non_mobile_assets": "other_non_current_assets",
            "total_non_current_assets": "total_non_current_assets",
            "tot_assets": "total_assets",
            "shortterm_loan": "short_term_borrowing",
            "accounts_payable": "accounts_payable",
            "advance_peceipts": "advances_from_customers",
            "empl_ben_payable": "employee_benefits_payable",
            "taxes_surcharges_payable": "taxes_payable",
            "int_payable": "interest_payable",
            "other_payable": "other_payable",
            "non_current_liability_in_one_year": "current_portion_of_non_current_liabilities",
            "other_current_liability": "other_current_liabilities",
            "total_current_liability": "total_current_liabilities",
            "long_term_loans": "long_term_borrowing",
            "bonds_payable": "fi_bond_payable",
            "longterm_account_payable": "long_term_payable",
            "deferred_income": "deferred_revenue",
            "deferred_tax_liab": "deferred_tax_liabilities",
            "other_non_current_liabilities": "other_non_current_liabilities",
            "non_current_liabilities": "total_non_current_liabilities",
            "tot_liab": "total_liabilities",
            "cap_stk": "capital",
            "cap_rsrv": "capital_reserve",
            "specific_reserves": "special_reserve",
            "surplus_rsrv": "surplus_reserve",
            "prov_nom_risks": "fi_generic_risk_reserve",
            "undistributed_profit": "undistributed_profits",
            "tot_shrhldr_eqy_excl_min_int": "equity",
            "minority_int": "equity_as_minority_interest",
            "total_equity": "total_equity",
            "tot_liab_shrhldr_eqy": "total_liabilities_and_equity",
            # Common alternate names (if API returns these)
            "total_assets": "total_assets",
            "total_liabilities": "total_liabilities",
            "monetary_funds": "cash_and_cash_equivalents",
            "fixed_assets": "fixed_assets",
            "intangible_assets": "intangible_assets",
        }

class QmtIncomeStatementRecorder(BaseQmtFinanceRecorder):
    data_schema = IncomeStatement
    table_name = "Income"

    def _get_column_map(self):
        # QMT Income field names (miniQMT doc) -> ZVT IncomeStatement column names
        return {
            "revenue": "operating_income",
            "revenue_inc": "operating_income",
            "total_operating_cost": "total_operating_costs",
            "total_expense": "operating_costs",
            "less_taxes_surcharges_ops": "business_taxes_and_surcharges",
            "sale_expense": "sales_costs",
            "less_gerl_admin_exp": "managing_costs",
            "financial_expense": "financing_costs",
            "research_expenses": "rd_costs",
            "less_impair_loss_assets": "assets_devaluation",
            "plus_net_invest_inc": "investment_income",
            "incl_inc_invest_assoc_jv_entp": "investment_income_from_related_enterprise",
            "oper_profit": "operating_profit",
            "plus_non_oper_rev": "non_operating_income",
            "less_non_oper_exp": "non_operating_costs",
            "tot_profit": "total_profits",
            "inc_tax": "tax_expense",
            "net_profit_incl_min_int_inc": "net_profit",
            "net_profit_excl_min_int_inc": "net_profit_as_parent",
            "minority_int_inc": "net_profit_as_minority_interest",
            "net_profit_incl_min_int_inc_after": "deducted_net_profit",
            "s_fa_eps_basic": "eps",
            "s_fa_eps_diluted": "diluted_eps",
            "other_compreh_inc": "other_comprehensive_income",
            "total_income": "total_comprehensive_income",
            "total_income_minority": "total_comprehensive_income_as_minority_interest",
            # Common alternate names
            "total_operating_revenue": "operating_income",
            "operating_revenue": "operating_income",
            "operating_cost": "operating_costs",
            "operating_profit": "operating_profit",
            "total_profit": "total_profits",
            "net_profit": "net_profit",
            "net_profit_attributable_to_owners_of_the_parent_company": "net_profit_as_parent",
        }

class QmtCashFlowRecorder(BaseQmtFinanceRecorder):
    data_schema = CashFlowStatement
    table_name = "CashFlow"

    def _get_column_map(self):
        # QMT CashFlow field names (miniQMT doc) -> ZVT CashFlowStatement column names
        return {
            "goods_sale_and_service_render_cash": "cash_from_selling",
            "tax_levy_refund": "tax_refund",
            "other_cash_recp_ral_oper_act": "cash_from_other_op",
            "stot_cash_inflows_oper_act": "total_op_cash_inflows",
            "goods_and_services_cash_paid": "cash_to_goods_services",
            "cash_pay_beh_empl": "cash_to_employees",
            "pay_all_typ_tax": "taxes_and_surcharges",
            "other_cash_pay_ral_oper_act": "cash_to_other_related_op",
            "stot_cash_outflows_oper_act": "total_op_cash_outflows",
            "net_cash_flows_oper_act": "net_op_cash_flows",
            "cash_recp_disp_withdrwl_invest": "cash_from_disposal_of_investments",
            "cash_recp_return_invest": "cash_from_returns_on_investments",
            "net_cash_recp_disp_fiolta": "cash_from_disposal_fixed_intangible_assets",
            "disposal_other_business_units": "cash_from_disposal_subsidiaries",
            "other_cash_recp_ral_inv_act": "cash_from_other_investing",
            "stot_cash_inflows_inv_act": "total_investing_cash_inflows",
            "cash_paid_invest": "cash_to_investments",
            "cash_pay_acq_const_fiolta": "cash_to_acquire_fixed_intangible_assets",
            "cash_paid_by_subsidiaries": "cash_to_acquire_subsidiaries",
            "stot_cash_outflows_inv_act": "total_investing_cash_outflows",
            "net_cash_flows_inv_act": "net_investing_cash_flows",
            "cash_recp_cap_contrib": "cash_from_accepting_investment",
            "cass_received_sub_abs": "cash_from_subsidiaries_accepting_minority_interest",
            "cash_recp_borrow": "cash_from_borrowings",
            "proc_issue_bonds": "cash_from_issuing_bonds",
            "other_cash_recp_ral_fnc_act": "cash_from_other_financing",
            "stot_cash_inflows_fnc_act": "total_financing_cash_inflows",
            "cash_prepay_amt_borr": "cash_to_repay_borrowings",
            "cash_pay_dist_dpcp_int_exp": "cash_to_pay_interest_dividend",
            "cass_received_sub_investments": "cash_to_pay_subsidiaries_minority_interest",
            "other_cash_pay_ral_fnc_act": "cash_to_other_financing",
            "stot_cash_outflows_fnc_act": "total_financing_cash_outflows",
            "net_cash_flows_fnc_act": "net_financing_cash_flows",
            "eff_fx_flu_cash": "foreign_exchange_rate_effect",
            "net_incr_cash_cash_equ": "net_cash_increase",
            "cash_cash_equ_beg_period": "cash_at_beginning",
            "cash_cash_equ_end_period": "cash",
            # Common alternate names
            "net_cash_flows_operating_activities": "net_op_cash_flows",
            "net_cash_flows_investing_activities": "net_investing_cash_flows",
            "net_cash_flows_financing_activities": "net_financing_cash_flows",
            "net_increase_in_cash_and_cash_equivalents": "net_cash_increase",
        }


class QmtValuationRecorder(FixedCycleDataRecorder):
    provider = "qmt"
    entity_provider = "qmt"
    entity_schema = Stock
    data_schema = StockValuation

    def __init__(self, force_update=True, sleeping_time=0, **kwargs):
        super().__init__(force_update, sleeping_time, **kwargs)

    def _get_capital_df(self, entity):
        qmt_code = f"{entity.code}.{entity.exchange.upper()}"
        try:
            data_dict = qmt_quote.get_financial_data(
                [qmt_code],
                table_list=["Capital"],
                report_type="report_time",
            )
        except Exception as e:
            self.logger.warning(f"query capital data failed for {qmt_code}: {e}")
            return pd.DataFrame(columns=["ts_merge", "capitalization", "circulating_cap"])

        capital_df = data_dict.get(qmt_code, {}).get("Capital")
        if capital_df is None or capital_df.empty:
            return pd.DataFrame(columns=["ts_merge", "capitalization", "circulating_cap"])

        capital_df = capital_df.copy()
        capital_df.columns = [str(c).lower().strip() if isinstance(c, str) else c for c in capital_df.columns]

        if "report_date" in capital_df.columns:
            report_date = BaseQmtFinanceRecorder._parse_qmt_datetime(capital_df["report_date"])
        elif "m_timetag" in capital_df.columns:
            report_date = BaseQmtFinanceRecorder._parse_qmt_datetime(capital_df["m_timetag"])
        elif isinstance(capital_df.index, pd.DatetimeIndex):
            report_date = pd.Series(capital_df.index, index=capital_df.index)
        else:
            report_date = BaseQmtFinanceRecorder._parse_qmt_datetime(capital_df.index)

        capital_df = capital_df.reset_index(drop=True)
        capital_df["report_date"] = report_date.reset_index(drop=True)
        if "m_anntime" in capital_df.columns:
            capital_df["ts_merge"] = BaseQmtFinanceRecorder._parse_qmt_datetime(capital_df["m_anntime"])
        else:
            capital_df["ts_merge"] = capital_df["report_date"]

        if "m_timetag" in capital_df.columns:
            timetag_ts = BaseQmtFinanceRecorder._parse_qmt_datetime(capital_df["m_timetag"])
            capital_df["ts_merge"] = capital_df["ts_merge"].fillna(timetag_ts)

        capital_df["ts_merge"] = capital_df["ts_merge"].fillna(capital_df["report_date"])
        capital_df["capitalization"] = pd.to_numeric(capital_df.get("total_capital"), errors="coerce")
        capital_df["circulating_cap"] = pd.to_numeric(capital_df.get("circulating_capital"), errors="coerce")
        capital_df = capital_df.dropna(subset=["ts_merge"]).sort_values("ts_merge")
        return capital_df[["ts_merge", "capitalization", "circulating_cap"]]

    def record(self, entity, start, end, size, timestamps):
        entity_id = entity.id
        self.logger.info(f"Calculating QMT valuation for {entity_id} ...")

        kdata_df = Stock1dKdata.query_data(
            entity_id=entity_id,
            provider="qmt",
            start_timestamp=start,
            end_timestamp=end,
            columns=["timestamp", "close", "volume"],
        )
        if kdata_df is None or kdata_df.empty:
            self.logger.warning(f"No qmt kdata for {entity_id}")
            return

        income_df = IncomeStatement.query_data(
            entity_id=entity_id,
            provider="qmt",
            columns=["report_period", "net_profit_as_parent", "operating_income"],
        )
        if income_df is None or income_df.empty:
            self.logger.warning(f"No qmt income data for {entity_id}")
            return

        balance_df = BalanceSheet.query_data(
            entity_id=entity_id,
            provider="qmt",
            columns=["report_period", "equity"],
        )
        if balance_df is None or balance_df.empty:
            self.logger.warning(f"No qmt balance data for {entity_id}")
            return

        cashflow_df = CashFlowStatement.query_data(
            entity_id=entity_id,
            provider="qmt",
            columns=["report_period", "net_op_cash_flows"],
        )
        if cashflow_df is None or cashflow_df.empty:
            cashflow_df = pd.DataFrame(columns=["report_period", "net_op_cash_flows"])

        capital_df = self._get_capital_df(entity)

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
        if not capital_df.empty:
            capital_df["ts_merge"] = pd.to_datetime(capital_df["ts_merge"])

        kdata_df = kdata_df.sort_values("ts_merge")
        income_df = income_df.sort_values("ts_merge")
        balance_df = balance_df.sort_values("ts_merge")
        cashflow_df = cashflow_df.sort_values("ts_merge")
        if not capital_df.empty:
            capital_df = capital_df.sort_values("ts_merge")

        income_annual = income_df[income_df["ts_merge"].dt.month == 12][["ts_merge", "net_profit_as_parent"]].copy()
        income_annual = income_annual.rename(columns={"net_profit_as_parent": "net_profit_annual"})
        income_df["ttm_net_profit"] = income_df["net_profit_as_parent"].rolling(4, min_periods=4).sum()

        df_merged = pd.merge_asof(
            kdata_df,
            income_df[["ts_merge", "net_profit_as_parent", "operating_income", "ttm_net_profit"]],
            on="ts_merge",
            direction="backward",
        )
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
            balance_df[["ts_merge", "equity"]],
            on="ts_merge",
            direction="backward",
        )
        df_merged = pd.merge_asof(
            df_merged,
            cashflow_df[["ts_merge", "net_op_cash_flows"]],
            on="ts_merge",
            direction="backward",
        )
        if not capital_df.empty:
            df_merged = pd.merge_asof(
                df_merged,
                capital_df[["ts_merge", "capitalization", "circulating_cap"]],
                on="ts_merge",
                direction="backward",
            )
        else:
            df_merged["capitalization"] = np.nan
            df_merged["circulating_cap"] = np.nan

        df_merged = df_merged.dropna(subset=["net_profit_as_parent", "capitalization"])
        if df_merged.empty:
            self.logger.warning(f"Merged qmt valuation data is empty for {entity_id}")
            return

        df_merged = df_merged[df_merged["capitalization"] > 0]
        if df_merged.empty:
            self.logger.warning(f"No valid capitalization for {entity_id}")
            return

        close = df_merged["close"].values
        capitalization = pd.to_numeric(df_merged["capitalization"], errors="coerce").values
        circulating_cap = pd.to_numeric(df_merged["circulating_cap"], errors="coerce").values
        equity = pd.to_numeric(df_merged["equity"], errors="coerce").values
        operating_income = pd.to_numeric(df_merged["operating_income"], errors="coerce").values
        net_op_cash = pd.to_numeric(df_merged["net_op_cash_flows"], errors="coerce").values
        volume = pd.to_numeric(df_merged["volume"], errors="coerce").values

        market_cap = close * capitalization
        circulating_market_cap = np.where(
            np.isfinite(circulating_cap) & (circulating_cap > 0),
            close * circulating_cap,
            np.nan,
        )
        turnover_ratio = np.where(
            np.isfinite(circulating_cap) & (circulating_cap > 0),
            volume / circulating_cap,
            np.nan,
        )

        net_profit_annual = pd.to_numeric(df_merged["net_profit_annual"], errors="coerce").values
        eps_annual = np.where(capitalization > 0, net_profit_annual / capitalization, np.nan)
        pe_static = np.where(np.isfinite(eps_annual) & (eps_annual > 0), close / eps_annual, np.nan)

        ttm_net_profit = pd.to_numeric(df_merged["ttm_net_profit"], errors="coerce").values
        eps_ttm = np.where(capitalization > 0, ttm_net_profit / capitalization, np.nan)
        pe_ttm = np.where(np.isfinite(eps_ttm) & (eps_ttm > 0), close / eps_ttm, np.nan)

        pb = np.where(
            np.isfinite(equity) & (equity > 0),
            market_cap / equity,
            np.nan,
        )
        ps = np.where(
            np.isfinite(operating_income) & (operating_income > 0),
            market_cap / operating_income,
            np.nan,
        )
        pcf = np.where(
            np.isfinite(net_op_cash) & (net_op_cash > 0),
            market_cap / net_op_cash,
            np.nan,
        )

        val_list = []
        for _, row in df_merged.assign(
            market_cap=market_cap,
            circulating_market_cap=circulating_market_cap,
            turnover_ratio=turnover_ratio,
            pe=np.round(pe_static, 2),
            pe_ttm=np.round(pe_ttm, 2),
            pb=np.round(pb, 2),
            ps=np.round(ps, 2),
            pcf=np.round(pcf, 2),
        ).iterrows():
            rec = {
                "id": f"{entity_id}_{row['ts_merge'].strftime('%Y-%m-%d')}",
                "entity_id": entity_id,
                "timestamp": row["ts_merge"],
                "code": entity.code,
                "name": entity.name,
                "pe": row["pe"] if pd.notna(row.get("pe")) else None,
                "pe_ttm": row["pe_ttm"] if pd.notna(row.get("pe_ttm")) else None,
                "pb": row["pb"] if pd.notna(row.get("pb")) else None,
                "ps": row["ps"] if pd.notna(row.get("ps")) else None,
                "pcf": row["pcf"] if pd.notna(row.get("pcf")) else None,
                "market_cap": row["market_cap"] if pd.notna(row.get("market_cap")) else None,
                "circulating_market_cap": row["circulating_market_cap"] if pd.notna(row.get("circulating_market_cap")) else None,
                "capitalization": row["capitalization"] if pd.notna(row.get("capitalization")) else None,
                "circulating_cap": row["circulating_cap"] if pd.notna(row.get("circulating_cap")) else None,
                "turnover_ratio": row["turnover_ratio"] if pd.notna(row.get("turnover_ratio")) else None,
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
            self.logger.info(f"Calculated {len(val_list)} qmt valuation records for {entity_id}")

if __name__ == "__main__":
    QmtBalanceSheetRecorder(codes=['000001']).run()


__all__ = [
    "BaseQmtFinanceRecorder",
    "QmtBalanceSheetRecorder",
    "QmtIncomeStatementRecorder",
    "QmtCashFlowRecorder",
    "QmtValuationRecorder",
]
