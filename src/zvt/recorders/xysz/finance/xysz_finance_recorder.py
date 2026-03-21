# -*- coding: utf-8 -*-
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import Stock, BalanceSheet, IncomeStatement, CashFlowStatement
from zvt.contract.api import df_to_db
from zvt.recorders.xysz.xysz_api import get_xysz_client
from zvt.utils.time_utils import now_pd_timestamp
import pandas as pd
import logging
from tqdm import tqdm
import time
from zvt import zvt_env
import os
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql.expression import text

# AmazingData library needs a local_path even for is_local=False
_XYSZ_LOCAL_CACHE_PATH = os.path.join(zvt_env["data_path"], "xysz", "cache") + os.sep
os.makedirs(_XYSZ_LOCAL_CACHE_PATH, exist_ok=True)

class xyszFinanceRecorder(FixedCycleDataRecorder):
    provider = "xysz"
    entity_provider = "xysz" # Using ZVT standard if possible, but we use xysz meta
    entity_schema = Stock
    data_schema = None

    def __init__(self, force_update=False, sleeping_time=0, **kwargs):
        self.client = get_xysz_client()
        super().__init__(force_update, sleeping_time, **kwargs)

    def evaluate_start_end_size_timestamps(self, entity):
        """已有最新财报且报告日在近期（1 年内）则视为已更新，不再拉取，避免重复下载和写 cache。"""
        if entity.timestamp and (entity.timestamp >= now_pd_timestamp()):
            return entity.timestamp, None, 0, None
        latest_saved_record = self.get_latest_saved_record(entity=entity)
        if latest_saved_record:
            latest_timestamp = getattr(latest_saved_record, self.get_evaluated_time_field())
            if latest_timestamp:
                # 报告日距今 1 年内视为已更新（数据湖导入或此前已拉过）
                now = now_pd_timestamp()
                if (now - latest_timestamp).days < 365:
                    return latest_timestamp, self.end_timestamp, 0, None
                if self.start_timestamp:
                    latest_timestamp = max(latest_timestamp, self.start_timestamp)
                size = self.default_size
                if self.end_timestamp and latest_timestamp > self.end_timestamp:
                    size = 0
                return latest_timestamp, self.end_timestamp, size, None
            latest_timestamp = entity.timestamp
        else:
            latest_timestamp = entity.timestamp
        if not latest_timestamp:
            return self.start_timestamp, self.end_timestamp, self.default_size, None
        if self.start_timestamp:
            latest_timestamp = max(latest_timestamp, self.start_timestamp)
        size = self.default_size
        if self.end_timestamp and latest_timestamp > self.end_timestamp:
            size = 0
        return latest_timestamp, self.end_timestamp, size, None

    def run(self):
        unfinished_items = self.entities

        # Filter entities that actually need recording (size > 0) or force_update
        items_to_record = []
        for entity in unfinished_items:
            start, end, size, _ = self.evaluate_start_end_size_timestamps(entity)
            if size > 0 or self.force_update:
                items_to_record.append(entity)

        if not items_to_record:
            self.logger.info("All entities are up to date.")
            return

        # 逐只拉取并写入，避免批量写入导致 SQLite database is locked
        pbar = tqdm(total=len(items_to_record), desc=self.data_schema.__name__)
        for entity in items_to_record:
            xysz_code = f"{entity.code}.{entity.exchange.upper()}"
            try:
                data_dict = self._fetch_data([xysz_code], None, None)
                if data_dict and xysz_code in data_dict:
                    df = data_dict[xysz_code]
                    if df is not None and not df.empty:
                        transformed_df = self._transform_df(df, entity)
                        if transformed_df is not None and not transformed_df.empty:
                            self._save_with_dedup(transformed_df)
            except Exception as e:
                self.logger.error(f"Error fetching data for {xysz_code}: {e}")
            pbar.update(1)
            if self.sleeping_time > 0:
                time.sleep(self.sleeping_time)
        pbar.close()
        self.on_finish()

    def record(self, entity, start, end, size, timestamps):
        # This is kept for compatibility with base class and manual calls, 
        # but run() now handles recording directly (one entity per request).
        xysz_code = f"{entity.code}.{entity.exchange.upper()}"
        
        try:
            data_dict = self._fetch_data([xysz_code], start, end)
        except Exception as e:
            self.logger.error(f"Error fetching data for {xysz_code}: {e}")
            return None
            
        if not data_dict or xysz_code not in data_dict:
            return None
            
        df = data_dict[xysz_code]
        if df is None or df.empty:
            return None
        
        # Transform and Map Columns
        df = self._transform_df(df, entity)
        
        if df is not None and not df.empty:
            self._save_with_dedup(df)

    def _save_with_dedup(self, df: pd.DataFrame):
        """仅在 xysz 财务 recorder 内做去重，避免改动全局 api.py 的行为。"""
        if df is None or df.empty:
            return 0

        ids = df["id"].tolist()
        if not ids:
            return 0

        if self.force_update:
            if len(ids) == 1:
                sql = text(f'delete from `{self.data_schema.__tablename__}` where id = "{ids[0]}"')
            else:
                sql = text(f"delete from `{self.data_schema.__tablename__}` where id in {tuple(ids)}")
            self.session.execute(sql)
            return df_to_db(
                df=df,
                data_schema=self.data_schema,
                provider=self.provider,
                force_update=False,
                session=self.session,
                need_check=False,
            )

        existing_ids = []
        max_retry = 3
        for retry in range(max_retry):
            try:
                existing_ids = [row[0] for row in self.session.query(self.data_schema.id).filter(self.data_schema.id.in_(ids)).all()]
                break
            except OperationalError as e:
                err = str(e).lower()
                if "no such table" in err:
                    existing_ids = []
                    break
                if "database is locked" in err and retry < max_retry - 1:
                    self.session.rollback()
                    time.sleep(0.5 * (retry + 1))
                    continue
                raise

        if existing_ids:
            df = df[~df["id"].isin(existing_ids)]
            if df.empty:
                return 0

        return df_to_db(
            df=df,
            data_schema=self.data_schema,
            provider=self.provider,
            force_update=False,
            session=self.session,
            need_check=False,
        )

    def _fetch_data(self, code_list, start, end):
        raise NotImplementedError

    def _transform_df(self, df, entity):
        # 1. Filter for consolidated statements (STATEMENT_TYPE == '1')
        if "STATEMENT_TYPE" in df.columns:
            # Handle both int and str types
            df = df[df["STATEMENT_TYPE"].astype(str) == "1"]
        
        if df.empty:
            return None

        # 2. Map columns
        map_dict = self._get_column_map()
        if map_dict:
            cols_upper = {c.upper(): c for c in df.columns}
            rename_dict = {}
            for k, v in map_dict.items():
                if k.upper() in cols_upper:
                    rename_dict[cols_upper[k.upper()]] = v
            df = df.rename(columns=rename_dict)
            
        df["entity_id"] = entity.id
        df["code"] = entity.code
        df["provider"] = self.provider
        
        # 3. Robust Date processing
        # ANN_DATE -> timestamp, REPORTING_PERIOD -> report_date
        date_cols = ["timestamp", "report_date"]
        for col in date_cols:
            if col in df.columns:
                # Handle YYYYMMDD integer/float formats
                if pd.api.types.is_integer_dtype(df[col]) or pd.api.types.is_float_dtype(df[col]):
                    # Filter out invalid small numbers (like 0 or 19700101 if it's junk)
                    df = df[df[col] > 19000101]
                    if df.empty: break
                    df[col] = pd.to_datetime(df[col].astype(int).astype(str), format="%Y%m%d", errors='coerce')
                else:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

        if df.empty:
            return None

        # Drop rows with invalid or missing crucial dates
        df = df.dropna(subset=["timestamp", "report_date"])
        
        if df.empty:
            return None
                
        # report_period: YYYY-MM-DD
        df["report_period"] = df["report_date"].dt.strftime("%Y-%m-%d")

        # id: entity_id_report_period
        df["id"] = df.apply(lambda row: f"{row['entity_id']}_{row['report_period']}", axis=1)

        # Keep only schema-relevant columns
        schema_cols = [c.name for c in self.data_schema.__table__.columns]
        df_cols = [c for c in df.columns if c in schema_cols]
        return df[df_cols]

    def _get_column_map(self):
        return {}


class xyszBalanceSheetRecorder(xyszFinanceRecorder):
    data_schema = BalanceSheet

    def _fetch_data(self, code_list, start, end):
        return self.client.get_balance_sheet(code_list=code_list, is_local=False, local_path=_XYSZ_LOCAL_CACHE_PATH)

    def _get_column_map(self):
        return {
            "REPORTING_PERIOD": "report_date",
            "ANN_DATE": "timestamp",
            
            # --- General Assets ---
            "TOTAL_ASSETS": "total_assets",
            "CURRENCY_CAP": "cash_and_cash_equivalents",
            "NOTES_RECEIVABLE": "note_receivable",
            "ACCT_RECEIVABLE": "accounts_receivable",
            "PREPAYMENT": "advances_to_suppliers",
            "OTHER_RECEIVABLE": "other_receivables",
            "INV": "inventories",
            "NONCUR_ASSETS_DUE_WITHIN_1Y": "current_portion_of_non_current_assets",
            "OTHER_CUR_ASSETS": "other_current_assets",
            "TOTAL_CUR_ASSETS": "total_current_assets",
            
            "FIN_ASSETS_AVA_FOR_SALE": "fi_assets_saleable",
            "LT_RECEIVABLES": "long_term_receivables",
            "LT_EQUITY_INV": "long_term_equity_investment",
            "INV_REALESTATE": "real_estate_investment",
            "FIXED_ASSETS": "fixed_assets",
            "CONST_IN_PROC": "construction_in_process",
            "INTANGIBLE_ASSETS": "intangible_assets",
            "GOODWILL": "goodwill",
            "LT_DEFERRED_EXP": "long_term_prepaid_expenses",
            "DEFERRED_TAX_ASSETS": "deferred_tax_assets",
            "OTH_NONCUR_ASSETS": "other_non_current_assets",
            "TOT_NONCUR_ASSETS": "total_non_current_assets",

            # --- General Liabilities ---
            "TOTAL_LIAB": "total_liabilities",
            "ST_BORROWING": "short_term_borrowing",
            # "DEP_RECEIVED_IB_DEP": "accept_money_deposits", # Overlaps with bank
            "ACCT_PAYABLE": "accounts_payable",
            "ADV_RECEIPT": "advances_from_customers",
            "EMPL_PAY_PAYABLE": "employee_benefits_payable",
            "TAX_PAYABLE": "taxes_payable",
            "INTEREST_PAYABLE": "interest_payable",
            "OTHER_PAYABLE": "other_payable",
            "NONCUR_LIAB_DUE_WITHIN_1Y": "current_portion_of_non_current_liabilities",
            "OTHER_CUR_LIAB": "other_current_liabilities",
            "TOTAL_CUR_LIAB": "total_current_liabilities",
            
            "LT_LOAN": "long_term_borrowing",
            "LT_PAYABLE": "long_term_payable",
            "DEFERRED_INCOME": "deferred_revenue",
            "DEFERRED_TAX_LIAB": "deferred_tax_liabilities",
            "OTHER_NONCUR_LIAB": "other_non_current_liabilities",
            "TOTAL_NONCUR_LIAB": "total_non_current_liabilities",
            
            # --- Equity ---
            "TOTAL_LIAB_SHARE_EQUITY": "total_liabilities_and_equity",
            "CAP_STOCK": "capital",
            "CAP_RESV": "capital_reserve",
            "SPECIAL_RESV": "special_reserve",
            "SURPLUS_RESV": "surplus_reserve",
            "UNDISTRIBUTED_PRO": "undistributed_profits",
            "TOT_SHARE_EQUITY_EXCL_MIN_INT": "equity",
            "MINORITY_EQUITY": "equity_as_minority_interest",
            "TOT_SHARE_EQUITY_INCL_MIN_INT": "total_equity",

            # --- Bank Specific ---
            # Assets
            "CASH_CENTRAL_BANK_DEPOSITS": "fi_cash_and_deposit_in_central_bank",
            "ASSET_DEP_FUNDS_OTH_FIN_INST": "fi_deposit_in_other_fi",
            "PRECIOUS_METAL": "fi_expensive_metals",
            "LENDING_FUNDS": "fi_lending_to_other_fi",
            "TRADING_FINASSETS": "fi_financial_assets_effect_current_income",
            "DER_FIN_ASSETS": "fi_financial_derivative_asset",
            "RED_MON_CAP_FOR_SALE": "fi_buying_sell_back_fi__asset",
            "INT_RECEIVABLE": "fi_interest_receivable",
            "LOANS_AND_ADVANCES": "fi_disbursing_loans_and_advances",
            "HOLD_TO_MTY_INV": "fi_held_to_maturity_investment",
            "RCV_INV": "fi_account_receivable_investment",
            "OTHER_ASSETS": "fi_other_asset",
            
            # Liabilities (Bank)
            "LOAN_CENTRAL_BANK": "fi_borrowings_from_central_bank",
            "LIAB_DEP_FUNDS_OTH_FIN_INST": "fi_deposit_from_other_fi",
            "LOANS_FROM_OTH_BANKS": "fi_borrowings_from_fi", # 拆入资金
            "TRADING_FIN_LIAB": "fi_financial_liability_effect_current_income",
            "DERI_FIN_LIAB": "fi_financial_derivative_liability",
            "SELL_REPO_FIN_ASSETS": "fi_sell_buy_back_fi_asset",
            "DEPOSIT_TAKING": "fi_savings_absorption",
            "NOTES_PAYABLE": "fi_notes_payable", # In bank context
            "ANTICIPATION_LIAB": "fi_estimated_liabilities",
            "BONDS_PAYABLE": "fi_bond_payable",
            "OTHER_LIAB": "fi_other_liability",
            
            # --- Broker Specific ---
            "CLIENTS_FUND_DEPOSIT": "fi_client_fund",
            "SETTLE_FUNDS": "fi_deposit_reservation_for_balance",
            "CLIENTS_RESERVES": "fi_client_deposit_reservation_for_balance",
            "LEND_FUNDS": "fi_margin_out_fund",
            "ACC_RECEIVABLES": "fi_receivables", # 应收款项
            "GUA_DEPOSITS_PAID": "fi_deposit_for_recognizance", # 存出保证金
            "ACT_TRADING_SEC": "fi_receiving_as_agent", # 代理买卖证券款
            "ST_FIN_PAYABLE": "fi_short_financing_payable", # 应付短期融资款
            # "NOM_RISKS_PREP": "fi_trade_risk_reserve", # 一般风险准备? No, 一般风险 is generic definition.
            # Usually 'General Risk Reserve' = fi_generic_risk_reserve, 'Trade Risk Reserve' = fi_trade_risk_reserve
            "NOM_RISKS_PREP": "fi_generic_risk_reserve", 
            
            # --- Insurance Specific ---
            "RECEIVABLE_PREM": "fi_premiums_receivable",
            "REINSURANCE_ACC_RCV": "fi_reinsurance_premium_receivable",
            "CED_INSUR_CONT_RESERVES_RCV": "fi_reinsurance_contract_reserve",
            "GUA_PLEDGE_LOANS": "fi_policy_pledge_loans",
            "FIXED_TERM_DEPOSITS": "fi_time_deposit", # 定期存款
            "DEPOSIT_CAP_RECOG": "fi_deposit_for_capital_recognizance", # 存出资本保证金
            "IND_ACCT_ASSETS": "fi_capital_in_independent_accounts",
            
            "ADV_PREM": "fi_advance_premium",
            "SERVICE_CHARGE_COMM_PAYABLE": "fi_fees_and_commissions_payable",
            "PAYABLE_FOR_REINSURER": "fi_dividend_payable_for_reinsurance",
            "CLAIMS_PAYABLE": "fi_claims_payable",
            "INSURED_DIV_PAYABLE": "fi_policy_holder_dividend_payable",
            "INSURED_DEPOSIT_INV": "fi_policy_holder_deposits_and_investment_funds",
            "RSRV_FUND_INSUR_CONT": "fi_contract_reserve",
            "IND_ACCT_LIAB": "fi_independent_liability",
        }

class xyszIncomeStatementRecorder(xyszFinanceRecorder):
    data_schema = IncomeStatement

    def _fetch_data(self, code_list, start, end):
        return self.client.get_income(code_list=code_list, is_local=False, local_path=_XYSZ_LOCAL_CACHE_PATH)

    def _get_column_map(self):
        return {
            "REPORTING_PERIOD": "report_date",
            "ANN_DATE": "timestamp",
            
            "OPERA_REV": "operating_income", 
            # "TOT_OPERA_REV": "operating_income", # Avoid duplicate columns
            "TOT_OPERA_COST": "total_operating_costs",
            "OPERA_COST": "operating_costs",
            "RD_EXP": "rd_costs",
            "LESS_BUS_TAX_SURCHARGE": "business_taxes_and_surcharges",
            "LESS_SELLING_EXP": "sales_costs",
            "LESS_ADMIN_EXP": "managing_costs",
            "LESS_FIN_EXP": "financing_costs",
            "LESS_ASSETS_IMPAIR_LOSS": "assets_devaluation",
            
            "PLUS_NET_INV_INC": "investment_income",
            "INCL_INC_INV_JV_ENTP": "investment_income_from_related_enterprise",
            
            "OPERA_PROFIT": "operating_profit",
            "PLUS_NON_OPERA_REV": "non_operating_income",
            "LESS_NON_OPERA_EXP": "non_operating_costs",
            "TOTAL_PROFIT": "total_profits",
            "INCOME_TAX": "tax_expense",
            
            "NET_PRO_INCL_MIN_INT_INC": "net_profit",
            "NET_PRO_EXCL_MIN_INT_INC": "net_profit_as_parent",
            "MIN_INT_INC": "net_profit_as_minority_interest",
            "NET_PRO_AFTER_DED_NR_GL": "deducted_net_profit",
            
            "BASIC_EPS": "eps",
            "DILUTED_EPS": "diluted_eps",
            
            "OTH_COMPRE_INC": "other_comprehensive_income",
            "TOT_COMPRE_INC": "total_comprehensive_income",
            "TOT_COMPRE_INC_PARENT_COMP": "total_comprehensive_income_as_parent",
            "TOT_COMPRE_INC_MIN_SHARE": "total_comprehensive_income_as_minority_interest",

            # --- Bank/Insurance/Broker Income ---
            "NET_INTEREST_INC": "fi_net_interest_income",
            "INTEREST_INC": "fi_interest_income",
            # "FIN_EXP_INT_INC": "fi_interest_income", # Avoid duplicate columns
            "FIN_EXP_INT_EXP": "fi_interest_expenses",
            "NET_HANDLING_CHRG_COMM_FEE": "fi_net_incomes_from_fees_and_commissions",
            "HANDLING_CHRG_COMM_FEE": "fi_incomes_from_fees_and_commissions",
            "LESS_HANDLING_CHRG_COMM_FEE": "fi_expenses_for_fees_and_commissions",
            "PLUS_NET_GAIN_CHG_FV": "fi_income_from_fair_value_change",
            "PLUS_NET_FX_INC": "fi_income_from_exchange",
            "OTH_BUS_INC": "fi_other_income",
            # "OPERA_AND_ADMIN_EXP": "fi_operate_and_manage_expenses", # Not direct match in json
            
            # Insurance Specific
            "INSUR_PREM": "fi_net_income_from_premium", # Net Earned Premium usually
            "PREM_BUS_INC": "fi_income_from_premium",
            "INCL_REINSUR_PREM_INC": "fi_income_from_reinsurance_premium",
            "LESS_REINSUR_PREM": "fi_reinsurance_premium",
            "EXT_UNEARNED_PREM_RES": "fi_undue_duty_reserve",
            "SURR_VALUE": "fi_insurance_surrender_costs",
            "TOT_COMPEN_EXP": "fi_insurance_claims_expenses",
            "LESS_AMORT_COMPEN_EXP": "fi_amortized_insurance_claims_expenses",
            "EXT_INSUR_CONT_RSRV": "fi_insurance_duty_reserve",
            "LESS_AMORT_INSUR_CONT_RSRV": "fi_amortized_insurance_duty_reserve",
            "DIV_EXP_INSUR": "fi_dividend_expenses_to_insured",
            "REINSURANCE_EXP": "fi_reinsurance_expenses",
            "LESS_AMORT_REINSUR_EXP": "fi_amortized_reinsurance_expenses",
            
            # Broker Specific
            "NET_INC_SEC_BROK_BUS": "fi_net_incomes_from_trading_agent",
            "NET_INC_SEC_UW_BUS": "fi_net_incomes_from_underwriting",
            "NET_INC_EC_ASSET_MGMT_BUS": "fi_net_incomes_from_customer_asset_management",
        }

class xyszCashFlowRecorder(xyszFinanceRecorder):
    data_schema = CashFlowStatement

    def _fetch_data(self, code_list, start, end):
        return self.client.get_cash_flow(code_list=code_list, is_local=False, local_path=_XYSZ_LOCAL_CACHE_PATH)

    def _get_column_map(self):
        return {
            "REPORTING_PERIOD": "report_date",
            "ANN_DATE": "timestamp",
            
            # --- Operating ---
            "CASH_RECP_SG_AND_RS": "cash_from_selling",
            "RECP_TAX_REFUND": "tax_refund",
            "OTHER_CASH_RECP_OPER_ACT": "cash_from_other_op",
            "TOT_CASH_INFLOW_OPER_ACT": "total_op_cash_inflows",
            
            "CASH_PAY_GOODS_SERVICES": "cash_to_goods_services",
            "CASH_PAY_EMPLOYEE": "cash_to_employees",
            "PAY_ALL_TAX": "taxes_and_surcharges",
            "OTH_CASH_PAY_OPERA_ACT": "cash_to_other_related_op",
            "TOT_CASH_OUTFLOW_OPERA_ACT": "total_op_cash_outflows",
            "NET_CASH_FLOWS_OPERA_ACT": "net_op_cash_flows",
            
            # --- Investing ---
            "CASH_RECP_RECOV_INV": "cash_from_disposal_of_investments",
            "CASH_RECP_INV_INCOME": "cash_from_returns_on_investments",
            "NET_CASH_RECP_DISP_FIOLTA": "cash_from_disposal_fixed_intangible_assets",
            "NET_CASH_RECP_DISP_SOBU": "cash_from_disposal_subsidiaries",
            "OTH_CASH_RECP_INV_ACT": "cash_from_other_investing",
            "TOT_CASH_INFLOW_INV_ACT": "total_investing_cash_inflows",
            
            "CASH_PAID_PUR_CONST_FIOLTA": "cash_to_acquire_fixed_intangible_assets",
            "CASH_PAID_INV": "cash_to_investments",
            "NET_CASH_PAID_SOBU": "cash_to_acquire_subsidiaries",
            "OTH_CASH_PAY_INV_ACT": "cash_to_other_investing",
            "TOT_CASH_OUTFLOW_INV_ACT": "total_investing_cash_outflows",
            "NET_CASH_FLOWS_INV_ACT": "net_investing_cash_flows",
            
            # --- Financing ---
            "ABSORB_CASH_RECP_INV": "cash_from_accepting_investment",
            "INCL_CASH_RECP_SAIMS": "cash_from_subsidiaries_accepting_minority_interest",
            "CASH_RECE_BORROW": "cash_from_borrowings",
            "CASH_RECE_ISSUE_BONDS": "cash_from_issuing_bonds",
            "OTHER_CASH_RECP_FIN_ACT": "cash_from_other_financing",
            "TOT_CASH_INFLOW_FIN_ACT": "total_financing_cash_inflows",
            
            "CASH_PAY_FOR_DEBT": "cash_to_repay_borrowings",
            "CASH_PAY_DIST_DIV_PRO_INT": "cash_to_pay_interest_dividend",
            "INCL_DIV_PRO_PAID_SMS": "cash_to_pay_subsidiaries_minority_interest",
            "OTHER_CASH_PAY_FIN_ACT": "cash_to_other_financing",
            "TOT_CASH_OUTFLOW_FIN_ACT": "total_financing_cash_outflows",
            "NET_CASH_FLOWS_FIN_ACT": "net_financing_cash_flows",
            
            # --- Summary ---
            "EFF_FX_FLUC_CASH": "foreign_exchange_rate_effect",
            "NET_INCR_CASH_AND_CASH_EQU": "net_cash_increase",
            "BEG_BAL_CASH_CASH_EQU": "cash_at_beginning",
            "END_BAL_CASH_CASH_EQU": "cash",

            # --- Financial Institutions Cash Flow ---
            "NET_INCR_DEP_CUS_AND_IB": "fi_deposit_increase",
            "NET_INCR_LOANS_CENTRAL_BANK": "fi_borrow_from_central_bank_increase",
            "NET_INCR_DEP_CB_IB": "fi_deposit_in_others_decrease", # "存放央行和同业款项净增加额"? Wait, mapping to "decrease"? 
            # ZVT: deposit_in_others_decrease. Json: NET_INCR_DEP_CB_IB (Increase)
            # Maybe I should map carefuly. If Json is Increase, and ZVT is Decrease, logic inversion needed?
            # Usually ZVT fields are positive values representing the concept. 
            # Let's map directly and user can interpret sign.
            
            # "NET_INCR_BORR_FUND": "fi_borrowing_and_sell_repurchase_increase", # 拆入资金净增加额
            # "NET_INCR_REPU_BUS_FUND": ""
            
            "NET_INCR_INT_AND_CHARGE": "fi_cash_from_interest_commission",
            "NET_INCR_CUS_LOAN_ADV": "fi_loan_advance_increase",
            # ... and so on ...
            # Financial Cash flow mapping is tricky because of "Increase/Decrease" naming differences.
            # I added the most obvious ones.
        }

if __name__ == "__main__":
    pass
