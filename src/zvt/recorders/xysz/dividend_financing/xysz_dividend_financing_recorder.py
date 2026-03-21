# -*- coding: utf-8 -*-
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.contract.api import df_to_db
from zvt.recorders.xysz.xysz_api import get_xysz_client
from zvt.domain import Stock, DividendDetail, RightsIssueDetail, SpoDetail
import pandas as pd
import logging
from zvt import zvt_env
import os

# AmazingData library needs a local_path even for is_local=False
_XYSZ_LOCAL_CACHE_PATH = os.path.join(zvt_env["data_path"], "xysz", "cache") + os.sep
os.makedirs(_XYSZ_LOCAL_CACHE_PATH, exist_ok=True)

class xyszDividendFinancingRecorder(FixedCycleDataRecorder):
    provider = "xysz"
    entity_provider = "xysz" 
    entity_schema = Stock
    data_schema = None 

    def __init__(self, force_update=False, sleeping_time=0, **kwargs):
        self.client = get_xysz_client()
        super().__init__(force_update, sleeping_time, **kwargs)

    def record(self, entity, start, end, size, timestamps):
        xysz_code = f"{entity.code}.{entity.exchange.upper()}"
        
        try:
            df = self._fetch_data(xysz_code, start, end)
        except Exception as e:
            self.logger.error(f"Error fetching data for {xysz_code}: {e}")
            return None
            
        if df is None or df.empty:
            return None
        
        # Transform and Map Columns
        df = self._transform_df(df, entity)
        
        if df is not None and not df.empty:
            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)

    def _fetch_data(self, code, start, end):
        raise NotImplementedError

    def _transform_df(self, df, entity):
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
        date_cols = ["timestamp", "announce_date", "record_date", "dividend_date"]
        for col in date_cols:
            if col in df.columns:
                # Handle YYYYMMDD integer/float formats
                if pd.api.types.is_integer_dtype(df[col]) or pd.api.types.is_float_dtype(df[col]):
                    df = df[df[col] > 19000101]
                    if df.empty: break
                    df[col] = pd.to_datetime(df[col].astype(int).astype(str), format="%Y%m%d", errors='coerce')
                else:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

        if df.empty:
            return None

        # Drop rows with invalid or missing crucial dates
        if "timestamp" in df.columns:
            df = df.dropna(subset=["timestamp"])
        
        if df.empty:
            return None

        # ID gen: {entity_id}_{timestamp}
        if "id" not in df.columns and "timestamp" in df.columns:
             df["id"] = df.apply(lambda row: f"{row['entity_id']}_{row['timestamp'].strftime('%Y-%m-%d')}", axis=1)

        schema_cols = [c.name for c in self.data_schema.__table__.columns]
        df_cols = [c for c in df.columns if c in schema_cols]
        return df[df_cols]

    def _get_column_map(self):
        return {}


class xyszDividendDetailRecorder(xyszDividendFinancingRecorder):
    data_schema = DividendDetail

    def _fetch_data(self, code, start, end):
        return self.client.get_dividend(code_list=[code], is_local=False, local_path=_XYSZ_LOCAL_CACHE_PATH)

    def _transform_df(self, df, entity):
        if "ANN_DATE" in df.columns:
            df["announce_date"] = df["ANN_DATE"]
            df["timestamp"] = df["ANN_DATE"]
        return super()._transform_df(df, entity)

    def _get_column_map(self):
        # Doc: MARKET_CODE, DIV_PROGRESS, DVD_PER_SHARE_STK, DATE_EQY_RECORD, DATE_EX, DATE_DVD_PAYOUT
        return {
            "DATE_EQY_RECORD": "record_date", # 股权登记日
            "DATE_EX": "dividend_date", # 除权除息日
            "DVD_PER_SHARE_STK": "dividend", 
        }

class xyszRightsIssueDetailRecorder(xyszDividendFinancingRecorder):
    data_schema = RightsIssueDetail

    def _fetch_data(self, code, start, end):
        return self.client.get_right_issue(code_list=[code], is_local=False, local_path=_XYSZ_LOCAL_CACHE_PATH)

    def _get_column_map(self):
        return {
             "ANN_DATE": "timestamp",
             "RATIO": "rights_issues", # 配股比例
             "PRICE": "rights_issue_price", 
             "COLLECTION_FUND": "rights_raising_fund"
        }

if __name__ == "__main__":
    pass
