# -*- coding: utf-8 -*-
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import Stock, TopTenHolder, TopTenTradableHolder, InstitutionalInvestorHolder, HolderNum
from zvt.utils.time_utils import to_pd_timestamp
from zvt.contract.api import df_to_db
from zvt.recorders.xysz.xysz_api import get_xysz_client
import pandas as pd
import logging
from zvt import zvt_env
import os

# AmazingData library needs a local_path even for is_local=False
_XYSZ_LOCAL_CACHE_PATH = os.path.join(zvt_env["data_path"], "xysz", "cache") + os.sep
os.makedirs(_XYSZ_LOCAL_CACHE_PATH, exist_ok=True)

class xyszHolderRecorder(FixedCycleDataRecorder):
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
        # ANN_DATE/ANN_DT -> timestamp, HOLDER_ENDDATE/REPORTING_PERIOD -> report_date
        date_cols = ["timestamp", "report_date"]
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
        if "timestamp" in df.columns and "report_date" in df.columns:
            df = df.dropna(subset=["timestamp", "report_date"])
        
        if df.empty:
            return None
                
        if "report_date" in df.columns:
            df["report_period"] = df["report_date"].dt.strftime("%Y-%m-%d")

        # Generate ID
        if "id" not in df.columns:
             if "holder_name" in df.columns and "report_period" in df.columns:
                  df["id"] = df.apply(lambda row: f"{row['entity_id']}_{row['report_period']}_{row['holder_name']}", axis=1)
             elif "report_period" in df.columns:
                  df["id"] = df.apply(lambda row: f"{row['entity_id']}_{row['report_period']}", axis=1)

        schema_cols = [c.name for c in self.data_schema.__table__.columns]
        df_cols = [c for c in df.columns if c in schema_cols]
        return df[df_cols]

    def _get_column_map(self):
        return {}


class xyszTopTenHolderRecorder(xyszHolderRecorder):
    data_schema = TopTenHolder

    def _fetch_data(self, code, start, end):
        df = self.client.get_share_holder(code_list=[code], is_local=False, local_path=_XYSZ_LOCAL_CACHE_PATH)
        if df is not None and not df.empty:
            # Filter for Top 10 Holders (HOLDER_TYPE = 10)
            return df[df["HOLDER_TYPE"] == 10]
        return df

    def _get_column_map(self):
        return {
            "HOLDER_ENDDATE": "report_date",
            "ANN_DATE": "timestamp",
            "HOLDER_NAME": "holder_name",
            "HOLDER_QUANTITY": "shareholding_numbers",
            "HOLDER_PCT": "shareholding_ratio",
            # "CHANGE_RATIO": "change_ratio", # Assuming exists or calculated
            # "CHANGE_QUANTITY": "change"
        }

class xyszTopTenTradableHolderRecorder(xyszHolderRecorder):
    data_schema = TopTenTradableHolder

    def _fetch_data(self, code, start, end):
        df = self.client.get_share_holder(code_list=[code], is_local=False, local_path=_XYSZ_LOCAL_CACHE_PATH)
        if df is not None and not df.empty:
            # Filter for Top 10 Tradable Holders (HOLDER_TYPE = 20)
            return df[df["HOLDER_TYPE"] == 20]
        return df

    def _get_column_map(self):
        return {
            "HOLDER_ENDDATE": "report_date",
            "ANN_DATE": "timestamp",
            "HOLDER_NAME": "holder_name",
            "HOLDER_QUANTITY": "shareholding_numbers",
            "HOLDER_PCT": "shareholding_ratio",
        }


class xyszHolderNumRecorder(xyszHolderRecorder):
    """股东户数：星河数智 get_holder_num，按统计截止日一条一条入库。"""

    data_schema = HolderNum

    def _fetch_data(self, code, start, end):
        start_int = int(start.strftime("%Y%m%d")) if start else None
        end_int = int(end.strftime("%Y%m%d")) if end else None
        kwargs = dict(code_list=[code], is_local=False, local_path=_XYSZ_LOCAL_CACHE_PATH)
        if start_int is not None:
            kwargs["begin_date"] = start_int
        if end_int is not None:
            kwargs["end_date"] = end_int
        return self.client.get_holder_num(**kwargs)

    def _get_column_map(self):
        return {
            "ANN_DT": "timestamp",
            "HOLDER_ENDDATE": "report_date",
            "HOLDER_NUM": "holder_num",
            "HOLDER_TOTAL_NUM": "total_holder_num",
        }

if __name__ == "__main__":
    pass
