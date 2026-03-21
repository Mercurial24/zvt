# -*- coding: utf-8 -*-
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import Stock, DragonAndTiger, BigDealTrading, MarginTrading
from zvt.contract.api import df_to_db
from zvt.recorders.xysz.xysz_api import get_xysz_client
from zvt.utils.time_utils import to_pd_timestamp
import pandas as pd
import logging
from zvt import zvt_env
import os

# AmazingData library needs a local_path even for is_local=False
_XYSZ_LOCAL_CACHE_PATH = os.path.join(zvt_env["data_path"], "xysz", "cache") + os.sep
os.makedirs(_XYSZ_LOCAL_CACHE_PATH, exist_ok=True)

class xyszTradingRecorder(FixedCycleDataRecorder):
    provider = "xysz"
    entity_provider = "xysz"
    entity_schema = Stock
    data_schema = None 

    def __init__(self, force_update=False, sleeping_time=0, **kwargs):
        self.client = get_xysz_client()
        super().__init__(force_update, sleeping_time, **kwargs)

    def evaluate_start_end_size_timestamps(self, entity):
        start, end, size, timestamps = super().evaluate_start_end_size_timestamps(entity)
        if size > 0 and not self.real_time:
            now = pd.Timestamp.now()
            if now.hour < 16:
                # 未过 16 点，逻辑目标日期为昨天
                if start and start.date() >= (now.date() - pd.Timedelta(days=1)):
                    return start, end, 0, timestamps
        return start, end, size, timestamps

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
        
        if entity:
            df["entity_id"] = entity.id
            df["code"] = entity.code
        
        df["provider"] = self.provider
        
        # 3. Robust Date processing
        # TRADE_DATE/ANN_DATE -> timestamp
        if "timestamp" in df.columns:
            # Handle YYYYMMDD integer/float formats
            if pd.api.types.is_integer_dtype(df["timestamp"]) or pd.api.types.is_float_dtype(df["timestamp"]):
                df = df[df["timestamp"] > 19000101]
                if not df.empty:
                    df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int).astype(str), format="%Y%m%d", errors='coerce')
            else:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors='coerce')

        if df.empty:
            return None

        # Drop rows with invalid or missing timestamp
        df = df.dropna(subset=["timestamp"])
        
        if df.empty:
            return None

        # ID Generation: {entity_id}_{timestamp}
        if "id" not in df.columns and entity:
            df["id"] = df.apply(lambda row: f"{row['entity_id']}_{row['timestamp'].strftime('%Y-%m-%d')}", axis=1)

        schema_cols = [c.name for c in self.data_schema.__table__.columns]
        df_cols = [c for c in df.columns if c in schema_cols]
        return df[df_cols]

    def _get_column_map(self):
        return {}


class xyszDragonAndTigerRecorder(xyszTradingRecorder):
    data_schema = DragonAndTiger

    def _fetch_data(self, code, start, end):
        return self.client.get_long_hu_bang(code_list=[code], is_local=False, local_path=_XYSZ_LOCAL_CACHE_PATH)

    def _transform_df(self, df, entity):
        if df is not None and not df.empty:
            # Calculate net_in manually if not present
            cols_upper = {c.upper(): c for c in df.columns}
            if "BUY_AMOUNT" in cols_upper and "SELL_AMOUNT" in cols_upper:
                df["net_in"] = df[cols_upper["BUY_AMOUNT"]] - df[cols_upper["SELL_AMOUNT"]]
        return super()._transform_df(df, entity)

    def _get_column_map(self):
        return {
            "TRADE_DATE": "timestamp",
            "REASON_TYPE_NAME": "reason",
            "CHANGE_RANGE": "change_pct",
            "TOTAL_AMOUNT": "turnover",
            "TOTAL_VOLUME": "volume",
            # "NET_AMOUNT": "net_in", # Calculate manually in _transform_df
        }

class xyszBigDealTradingRecorder(xyszTradingRecorder):
    data_schema = BigDealTrading

    def _fetch_data(self, code, start, end):
        return self.client.get_block_trading(code_list=[code], is_local=False, local_path=_XYSZ_LOCAL_CACHE_PATH)
        
    def _get_column_map(self):
        return {
            "TRADE_DATE": "timestamp",
            "B_SHARE_PRICE": "price",
            "B_SHARE_VOLUME": "volume",
            "B_SHARE_AMOUNT": "turnover",
            "B_BUYER_NAME": "buy_broker",
            "B_SELLER_NAME": "sell_broker",
        }

class xyszMarginTradingRecorder(xyszTradingRecorder):
    data_schema = MarginTrading

    def _fetch_data(self, code, start, end):
        return self.client.get_margin_detail(code_list=[code], is_local=False, local_path=_XYSZ_LOCAL_CACHE_PATH)

    def _get_column_map(self):
        return {
            "TRADE_DATE": "timestamp",
            "BORROW_MONEY_BALANCE": "fin_value",
            "PURCH_WITH_BORROW_MONEY": "fin_buy_value",
            "REPAYMENT_OF_BORROW_MONEY": "fin_refund_value",
            "SEC_LENDING_BALANCE": "sec_value",
            "SALES_OF_BORROWED_SEC": "sec_sell_value",
            "REPAYMENT_OF_BORROW_SEC": "sec_refund_value",
            "MARGIN_TRADE_BALANCE": "fin_sec_value"
        }

if __name__ == "__main__":
    pass
