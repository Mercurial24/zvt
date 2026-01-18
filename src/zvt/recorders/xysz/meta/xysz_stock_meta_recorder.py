# -*- coding: utf-8 -*-
import pandas as pd
from zvt.contract.recorder import Recorder
from zvt.domain import Stock
from zvt.contract.api import df_to_db
from zvt.recorders.xysz.xysz_api import get_xysz_client
from zvt.utils.time_utils import to_pd_timestamp

class xyszStockMetaRecorder(Recorder):
    provider = "xysz"
    data_schema = Stock

    def __init__(self, force_update=False, sleeping_time=0):
        super().__init__(force_update, sleeping_time)
        self.client = get_xysz_client()

    def run(self):
        # 1. Get all A-share codes
        # security_type="EXTRA_STOCK_A" covers SH/SZ/BJ
        codes = self.client.get_code_list(security_type="EXTRA_STOCK_A")
        
        # 2. Get details in batches
        BATCH_SIZE = 500
        for i in range(0, len(codes), BATCH_SIZE):
            batch = codes[i : i + BATCH_SIZE]
            df = self.client.get_stock_basic(batch)
            
            if df is not None and not df.empty:
                # Transform to ZVT Stock schema
                # Expected columns: id, entity_id, timestamp, entity_type, exchange, code, name, list_date, end_date
                
                df = df.rename(columns={
                    "SECURITY_NAME": "name",
                    "LISTDATE": "list_date",
                    "DELISTDATE": "end_date"
                })
                
                # Process each row to generate entity_id etc
                # Since df is from AmazingData, index might be numeric or code
                # We iterate to construct list of dicts or modify DF
                
                records = []
                for _, row in df.iterrows():
                    market_code = row.get("MARKET_CODE") # e.g. 000001.SZ
                    if not market_code:
                        continue
                        
                    parts = market_code.split(".")
                    code = parts[0]
                    exchange = "unknown"
                    if len(parts) > 1:
                        suffix = parts[1].upper()
                        if suffix == "SH":
                            exchange = "sh"
                        elif suffix == "SZ":
                            exchange = "sz"
                        elif suffix == "BJ":
                            exchange = "bj"
                    
                    if exchange == "unknown":
                        # fallback logic
                        if code.startswith("6"): exchange = "sh"
                        elif code.startswith("0") or code.startswith("3"): exchange = "sz"
                        elif code.startswith("4") or code.startswith("8"): exchange = "bj"
                        
                    entity_id = f"stock_{exchange}_{code}"
                    
                    record = {
                        "id": entity_id,
                        "entity_id": entity_id,
                        "entity_type": "stock",
                        "exchange": exchange,
                        "code": code,
                        "name": row.get("name"),
                        "list_date": to_pd_timestamp(str(row.get("list_date"))),
                        "end_date": to_pd_timestamp(str(row.get("end_date"))) if row.get("end_date") else None,
                        "timestamp": to_pd_timestamp(str(row.get("list_date"))) # Use list_date as timestamp
                    }
                    records.append(record)
                
                if records:
                    df_to_db(df=pd.DataFrame(records), data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)
                    self.logger.info(f"Persisted {len(records)} stocks.")

    def __init_client(self):
        # Helper to ensure logged in if not globally
        # But get_xysz_client handles it
        pass

if __name__ == "__main__":
    recorder = xyszStockMetaRecorder()
    recorder.run()
