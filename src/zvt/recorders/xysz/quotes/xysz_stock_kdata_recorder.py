# -*- coding: utf-8 -*-
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import Stock, StockKdataCommon
from zvt.recorders.xysz.xysz_api import AmazingDataClient, get_xysz_client
from zvt.contract.api import df_to_db
from zvt.api.kdata import get_kdata_schema
from zvt.contract import IntervalLevel, AdjustType
from zvt.utils.time_utils import to_pd_timestamp, to_date_time_str
import pandas as pd
import AmazingData as ad

class xyszStockKdataRecorder(FixedCycleDataRecorder):
    provider = 'xysz'
    data_schema = StockKdataCommon
    entity_provider = 'xysz'
    entity_schema = Stock

    def __init__(self,
                 force_update=True,
                 sleeping_time=0,
                 exchanges=None,
                 entity_id=None,
                 entity_ids=None,
                 code=None,
                 codes=None,
                 day_data=False,
                 entity_filters=None,
                 ignore_failed=True,
                 real_time=False,
                 fix_duplicate_way='ignore',
                 start_timestamp=None,
                 end_timestamp=None,
                 level=IntervalLevel.LEVEL_1DAY,
                 kdata_use_begin_time=False,
                 one_day_trading_minutes=24 * 60,
                 adjust_type=AdjustType.qfq,
                 return_unfinished=False):
                 
        level = IntervalLevel(level)
        self.adjust_type = AdjustType(adjust_type)
        self.entity_type = self.entity_schema.__name__.lower()
        self.client = get_xysz_client()

        # Schema for specific level/adjust_type
        self.data_schema = get_kdata_schema(entity_type=self.entity_type, level=level, adjust_type=self.adjust_type)
        
        super().__init__(force_update, sleeping_time, exchanges, entity_id, entity_ids, code, codes, day_data, entity_filters, ignore_failed, real_time, fix_duplicate_way, start_timestamp, end_timestamp, level, kdata_use_begin_time, one_day_trading_minutes, return_unfinished)

    def evaluate_start_end_size_timestamps(self, entity):
        start, end, size, timestamps = super().evaluate_start_end_size_timestamps(entity)
        if size > 0 and not self.real_time:
            now = pd.Timestamp.now()
            # 日线在库中已经有“今天”时，不需要再请求一次同日数据。
            # FixedCycleDataRecorder 的 size 计算包含起点，latest=today 会得到 size=1。
            if start and start.date() >= now.date():
                return start, end, 0, timestamps
                
            # 处理周末和周一早上的情况，避免这些时候再去重复请求
            if now.dayofweek == 5:  # 周六
                if start and start.date() >= (now.date() - pd.Timedelta(days=1)): # 最新的应该是周五
                    return start, end, 0, timestamps
            elif now.dayofweek == 6:  # 周日
                if start and start.date() >= (now.date() - pd.Timedelta(days=2)): # 最新的应该是周五
                    return start, end, 0, timestamps
            elif now.dayofweek == 0 and now.hour < 16:  # 周一 16点前
                if start and start.date() >= (now.date() - pd.Timedelta(days=3)): # 最新的应该是周五
                    return start, end, 0, timestamps
            else:
                if now.hour < 16: # 其他工作日 16点前
                    # 如果还没到 16 点，且最新数据已经是昨天或之后，则认为不需要更新
                    if start and start.date() >= (now.date() - pd.Timedelta(days=1)):
                        return start, end, 0, timestamps
                        
        return start, end, size, timestamps

    def record(self, entity, start, end, size, timestamps):
        if not start:
            start = to_pd_timestamp("2010-01-01")
        if not end:
            now = pd.Timestamp.now()
            if not self.real_time and now.hour < 16:
                end = now - pd.Timedelta(days=1)
            else:
                end = now
            
        start_int = int(start.strftime("%Y%m%d"))
        end_int = int(end.strftime("%Y%m%d"))
        
        xysz_code = f"{entity.code}.{entity.exchange.upper()}"
        
        period = ad.constant.Period.day.value
        if self.level == IntervalLevel.LEVEL_1WEEK:
            period = ad.constant.Period.week.value
        elif self.level == IntervalLevel.LEVEL_1MON:
            period = ad.constant.Period.month.value
            
        # Call API
        try:
            kline_dict = self.client.query_kline(
                code_list=[xysz_code],
                begin_date=start_int,
                end_date=end_int,
                period=period
            )
        except Exception as e:
            self.logger.error(f"Error querying kline for {xysz_code}: {e}")
            return None
        
        if not kline_dict or xysz_code not in kline_dict:
            return None
            
        df = kline_dict[xysz_code]
        if df is None or df.empty:
            return None
            
        # -------------------------------------------------------
        # Robustly find the date/timestamp column
        # -------------------------------------------------------
        # The AmazingData manual says: "index为日期（datetime）"
        if isinstance(df.index, pd.DatetimeIndex):
            df.index.name = 'timestamp'
            df = df.reset_index()
        else:
            df = df.reset_index()
            # If not in index, look for any known column name
            date_col = None
            for col in ['TRADE_DATE', 'trade_date', 'date', 'DATE', 'datetime', 'time', 'timestamp', 'kline_time']:
                if col in df.columns:
                    # Verify it has actual data
                    if not df[col].dropna().empty:
                        date_col = col
                        break
            
            if date_col:
                df = df.rename(columns={date_col: 'timestamp'})
            else:
                # Fallback: scan for YYYYMMDD integer columns (> 19000101)
                for col in df.columns:
                    if pd.api.types.is_integer_dtype(df[col]):
                        col_data = df[col].dropna()
                        if not col_data.empty and col_data.iloc[0] > 19000101:
                            df = df.rename(columns={col: 'timestamp'})
                            break
                else:
                    self.logger.error(f"Failed to find date column for {xysz_code}. Columns: {df.columns.tolist()}")
                    return None

        # Parse timestamp
        if pd.api.types.is_integer_dtype(df["timestamp"]) or pd.api.types.is_float_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int).astype(str), format="%Y%m%d")
        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Mapping OHLCV columns (case-insensitive)
        ohlcv_map = {
            "OPEN": "open", "OPEN_PRICE": "open",
            "HIGH": "high", "HIGH_PRICE": "high",
            "LOW": "low", "LOW_PRICE": "low",
            "CLOSE": "close", "CLOSE_PRICE": "close",
            "VOLUME": "volume", "VOL": "volume",
            "TURNOVER": "turnover", "AMOUNT": "turnover", # Map amount to turnover as per ZVT schema
            "CHANGE_PCT": "change_pct",
            "TURNOVER_RATE": "turnover_rate",
        }
        cols_upper = {c.upper(): c for c in df.columns}
        rename_dict = {}
        for k, v in ohlcv_map.items():
            if k in cols_upper and v not in df.columns:
                rename_dict[cols_upper[k]] = v
        if rename_dict:
            df = df.rename(columns=rename_dict)

        df["entity_id"] = entity.id
        df["provider"] = self.provider
        df["code"] = entity.code
        df["name"] = entity.name
        df["level"] = self.level.value
        
        # Generate ID: {entity_id}_{timestamp}
        df["id"] = df.apply(lambda row: f"{row['entity_id']}_{to_date_time_str(row['timestamp'])}", axis=1)

        self.logger.debug(
            f"API returned {len(df)} rows for {xysz_code}, "
            f"date range: {df['timestamp'].min()} ~ {df['timestamp'].max()}"
        )

        df_to_db(df=df, data_schema=self.data_schema, provider=self.provider,
                 force_update=self.force_update, session=self.session)
        
        return None

if __name__ == "__main__":
    # Test
    # recorder = xyszStockKdataRecorder(codes=['002594'])
    # recorder.run()
    pass
