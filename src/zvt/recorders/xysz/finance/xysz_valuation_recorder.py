# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import Stock, StockValuation, IncomeStatement, Stock1dKdata, BalanceSheet
from zvt.contract.api import get_db_session, df_to_db
import logging

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

        # 1. 获取行情数据
        kdata_df = Stock1dKdata.query_data(
            entity_id=entity_id, 
            provider="xysz",
            start_timestamp=start,
            end_timestamp=end,
            columns=['timestamp', 'close']
        )
        if kdata_df is None or kdata_df.empty:
            self.logger.warning(f"No kdata for {entity_id}")
            return

        # 2. 获取利润表数据
        income_df = IncomeStatement.query_data(
            entity_id=entity_id,
            provider="xysz",
            columns=['report_period', 'net_profit_as_parent']
        )
        if income_df is None or income_df.empty:
            self.logger.warning(f"No income data for {entity_id}")
            return

        # 3. 获取资产负债表数据
        balance_df = BalanceSheet.query_data(
            entity_id=entity_id,
            provider="xysz",
            columns=['report_period', 'capital']
        )
        if balance_df is None or balance_df.empty:
            self.logger.warning(f"No balance sheet data for {entity_id}")
            return

        # --- 数据预处理 ---
        # 避免 reset_index 带来的命名冲突，直接创建辅助列
        kdata_df = kdata_df.copy()
        income_df = income_df.copy()
        balance_df = balance_df.copy()

        # ZVT query_data 返回的 index 通常包含 timestamp 或 entity_id, timestamp
        if 'timestamp' in kdata_df.columns:
            kdata_df['ts_merge'] = pd.to_datetime(kdata_df['timestamp'])
        else:
            kdata_df['ts_merge'] = pd.to_datetime(kdata_df.index.get_level_values('timestamp'))

        income_df['ts_merge'] = pd.to_datetime(income_df['report_period'])
        balance_df['ts_merge'] = pd.to_datetime(balance_df['report_period'])
        
        # 排序
        kdata_df = kdata_df.sort_values('ts_merge')
        income_df = income_df.sort_values('ts_merge')
        balance_df = balance_df.sort_values('ts_merge')

        # 第一次 merge
        df_merged = pd.merge_asof(
            kdata_df, 
            income_df[['ts_merge', 'net_profit_as_parent']], 
            on='ts_merge', 
            direction='backward'
        )
        
        # 第二次 merge
        df_merged = pd.merge_asof(
            df_merged, 
            balance_df[['ts_merge', 'capital']], 
            on='ts_merge', 
            direction='backward'
        )
        
        # 结果计算
        df_merged = df_merged.dropna(subset=['net_profit_as_parent', 'capital'])
        if df_merged.empty:
            self.logger.warning(f"Merged valuation data is empty for {entity_id}")
            return
            
        df_merged = df_merged[df_merged['capital'] > 0]
        # PE = Price / (Profit / Capital)
        df_merged['pe_ttm'] = df_merged['close'] / (df_merged['net_profit_as_parent'] / df_merged['capital'])
        df_merged['market_cap'] = df_merged['close'] * df_merged['capital']
        
        # 清理异常值
        df_merged['pe_ttm'] = df_merged['pe_ttm'].apply(lambda x: round(x, 2) if np.isfinite(x) else 0)

        val_list = []
        for _, row in df_merged.iterrows():
            val_list.append({
                'id': f"{entity_id}_{row['ts_merge'].strftime('%Y-%m-%d')}",
                'entity_id': entity_id,
                'timestamp': row['ts_merge'],
                'code': entity.code,
                'name': entity.name,
                'pe_ttm': row['pe_ttm'],
                'market_cap': row['market_cap'],
                'capitalization': row['capital'],
                'provider': self.provider
            })

        if val_list:
            df = pd.DataFrame(val_list)
            df_to_db(df=df, data_schema=StockValuation, provider=self.provider, force_update=self.force_update)
            self.logger.info(f"Calculated {len(val_list)} valuation records for {entity_id}")

if __name__ == "__main__":
    recorder = xyszValuationRecorder(codes=['000001', '000736', '601599'])
    recorder.run()
