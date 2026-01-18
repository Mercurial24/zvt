# -*- coding: utf-8 -*-
"""
My First ZVT Strategy Demo: Moving Average Crossover (MA5 vs MA10)
A simple "Gold Cross" (buy) and "Death Cross" (sell) strategy.
"""
import os
import sys

# Ensure the script can find the zvt source code if run from outside the root
project_root = "/data/code/zvt"
if project_root not in sys.path:
    sys.path.insert(0, os.path.join(project_root, 'src'))

from zvt.contract import IntervalLevel, AdjustType
from zvt.factors.ma.ma_factor import CrossMaFactor
from zvt.trader.trader import StockTrader

class SimpleMaTrader(StockTrader):
    """
    一个简单的均线策略交易器：
    当 5 日线向上穿过 10 日线时全仓买入。
    当 5 日线向下穿过 10 日线时清仓卖出。
    """
    def init_factors(
        self, entity_ids, entity_schema, exchanges, codes, start_timestamp, end_timestamp, adjust_type=None
    ):
        # 核心逻辑：使用 ZVT 内置的 CrossMaFactor 产生信号
        return [
            CrossMaFactor(
                entity_ids=entity_ids,
                entity_schema=entity_schema,
                exchanges=exchanges,
                codes=codes,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                windows=[5, 10],   # 设置两个移动平均窗口：5天和10天
                need_persist=False, # 仅用于本地回测，暂不需要保存到数据库
                provider="xysz",
                entity_provider="xysz",
                adjust_type=AdjustType.hfq    # xysz 支持动态计算或者读取后复权数据
            )
        ]

if __name__ == "__main__":
    # 配置回测参数
    # 这里使用您确定有数据的股票：华发股份 (600325)
    target_code = "000001" 
    
    print(f"🚀 开始回测策略: 5/10日均线金叉死时 - 标的: {target_code}")
    
    trader = SimpleMaTrader(
        codes=[target_code],            # 标的代码列表
        
        level=IntervalLevel.LEVEL_1DAY, # 交易级别：日线
        start_timestamp="2023-01-01",  # 回测开始时间
        end_timestamp="2024-12-31",    # 回测结束时间
        trader_name=f"ma_demo_{target_code}_hfq",
        provider="xysz",
        adjust_type=AdjustType.hfq
    )
    
    # 运行策略
    trader.run()
    
    print("\n✅ 回测完成！")
    print(f"你的回测记录保存在: {os.path.join(os.path.expanduser('~'), 'zvt-home', 'data', 'zvt', 'zvt_trader_info.db')}")
