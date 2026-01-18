# -*- coding: utf-8 -*-
"""
获取历史 Tick 数据示例脚本 - Windows 端
前置要求：运行前请确保当前环境已安装 xtquant，并且 QMT 客户端处于运行和登录状态。
步骤：
1. 下载指定时间段的 Tick 数据到本地硬盘
2. 从本地读取最近的 N 条 Tick 数据进行展示
"""
import sys
import pandas as pd
from xtquant import xtdata

def download_tick_data(stock_list, start_time, end_time):
    """
    增量下载指定品种的 Tick 数据到本地
    """
    print(f"[{start_time} - {end_time}] 开始下载股票 {stock_list} 的 Tick 数据...")
    for code in stock_list:
        xtdata.download_history_data(
            code,
            period='tick',
            start_time=start_time,
            end_time=end_time,
            incrementally=True  # 增量下载，避免重复下载已有数据
        )
    print("历史数据下载完成。")

def read_and_print_tick_data(stock_list, count=10):
    """
    读取并打印本地缓存的最新的 N 条 Tick 数据
    """
    print(f"\n开始读取本地缓存，提取最新 {count} 条 Tick 数据...")
    try:
        # 获取本地数据，不指定 start_time 和 end_time，而是使用 count 获取最新数据
        data = xtdata.get_market_data_ex(
            field_list=[],  # 填空表示获取所有可用字段 (对于 tick 数据通常有效)
            stock_list=stock_list,
            period='tick',
            start_time='',
            end_time='',
            count=count
        )
        
        if not data:
            print("未获取到任何数据，请检查是否已正确下载或标的是否有行情。")
            return
            
        for code in stock_list:
            if code in data:
                print(f"\n============== [{code}] 最新 {count} 条数据 ==============")
                df = data[code]
                
                # 如果返回格式是 DataFrame，直接使用 pandas 的自带打印方式
                if isinstance(df, pd.DataFrame):
                    # 为了排版美观，可以调整显示的列数和宽度
                    pd.set_option('display.max_columns', None)
                    pd.set_option('display.width', 1000)
                    print(df.tail(count))
                else:
                    # 如果不是 DataFrame (有些旧版本 QMT 返回的是字典列表)
                    for item in df[-count:]:
                        print(item)
            else:
                 print(f"\n缺少 [{code}] 的数据，请先执行下载。")

    except Exception as e:
        import traceback
        print("调用失败，异常信息如下:")
        traceback.print_exc()

def main():
    print(f"当前 Python 版本: {sys.version}")
    
    # === 参数配置区 ===
    # 待获取 tick 的股票代码列表
    code_list = ["000786.SZ"]
    
    # 想要下载的时间范围 (包含开始不包含结束，需与目前 QMT 实际交易日匹配)
    # 因为只需要试一下，我帮您设定一个 1 小时级别左右的小时间范围，格式: YYYYMMDDHHMMSS
    # 昨天 (2026/02/24) 尾盘一小时 14:00:00 - 15:00:00 的 Tick 数据
    target_start_time = "20260224140000"
    target_end_time = "20260224150000"
    
    # 需要在屏幕上打印出来的数量
    display_count = 10
    
    # === 执行流程 ===
    # 1. 触发后台下载逻辑 (建议每天开盘前或盘后执行一次)
    download_tick_data(code_list, target_start_time, target_end_time)
    
    # 2. 读取并在终端展示
    read_and_print_tick_data(code_list, count=display_count)

if __name__ == "__main__":
    main()
