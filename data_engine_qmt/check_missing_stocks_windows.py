# -*- coding: utf-8 -*-
"""
在 Windows 端运行此脚本，测试 QMT 是否能正常获取这几只全新股票的数据
请确保已经打开 QMT/MiniQMT 并且登录成功。
运行方式：在 Windows conda 环境下执行 `python check_missing_stocks_windows.py`
"""
import time
from xtquant import xtdata

def main():
    missing_codes = ['920168.BJ', '920187.BJ', '301680.SZ']
    
    # 获取所有的板块行情以确认是否在内
    print("\n1. 测试各大板块列表中是否包含这些股票:")
    all_stocks = []
    for sector in ["沪深A股", "京市A股", "深市A股", "中小企业板", "创业板"]:
        stocks = xtdata.get_stock_list_in_sector(sector)
        if stocks:
            all_stocks.extend(stocks)
    all_stocks = set(all_stocks)
    
    for code in missing_codes:
        print(f"  [{code}] 在 QMT 股票列表中: {'是' if code in all_stocks else '否'}")
        
    print("\n2. 测试基础信息 (get_instrument_detail):")
    for code in missing_codes:
        detail = xtdata.get_instrument_detail(code, True)
        if detail:
            print(f"  [{code}] 获取成功 -> {detail.get('InstrumentName')}, 上市日: {detail.get('OpenDate')}")
        else:
            print(f"  [{code}] 获取基础信息失败或为空")
            
    print("\n3. 尝试强制下载历史日 K 线数据 (download_history_data)...")
    for code in missing_codes:
        try:
            print(f"  正在请求下载 [{code}] 的 1d 数据...")
            xtdata.download_history_data(stock_code=code, period='1d')
            print(f"  [{code}] 请求发送完成。")
        except Exception as e:
            print(f"  [{code}] 请求下载时抛出异常: {e}")
            
    print("  等待 3 秒钟让 QMT 客户端落盘...")
    time.sleep(3)
    
    print("\n4. 测试读取历史日 K 线数据:")
    for code in missing_codes:
        try:
            records = xtdata.get_market_data(
                stock_list=[code],
                period='1d',
                start_time='',
                end_time='',
                dividend_type='none',
                fill_data=False
            )
            # 因为 get_market_data 结构比较特殊，按列切片
            if records and code in records and records[code] is not None:
                # pandas 的行列翻转
                df = records[code].T
                if not df.empty and not df.isna().all().all():
                    print(f"  [{code}] 成功拉取到 {len(df)} 条 K 线数据。最新样例:")
                    print(df.tail(2))
                else:
                    print(f"  [{code}] 读取结果为空 DataFrame (无行情)")
            else:
                print(f"  [{code}] 获取历史数据返回为空字典或 None")
        except Exception as e:
            print(f"  [{code}] 获取历史行情时报错: {e}")

if __name__ == '__main__':
    print("=== 开始排查 QMT 最新股票状况 ===")
    main()
