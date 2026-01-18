# -*- coding: utf-8 -*-
"""
QMT 本地实时行情订阅脚本 - Windows 端
直接使用 xtquant 库的异步回调机制，接收实时行情推送。
运行前请确保当前环境（Windows）已正确安装了 xtquant 并且 QMT 客户端在后台运行。
"""
from xtquant import xtdata

def on_data(data):
    """
    行情回调函数
    当有新的行情推送过来时，会自动调用这个函数
    data 是一个字典，结构类似于:
    {
        '600000.SH': [{
            'timetag': '20260225 10:00:00',
            'lastPrice': 10.5,
            'volume': 12345,
            'bidPrice': [10.49, 10.48, ...],
            'askPrice': [10.51, 10.52, ...],
            ...
        }],
        ...
    }
    注意：在 subscribe_quote 的 period='tick' 情况下，值是一个列表。
    """
    for code, ticks in data.items():
        # ticks 可能是列表也可能是字典，取决于订阅方式和全推/部分推送的区别
        # 这里为了兼容，如果是列表则取最新的一个 tick
        if isinstance(ticks, list):
            for tick in ticks:
                _print_tick(code, tick)
        elif isinstance(ticks, dict):
             _print_tick(code, ticks)

def _print_tick(code, tick):
    price = tick.get("lastPrice", 0)
    volume = tick.get("volume", 0)
    bidPrice = tick.get("bidPrice", [])
    askPrice = tick.get("askPrice", [])
    
    b1 = bidPrice[0] if bidPrice else 0
    a1 = askPrice[0] if askPrice else 0
    timetag = tick.get("timetag", "")
    
    print(f"[{timetag}] {code} 最新价: {price:.2f} | 买一: {b1:.2f} 卖一: {a1:.2f} | 成交量: {volume}")

def main():
    code_list = ["600000.SH", "000001.SZ"]
    print(f"开始订阅标的实时行情: {code_list}")
    
    # 方法一：对特定股票逐个订阅 tick 周期行情，传入 callback
    # for code in code_list:
    #     # 订阅 tick 数据，当有新数据时会触发 on_data 回调
    #     seq = xtdata.subscribe_quote(code, period='tick', callback=on_data)
    #     print(f"标的 {code} 订阅成功, 订阅号(seq): {seq}")
    
    # 方法二：订阅全推行情 (注意参数必须是代码列表，如需要上交所深交所所有股票，可能需要拉取列表然后再订阅)
    seq = xtdata.subscribe_whole_quote(code_list, callback=on_data)
    
    print("开始接收行情推送... 请保持程序运行，按 Ctrl+C 停止")
    
    try:
        # 阻塞主线程，保持程序不退出，使回调函数能够持续工作
        xtdata.run()
    except KeyboardInterrupt:
        print("\n监测到退出指令，停止接收行情。")

if __name__ == "__main__":
    main()
