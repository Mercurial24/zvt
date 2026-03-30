# -*- coding: utf-8 -*-
"""
Windows 行情推行脚本 (集成模拟与实盘模式 - 稳健版)
功能: 
1. 实时获取股票、指数、板块行情。
2. 同步推送到 Linux Redis 总线。
3. 支持非交易时间模拟模式 (Mock Mode)。
4. 【增强】具备断线重连和静默容错机制，不怕 Linux 宕机。
"""
import redis
import json
import time
import random
from datetime import datetime

# ================= 配置区 =================
LINUX_IP = "192.168.48.210"
REDIS_PWD = "zvt_quant_2026"
REDIS_PORT = 6379
CHANNEL = "zvt_market_quotes"

# ================= 核心推送逻辑 =================
class MarketPusher:
    def __init__(self):
        self.r = None
        self._connect()

    def _connect(self):
        """建立 Redis 连接，带重试机制"""
        while True:
            try:
                self.r = redis.Redis(host=LINUX_IP, port=REDIS_PORT, password=REDIS_PWD, db=0, socket_timeout=3)
                self.r.ping()
                print(f"[OK] 已成功连接到 Linux Redis: {LINUX_IP}")
                break
            except Exception as e:
                print(f"[WARN] 无法连接到 Redis ({e})，5 秒后重试...")
                time.sleep(5)

    def publish_quote(self, code, tick, entity_type="stock"):
        msg = {
            "entity_type": entity_type,
            "code": code,
            "p": tick.get("lastPrice", 0),
            "v": tick.get("volume", 0),
            "b1": tick.get("bidPrice", [0])[0] if tick.get("bidPrice") else 0,
            "a1": tick.get("askPrice", [0])[0] if tick.get("askPrice") else 0,
            "t": tick.get("timetag", datetime.now().strftime("%Y%m%d %H:%M:%S"))
        }
        try:
            self.r.publish(CHANNEL, json.dumps(msg))
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            print("[WARN] 链接丢失，正在尝试重连...")
            self._connect()
        except Exception as e:
            print(f"[ERR] 推送失败: {e}")

    def run_mock_mode(self):
        print(">>> 进入模拟模式 (Mock Mode)...")
        stocks = ["600000.SH", "000001.SZ", "300750.SZ"]
        indices = ["000300.SH", "000016.SH"]
        blocks = ["BK0001", "BK0447"]

        while True:
            for s in stocks:
                tick = {"lastPrice": random.uniform(10, 100), "volume": random.randint(100, 5000)}
                self.publish_quote(s, tick, entity_type="stock")
            for idx in indices:
                tick = {"lastPrice": random.uniform(3000, 5000), "volume": random.randint(10000, 50000)}
                self.publish_quote(idx, tick, entity_type="index")
            for b in blocks:
                tick = {"lastPrice": random.uniform(1000, 2000), "volume": random.randint(5000, 20000)}
                self.publish_quote(b, tick, entity_type="block")

            # print(f"[{datetime.now().strftime('%H:%M:%S')}] 已刷新")
            time.sleep(2)

    def run_real_mode(self):
        print(">>> 进入实盘模式 (Real QMT Mode)...")
        try:
            from xtquant import xtdata
        except ImportError:
            print("[ERR] Windows 未安装 xtquant，无法运行实盘模式！")
            return

        def on_data(data):
            for code, tick_data in data.items():
                tick = tick_data[-1] if isinstance(tick_data, list) else tick_data
                etype = "stock"
                if code.endswith(".SH") and code.startswith("000"): etype = "index"
                elif code.startswith("399"): etype = "index"
                elif code.startswith("BK"): etype = "block"
                self.publish_quote(code, tick, entity_type=etype)

        code_list = ["600000.SH", "000001.SZ", "000300.SH", "BK0001"]
        print(f"正在订阅标的: {code_list}")
        xtdata.subscribe_whole_quote(code_list, callback=on_data)
        xtdata.run()

if __name__ == "__main__":
    pusher = MarketPusher()
    IS_MOCK = True # 模拟开关
    if IS_MOCK:
        pusher.run_mock_mode()
    else:
        pusher.run_real_mode()
