# -*- coding: utf-8 -*-
"""
Redis 实时行情消费者 (Linux 端 - 增强版)
功能:
1. 监听 Redis 频道，识别 Stock, Index, Block 不同品类。
2. 采用彩色打印和分类路径处理行情。
3. 预留 ZVT 选股层 (Selector) 触发接口。
"""
import redis
import json
import logging
import sys
from datetime import datetime

# 设置日志格式
class CustomFormatter(logging.Formatter):
    """为不同实体类型提供不同的日志颜色"""
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    blue = "\x1b[34;20m"
    green = "\x1b[32;20m"
    reset = "\x1b[0m"
    format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    FORMATS = {
        'INDEX': blue + format_str + reset,
        'BLOCK': yellow + format_str + reset,
        'STOCK': green + format_str + reset,
        logging.ERROR: red + format_str + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.msg.split(']')[0][1:], self.grey + self.format_str + self.reset) if ']' in record.msg else self.grey + self.format_str + self.reset
        formatter = logging.Formatter(log_fmt, datefmt='%H:%M:%S')
        return formatter.format(record)

logger = logging.getLogger("ZVT-Market")
handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# 配置
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_PWD = 'zvt_quant_2026'
CHANNEL = 'zvt_market_quotes'

def process_index(data):
    """处理指数行情：如大盘风险预警"""
    logger.info(f"[INDEX] {data['code']} 走势: {data['p']:.2f}")

def process_block(data):
    """处理板块行情：如热点切换监控"""
    logger.info(f"[BLOCK] {data['code']} 强度: {data['p']:.2f}")

def process_stock(data):
    """处理股票行情：如异动抓捕逻辑"""
    logger.info(f"[STOCK] {data['code']} 最新: {data['p']:.2f} | 买一: {data['b1']:.2f} 卖一: {data['a1']:.2f}")

def main():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD, db=0)
        r.ping()
        logger.info("[INFO] 已成功连接 Redis，正在等待行情流量...")
    except Exception as e:
        logger.error(f"[ERROR] Redis 连接失败: {e}")
        sys.exit(1)

    p = r.pubsub()
    p.subscribe(CHANNEL)

    try:
        for message in p.listen():
            if message['type'] == 'message':
                try:
                    data = json.loads(message['data'])
                    etype = data.get('entity_type', 'stock').upper()
                    
                    if etype == 'INDEX':
                        process_index(data)
                    elif etype == 'BLOCK':
                        process_block(data)
                    else:
                        process_stock(data)
                        
                except Exception as e:
                    logger.error(f"[ERROR] 数据解析异常: {e}")
    except KeyboardInterrupt:
        logger.info("[INFO] 接收任务已手动停止。")
    finally:
        p.unsubscribe(CHANNEL)

if __name__ == "__main__":
    main()
