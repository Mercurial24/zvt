# -*- coding: utf-8 -*-
import sys
from pathlib import Path

# 添加项目路径
root_path = str(Path(__file__).resolve().parent.parent / 'src')
if root_path not in sys.path:
    sys.path.append(root_path)

from zvt.recorders.qmt.meta.qmt_stock_meta_recorder import QMTStockRecorder
from zvt.recorders.qmt.quotes.qmt_kdata_recorder import QMTStockKdataRecorder
from zvt.contract import AdjustType

def run_update():
    # 现在 QMTStockRecorder 已经自动支持远程 QMT
    print("正在从 QMT 获取全市场股票列表...")
    QMTStockRecorder(sleeping_time=0).run()
    
    # 2. 下载全市场数据
    # 现在 QMTStockKdataRecorder 已经自动支持远程 QMT
    print("正在下载日线数据 (前复权)...")
    recorder = QMTStockKdataRecorder(adjust_type=AdjustType.hfq, sleeping_time=0.2, ignore_failed=True)
    recorder.run()
    print("数据！")

if __name__ == "__main__":
    print("开始从 QMT 下载全市场日线数据...")
    run_update()
    print("数据下载完成！")
