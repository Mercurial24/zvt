# -*- coding: utf-8 -*-
import sys
from pathlib import Path

# 添加项目路径
root_path = str(Path(__file__).resolve().parent.parent / 'src')
if root_path not in sys.path:
    sys.path.append(root_path)

from zvt.recorders.qmt.meta.qmt_stock_meta_recorder import QMTStockRecorder
from zvt.recorders.qmt.quotes.qmt_kdata_recorder import QMTStockKdataRecorder
from zvt.recorders.qmt.index.qmt_index_recorder import QmtIndexRecorder
from zvt.recorders.qmt.finance.qmt_finance_recorder import (
    QmtBalanceSheetRecorder,
    QmtIncomeStatementRecorder,
    QmtCashFlowRecorder,
    QmtValuationRecorder,
)
from zvt.consts import IMPORTANT_INDEX
from zvt.contract import AdjustType

def run_update():
    # 1. 获取全市场股票及指数列表
    print("正在从 QMT 获取全市场股票列表...")
    QMTStockRecorder(sleeping_time=0).run()
    
    # 2. 同步指数日线数据
    print("正在同步重要指数日线数据...")
    QmtIndexRecorder(codes=IMPORTANT_INDEX, level='1d', sleeping_time=0).run()

    # 3. 同步财务数据
    print("正在同步财务报表数据...")
    QmtBalanceSheetRecorder(sleeping_time=0.1).run()
    QmtIncomeStatementRecorder(sleeping_time=0.1).run()
    QmtCashFlowRecorder(sleeping_time=0.1).run()
    # 4. 下载全市场股票数据
    # 现在 QMTStockKdataRecorder 已经自动支持远程 QMT
    print("正在下载日线数据 (不复权)...")
    recorder = QMTStockKdataRecorder(adjust_type=AdjustType.bfq, sleeping_time=0.2, ignore_failed=True)
    recorder.run()

    print("正在下载日线数据 (后复权)...")
    hfq_recorder = QMTStockKdataRecorder(adjust_type=AdjustType.hfq, sleeping_time=0.2, ignore_failed=True)
    hfq_recorder.run()

    print("正在计算估值数据...")
    QmtValuationRecorder(sleeping_time=0).run()
    print("数据下载完成！")

if __name__ == "__main__":
    print("开始从 QMT 下载全市场日线数据...")
    run_update()
    print("数据下载完成！")
