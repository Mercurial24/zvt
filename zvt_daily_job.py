# -*- coding: utf-8 -*-
"""
ZVT 每日数据自动更新与数据湖同步脚本

阶段 1：更新 Parquet 数据湖（可选，由 daily_update 等脚本写入）。
阶段 2：先从数据湖 Parquet 导入能导入的财务等数据到 ZVT，再对数据湖不存在或仍缺的数据用 API Recorder 拉取，
        避免重复下载和重复缓存 h5。
"""
import gc
import logging
import os
import sys
import time
import traceback
import argparse
import subprocess
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from zvt.recorders.xysz.meta.xysz_stock_meta_recorder import xyszStockMetaRecorder
from zvt.recorders.xysz.meta.xysz_industry_recorder import xyszIndustryBlockRecorder, xyszIndustryBlockStockRecorder
from zvt.recorders.akshare.meta.akshare_block_recorder import AkshareBlockRecorder
from zvt.recorders.xysz.quotes.xysz_stock_kdata_recorder import xyszStockKdataRecorder
from zvt.recorders.xysz.quotes.xysz_stock_adj_factor_recorder import xyszStockAdjFactorRecorder
from zvt.recorders.xysz.finance.xysz_finance_recorder import (
    xyszBalanceSheetRecorder, 
    xyszIncomeStatementRecorder, 
    xyszCashFlowRecorder
)
from zvt.recorders.xysz.finance.xysz_valuation_recorder import xyszValuationRecorder
from zvt.recorders.akshare.macro.china_money_supply_recorder import ChinaMoneySupplyRecorder
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
from zvt.informer.wechat_webhook import WechatWebhookInformer
from scripts.compute_xysz_hfq_kdata import compute_and_save_xysz_hfq

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ZvtDailyJob")


def _format_elapsed(seconds: float) -> str:
    """将秒数格式化为人类可读的耗时字符串"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m{secs:.0f}s"


def run_legacy_daily_update():
    """运行旧版的 daily_update.py 以更新 Parquet 数据湖，实时流式打印子脚本输出"""
    legacy_script = "/data/code/my_quant_begin/data_engine_xysz/daily_update.py"
    logger.info(f"正在执行旧版数据湖增量更新: {legacy_script}")

    # 注意：不要无界 append 到 list，子进程输出过多时会爆内存；仅打印或保留最后几行即可
    stdout_lines = []
    stderr_lines = []

    proc = subprocess.Popen(
        [sys.executable, "-u", legacy_script],  # -u 禁用 Python 内部输出缓冲
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,  # 行缓冲
    )

    # 实时读取并打印 stdout（仅打印，不长期保存全部行以防 OOM）
    for line in proc.stdout:
        line_stripped = line.rstrip("\n")
        print(f"[daily_update] {line_stripped}", flush=True)
        stdout_lines.append(line_stripped)

    # 等待进程结束，收集 stderr
    _, stderr_text = proc.communicate()
    for line in stderr_text.splitlines():
        stderr_lines.append(line)

    if proc.returncode != 0:
        raise Exception(f"Parquet 数据湖更新失败 (exit={proc.returncode}): \n" + "\n".join(stderr_lines))

    return "\n".join(stdout_lines)


# 导入源：数据湖 Parquet 根目录（阶段1 daily_update 写入的目录），与 import_xysz_parquet_to_zvt.py 默认一致。
# 可通过环境变量 XYSZ_PARQUET_BASE_DIR 覆盖。
XYSZ_PARQUET_BASE_DIR = os.environ.get("XYSZ_PARQUET_BASE_DIR", "/data/stock_data/xysz_data/base_data")


def run_import_xysz_parquet_to_zvt(base_dir: str = None):
    """
    从数据湖 Parquet 导入日线/财务/分红/配股/复权因子到 ZVT 库（调用 import_xysz_parquet_to_zvt.py）。

    导入源（base_dir）：Parquet 文件所在目录，默认 XYSZ_PARQUET_BASE_DIR。
      例如 base_dir/klines_daily_dir、base_dir/balance_sheet.parquet、base_dir/income.parquet 等。

    导入目标：ZVT 的 SQLite 库，路径为 zvt_env["data_path"]/xysz/{xysz}_{db_name}.db，
      例如 {data_path}/xysz/xysz_finance.db、xysz_kdata.db 等（由 df_to_db 按 schema 决定）。

    数据湖中不存在对应文件时脚本会跳过并返回 0，不抛错。
    子进程 stdout 实时打印到当前终端（与阶段1 daily_update 一致）。
    """
    base_dir = base_dir or XYSZ_PARQUET_BASE_DIR
    script_dir = os.path.dirname(os.path.abspath(__file__))
    import_script = os.path.join(script_dir, "scripts", "import_xysz_parquet_to_zvt.py")
    if not os.path.isfile(import_script):
        logger.warning("未找到导入脚本 %s，跳过数据湖导入", import_script)
        return False
    try:
        proc = subprocess.Popen(
            [
                sys.executable,
                "-u",
                import_script,
                "--base-dir",
                base_dir,
                "--only",
                "klines_daily,balance_sheet,income,cash_flow,holder_num,share_holder,dividend,right_issue,industry_base_info,industry_constituent",
                # "klines_daily,",

                "--count-rows",  # klines_daily 为目录时预扫总行数，显示百分比进度条（会多读一遍）
            ],
            cwd=script_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # 合并 stderr，导入进度用 logger 打到 stderr，这样终端才能看到
            text=True,
            bufsize=1,
        )
        for line in proc.stdout:
            line_stripped = line.rstrip("\n")
            print(f"[import_parquet] {line_stripped}", flush=True)
        proc.wait()
        if proc.returncode != 0:
            logger.warning("数据湖导入脚本退出码 %s", proc.returncode)
            return False
        return True
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        logger.error("数据湖导入脚本超时")
        return False
    except Exception as e:
        logger.exception("数据湖导入失败: %s", e)
        return False


def run_daily_job():
    job_start = time.time()
    informer = WechatWebhookInformer()
    informer.send_message(content="🔔 ZVT 每日综合更新任务开始执行")
    
    success_count = 0
    fail_count = 0
    summary = []
    
    # 阶段 1: 更新 Parquet 数据湖 (原始格式)
    try:
        t0 = time.time()
        stdout_text = run_legacy_daily_update()
        elapsed = _format_elapsed(time.time() - t0)
        logger.info("任务 Parquet 数据湖更新 执行成功。")
        
        # 提取关键更新信息以发送至微信
        detail_lines = []
        for line in stdout_text.splitlines():
            line = line.strip()
            # 过滤包含重要执行结果的行（通常以 [模块名] 开头）
            if line.startswith("[") and any(kw in line for kw in ["保存", "覆盖", "跳过", "合并"]):
                # 排除部分纯过程打印
                if "批次" in line and "已下载" in line:
                    continue
                detail_lines.append(f"    • {line}")
                
        summary.append(f"✅ Parquet 数据湖更新 ({elapsed})")
        if detail_lines:
            summary.extend(detail_lines)
            
        success_count += 1
    except Exception as e:
        elapsed = _format_elapsed(time.time() - t0)
        error_msg = f"❌ Parquet 数据湖更新 失败 ({elapsed}): {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        summary.append(error_msg)
        fail_count += 1

    # 阶段 2: 更新 ZVT 数据库（先数据湖导入，再 API 补缺）
    # 2a: 从数据湖 Parquet 导入能导入的财务数据，避免重复下载和重复缓存 h5
    try:
        t0 = time.time()
        logger.info("正在执行: 数据湖→ZVT 导入 (klines_daily, 财务三表, holder_num, share_holder, dividend, right_issue, industry_base_info, industry_constituent)...")
        ok = run_import_xysz_parquet_to_zvt()
        elapsed = _format_elapsed(time.time() - t0)
        if ok:
            summary.append(f"✅ 数据湖→ZVT 导入 ({elapsed})")
            success_count += 1
        else:
            summary.append(f"⚠️ 数据湖→ZVT 导入 跳过或失败 ({elapsed})")
    except Exception as e:
        elapsed = _format_elapsed(time.time() - t0)
        logger.exception("数据湖→ZVT 导入异常: %s", e)
        summary.append(f"⚠️ 数据湖→ZVT 导入 异常 ({elapsed}): {e}")

    # 2b: 运行 Recorder，只会对数据湖不存在或 ZVT 仍缺的数据拉取（evaluate_start_end_size_timestamps 会跳过已满的实体）
    # 注：股票基础信息/板块/行业/货币供应量/复权因子 数据湖 import 脚本未实现，仅通过 API 拉取
    import pandas as pd
    tasks = [
        ("股票基础信息 (xysz)", xyszStockMetaRecorder),
        ("板块基础信息 (Akshare)", AkshareBlockRecorder),
        ("行业板块信息 (xysz)", xyszIndustryBlockRecorder),
        ("行业成分股映射 (xysz)", xyszIndustryBlockStockRecorder),
        ("中国货币供应量 (Akshare)", ChinaMoneySupplyRecorder),
        ("日线 K 线数据 (xysz)", lambda: xyszStockKdataRecorder(level='1d')),
        ("后复权日线 (xysz)", lambda: compute_and_save_xysz_hfq(start_timestamp=pd.Timestamp.now() - pd.Timedelta(days=15))),
        ("资产负债表 (xysz)", xyszBalanceSheetRecorder),
        ("利润表 (xysz)", xyszIncomeStatementRecorder),
        ("现金流量表 (xysz)", xyszCashFlowRecorder),
        ("xysz 估值", lambda: xyszValuationRecorder(sleeping_time=0)),
        # QMT 数据更新任务
        ("QMT 股票列表", lambda: QMTStockRecorder(sleeping_time=0)),
        ("QMT 指数日线", lambda: QmtIndexRecorder(codes=IMPORTANT_INDEX, level='1d', sleeping_time=0)),
        ("QMT 资产负债表", lambda: QmtBalanceSheetRecorder(sleeping_time=0.1)),
        ("QMT 利润表", lambda: QmtIncomeStatementRecorder(sleeping_time=0.1)),
        ("QMT 现金流量表", lambda: QmtCashFlowRecorder(sleeping_time=0.1)),
        ("QMT 日线 (不复权)", lambda: QMTStockKdataRecorder(adjust_type=AdjustType.bfq, sleeping_time=0.2, ignore_failed=True)),
        ("QMT 日线 (后复权)", lambda: QMTStockKdataRecorder(adjust_type=AdjustType.hfq, sleeping_time=0.2, ignore_failed=True)),
        ("QMT 估值", lambda: QmtValuationRecorder(sleeping_time=0)),
    ]

    for name, recorder_factory in tasks:
        t0 = time.time()
        try:
            logger.info(f"正在执行任务: {name}...")
            recorder = recorder_factory() if callable(recorder_factory) else recorder_factory
            if hasattr(recorder, 'run'):
                recorder.run()
            elapsed = _format_elapsed(time.time() - t0)
            summary.append(f"✅ {name} ({elapsed})")
            success_count += 1
        except Exception as e:
            elapsed = _format_elapsed(time.time() - t0)
            error_msg = f"❌ {name} 失败 ({elapsed}): {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            summary.append(error_msg)
            fail_count += 1
        finally:
            # 每项任务结束后主动回收内存，避免连续跑多个 Recorder 时内存累积导致 OOM
            gc.collect()
    
    # 生成并发送最终报告
    total_elapsed = _format_elapsed(time.time() - job_start)
    status_str = "全部完成 ✅" if fail_count == 0 else f"完成 (存在 {fail_count} 个失败) ⚠️"
    report_content = (
        f"每日数据更新报告\n"
        f"━━━━━━━━━━━━━━━\n"
        f"状态: {status_str}\n"
        f"总耗时: {total_elapsed}\n"
        f"成功: {success_count} | 失败: {fail_count}\n\n"
        f"任务明细:\n" + "\n".join(summary)
    )
    logger.info(f"更新报告:\n{report_content}")
    try:
        informer.send_message(content=report_content)
    except Exception as e:
        logger.error(f"发送微信报告失败: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ZVT Daily Data Update Job')
    parser.add_argument('--now', action='store_true', help='立即执行一次更新任务')
    parser.add_argument('--time', type=str, default='16:30', help='每日定时执行的时间 (格式 HH:MM, 默认 16:30)')
    
    args = parser.parse_args()

    if args.now:
        logger.info("响应 --now 参数，立即开始执行...")
        run_daily_job()
        sys.exit(0)

    # 设置定时任务
    scheduler = BlockingScheduler()
    hour, minute = args.time.split(':')
    
    logger.info(f"📅 定时任务已启动，将在每天 {args.time} (周一至周五) 执行")
    
    scheduler.add_job(
        run_daily_job, 
        trigger=CronTrigger(hour=int(hour), minute=int(minute), day_of_week='mon-fri'),
        id='zvt_daily_update'
    )
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("任务已手动停止")
