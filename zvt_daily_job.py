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
import subprocess
import sys
import time
import traceback

import pandas as pd

from zvt.consts import IMPORTANT_INDEX
from zvt.informer.wechat_webhook import WechatWebhookInformer
from zvt.recorders.akshare.macro.china_money_supply_recorder import ChinaMoneySupplyRecorder
from zvt.recorders.akshare.meta.akshare_block_recorder import AkshareBlockRecorder
from zvt.recorders.qmt.finance.qmt_finance_recorder import (
    QmtBalanceSheetRecorder,
    QmtCashFlowRecorder,
    QmtIncomeStatementRecorder,
    QmtValuationRecorder,
    QmtFinanceFactorRecorder,
    QmtHolderNumRecorder,
    QmtTopTenHolderRecorder,
    QmtTopTenTradableHolderRecorder,
)
from zvt.recorders.qmt.index.qmt_index_recorder import QmtIndexRecorder
from zvt.recorders.qmt.meta.qmt_stock_meta_recorder import QMTStockRecorder
from zvt.recorders.xysz.finance.xysz_valuation_recorder import xyszValuationRecorder
from scripts.compute_xysz_hfq_kdata import compute_and_save_xysz_hfq
from scripts.daily_update_qmt import run_full_update as run_qmt_data_lake
from scripts.daily_update_xysz import run_full_update as run_xysz_data_lake


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ZvtDailyJob")


def _format_elapsed(seconds: float) -> str:
    """将秒数格式化为人类可读的耗时字符串"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m{secs:.0f}s"


def run_parquet_data_lake_updates():
    """直接调用 daily_update 的 run_full_update 及 xysz 后复权日线，更新 Parquet 数据湖。"""
    if run_xysz_data_lake is not None:
        logger.info("正在执行数据湖增量更新: daily_update_xysz")
        run_xysz_data_lake()
        logger.info("正在执行数据湖增量更新: xysz 后复权日线")
        compute_and_save_xysz_hfq(start_timestamp=pd.Timestamp.now() - pd.Timedelta(days=15))
    else:
        logger.warning("未找到 daily_update_xysz，跳过")
    if run_qmt_data_lake is not None:
        logger.info("正在执行数据湖增量更新: daily_update_qmt")
        run_qmt_data_lake()
    else:
        logger.warning("未找到 daily_update_qmt，跳过")
    return ""


# 导入源：数据湖 Parquet 根目录（阶段1 daily_update 写入的目录），与 import_xysz_parquet_to_zvt.py 默认一致。
# 可通过环境变量 XYSZ_PARQUET_BASE_DIR 覆盖。
XYSZ_PARQUET_BASE_DIR = os.environ.get("XYSZ_PARQUET_BASE_DIR", "/mnt/point/stock_data/xysz_data/base_data")


def _flush_all_sessions():
    """关闭并清除所有全局缓存的 SQLite session，释放数据库写锁。"""
    from zvt.contract import zvt_context

    for key, session in list(zvt_context.sessions.items()):
        try:
            session.close()
        except Exception:
            pass
    zvt_context.sessions.clear()


def _cleanup_amazingdata_sdk():
    """注销 AmazingData SDK 并清理全局状态，防止其 C++ 后台线程干扰后续 QMT 任务。

    AmazingData SDK 的 SWIG C++ 层在长时间运行后会累积内部状态，
    导致 IGMDSpi.OnLog 回调触发 Swig::DirectorMethodException 崩溃。
    """
    try:
        import zvt.recorders.xysz.xysz_api as xysz_api_mod
        user = os.environ.get("AMAZING_DATA_USER", "10100223966")
        if xysz_api_mod._client_instance is not None:
            try:
                xysz_api_mod.AmazingDataClient.logout(user)
                logger.info("AmazingData SDK 已注销登录")
            except Exception as e:
                logger.warning("AmazingData logout 异常（可忽略）: %s", e)
            xysz_api_mod._client_instance = None
    except Exception:
        pass

    try:
        import sys
        ad_modules = [k for k in sys.modules if k.startswith("AmazingData") or k == "tgw"]
        for mod_name in ad_modules:
            del sys.modules[mod_name]
        if ad_modules:
            logger.info("已从 sys.modules 卸载: %s", ad_modules)
    except Exception:
        pass

    gc.collect()


def _run_recorder_task(name, factory, summary, success_count, fail_count):
    """执行单个 Recorder 任务，更新 summary 与计数，返回 (success_count, fail_count)。"""
    t0 = time.time()
    try:
        logger.info("正在执行任务: %s...", name)
        recorder = factory() if callable(factory) else factory
        if hasattr(recorder, "run"):
            recorder.run()
        summary.append(f"✅ {name} ({_format_elapsed(time.time() - t0)})")
        return success_count + 1, fail_count
    except Exception as e:
        elapsed = _format_elapsed(time.time() - t0)
        summary.append(f"❌ {name} 失败 ({elapsed}): {e}")
        logger.exception("%s 失败: %s", name, e)
        return success_count, fail_count + 1
    finally:
        _flush_all_sessions()


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
                # 股票基础信息由阶段1 stock_meta.parquet 导入；K 线不导入 SQLite，仅保留在 Parquet
                "stock_meta,balance_sheet,income,cash_flow,holder_num,share_holder,dividend,right_issue,industry_base_info,industry_constituent",
                "--count-rows",
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
    
    # 阶段 1: 更新 Parquet 数据湖
    try:
        t0 = time.time()
        run_parquet_data_lake_updates()
        elapsed = _format_elapsed(time.time() - t0)
        summary.append(f"✅ Parquet 数据湖更新 ({elapsed})")
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
        logger.info("正在执行: 数据湖→ZVT 导入 (stock_meta, 财务三表, holder_num, share_holder, dividend, right_issue, industry)...")
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

    # 2b: 运行 Recorder，对数据湖不存在或 ZVT 仍缺的数据拉取
    # 股票基础信息、财务三表、行业板块/成分股已由 2a 从数据湖 parquet 导入，此处不再重复拉取
    xysz_tasks = [
        ("板块基础信息 (Akshare)", AkshareBlockRecorder),
        ("中国货币供应量 (Akshare)", ChinaMoneySupplyRecorder),
        ("xysz 估值", lambda: xyszValuationRecorder(sleeping_time=0)),
    ]
    for name, factory in xysz_tasks:
        success_count, fail_count = _run_recorder_task(name, factory, summary, success_count, fail_count)
        gc.collect()

    # xysz 任务全部完成后，注销 AmazingData SDK 并卸载 C++ 模块，
    # 避免其后台线程在 QMT 长时间任务运行期间触发 IGMDSpi.OnLog 崩溃
    _cleanup_amazingdata_sdk()

    qmt_tasks = [
        ("QMT 股票列表", lambda: QMTStockRecorder(sleeping_time=0)),
        ("QMT 指数日线", lambda: QmtIndexRecorder(codes=IMPORTANT_INDEX, level='1d', sleeping_time=0)),
        ("QMT 资产负债表", lambda: QmtBalanceSheetRecorder(sleeping_time=0.1)),
        ("QMT 利润表", lambda: QmtIncomeStatementRecorder(sleeping_time=0.1)),
        ("QMT 现金流量表", lambda: QmtCashFlowRecorder(sleeping_time=0.1)),
        ("QMT 财务指标", lambda: QmtFinanceFactorRecorder(sleeping_time=0.1)),
        ("QMT 估值", lambda: QmtValuationRecorder(sleeping_time=0)),
        ("QMT 股东人数", lambda: QmtHolderNumRecorder(sleeping_time=0.1)),
        ("QMT 十大股东", lambda: QmtTopTenHolderRecorder(sleeping_time=0.1)),
        ("QMT 十大流通股东", lambda: QmtTopTenTradableHolderRecorder(sleeping_time=0.1)),
    ]
    for name, factory in qmt_tasks:
        success_count, fail_count = _run_recorder_task(name, factory, summary, success_count, fail_count)
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
    logger.info("更新报告:\n%s", report_content)
    try:
        informer.send_message(content=report_content)
    except Exception as e:
        logger.error("发送微信报告失败: %s", e)


if __name__ == "__main__":
    run_daily_job()
