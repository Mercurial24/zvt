# -*- coding: utf-8 -*-
"""
xtquant 下载数据函数封装。
需在已安装 QMT 且 MiniQMT 已连接的环境下运行（如 Windows 本机）。
"""
from __future__ import annotations

import os
import sys
import time
from typing import List, Optional

import schedule

from xtquant import xtdata

# 保证可导入项目 utils（运行目录可能为 qmt_forward）
_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.abspath(os.path.join(_script_dir, "..", ".."))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
try:
    from utils.message_push import push_trade_msg
except ImportError:
    push_trade_msg = None  # 未安装 requests 或 utils 不可用时静默跳过

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable


# ---------------------------------------------------------------------------
# 周期 period 枚举（K线 / 分笔 / Level2）
# ---------------------------------------------------------------------------
# Level1 K线: '1m' | '5m' | '15m' | '30m' | '1h' | '1d' | '1w' | '1mon' | '1q' | '1hy' | '1y'
# 分笔:       'tick'
# Level2:     'l2quote' | 'l2order' | 'l2transaction' | 'l2quoteaux' | 'l2orderqueue' | 'l2thousand'
# 投研特色:   'warehousereceipt' | 'futureholderrank' | 'transactioncount1m' | 'transactioncount1d' 等

# 财务表名 table_list 枚举
# ---------------------------------------------------------------------------
# 'Balance'         - 资产负债表
# 'Income'           - 利润表
# 'CashFlow'         - 现金流量表
# 'Capital'          - 股本表
# 'Holdernum'        - 股东数
# 'Top10holder'      - 十大股东
# 'Top10flowholder'  - 十大流通股东
# 'Pershareindex'    - 每股指标

# 报表筛选 report_type 枚举（用于 get_financial_data，此处仅作参考）
# ---------------------------------------------------------------------------
# 'report_time'  - 按截止日期
# 'announce_time'- 按披露日期

# 已有数据行为说明（官方文档为「补充」数据，非「全量覆盖」）
# ---------------------------------------------------------------------------
# - download_history_data / download_history_data2：
#   增量下载（incrementally=True 或 start_time 为空且 incrementally=None）：
#     在已有数据基础上往后补，已有时间区间不会重复下载，即跳过已有、只补缺失。
#   指定 start_time/end_time 且非增量（incrementally=False）：
#     官方未明确写是否覆盖该区间内已有数据，通常为按区间补充/合并，具体以 QMT 实际行为为准。
# - download_financial_data / download_financial_data2：
#   文档未单独说明；一般同为补充/合并逻辑，不会整库清空再下。
# - download_cb_data / download_etf_info / download_holiday_data / download_sector_data / download_index_weight：
#   多为全量刷新该类别数据，可能覆盖同类型已有数据（如节假日、板块列表等），具体以实际为准。

# 复权 / 除权说明（无单独「下载复权数据」接口）
# ---------------------------------------------------------------------------
# - 下载的 K 线为未复权原始数据。复权在「读取」时通过 get_market_data(..., dividend_type='front'/'back'/'back_ratio' 等) 指定即可。

# A股量化研究 - 建议下载数据一览（本脚本已覆盖或可补充）
# ---------------------------------------------------------------------------
# 已有（当前 __main__ 会下载）：
#   - 全市场日 K 线、全市场财务数据
# 强烈建议补充（回测/选股常用）：
#   - 基础数据 download_all_basic()：板块、节假日、指数权重
#     （交易日历 get_trading_calendar 依赖节假日；选股/行业分类依赖板块；指数成分依赖 index_weight）
#   - ETF 日 K：先通过板块获取 ETF 代码列表，再 download_history_data2 下载行情，见 download_all_etf_daily
#   - 主要指数日 K：沪深300/中证500/上证50 等，用于基准收益、择时、风格暴露
# 按需补充：
#   - 分钟 K（1m/5m）：日内或短周期策略，数据量大，用 download_history_data2(stock_list, '1m'/'5m', ...)
#   - 指数成分外的其他指数日 K：用 download_history_data2([指数代码], '1d', ...)
#   - Level2 / tick：高频需单独下载，见 period 枚举

# 删除已下载数据（官方无删除 API，只能删本地文件）
# ---------------------------------------------------------------------------
# 1) 获取数据根目录：get_data_dir()，即 xtdata.data_dir（一般为 MiniQMT 的 userdata_mini 下的 datadir）。
# 2) 行情数据常见结构：datadir / 市场(SH|SZ) / 周期(秒) / 代码.DAT，例如 datadir/SH/86400/600000.DAT（日线）。
#    周期与秒数对应：tick=0, 1m=60, 5m=300, 15m=900, 30m=1800, 1h=3600, 1d=86400（不同版本可能不同）。
# 3) 删除方式：在程序外手动删除上述目录下对应文件或文件夹，或使用下面的 get_data_dir / delete_history_data 辅助。


def download_history_data(
    stock_code: str,
    period: str,
    start_time: str = "",
    end_time: str = "",
    incrementally: Optional[bool] = None,
) -> None:
    """
    下载单只合约的历史行情数据（K线或分笔），同步执行，完成后返回。
    已有数据：增量下载时跳过已有区间只补缺失；指定区间且非增量时行为以 QMT 为准，通常为补充/合并。

    参数
    -----
    stock_code : str
        合约代码，格式 "代码.市场"，如 "600000.SH", "000001.SZ", "000300.SH"。
    period : str
        周期/数据类型。常用枚举：
        - K线: '1m','5m','15m','30m','1h','1d','1w','1mon','1q','1hy','1y'
        - 分笔: 'tick'
        - Level2: 'l2quote','l2order','l2transaction','l2quoteaux','l2orderqueue','l2thousand'
    start_time : str
        起始时间，8位日期如 "20240101"，为空表示从最早开始。
    end_time : str
        结束时间，8位日期如 "20241231"，为空表示到最新。
    incrementally : Optional[bool]
        是否增量下载：
        - True   : 增量下载（在已有数据基础上往后补）
        - False  : 全量下载
        - None   : 由 start_time 决定，start_time 为空则按增量下载。
    """
    xtdata.download_history_data(
        stock_code=stock_code,
        period=period,
        start_time=start_time,
        end_time=end_time,
        incrementally=incrementally,
    )


def download_history_data2(
    stock_list: List[str],
    period: str,
    start_time: str = "",
    end_time: str = "",
) -> None:
    """
    批量下载多只合约的历史行情数据，同步执行，按单只循环下载并显示 tqdm 进度条。
    已有数据：同上，增量逻辑下跳过已有只补缺失。

    参数
    -----
    stock_list : List[str]
        合约代码列表，如 ["600000.SH", "000001.SZ"]。
    period : str
        周期，同 download_history_data。枚举：'1m','5m','15m','30m','1h','1d','1w','1mon','1q','1hy','1y','tick' 及 Level2 等。
    start_time : str
        起始时间，8位日期如 "20240101"，为空表示从最早。
    end_time : str
        结束时间，8位日期如 "20241231"，为空表示到最新。
    """
    for stock in tqdm(stock_list, desc=f"下载行情({period})", unit="只"):
        download_history_data(
            stock_code=stock,
            period=period,
            start_time=start_time,
            end_time=end_time,
        )


def download_financial_data(
    stock_list: List[str],
    table_list: Optional[List[str]] = None,
) -> None:
    """
    下载财务数据（旧版接口，不指定时间范围），同步执行。

    参数
    -----
    stock_list : List[str]
        合约代码列表，如 ["600000.SH", "000001.SZ"]。
    table_list : Optional[List[str]]
        要下载的财务表名列表，为 None 或 [] 时由接口默认处理。枚举：
        - 'Balance'         : 资产负债表
        - 'Income'           : 利润表
        - 'CashFlow'         : 现金流量表
        - 'Capital'          : 股本表
        - 'Holdernum'        : 股东数
        - 'Top10holder'      : 十大股东
        - 'Top10flowholder'  : 十大流通股东
        - 'Pershareindex'    : 每股指标
    """
    xtdata.download_financial_data(
        stock_list=stock_list,
        table_list=table_list or [],
    )


def download_financial_data2(
    stock_list: List[str],
    table_list: Optional[List[str]] = None,
    start_time: str = "",
    end_time: str = "",
) -> None:
    """
    下载财务数据（新版，支持时间范围），按披露日期 m_anntime 在 [start_time, end_time] 内筛选。
    按单只循环下载并显示 tqdm 进度条。

    参数
    -----
    stock_list : List[str]
        合约代码列表。
    table_list : Optional[List[str]]
        财务表名列表，枚举同 download_financial_data。
    start_time : str
        起始日期，8位如 "20240101"，为空表示不限制起点。
    end_time : str
        结束日期，8位如 "20241231"，为空表示不限制终点。
    """
    tables = table_list or []
    for stock in tqdm(stock_list, desc="下载财务数据", unit="只"):
        xtdata.download_financial_data2(
            stock_list=[stock],
            table_list=tables,
            start_time=start_time,
            end_time=end_time,
        )


def download_cb_data() -> None:
    """
    下载全部可转债基础信息。无参数。同步执行，完成后返回。
    下载后可调用 get_cb_info(stockcode) 获取指定可转债信息。
    """
    xtdata.download_cb_data()


def download_etf_info() -> None:
    """
    下载所有 ETF 申赎清单信息。无参数。同步执行。
    下载后可调用 get_etf_info() 获取全部申赎数据。
    """
    xtdata.download_etf_info()


def download_holiday_data() -> None:
    """
    下载节假日数据。无参数。同步执行。
    用于 get_trading_calendar 等接口获取交易日历时需先下载节假日列表。
    """
    xtdata.download_holiday_data()


def download_sector_data() -> None:
    """
    下载板块分类信息。无参数。同步执行。
    下载后可调用 get_sector_list()、get_stock_list_in_sector(sector_name) 等。
    """
    xtdata.download_sector_data()


def download_index_weight() -> None:
    """
    下载指数成分权重信息。无参数。同步执行。
    下载后可调用 get_index_weight(index_code) 获取指定指数成分权重。
    """
    xtdata.download_index_weight()


# ---------------------------------------------------------------------------
# 删除已下载数据（无官方 API，通过删除本地文件实现）
# ---------------------------------------------------------------------------

# 周期 period -> 部分 QMT 使用的目录名（秒数），不同版本可能不一致，仅作参考
_PERIOD_TO_DIR: dict[str, str] = {
    "tick": "0",
    "1m": "60",
    "5m": "300",
    "15m": "900",
    "30m": "1800",
    "1h": "3600",
    "1d": "86400",
}


def get_data_dir() -> str:
    """
    返回当前 xtdata 使用的本地数据根目录（一般为 MiniQMT 的 userdata_mini 下的 datadir）。
    删除已下载数据时，可先调用此函数确认路径，再手动删除其下对应文件或目录。
    """
    return getattr(xtdata, "data_dir", "") or ""


def delete_history_data(
    stock_code: str,
    period: str,
    data_dir: Optional[str] = None,
) -> bool:
    """
    删除指定合约、指定周期的本地行情数据文件（若存在）。
    依赖本地目录结构为 数据根目录/市场/周期目录/代码.DAT，不同 QMT 版本可能不同，仅作参考。

    参数
    -----
    stock_code : str
        合约代码，如 "600000.SH", "000001.SZ"。
    period : str
        周期，如 "1d", "1m", "5m", "tick"。
    data_dir : Optional[str]
        数据根目录，为空则使用 xtdata.data_dir。

    返回
    -----
    bool
        是否删除了文件（若文件不存在或未找到目录则返回 False）。
    """
    import os

    root = (data_dir or "").strip() or get_data_dir()
    if not root or not os.path.isdir(root):
        return False
    # 解析 600000.SH -> 市场 SH, 代码 600000
    parts = stock_code.upper().split(".")
    if len(parts) != 2:
        return False
    code, market = parts[0], parts[1]
    dir_name = _PERIOD_TO_DIR.get(period)
    if not dir_name:
        return False
    # 常见为 代码.DAT 或 代码 等，按实际为准
    path = os.path.join(root, market, dir_name, f"{code}.DAT")
    if os.path.isfile(path):
        try:
            os.remove(path)
            return True
        except OSError:
            pass
    return False


def delete_all_history_in_data_dir(
    data_dir: Optional[str] = None,
    confirm: bool = False,
) -> bool:
    """
    删除数据根目录下所有行情数据（即清空 datadir 下各市场、各周期下的 .DAT 等文件）。
    请谨慎使用。若 confirm 不为 True，则不执行删除，仅返回 False。

    参数
    -----
    data_dir : Optional[str]
        数据根目录，为空则使用 xtdata.data_dir。
    confirm : bool
        必须为 True 才会真正执行删除，默认 False。

    返回
    -----
    bool
        是否执行了删除（confirm=False 时恒为 False）。
    """
    import os

    if not confirm:
        return False
    root = (data_dir or "").strip() or get_data_dir()
    if not root or not os.path.isdir(root):
        return False
    removed = 0
    for market in os.listdir(root):
        mp = os.path.join(root, market)
        if not os.path.isdir(mp):
            continue
        for period_dir in os.listdir(mp):
            pp = os.path.join(mp, period_dir)
            if not os.path.isdir(pp):
                continue
            for name in os.listdir(pp):
                fpath = os.path.join(pp, name)
                if os.path.isfile(fpath):
                    try:
                        os.remove(fpath)
                        removed += 1
                    except OSError:
                        pass
    return removed > 0


# ---------------------------------------------------------------------------
# 便捷：一次性调用所有“无参”下载（按需取消注释使用）
# ---------------------------------------------------------------------------
def download_all_basic() -> None:
    """下载基础/无参数据：节假日、板块、指数权重（不含可转债、不含 ETF 申赎清单）。"""
    # print("开始下载 节假日数据 ...")
    # download_holiday_data()
    
    print("开始下载 板块分类信息 ...")
    download_sector_data()
    
    print("开始下载 指数权重信息 ...")
    download_index_weight()
    print("基础数据下载完成！")


def get_all_stock_list(sector_name: str = "沪深A股") -> List[str]:
    """
    获取指定板块下的全部股票代码列表。需要先下载板块数据（如先调用 download_sector_data 或 download_all_basic）。

    参数
    -----
    sector_name : str
        板块名称，默认 "沪深A股" 表示沪深两市 A 股。

    返回
    -----
    List[str]
        合约代码列表，如 ["600000.SH", "000001.SZ", ...]。
    """
    return xtdata.get_stock_list_in_sector(sector_name) or []


def download_all_stocks_daily(
    start_time: str = "",
    end_time: str = "",
    sector_name: str = "沪深A股",
) -> None:
    """
    下载「指定板块」下所有股票的历史日 K 线。默认下载「沪深A股」全部股票的日线。
    会先下载板块数据以获取股票列表，再按单只循环下载并显示进度条。

    参数
    -----
    start_time : str
        起始日期，8 位如 "20200101"，为空表示从最早。
    end_time : str
        结束日期，8 位如 "20241231"，为空表示到最新。
    sector_name : str
        板块名称，默认 "沪深A股"。
    """
    stock_list_1 =  xtdata.get_stock_list_in_sector( "沪深A股")
    stock_list_2 = xtdata.get_stock_list_in_sector( "京市A股")
    stock_list = stock_list_1 + stock_list_2
    if not stock_list:
        print("未获取到股票列表，请确认已连接 MiniQMT 并已下载板块数据。")
        return
    print(f"共 {len(stock_list)} 只股票，开始下载日 K 线（{start_time or '最早'} ~ {end_time or '最新'}）...")
    download_history_data2(
        stock_list=stock_list,
        period="1d",
        start_time=start_time,
        end_time=end_time,
    )
    print("日 K 线下载完成。")


# 常用财务表全量（用于 download_all_stocks_financial）
FINANCIAL_TABLES_ALL: List[str] = [
    "Balance",           # 资产负债表
    "Income",            # 利润表
    "CashFlow",          # 现金流量表
    "Capital",           # 股本表
    "Holdernum",         # 股东数
    "Top10holder",       # 十大股东
    "Top10flowholder",   # 十大流通股东
    "Pershareindex",     # 每股指标
]


def download_all_stocks_financial(
    start_time: str = "",
    end_time: str = "",
    sector_name: str = "沪深A股",
    table_list: Optional[List[str]] = None,
) -> None:
    """
    下载「指定板块」下所有股票的历史财务数据。默认下载「沪深A股」全部股票、全部常用财务表。
    按披露日期 m_anntime 在 [start_time, end_time] 内筛选；时间为空表示不限制。按单只循环下载并显示进度条。

    参数
    -----
    start_time : str
        起始披露日期，8 位如 "20000101"，为空表示不限制。
    end_time : str
        结束披露日期，8 位如 "20241231"，为空表示不限制。
    sector_name : str
        板块名称，默认 "沪深A股"。
    table_list : Optional[List[str]]
        财务表名列表，为 None 时使用 FINANCIAL_TABLES_ALL（全部常用表）。
    """
    stock_list_1 = xtdata.get_stock_list_in_sector( "沪深A股")    
    stock_list_2 = xtdata.get_stock_list_in_sector( "京市A股")
    stock_list = stock_list_1 + stock_list_2
    if not stock_list:
        print("未获取到股票列表，请确认已连接 MiniQMT 并已下载板块数据。")
        return
    tables = table_list if table_list is not None else FINANCIAL_TABLES_ALL
    print(f"共 {len(stock_list)} 只股票、{len(tables)} 张财务表，开始下载财务数据（{start_time or '不限制'} ~ {end_time or '不限制'}）...")
    download_financial_data2(
        stock_list=stock_list,
        table_list=tables,
        start_time=start_time,
        end_time=end_time,
    )
    print("财务数据下载完成。")


# 常用 A 股指数代码（用于 download_main_index_daily）
# 不含中小板指（399005.SZ，已合并至主板；需历史数据时可单独传 index_list 补充）。
INDEX_MAIN_A: List[str] = [
    "000001.SH",   # 上证指数
    "000300.SH",   # 沪深300
    "000905.SH",   # 中证500
    "000016.SH",   # 上证50
    "000010.SH",   # 上证180
    "000009.SH",   # 上证380
    "000852.SH",   # 中证1000
    "000906.SH",   # 中证800
    "000688.SH",   # 科创50
    "399006.SZ",   # 创业板指
    "399673.SZ",   # 创业板50
    "399303.SZ",   # 国证2000
    "399001.SZ",   # 深证成指
    "399106.SZ",   # 深证综指
]


def download_main_index_daily(
    start_time: str = "",
    end_time: str = "",
    index_list: Optional[List[str]] = None,
) -> None:
    """
    下载主要 A 股指数日 K 线（如沪深300、中证500、上证50 等），用于基准收益、择时、风格暴露。

    参数
    -----
    start_time : str
        起始日期，8 位如 "20100101"，为空表示从最早。
    end_time : str
        结束日期，8 位如 "20241231"，为空表示到最新。
    index_list : Optional[List[str]]
        指数代码列表，为 None 时使用 INDEX_MAIN_A。
    """
    codes = index_list if index_list is not None else INDEX_MAIN_A
    if not codes:
        return
    print(f"共 {len(codes)} 个指数，开始下载指数日 K 线（{start_time or '最早'} ~ {end_time or '最新'}）...")
    download_history_data2(
        stock_list=codes,
        period="1d",
        start_time=start_time,
        end_time=end_time,
    )
    print("指数日 K 线下载完成。")


def get_etf_list(sector_name: str = "ETF") -> List[str]:
    """
    通过板块获取 ETF 代码列表。需先有板块数据（如先调用 download_sector_data 或 download_all_basic）。
    若返回空列表，可用 xtdata.get_sector_list() 查看当前可用板块名（如 "沪深ETF"、"ETF基金" 等），
    再传入正确的 sector_name。

    参数
    -----
    sector_name : str
        板块名称，默认 "ETF"。部分 QMT 版本可能为 "沪深ETF" 等。

    返回
    -----
    List[str]
        ETF 合约代码列表，如 ["510300.SH", "159915.SZ", ...]。
    """
    download_sector_data()
    return xtdata.get_stock_list_in_sector(sector_name) or []


def download_all_etf_daily(
    start_time: str = "",
    end_time: str = "",
    sector_name: str = "ETF",
) -> None:
    """
    下载「指定板块」下全部 ETF 的历史日 K 线。会先通过板块获取 ETF 代码列表，再按单只循环下载并显示进度条。

    参数
    -----
    start_time : str
        起始日期，8 位如 "20200101"，为空表示从最早。
    end_time : str
        结束日期，8 位如 "20241231"，为空表示到最新。
    sector_name : str
        板块名称，默认 "ETF"；若取不到列表可改为 "沪深ETF" 或先 get_sector_list() 查看。
    """
    etf_list = get_etf_list(sector_name)
    if not etf_list:
        print("未获取到 ETF 列表，请确认已连接 MiniQMT 并已下载板块数据；若仍为空可尝试 sector_name='沪深ETF' 或 get_sector_list() 查看可用板块。")
        return
    print(f"共 {len(etf_list)} 只 ETF，开始下载日 K 线（{start_time or '最早'} ~ {end_time or '最新'}）...")
    download_history_data2(
        stock_list=etf_list,
        period="1d",
        start_time=start_time,
        end_time=end_time,
    )
    print("ETF 日 K 线下载完成。")


def _run_daily_download() -> None:
    """每日定时任务：下载股票、指数、财务等数据。"""
    # 下载所有股票（沪深A股）的历史日 K 线
    download_all_stocks_daily(start_time="", end_time="")
    push_trade_msg(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  QMT下载股票日k线数据完成。")

    # 下载主要指数日 K（上证、沪深300、中证500 等），用于基准/择时
    download_main_index_daily(start_time="", end_time="")
    push_trade_msg(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  QMT下载指数日k线数据完成。")
    # 下载所有股票（沪深A股）的全部历史财务数据
    download_all_stocks_financial(start_time="", end_time="")
    push_trade_msg(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  QMT下载财务数据完成。")
    # 下载全部 ETF 日 K 线（通过板块获取 ETF 代码列表后批量下载）
    # download_all_etf_daily(start_time="", end_time="")

    # print(f"QMT下载数据{time.strftime('%Y-%m-%d %H:%M:%S')} 完成。")



if __name__ == "__main__":
    # _run_daily_download()
    # 每天下午 17:00 自动执行下载任务
    schedule.every().day.at("17:00").do(_run_daily_download)
    print("已设置每日 17:00 执行下载任务，程序常驻运行...")
    while True:
        schedule.run_pending()
        time.sleep(60)
