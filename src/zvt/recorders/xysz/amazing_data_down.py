# -*- coding: utf-8 -*-
"""
中国银河证券 星耀数智 AmazingData 开发手册 - Python 接口封装

本模块将 AmazingData 开发手册中的全部 Python 函数接口封装为一个类，
便于统一调用。使用前需安装 tgw 与 AmazingData 的 wheel 包，并先调用 login 完成登录。

数据说明：
- 行情数据：股票/指数/债券/基金/期权/港股通/期货 Level-1 快照、K 线等
- 基础数据：每日最新证券信息、代码表、复权因子、交易日历、证券基础信息等
- 财务数据：资产负债表、现金流量表、利润表、业绩快报、业绩预告
- 股东股本：十大股东、股东户数、股本结构、股权冻结质押、限售股解禁
- 股东权益：分红、配股；融资融券；龙虎榜、大宗交易；期权/ETF/可转债/国债收益率等
"""

from typing import Optional, List, Dict, Any, Union
import os
import AmazingData as ad  # type: ignore  # 需先 pip install AmazingData-*.whl 及 tgw
import pandas as pd  # 用于 K 线保存/读取
import tqdm

class AmazingDataClient:
    """
    星耀数智 AmazingData 全量接口封装类。

    使用步骤：
        1. 调用 login() 登录
        2. 实例化本类：client = AmazingDataClient()
        3. 按需调用各类数据接口（基础数据、行情、财务、股东等）
        4. 订阅实时数据时可使用 get_subscribe_data() 获取订阅对象并注册回调
    """

    def __init__(self, calendar: Optional[List] = None):
        """
        初始化客户端。部分行情接口需要交易日历，可传入或稍后通过 get_calendar 获取后再创建 MarketData。

        :param calendar: 可选。交易日历列表，用于历史行情查询。若不传则在使用 MarketData 相关方法时内部获取。
        """
        self._base_data = None
        self._info_data = None
        self._market_data = None
        self._calendar = calendar

    # ------------------------------- 基础接口（模块级，委托给 ad） -------------------------------

    @staticmethod
    def login(
        username: str,
        password: str,
        host: str,
        port: int,
    ) -> None:
        """
        API 登录。调用任何数据接口之前必须先调用本接口。

        :param username: 账号
        :param password: 密码
        :param host: 服务器 IP
        :param port: 服务器端口号
        """
        ad.login(username=username, password=password, host=host, port=port)

    @staticmethod
    def logout(username: str) -> None:
        """
        API 退出登录。必须在登录状态下使用；正常使用一般无需调用。

        :param username: 用户名
        """
        ad.logout(username=username)

    @staticmethod
    def update_password(username: str, old_password: str, new_password: str) -> None:
        """
        更新密码。必须先登录才能修改密码。

        :param username: 用户名
        :param old_password: 旧密码
        :param new_password: 新密码
        """
        ad.update_password(
            username=username,
            old_password=old_password,
            new_password=new_password,
        )

    # ------------------------------- 内部获取子对象 -------------------------------

    @property
    def base_data(self) -> Any:
        """获取基础数据查询对象 BaseData。"""
        if self._base_data is None:
            self._base_data = ad.BaseData()
        return self._base_data

    @property
    def info_data(self) -> Any:
        """获取资讯/财务等数据查询对象 InfoData。"""
        if self._info_data is None:
            self._info_data = ad.InfoData()
        return self._info_data

    def _get_market_data(self) -> Any:
        """获取历史行情查询对象 MarketData，需要交易日历。"""
        if self._market_data is None:
            cal = self._calendar
            if cal is None:
                cal = self.base_data.get_calendar()
            self._market_data = ad.MarketData(cal)
        return self._market_data

    @staticmethod
    def get_subscribe_data() -> Any:
        """获取实时订阅对象 SubscribeData，用于注册回调并 run()。"""
        return ad.SubscribeData()

    # ------------------------------- 3.5.2 基础数据 -------------------------------

    def get_code_info(self, security_type: str = "EXTRA_STOCK_A") -> Any:
        """
        获取每日最新证券信息，交易日早上 9 点前更新当日最新。

        :param security_type: 代码类型，见附录 security_type(沪深北)，默认 EXTRA_STOCK_A（上交所A股、深交所A股和北交所股票列表）
        :return: code_info，DataFrame，index 为股票代码，column 含：symbol(证券简称)、security_status(产品状态标志)、pre_close(昨收价)、high_limited(涨停价)、low_limited(跌停价)、price_tick(最小价格变动单位)
        """
        return self.base_data.get_code_info(security_type=security_type)

    def get_code_list(self, security_type: str = "EXTRA_STOCK_A") -> List[str]:
        """
        获取每日最新代码表（沪深北），交易日早上 9 点前更新。此接口无法获取历史代码表。

        :param security_type: 代码类型，见附录，默认 EXTRA_STOCK_A
        :return: code_list，证券代码列表
        """
        return self.base_data.get_code_list(security_type=security_type)

    def get_future_code_list(self, security_type: str = "EXTRA_FUTURE") -> List[str]:
        """
        获取每日最新代码表（期货交易所），交易日早上 9 点前更新。

        :param security_type: 代码类型 security_type(期货交易所)，默认 EXTRA_FUTURE（中金所/上期所/大商所/郑商所/上海国际能源交易中心）
        :return: code_list，证券代码列表
        """
        return self.base_data.get_future_code_list(security_type=security_type)

    def get_option_code_list(self, security_type: str = "EXTRA_ETF_OP") -> List[str]:
        """
        获取每日最新代码表（期权），交易日早上 9 点前更新。

        :param security_type: 代码类型 security_type(期权)，默认 EXTRA_ETF_OP（ETF期权，上交所和深交所）
        :return: code_list，证券代码列表
        """
        return self.base_data.get_option_code_list(security_type=security_type)

    def get_backward_factor(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Any:
        """
        获取后复权因子并支持本地存储。复权因子由交易所行情数据计算得出的后复权因子。

        :param code_list: 代码列表，支持股票、ETF
        :param local_path: 本地存储复权因子数据的文件夹绝对路径，如 'D://AmazingData_local_data//'
        :param is_local: 是否优先使用本地数据。True：有则用本地（可能非最新）；无则从服务器取并更新本地。False：始终从服务器取并更新本地
        :return: backward_factor，DataFrame，index 为交易日期，column 为股票代码
        """
        return self.base_data.get_backward_factor(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_adj_factor(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Any:
        """
        获取单次复权因子并支持本地存储。复权因子由交易所行情数据计算得出的单次复权因子。

        :param code_list: 代码列表，支持股票、ETF
        :param local_path: 本地存储文件夹绝对路径
        :param is_local: 是否使用本地缓存，同 get_backward_factor
        :return: adj_factor，DataFrame，index 为交易日期，column 为股票代码
        """
        return self.base_data.get_adj_factor(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_hist_code_list(
        self,
        security_type: str = "EXTRA_STOCK_A_SH_SZ",
        start_date: int = 20240101,
        end_date: int = 20240701,
        local_path: str = "",
    ) -> List[str]:
        """
        获取历史代码表。先检查本地数据，再从服务端补充，最后返回。

        :param security_type: 代码类型，默认沪深 A 股；支持附录 security_type(沪深北) 和 (期货交易所)
        :param start_date: 开始日期（闭区间），如 20240101
        :param end_date: 结束日期（闭区间）
        :param local_path: 本地存储路径，需绝对路径，如 'D://AmazingData_local_data//'
        :return: code_list，证券代码列表
        """
        return self.base_data.get_hist_code_list(
            security_type=security_type,
            start_date=start_date,
            end_date=end_date,
            local_path=local_path,
        )

    def get_calendar(
        self,
        data_type: str = "str",
        market: str = "SH",
    ) -> List:
        """
        获取交易所交易日历。

        :param data_type: 返回数据类型，默认 str，可选 datetime 或 str
        :param market: 市场，见附录 market，默认 SH（上海）
        :return: calendar，日期列表
        """
        return self.base_data.get_calendar(data_type=data_type, market=market)

    def get_stock_basic(self, code_list: List[str]) -> Any:
        """
        获取指定股票列表的证券基础数据（沪深北），含所有股票（含已退市）的中英文名称、上市日期、退市日期、上市板块等。

        :param code_list: 沪深北三个交易所的代码列表
        :return: stock_basic，DataFrame，column 为字段，index 为序号。字段含：MARKET_CODE(证券代码)、SECURITY_NAME(证券简称)、COMP_NAME(证券中文名称)、PINYIN、COMP_NAME_ENG、LISTDATE(上市日期)、DELISTDATE(退市日期)、LISTPLATE_NAME(上市板块名称)、IS_LISTED(上市状态 1 上市 3 终止上市) 等
        """
        return self.info_data.get_stock_basic(code_list)

    def get_history_stock_status(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取指定股票列表的历史证券数据（日频），含历史涨跌停、ST、除权除息等。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径，绝对路径
        :param is_local: 是否使用本地缓存，默认 True
        :param begin_date: 可选，交易日（本地缓存方案）
        :param end_date: 可选，交易日（本地缓存方案）
        :return: history_stock_status，DataFrame。字段含：MARKET_CODE、TRADE_DATE、PRECLOSE、HIGH_LIMITED、LOW_LIMITED、PRICE_HIGH_LMT_RATE、PRICE_LOW_LMT_RATE、IS_ST_SEC、IS_SUSP_SEC、IS_WD_SEC(除息)、IS_XR_SEC(除权) 等
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_history_stock_status(**kwargs)

    def get_bj_code_mapping(
        self,
        local_path: str,
        is_local: bool = True,
    ) -> Any:
        """
        获取北交所存量上市公司股票新旧代码对照表。

        :param local_path: 本地存储路径，绝对路径
        :param is_local: 默认 True：首选本地，失败再服务器；False：以本地为基础增量从服务器取
        :return: bj_code_mapping，DataFrame。字段：OLD_CODE(旧代码)、NEW_CODE(新代码)、SECURITY_NAME、LISTING_DATE(上市日期)
        """
        return self.info_data.get_bj_code_mapping(local_path=local_path, is_local=is_local)

    def get_etf_pcf(self, code_list: List[str]) -> tuple:
        """
        获取指定 ETF 的申赎和成分股数据（沪深交易所）。

        :param code_list: 沪深 ETF 代码列表
        :return: (etf_pcf_info, etf_pcf_constituent)。etf_pcf_info 为 DataFrame，index 为 ETF 代码；etf_pcf_constituent 为 dict，key 为 ETF 代码，value 为成分股 DataFrame。字段见文档 etf_pcf_info / etf_pcf_constituent 说明
        """
        return self.base_data.get_etf_pcf(code_list)

    # ------------------------------- 3.5.4 历史行情数据 -------------------------------

    def query_snapshot(
        self,
        code_list: List[str],
        begin_date: int,
        end_date: int,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        快照历史数据查询。支持北交所/上交所/深交所的可转债、股票、指数、ETF、港股通、ETF 期权等。

        :param code_list: 代码列表
        :param begin_date: 开始日期，8 位整型如 20240101
        :param end_date: 结束日期
        :param begin_time: 可选，时分秒毫秒时间戳，如 90000000 表示 9:00
        :param end_time: 可选，如 172500000 表示 17:25
        :return: snapshot_dict，dict，key 为代码，value 为 DataFrame（column 为快照字段，index 为日期 datetime）。指数为 SnapshotIndex，股票/ETF/可转债为 Snapshot，港股通为 SnapshotHKT，ETF 期权为 SnapshotOption
        """
        kwargs = {
            "code_list": code_list,
            "begin_date": begin_date,
            "end_date": end_date,
        }
        if begin_time is not None:
            kwargs["begin_time"] = begin_time
        if end_time is not None:
            kwargs["end_time"] = end_time
        return self._get_market_data().query_snapshot(**kwargs)

    def query_kline(
        self,
        code_list: List[str],
        begin_date: int,
        end_date: int,
        period: Any,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        K 线历史数据查询，支持全部周期。支持股票/指数/ETF/可转债/ETF 期权及期货（中金所/上期所/大商所/郑商所/上期能源）。

        :param code_list: 代码列表
        :param begin_date: 开始日期
        :param end_date: 结束日期
        :param period: 数据周期 Period，见附录
        :param begin_time: 可选，时分时间戳，如 900、1725
        :param end_time: 可选
        :return: kline_dict，dict，key 为代码，value 为 DataFrame（column 为 Kline 字段，index 为日期）
        """
        kwargs = {
            "code_list": code_list,
            "begin_date": begin_date,
            "end_date": end_date,
            "period": period,
        }
        if begin_time is not None:
            kwargs["begin_time"] = begin_time
        if end_time is not None:
            kwargs["end_time"] = end_time
        return self._get_market_data().query_kline(**kwargs)

    # ------------------------------- 3.5.5 财务数据 -------------------------------

    def get_balance_sheet(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取指定股票列表的上市公司资产负债表数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，报告期
        :param end_date: 可选，报告期
        :return: balance_sheet，dict，key 为 code，value 为 DataFrame。column 为资产负债表字段（如 MARKET_CODE、SECURITY_NAME、STATEMENT_TYPE、REPORT_TYPE、REPORTING_PERIOD、ANN_DATE、各类资产/负债科目等），见附录 balance_sheet 字段说明
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_balance_sheet(**kwargs)

    def get_cash_flow(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取指定股票列表的上市公司现金流量表数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，报告期
        :param end_date: 可选，报告期
        :return: cash_flow，dict，key 为 code，value 为 DataFrame。column 为现金流量表字段（经营/投资/筹资活动现金流等），见附录 cash_flow 字段说明
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_cash_flow(**kwargs)

    def get_income(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取指定股票列表的上市公司利润表数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，报告期
        :param end_date: 可选，报告期
        :return: income，dict，key 为 code，value 为 DataFrame。column 为利润表字段（营业收入、营业成本、净利润、每股收益等），见附录 income 字段说明
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_income(**kwargs)

    def get_profit_express(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取指定股票列表的上市公司业绩快报数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，报告期
        :param end_date: 可选，报告期
        :return: profit_express，DataFrame。字段含 MARKET_CODE、REPORTING_PERIOD、ANN_DATE、ACTUAL_ANN_DATE、TOTAL_ASSETS、NET_PRO_EXCL_MIN_INT_INC、TOT_OPERA_REV、OPERA_PROFIT、EPS_BASIC、ROE_WEIGHTED 等，见附录
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_profit_express(**kwargs)

    def get_profit_notice(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取指定股票列表的上市公司业绩预告数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，报告期
        :param end_date: 可选，报告期
        :return: profit_notice，DataFrame。字段含 MARKET_CODE、SECURITY_NAME、P_TYPECODE(业绩预告类型)、REPORTING_PERIOD、ANN_DATE、P_CHANGE_MAX/MIN、NET_PROFIT_MAX/MIN、P_REASON、P_SUMMARY 等，见附录
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_profit_notice(**kwargs)

    # ------------------------------- 3.5.6 股东股本数据 -------------------------------

    def get_share_holder(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取指定股票列表的上市公司十大股东数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，到期日期
        :param end_date: 可选，到期日期
        :return: share_holder，DataFrame。字段含 ANN_DATE、MARKET_CODE、HOLDER_ENDDATE、HOLDER_TYPE(10 十大股东/20 流通股前十大)、QTY_NUM、HOLDER_NAME、HOLDER_HOLDER_CATEGORY、HOLDER_QUANTITY、HOLDER_PCT、HOLDER_SHARECATEGORYNAME、FLOAT_QTY 等
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_share_holder(**kwargs)

    def get_holder_num(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取指定股票列表的上市公司股东户数数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，股东户数统计截止日期
        :param end_date: 可选
        :return: holder_num，DataFrame。字段含 MARKET_CODE、ANN_DT、HOLDER_ENDDATE、HOLDER_TOTAL_NUM、HOLDER_NUM(A 股股东户数) 等
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_holder_num(**kwargs)

    def get_equity_structure(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取指定股票列表的上市公司股本结构数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，变动日期
        :param end_date: 可选，变动日期
        :return: equity_structure，DataFrame。字段含 MARKET_CODE、ANN_DATE、CHANGE_DATE、SHARE_CHANGE_REASON_STR、EX_CHANGE_DATE、TOT_SHARE、FLOAT_SHARE、各类限售/非流通股等，见附录
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_equity_structure(**kwargs)

    def get_equity_pledge_freeze(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取指定股票列表的上市公司股权冻结/质押数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，公告日期
        :param end_date: 可选，公告日期
        :return: equity_pledge_freeze，dict，key 为 code，value 为 DataFrame。字段含 MARKET_CODE、ANN_DATE、HOLDER_NAME、TOTAL_HOLDING_SHR、FRO_SHARES、BEGIN_DATE、END_DATE、FREEZE_TYPE 等，见附录
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_equity_pledge_freeze(**kwargs)

    def get_equity_restricted(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取指定股票列表的上市公司限售股解禁数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，解禁日期
        :param end_date: 可选，解禁日期
        :return: equity_restricted，dict，key 为 code，value 为 DataFrame。字段含 MARKET_CODE、LIST_DATE(解禁日期)、SHARE_RATIO、SHARE_LST_TYPE_NAME、SHARE_LST、CLOSE_PRICE、SHARE_LST_MARKET_VALUE 等
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_equity_restricted(**kwargs)

    # ------------------------------- 3.5.7 股东权益数据 -------------------------------

    def get_dividend(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取指定股票列表的上市公司分红数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，公告日期
        :param end_date: 可选，公告日期
        :return: dividend，DataFrame。字段含 MARKET_CODE、DIV_PROGRESS(方案进度)、DVD_PER_SHARE_STK、DATE_EQY_RECORD、DATE_EX、DATE_DVD_PAYOUT、LISTINGDATE_OF_DVD_SHR 等，见附录
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_dividend(**kwargs)

    def get_right_issue(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取指定股票列表的上市公司配股数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，公告日期
        :param end_date: 可选，公告日期
        :return: right_issue，DataFrame。字段含 MARKET_CODE、PROGRESS(方案进度)、PRICE、RATIO、AMT_PLAN、AMT_REAL、SHAREB_REG_DATE、EX_DIVIDEND_DATE、LISTED_DATE 等，见附录
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_right_issue(**kwargs)

    # ------------------------------- 3.5.8 融资融券数据 -------------------------------

    def get_margin_summary(
        self,
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取指定日期的融资融券成交汇总数据（全市场）。

        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，交易日
        :param end_date: 可选，交易日
        :return: margin_summary，DataFrame。字段含 TRADE_DATE、SUM_BORROW_MONEY_BALANCE、SUM_PURCH_WITH_BORROW_MONEY、SUM_REPAYMENT_OF_BORROW_MONEY、SUM_SEC_LENDING_BALANCE、SUM_SALES_OF_BORROWED_SEC、SUM_MARGIN_TRADE_BALANCE 等
        """
        kwargs = {"local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_margin_summary(**kwargs)

    def get_margin_detail(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取指定股票列表的融资融券交易明细数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，交易日
        :param end_date: 可选，交易日
        :return: margin_detail，dict，key 为 code，value 为 DataFrame。字段含 MARKET_CODE、SECURITY_NAME、TRADE_DATE、BORROW_MONEY_BALANCE、PURCH_WITH_BORROW_MONEY、REPAYMENT_OF_BORROW_MONEY、SEC_LENDING_BALANCE、SALES_OF_BORROWED_SEC、REPAYMENT_OF_BORROW_SEC、MARGIN_TRADE_BALANCE 等
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_margin_detail(**kwargs)

    # ------------------------------- 3.5.9 交易异动数据 -------------------------------

    def get_long_hu_bang(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取指定股票列表的龙虎榜数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，交易日
        :param end_date: 可选，交易日
        :return: long_hu_bang，DataFrame。字段含 MARKET_CODE、TRADE_DATE、SECURITY_NAME、REASON_TYPE、REASON_TYPE_NAME、CHANGE_RANGE、TRADER_NAME、BUY_AMOUNT、SELL_AMOUNT、FLOW_MARK、TOTAL_AMOUNT、TOTAL_VOLUME 等
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_long_hu_bang(**kwargs)

    def get_block_trading(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取指定股票列表的大宗交易数据。

        :param code_list: 沪深 A 股代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，交易日
        :param end_date: 可选，交易日
        :return: block_trading，DataFrame。字段含 MARKET_CODE、TRADE_DATE、B_SHARE_PRICE、B_SHARE_VOLUME、B_FREQUENCY、BLOCK_AVG_VOLUME、B_SHARE_AMOUNT、B_BUYER_NAME、B_SELLER_NAME 等
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_block_trading(**kwargs)

    # ------------------------------- 3.5.10 期权数据 -------------------------------

    def get_option_basic_info(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Any:
        """
        获取指定期权的基本资料（沪深交易所 ETF 期权）。

        :param code_list: 沪深 ETF 期权代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :return: option_basic_info，DataFrame。字段含 CONTRACT_FULL_NAME、CONTRACT_TYPE(C/P)、DELIVERY_MONTH、EXPIRY_DATE、EXERCISE_PRICE、EXERCISE_END_DATE、START_TRADE_DATE、LISTING_REF_PRICE、LAST_TRADE_DATE、CONTRACT_UNIT、MARKET_CODE 等
        """
        return self.info_data.get_option_basic_info(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_option_std_ctr_specs(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Any:
        """
        获取指定期权标准合约属性（沪深交易所 ETF 期权）。code_list 支持部分 ETF 如 510050.SH、159919.SZ 等。

        :param code_list: 支持沪深 ETF 代码列表（见文档所列）
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :return: option_std_ctr_specs，DataFrame。字段含 EXERCISE_DATE、CONTRACT_UNIT、LAST_TRADING_DATE、POSITION_LIMIT、DELIVERY_METHOD、OPTION_STRIKE_PRICE、MARKET_CODE 等
        """
        return self.info_data.get_option_std_ctr_specs(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_option_mon_ctr_specs(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Any:
        """
        获取指定期权月合约属性变动（沪深交易所 ETF 期权）。

        :param code_list: 沪深 ETF 期权代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :return: option_mon_ctr_specs，DataFrame。字段含 CODE_OLD、CHANGE_DATE、MARKET_CODE、NAME_NEW、EXERCISE_PRICE_NEW、NAME_OLD、CODE_NEW、EXERCISE_PRICE_OLD、UNIT_OLD、UNIT_NEW、CHANGE_REASON 等
        """
        return self.info_data.get_option_mon_ctr_specs(
            code_list, local_path=local_path, is_local=is_local
        )

    # ------------------------------- 3.5.11 ETF 数据 -------------------------------

    def get_fund_share(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取指定 ETF 列表的基金份额数据。

        :param code_list: 沪深 ETF 代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，变动日期
        :param end_date: 可选，变动日期
        :return: fund_share，dict，key 为 code，value 为 DataFrame。字段含 FUND_SHARE、CHANGE_REASON、MARKET_CODE、ANN_DATE、TOTAL_SHARE、CHANGE_DATE、FLOAT_SHARE 等
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_fund_share(**kwargs)

    def get_fund_iopv(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取指定 ETF 列表的每日收盘 IOPV（净值）数据。

        :param code_list: 沪深 ETF 代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，变动日期
        :param end_date: 可选，变动日期
        :return: fund_iopv，dict，key 为 code，value 为 DataFrame。字段含 MARKET_CODE、PRICE_DATE、IOPV_NAV 等
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_fund_iopv(**kwargs)

    # ------------------------------- 3.5.12 交易所指数数据 -------------------------------

    def get_index_constituent(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Dict[str, Any]:
        """
        获取指定交易所指数列表的成分股数据。仅支持常用指数（约 600 多只），无返回则不支持。

        :param code_list: 沪深指数代码列表
        :param local_path: 本地存储路径
        :param is_local: True 仅从本地获取；False 仅从服务器获取（首次建议 False 以获取最新剔除日期）
        :return: index_constituent，dict，key 为 code，value 为 DataFrame。字段含 INDEX_CODE、CON_CODE、INDATE、OUTDATE、INDEX_NAME 等
        """
        return self.info_data.get_index_constituent(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_index_weight(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取指定交易所指数列表的成分股日权重。指数代码支持：000016.SH(上证50)、000300.SH(沪深300)、000905.SH(中证500)、000906.SH(中证800)、000852.SH(中证1000)。

        :param code_list: 指数代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，变动日期
        :param end_date: 可选，变动日期
        :return: index_weight，dict，key 为 code，value 为 DataFrame。字段含 INDEX_CODE、CON_CODE、TRADE_DATE、TOTAL_SHARE、FREE_SHARE_RATIO、CALC_SHARE、WEIGHT_FACTOR、WEIGHT、CLOSE 等
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_index_weight(**kwargs)

    # ------------------------------- 3.5.13 行业指数数据 -------------------------------

    def get_industry_base_info(
        self,
        local_path: str,
        is_local: bool = True,
    ) -> Any:
        """
        获取行业指数的基本信息数据。

        :param local_path: 本地存储路径
        :param is_local: True 仅本地；False 仅服务器（首次建议 False）
        :return: industry_base_info，dict，key 为 code，value 为 DataFrame。字段含 INDEX_CODE、INDUSTRY_CODE、LEVEL_TYPE、LEVEL1_NAME、LEVEL2_NAME、LEVEL3_NAME、IS_PUB、CHANGE_REASON 等
        """
        return self.info_data.get_industry_base_info(
            local_path=local_path, is_local=is_local
        )

    def get_industry_constituent(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Dict[str, Any]:
        """
        获取指定行业指数列表的成分股数据。code_list 建议从 get_industry_base_info 取得。

        :param code_list: 行业指数代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否仅本地/仅服务器，同 get_index_constituent
        :return: industry_constituent，dict，key 为 code，value 为 DataFrame。字段含 INDEX_CODE、CON_CODE、INDATE、OUTDATE、INDEX_NAME 等
        """
        return self.info_data.get_industry_constituent(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_industry_weight(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取指定行业指数列表的成分股日权重数据。

        :param code_list: 行业指数代码列表（来自 get_industry_base_info）
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，交易日期
        :param end_date: 可选，交易日期
        :return: industry_weight，dict，key 为 code，value 为 DataFrame。字段含 WEIGHT、CON_CODE、TRADE_DATE、INDEX_CODE 等
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_industry_weight(**kwargs)

    def get_industry_daily(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取指定行业指数列表的日行情数据。

        :param code_list: 行业指数代码列表（来自 get_industry_base_info）
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，交易日期
        :param end_date: 可选，交易日期
        :return: industry_daily，dict，key 为 code，value 为 DataFrame。字段含 OPEN、HIGH、CLOSE、LOW、AMOUNT、VOLUME、PB、PE、TOTAL_CAP、A_FLOAT_CAP、INDEX_CODE、PRE_CLOSE、TRADE_DATE 等
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_industry_daily(**kwargs)

    # ------------------------------- 3.5.14 可转债数据 -------------------------------

    def get_kzz_issuance(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Dict[str, Any]:
        """
        获取指定可转债列表的可转债发行数据。

        :param code_list: 可转债代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :return: kzz_issuance，dict，key 为 code，value 为 DataFrame。字段含 MARKET_CODE、STOCK_CODE、LISTED_DATE、PLAN_SCHEDULE、CLAUSE_INI_CONV_PRICE、COUPON_RATE 等大量发行相关字段，见附录
        """
        return self.info_data.get_kzz_issuance(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_kzz_share(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Dict[str, Any]:
        """
        获取指定可转债列表的可转债份额数据。

        :param code_list: 可转债代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :return: kzz_share，dict，key 为 code，value 为 DataFrame。字段含 CHANGE_DATE、ANN_DATE、MARKET_CODE、BOND_SHARE、CONV_SHARE、CHANGE_REASON 等
        """
        return self.info_data.get_kzz_share(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_kzz_conv(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Dict[str, Any]:
        """
        获取指定可转债列表的可转债转股数据。

        :param code_list: 可转债代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :return: kzz_conv，dict，key 为 code，value 为 DataFrame。字段含 MARKET_CODE、CONV_PRICE、CONV_START_DATE、CONV_END_DATE、FORCED_CONV_DATE 等
        """
        return self.info_data.get_kzz_conv(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_kzz_conv_change(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Dict[str, Any]:
        """
        获取指定可转债列表的可转债转股变动数据。

        :param code_list: 可转债代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :return: kzz_conv_change，dict，key 为 code，value 为 DataFrame。字段含 MARKET_CODE、CHANGE_DATE、ANN_DATE、CONV_PRICE、CHANGE_REASON 等
        """
        return self.info_data.get_kzz_conv_change(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_kzz_corr(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Dict[str, Any]:
        """
        获取指定可转债列表的可转债修正数据。

        :param code_list: 可转债代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :return: kzz_corr，dict，key 为 code，value 为 DataFrame。字段含特别修正起止时间、触发比例、修正次数等，见附录
        """
        return self.info_data.get_kzz_corr(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_kzz_call(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Dict[str, Any]:
        """
        获取指定可转债列表的可转债赎回数据。

        :param code_list: 可转债代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :return: kzz_call，dict，key 为 code，value 为 DataFrame。字段含 MARKET_CODE、CALL_PRICE、BEGIN_DATE、END_DATE、TRI_RATIO(触发比例) 等
        """
        return self.info_data.get_kzz_call(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_kzz_put(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Dict[str, Any]:
        """
        获取指定可转债列表的可转债回售数据。

        :param code_list: 可转债代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :return: kzz_put，dict，key 为 code，value 为 DataFrame。字段含 MARKET_CODE、PUT_PRICE、BEGIN_DATE、END_DATE、TRI_RATIO 等
        """
        return self.info_data.get_kzz_put(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_kzz_put_call_item(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Dict[str, Any]:
        """
        获取指定可转债列表的可转债回售赎回条款数据。

        :param code_list: 可转债代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :return: kzz_put_call_item，dict，key 为 code，value 为 DataFrame。字段含无条件/有条件回售与赎回相关条款字段，见附录
        """
        return self.info_data.get_kzz_put_call_item(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_kzz_put_explanation(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Dict[str, Any]:
        """
        获取指定可转债列表的可转债回售条款执行说明数据。

        :param code_list: 可转债代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :return: kzz_put_explanation，dict，key 为 code，value 为 DataFrame。字段含回售资金到账日、回售价格、回售公告日、回售履行结果公告日等，见附录
        """
        return self.info_data.get_kzz_put_explanation(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_kzz_call_explanation(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Dict[str, Any]:
        """
        获取指定可转债列表的可转债赎回条款执行说明数据。

        :param code_list: 可转债代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :return: kzz_call_explanation，dict，key 为 code，value 为 DataFrame。字段含赎回日、赎回价格、赎回公告日、赎回履行结果公告日、赎回原因等，见附录
        """
        return self.info_data.get_kzz_call_explanation(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_kzz_suspend(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
    ) -> Dict[str, Any]:
        """
        获取指定可转债列表的可转债停复牌信息数据。

        :param code_list: 可转债代码列表
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :return: kzz_suspend，dict，key 为 code，value 为 DataFrame。字段含 MARKET_CODE、SUSPEND_DATE、SUSPEND_TYPE、RESUMP_DATE、CHANGE_REASON、RESUMP_TIME 等
        """
        return self.info_data.get_kzz_suspend(
            code_list, local_path=local_path, is_local=is_local
        )

    # ------------------------------- 3.5.15 国债收益率数据 -------------------------------

    def get_treasury_yield(
        self,
        code_list: List[str],
        local_path: str,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取指定期限的国债收益率数据。

        :param code_list: 期限列表，可选 'm3','m6','y1','y2','y3','y5','y7','y10','y30'（注意文档中 y10 为 20 年，以实际 API 为准）
        :param local_path: 本地存储路径
        :param is_local: 是否使用本地缓存
        :param begin_date: 可选，变动日期
        :param end_date: 可选，变动日期
        :return: treasury_yield，dict，key 为期限，value 为 DataFrame，column 含 YIELD（国债收益率），index 为日期
        """
        # 底层接口第一个必选参数为 code_list（期限代码列表），与封装层的 code_list 对应
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_treasury_yield(**kwargs)


# ------------------------------- 数据目录约定（推荐组织形式） -------------------------------
#
# 推荐在脚本同目录下用统一根目录组织数据，便于回测与增量更新：
#
#   data/
#   ├── klines/
#   │   └── daily.parquet          # 日线，(code, date) 多级索引
#   ├── financial/
#   │   ├── balance_sheet.parquet   # 资产负债表，(code, report_period)
#   │   ├── cash_flow.parquet      # 现金流量表
#   │   ├── income.parquet         # 利润表
#   │   ├── profit_express.parquet # 业绩快报，整表
#   │   └── profit_notice.parquet  # 业绩预告，整表
#   └── equity/                    # 股东/股本/分红/配股
#       ├── share_holder.parquet   # 十大股东，整表，按 MARKET_CODE 查单股
#       ├── holder_num.parquet     # 股东户数，整表
#       ├── equity_structure.parquet   # 股本结构，整表
#       ├── equity_pledge_freeze.parquet  # 股权冻结质押，(code, report_period)
#       ├── equity_restricted.parquet     # 限售股解禁，(code, report_period)
#       ├── dividend.parquet       # 分红，整表
#       └── right_issue.parquet    # 配股，整表
#
# ------------------------------- K 线 dict 的保存/读取（量化常用） -------------------------------


def save_kline_dict_to_parquet(
    kline_dict: Dict[str, pd.DataFrame],
    path: str,
    single_file: bool = True,
    partitioned: bool = False,
) -> None:
    """
    将 query_kline 返回的 kline_dict 保存为 Parquet，便于量化回测与筛选。

    三种方式：
    - partitioned=True（推荐）：按日期分区的目录结构，增量更新不会爆内存，回测时可按日期过滤加载。
    - single_file=True：一个文件，多级索引 (code, date)。数据量小时方便，但数据量大时会爆内存。
    - single_file=False：目录下每只股票一个 .parquet 文件。单股更新、按需读取某几只时更合适。

    :param kline_dict: code -> DataFrame(OPEN/HIGH/CLOSE/LOW 等)，如 query_kline 的返回值
    :param path: 文件路径或目录路径
    :param single_file: True 存成单文件，False 按股票分文件（partitioned=True 时忽略此参数）
    :param partitioned: True 按日期分区存储到目录（推荐大数据量使用）
    """
    if not kline_dict:
        return

    # 公共步骤：将 kline_dict 转为统一的 DataFrame 列表
    # AmazingData API 固定返回 kline_time 作为日期列（datetime64 类型），直接使用
    def _prepare_parts():
        parts = []
        for code, df in kline_dict.items():
            if df is None or df.empty:
                continue
            df = df.copy().reset_index(drop=True)
            if "kline_time" not in df.columns:
                print(f"[kline] 警告: 股票 {code} 未找到 kline_time 列，实际列名: {df.columns.tolist()}，跳过")
                continue
            df["date"] = pd.to_datetime(df["kline_time"]).dt.strftime('%Y%m%d').astype('int64')
            df["code"] = code
            parts.append(df)
        return parts

    if partitioned:
        # 按日期分区存储，不会爆内存
        parts = _prepare_parts()
        if not parts:
            return
        out = pd.concat(parts, axis=0, ignore_index=True)
        out = out.drop_duplicates(subset=["code", "date"], keep="last")
        out["code"] = out["code"].astype("string")
        try:
            out.to_parquet(
                path, index=False, partition_cols=["date"],
                engine="pyarrow", existing_data_behavior='delete_matching',
            )
        except TypeError:
            out.to_parquet(
                path, index=False, partition_cols=["date"], engine="pyarrow",
            )
    elif single_file:
        parts = _prepare_parts()
        if not parts:
            return
        out = pd.concat(parts, axis=0, ignore_index=True)
        if "date" in out.columns:
            out = out.set_index(["code", "date"])
        out.to_parquet(path, index=True)
    else:
        os.makedirs(path, exist_ok=True)
        for code, df in kline_dict.items():
            if df is None or df.empty:
                continue
            # 文件名用点替换为下划线，避免部分系统歧义
            safe = code.replace(".", "_")
            df.to_parquet(os.path.join(path, f"{safe}.parquet"), index=True)


def load_parquet_to_kline_dict(
    path: str,
    codes: Optional[List[str]] = None,
    single_file: bool = True,
) -> Dict[str, pd.DataFrame]:
    """
    从 Parquet 读回 kline_dict（code -> DataFrame）。

    :param path: 单文件路径或目录路径（与保存时一致）
    :param codes: 若指定则只读这些代码，None 表示全部
    :param single_file: 与 save 时一致
    :return: kline_dict
    """
    if single_file:
        df = pd.read_parquet(path)
        if df.index.names and "code" in (df.index.names or []):
            out = {}
            for code in df.index.get_level_values("code").unique():
                out[code] = df.loc[code].copy()
        elif "code" in df.columns:
            out = {}
            for code, grp in df.groupby("code"):
                grp = grp.copy()
                if "date" in grp.columns:
                    grp = grp.set_index("date")
                out[code] = grp
        else:
            out = {"": df}
        if codes is not None:
            out = {k: v for k, v in out.items() if k in codes}
        return out
    else:
        out = {}
        for f in os.listdir(path):
            if not f.endswith(".parquet"):
                continue
            code = f.replace(".parquet", "").replace("_", ".")
            if codes is not None and code not in codes:
                continue
            out[code] = pd.read_parquet(os.path.join(path, f))
        return out


# ------------------------------- 财报 dict 的保存/读取（与上面目录约定一致） -------------------------------


def _report_period_column(df: pd.DataFrame) -> Optional[str]:
    """财报 DataFrame 中用作报告期的列名。"""
    for col in ("REPORTING_PERIOD", "reporting_period", "报告期", "report_period"):
        if col in df.columns:
            return col
    return None


def save_financial_dict_to_parquet(
    financial_dict: Dict[str, pd.DataFrame],
    path: str,
    period_col: Optional[str] = None,
) -> None:
    """
    将 get_balance_sheet / get_cash_flow / get_income 返回的 dict(code -> DataFrame) 存为单文件 Parquet。
    索引为 (code, 报告期)，便于按标的或按报告期筛选。

    :param financial_dict: code -> DataFrame（含 REPORTING_PERIOD 等列）
    :param path: 输出文件路径，如 'data/financial/balance_sheet.parquet'
    :param period_col: 报告期列名，None 时自动从 REPORTING_PERIOD 等推断
    """
    if not financial_dict:
        return
    parts = []
    for code, df in tqdm.tqdm(financial_dict.items(), desc="Saving financial dict to parquet"):
        if df is None or df.empty:
            continue
        df = df.copy()
        df["code"] = code
        col = period_col or _report_period_column(df)
        if col:
            df["report_period"] = df[col]
        else:
            df = df.reset_index()
            if "index" in df.columns:
                df = df.rename(columns={"index": "report_period"})
        parts.append(df)
    out = pd.concat(parts, axis=0, ignore_index=True)
    if "report_period" in out.columns:
        out = out.set_index(["code", "report_period"])
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    out.to_parquet(path, index=True)


def load_financial_parquet_to_dict(
    path: str,
    codes: Optional[List[str]] = None,
) -> Dict[str, pd.DataFrame]:
    """
    从 Parquet 读回财报 dict（code -> DataFrame）。与 save_financial_dict_to_parquet 对应。

    :param path: 如 'data/financial/balance_sheet.parquet'
    :param codes: 只读这些代码，None 表示全部
    :return: dict, key=code, value=DataFrame
    """
    df = pd.read_parquet(path)
    if df.index.names and "code" in (df.index.names or []):
        out = {code: df.loc[code].copy() for code in df.index.get_level_values("code").unique()}
    elif "code" in df.columns:
        out = {}
        for code, grp in df.groupby("code"):
            out[code] = grp.copy()
            if "report_period" in grp.columns:
                out[code] = out[code].set_index("report_period")
    else:
        out = {"": df}
    if codes is not None:
        out = {k: v for k, v in out.items() if k in codes}
    return out


if __name__ == "__main__":
    AmazingDataClient.login(username="10100223966", password="10100223966@2026", host="140.206.44.234", port=8600)
    client = AmazingDataClient()

    # 获取所有 A 股日线特定日期并按分区保存为 Parquet
    all_a_codes = client.get_code_list(security_type="EXTRA_STOCK_A")  # 沪深北 A 股
    batch_size = 1000
    begin_date = 20260211  # 你可以在这里修改需要下载的特定日期
    end_date =   20260301
    kline_dict = {}  # 合并多批结果
    for i in range(0, len(all_a_codes), batch_size):
        batch = all_a_codes[i : i + batch_size]
        batch_dict = client.query_kline(
            code_list=batch,
            begin_date=begin_date,
            end_date=end_date,
            period=ad.constant.Period.day.value,  # 日线
        )
        kline_dict.update(batch_dict)
        print(f"[{begin_date}到{end_date}] 已拉取 {len(kline_dict)} 只日线...")
        
    # 保存：使用分区模式覆盖保存到标准日线目录
    KLINE_DAILY_PATH = "/data/stock_data/xysz_data/base_data/klines_daily_dir"
    save_kline_dict_to_parquet(
        kline_dict, 
        path=KLINE_DAILY_PATH, 
        single_file=False, 
        partitioned=True
    )
    print(f"[{begin_date}到{end_date}] 已保存 {len(kline_dict)} 只, 全 A 股数量: {len(all_a_codes)}")

    # 获取财报数据并按统一目录组织保存（见上方 数据目录约定）
    financial_dir = "/data/stock_data/xysz_data"
    os.makedirs(financial_dir, exist_ok=True)

    report_codes = client.get_code_list(security_type="EXTRA_STOCK_A")
    report_begin, report_end = 19900101, 20260215

    balance_sheet = {}
    for code in tqdm.tqdm(report_codes, desc="资产负债表"):
        one = client.get_balance_sheet(
            code_list=[code],
            local_path=financial_dir,
            is_local=False,
            begin_date=report_begin,
            end_date=report_end,
        )
        if one and isinstance(one, dict):
            balance_sheet.update(one)
    save_financial_dict_to_parquet(balance_sheet, os.path.join(financial_dir, "balance_sheet.parquet"))
    print(f"资产负债表: {len(balance_sheet)} 只, 已保存")

    cash_flow = {}
    for code in tqdm.tqdm(report_codes, desc="现金流量表"):
        one = client.get_cash_flow(
            code_list=[code],
            local_path=financial_dir,
            is_local=False,
            begin_date=report_begin,
            end_date=report_end,
        )
        if one and isinstance(one, dict):
            cash_flow.update(one)
    save_financial_dict_to_parquet(cash_flow, os.path.join(financial_dir, "cash_flow.parquet"))
    print(f"现金流量表: {len(cash_flow)} 只, 已保存")

    income = {}
    for code in tqdm.tqdm(report_codes, desc="利润表"):
        one = client.get_income(
            code_list=[code],
            local_path=financial_dir,
            is_local=False,
            begin_date=report_begin,
            end_date=report_end,
        )
        if one and isinstance(one, dict):
            income.update(one)
    save_financial_dict_to_parquet(income, os.path.join(financial_dir, "income.parquet"))
    print(f"利润表: {len(income)} 只, 已保存")

    profit_express_list = []
    for code in tqdm.tqdm(report_codes, desc="业绩快报"):
        one = client.get_profit_express(
            code_list=[code],
            local_path=financial_dir,
            is_local=False,
            begin_date=report_begin,
            end_date=report_end,
        )
        if one is not None and isinstance(one, pd.DataFrame) and not one.empty:
            profit_express_list.append(one)
    profit_express = pd.concat(profit_express_list, ignore_index=True) if profit_express_list else pd.DataFrame()
    if not profit_express.empty:
        profit_express.to_parquet(os.path.join(financial_dir, "profit_express.parquet"), index=True)
    print(f"业绩快报: {len(profit_express)} 行, 已保存")

    profit_notice_list = []
    for code in tqdm.tqdm(report_codes, desc="业绩预告"):
        one = client.get_profit_notice(
            code_list=[code],
            local_path=financial_dir,
            is_local=False,
            begin_date=report_begin,
            end_date=report_end,
        )
        if one is not None and isinstance(one, pd.DataFrame) and not one.empty:
            profit_notice_list.append(one)
    profit_notice = pd.concat(profit_notice_list, ignore_index=True) if profit_notice_list else pd.DataFrame()
    if not profit_notice.empty:
        profit_notice.to_parquet(os.path.join(financial_dir, "profit_notice.parquet"), index=True)
    print(f"业绩预告: {len(profit_notice)} 行, 已保存")

    # 股东/股本/分红/配股：拉取并保存到 data/equity/
    equity_dir = "/data/stock_data/xysz_data"
    os.makedirs(equity_dir, exist_ok=True)
    equity_codes = client.get_code_list(security_type="EXTRA_STOCK_A")  # 全 A 股；可改为 report_codes 做小范围测试
    equity_begin, equity_end = 19900101, 20260215

    share_holder_list = []
    for code in tqdm.tqdm(equity_codes, desc="十大股东"):
        one = client.get_share_holder(
            code_list=[code],
            local_path=equity_dir,
            is_local=False,
            begin_date=equity_begin,
            end_date=equity_end,
        )
        if one is not None and isinstance(one, pd.DataFrame) and not one.empty:
            share_holder_list.append(one)
    share_holder = pd.concat(share_holder_list, ignore_index=True) if share_holder_list else pd.DataFrame()
    if not share_holder.empty:
        share_holder.to_parquet(os.path.join(equity_dir, "share_holder.parquet"), index=True)
    print(f"十大股东: {len(share_holder)} 行, 已保存")

    holder_num_list = []
    for code in tqdm.tqdm(equity_codes, desc="股东户数"):
        one = client.get_holder_num(
            code_list=[code],
            local_path=equity_dir,
            is_local=False,
            begin_date=equity_begin,
            end_date=equity_end,
        )
        if one is not None and isinstance(one, pd.DataFrame) and not one.empty:
            holder_num_list.append(one)
    holder_num = pd.concat(holder_num_list, ignore_index=True) if holder_num_list else pd.DataFrame()
    if not holder_num.empty:
        holder_num.to_parquet(os.path.join(equity_dir, "holder_num.parquet"), index=True)
    print(f"股东户数: {len(holder_num)} 行, 已保存")

    equity_structure_list = []
    for code in tqdm.tqdm(equity_codes, desc="股本结构"):
        one = client.get_equity_structure(
            code_list=[code],
            local_path=equity_dir,
            is_local=False,
            begin_date=equity_begin,
            end_date=equity_end,
        )
        if one is not None and isinstance(one, pd.DataFrame) and not one.empty:
            equity_structure_list.append(one)
    equity_structure = pd.concat(equity_structure_list, ignore_index=True) if equity_structure_list else pd.DataFrame()
    if not equity_structure.empty:
        equity_structure.to_parquet(os.path.join(equity_dir, "equity_structure.parquet"), index=True)
    print(f"股本结构: {len(equity_structure)} 行, 已保存")

    equity_pledge_freeze = {}
    for code in tqdm.tqdm(equity_codes, desc="股权冻结质押"):
        one = client.get_equity_pledge_freeze(
            code_list=[code],
            local_path=equity_dir,
            is_local=False,
            begin_date=equity_begin,
            end_date=equity_end,
        )
        if one and isinstance(one, dict):
            equity_pledge_freeze.update(one)
    save_financial_dict_to_parquet(
        equity_pledge_freeze,
        os.path.join(equity_dir, "equity_pledge_freeze.parquet"),
        period_col="ANN_DATE",
    )
    print(f"股权冻结质押: {len(equity_pledge_freeze)} 只, 已保存")

    equity_restricted = {}
    for code in tqdm.tqdm(equity_codes, desc="限售股解禁"):
        one = client.get_equity_restricted(
            code_list=[code],
            local_path=equity_dir,
            is_local=False,
            begin_date=equity_begin,
            end_date=equity_end,
        )
        if one and isinstance(one, dict):
            equity_restricted.update(one)
    save_financial_dict_to_parquet(
        equity_restricted,
        os.path.join(equity_dir, "equity_restricted.parquet"),
        period_col="LIST_DATE",
    )
    print(f"限售股解禁: {len(equity_restricted)} 只, 已保存")

    dividend_list = []
    for code in tqdm.tqdm(equity_codes, desc="分红"):
        one = client.get_dividend(
            code_list=[code],
            local_path=equity_dir,
            is_local=False,
            begin_date=equity_begin,
            end_date=equity_end,
        )
        if one is not None and isinstance(one, pd.DataFrame) and not one.empty:
            dividend_list.append(one)
    dividend = pd.concat(dividend_list, ignore_index=True) if dividend_list else pd.DataFrame()
    if not dividend.empty:
        dividend.to_parquet(os.path.join(equity_dir, "dividend.parquet"), index=True)
    print(f"分红: {len(dividend)} 行, 已保存")

    right_issue_list = []
    for code in tqdm.tqdm(equity_codes, desc="配股"):
        one = client.get_right_issue(
            code_list=[code],
            local_path=equity_dir,
            is_local=False,
            begin_date=equity_begin,
            end_date=equity_end,
        )
        if one is not None and isinstance(one, pd.DataFrame) and not one.empty:
            right_issue_list.append(one)
    right_issue = pd.concat(right_issue_list, ignore_index=True) if right_issue_list else pd.DataFrame()
    if not right_issue.empty:
        right_issue.to_parquet(os.path.join(equity_dir, "right_issue.parquet"), index=True)
    print(f"配股: {len(right_issue)} 行, 已保存")

    # ------------------- 复权因子 -------------------
    # get_backward_factor / get_adj_factor 不支持 begin_date/end_date，
    # 但会以 local_path 为缓存目录做增量更新；is_local=False 表示始终从服务器取最新并刷新本地缓存。
    # 保存形式：宽表 parquet，行索引=交易日期，列=股票代码，便于按日期切片使用。
    adj_dir = "/data/stock_data/xysz_data/base_data"
    os.makedirs(adj_dir, exist_ok=True)
    adj_codes = client.get_code_list(security_type="EXTRA_STOCK_A")

    print("[复权因子] 开始下载后复权因子 (backward_factor)...")
    backward_factor_df = client.get_backward_factor(
        code_list=adj_codes,
        local_path=adj_dir,
        is_local=False,  # False：始终从服务器获取最新并刷新本地缓存
    )
    if backward_factor_df is not None and not backward_factor_df.empty:
        backward_factor_df.to_parquet(os.path.join(adj_dir, "backward_factor.parquet"), index=True)
        print(f"[复权因子] 后复权因子已保存: {backward_factor_df.shape[0]} 个交易日, {backward_factor_df.shape[1]} 只股票")
    else:
        print("[复权因子] 后复权因子无数据，跳过保存")

    print("[复权因子] 开始下载单次复权因子 (adj_factor)...")
    adj_factor_df = client.get_adj_factor(
        code_list=adj_codes,
        local_path=adj_dir,
        is_local=False,
    )
    if adj_factor_df is not None and not adj_factor_df.empty:
        adj_factor_df.to_parquet(os.path.join(adj_dir, "adj_factor.parquet"), index=True)
        print(f"[复权因子] 单次复权因子已保存: {adj_factor_df.shape[0]} 个交易日, {adj_factor_df.shape[1]} 只股票")
    else:
        print("[复权因子] 单次复权因子无数据，跳过保存")

    # 国债收益率（原逻辑）
    # print(client.get_treasury_yield(code_list=["y10"], local_path="", is_local=True, begin_date=20250101, end_date=20260209))
# ------------------------------- 附录常量参考（便于代码中使用） -------------------------------
# security_type(沪深北): EXTRA_STOCK_A, SH_A, SZ_A, BJ_A, EXTRA_STOCK_A_SH_SZ, EXTRA_INDEX_A, EXTRA_ETF, EXTRA_KZZ, EXTRA_HKT, EXTRA_GLRA 等
# security_type(期货): EXTRA_FUTURE, ZJ_FUTURE, SQ_FUTURE, DS_FUTURE, ZS_FUTURE, SN_FUTURE
# security_type(期权): EXTRA_ETF_OP, SH_OPTION, SZ_OPTION
# market: SH, SZ, BJ, SHF, CFE, DCE, CZC, INE, SHN, SZN, HK
# Period: ad.constant.Period.min1.value, Period.day.value, Period.snapshot.value, Period.snapshot_future.value, Period.snapshotoption.value 等
# 实时订阅示例：sub = AmazingDataClient.get_subscribe_data(); @sub.register(code_list=..., period=ad.constant.Period.snapshot.value); def on_snap(data, period): ...; sub.run()
