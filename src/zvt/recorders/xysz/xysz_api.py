# -*- coding: utf-8 -*-
"""
中国银河证券 星耀数智 AmazingData - ZVT 对应 API 封装
"""
import os
import logging
from typing import Optional, List, Dict, Any

import pandas as pd

logger = logging.getLogger(__name__)


def _get_ad():
    """延迟导入 AmazingData，避免模块加载时就初始化 C++ SWIG 库。"""
    try:
        import AmazingData as ad
        return ad
    except ImportError:
        return None


class AmazingDataClient:
    """
    星耀数智 AmazingData 全量接口封装类。
    """

    def __init__(self, calendar: Optional[List] = None):
        ad = _get_ad()
        if ad is None:
            raise ImportError("Please install AmazingData package first.")

        self._base_data = None
        self._info_data = None
        self._market_data = None
        self._calendar = calendar

    # ------------------------------- 基础接口 -------------------------------

    @staticmethod
    def login(
        username: str,
        password: str,
        host: str,
        port: int,
    ) -> None:
        ad = _get_ad()
        if ad is None:
            raise ImportError("Please install AmazingData package first.")
        ad.login(username=username, password=password, host=host, port=port)

    @staticmethod
    def logout(username: str) -> None:
        ad = _get_ad()
        if ad:
            ad.logout(username=username)

    # ------------------------------- 内部获取子对象 -------------------------------

    @property
    def base_data(self) -> Any:
        if self._base_data is None:
            self._base_data = _get_ad().BaseData()
        return self._base_data

    @property
    def info_data(self) -> Any:
        if self._info_data is None:
            self._info_data = _get_ad().InfoData()
        return self._info_data

    def _get_market_data(self) -> Any:
        if self._market_data is None:
            cal = self._calendar
            if cal is None:
                cal = self.base_data.get_calendar()
            self._market_data = _get_ad().MarketData(cal)
        return self._market_data

    # ------------------------------- 3.5.2 基础数据 -------------------------------

    def get_code_info(self, security_type: str = "EXTRA_STOCK_A") -> Any:
        return self.base_data.get_code_info(security_type=security_type)

    def get_code_list(self, security_type: str = "EXTRA_STOCK_A") -> List[str]:
        return self.base_data.get_code_list(security_type=security_type)

    def get_calendar(
        self,
        data_type: str = "str",
        market: str = "SH",
    ) -> List:
        return self.base_data.get_calendar(data_type=data_type, market=market)
        
    def get_stock_basic(self, code_list: List[str]) -> Any:
        return self.info_data.get_stock_basic(code_list)
        
    def get_backward_factor(
        self,
        code_list: List[str],
        local_path: str = None,
        is_local: bool = True,
    ) -> Any:
        """
        获取后复权因子
        """
        return self.base_data.get_backward_factor(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_adj_factor(
        self,
        code_list: List[str],
        local_path: str = None,
        is_local: bool = True,
    ) -> Any:
        """
        获取单次复权因子
        """
        return self.base_data.get_adj_factor(
            code_list, local_path=local_path, is_local=is_local
        )

    def get_history_stock_status(
        self,
        code_list: List[str],
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取历史证券状态（ST、停牌等）
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_history_stock_status(**kwargs)

    # ------------------------------- 3.5.4 历史行情数据 -------------------------------

    def query_kline(
        self,
        code_list: List[str],
        begin_date: int,
        end_date: int,
        period: Any,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Dict[str, Any]:
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
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        获取指定股票列表的上市公司资产负债表数据。
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
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        获取指定股票列表的上市公司现金流量表数据。
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
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        获取指定股票列表的上市公司利润表数据。
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_income(**kwargs)

    # ------------------------------- 3.5.6 股东股本数据 -------------------------------

    def get_share_holder(
        self,
        code_list: List[str],
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取十大股东数据
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
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取股东户数
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
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取股本结构
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
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取股权冻结/质押
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
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取限售股解禁
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
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取分红数据
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
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取配股数据
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
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取融资融券成交汇总
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
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取融资融券交易明细
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
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取龙虎榜数据
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
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Any:
        """
        获取大宗交易数据
        """
        kwargs = {"code_list": code_list, "local_path": local_path, "is_local": is_local}
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_block_trading(**kwargs)

    # ------------------------------- 3.5.13 行业指数数据 -------------------------------

    def get_industry_base_info(
        self,
        local_path: str = None,
        is_local: bool = True,
    ) -> Any:
        """
        获取行业指数的基本信息数据。

        :return: industry_base_info，dict，key 为 code，value 为 DataFrame。
                 字段含 INDEX_CODE、INDUSTRY_CODE、LEVEL_TYPE、LEVEL1_NAME、LEVEL2_NAME、LEVEL3_NAME、IS_PUB、CHANGE_REASON 等
        """
        kwargs = {"is_local": is_local}
        if local_path:
            kwargs["local_path"] = local_path
        return self.info_data.get_industry_base_info(**kwargs)

    def get_industry_constituent(
        self,
        code_list: List[str],
        local_path: str = None,
        is_local: bool = True,
    ) -> Dict[str, Any]:
        """
        获取指定行业指数列表的成分股数据。

        :param code_list: 行业指数代码列表（来自 get_industry_base_info）
        :return: dict，key 为 code，value 为 DataFrame。字段含 INDEX_CODE、CON_CODE、INDATE、OUTDATE、INDEX_NAME 等
        """
        kwargs = {"code_list": code_list, "is_local": is_local}
        if local_path:
            kwargs["local_path"] = local_path
        return self.info_data.get_industry_constituent(**kwargs)

    def get_industry_daily(
        self,
        code_list: List[str],
        local_path: str = None,
        is_local: bool = True,
        begin_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取指定行业指数列表的日行情数据。

        :param code_list: 行业指数代码列表（来自 get_industry_base_info）
        :return: dict，key 为 code，value 为 DataFrame。
                 字段含 OPEN、HIGH、CLOSE、LOW、AMOUNT、VOLUME、PB、PE、TOTAL_CAP、A_FLOAT_CAP、INDEX_CODE、PRE_CLOSE、TRADE_DATE 等
        """
        kwargs = {"code_list": code_list, "is_local": is_local}
        if local_path:
            kwargs["local_path"] = local_path
        if begin_date is not None:
            kwargs["begin_date"] = begin_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        return self.info_data.get_industry_daily(**kwargs)


# Global instance management
_client_instance = None

def get_xysz_client() -> AmazingDataClient:
    global _client_instance
    if _client_instance is None:
        # 硬编码账户信息
        user = os.environ.get("AMAZING_DATA_USER", "10100223966")
        pwd = os.environ.get("AMAZING_DATA_PASSWORD", "10100223966@2026")
        host = os.environ.get("AMAZING_DATA_HOST", "140.206.44.234")
        port = int(os.environ.get("AMAZING_DATA_PORT", 8600))
        
        logger.info(f"Logging in to AmazingData as {user}")
        AmazingDataClient.login(username=user, password=pwd, host=host, port=port)
        _client_instance = AmazingDataClient()
            
    return _client_instance

def ensure_login(user, pwd, host="140.206.44.234", port=8600):
    global _client_instance
    AmazingDataClient.login(username=user, password=pwd, host=host, port=int(port))
    _client_instance = AmazingDataClient()
    return _client_instance
