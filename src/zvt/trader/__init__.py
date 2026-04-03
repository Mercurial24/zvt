# -*- coding: utf-8 -*-
from enum import Enum
from typing import Union, List

import pandas as pd

from zvt.contract import IntervalLevel
from zvt.utils.decorator import to_string


class TradingSignalType(Enum):
    """交易信号类型：代表策略大脑发出的投资意图"""
    open_long = "open_long"      # 开多（买入）
    open_short = "open_short"    # 开空（卖空融券）
    keep_long = "keep_long"      # 保持做多（通常不动作）
    keep_short = "keep_short"    # 保持做空（通常不动作）
    close_long = "close_long"    # 平多（卖出手中持仓）
    close_short = "close_short"  # 平空（买回还券）


class OrderType(Enum):
    """柜台报单类型：代表物理层面上要对券商执行的 API 发单动作"""
    order_long = "order_long"              # 对应券商买入单
    order_short = "order_short"            # 对应券商卖空单
    order_close_long = "order_close_long"  # 对应券商平仓卖出
    order_close_short = "order_close_short"# 对应券商买回归还


def trading_signal_type_to_order_type(trading_signal_type):
    if trading_signal_type == TradingSignalType.open_long:
        return OrderType.order_long
    elif trading_signal_type == TradingSignalType.open_short:
        return OrderType.order_short
    elif trading_signal_type == TradingSignalType.close_long:
        return OrderType.order_close_long
    elif trading_signal_type == TradingSignalType.close_short:
        return OrderType.order_close_short


@to_string
class TradingSignal:
    def __init__(
        self,
        entity_id: str,
        due_timestamp: Union[str, pd.Timestamp],
        happen_timestamp: Union[str, pd.Timestamp],
        trading_level: IntervalLevel,
        trading_signal_type: TradingSignalType,
        position_pct: float = None,
        order_money: float = None,
        order_amount: int = None,
    ):
        """

        :param entity_id: the entity id
        :param due_timestamp: the signal due time
        :param happen_timestamp: the time when generating the signal
        :param trading_level: the level
        :param trading_signal_type:
        :param position_pct: percentage of account to order
        :param order_money: money to order
        :param order_amount: amount to order
        """
        self.entity_id = entity_id
        self.due_timestamp = due_timestamp
        self.happen_timestamp = happen_timestamp
        self.trading_level = trading_level
        self.trading_signal_type = trading_signal_type

        if len([x for x in (position_pct, order_money, order_amount) if x is not None]) != 1:
            assert False
        # use position_pct or order_money or order_amount
        self.position_pct = position_pct
        # when close the position,just use position_pct
        self.order_money = order_money
        self.order_amount = order_amount


class TradingListener(object):
    """交易监听器：订阅策略执行周期的事件（观察者模式）。任何想要响应开盘/信号等事件的类（如Account）都需要继承这个接口。"""
    def on_trading_open(self, timestamp):
        """每日（或每周期）开盘时的回调，用于结算资产或初始化环境"""
        raise NotImplementedError

    def on_trading_signals(self, trading_signals: List[TradingSignal]):
        """核心枢纽：收到策略大脑产生的一批交易信号，开始在实盘或回测进行消化和下单"""
        raise NotImplementedError

    def on_trading_close(self, timestamp):
        """收盘时的回调，用于清算持仓身价、统计日收益"""
        raise NotImplementedError

    def on_trading_finish(self, timestamp):
        """数据序列全部跑完时的回调（用于画图或最后收尾）"""
        raise NotImplementedError

    def on_trading_error(self, timestamp, error):
        """发生错误时的回调"""
        raise NotImplementedError


class AccountService(TradingListener):
    """
    账户服务基类：这是系统的【交易执行之手】。
    作为监听器，它消化收到的买卖信号；同时作为账户，它掌管资产查询和向下（券商柜台/模拟撮合引擎）发送委托的方法。
    """
    def get_positions(self):
        """获取账户当前所有持仓列表"""
        pass

    def get_current_position(self, entity_id, create_if_not_exist=False):
        """获取账户中对于某个具体股票（entity_id）的持仓详情"""
        pass

    def get_current_account(self):
        """获取当前账户的资金维度数据（如总资产，可用金额等）"""
        pass

    def order_by_position_pct(
        self,
        entity_id,
        order_price,
        order_timestamp,
        order_type,
        order_position_pct: float,
    ):
        """【发单模式一】：按占用总资金的百分比下单（到底能买多少股，交由底层根据这支票的价格去换算。很适合回测体系）"""
        pass

    def order_by_money(
        self,
        entity_id,
        order_price,
        order_timestamp,
        order_type,
        order_money,
    ):
        """【发单模式二】：扔进去固定的资金额度，能买多少股就买多少"""
        pass

    def order_by_amount(
        self,
        entity_id,
        order_price,
        order_timestamp,
        order_type,
        order_amount,
    ):
        """【发单模式三】：极其精准地报确定的股数/手数（实盘 QMT 和高频打板策略通常用精准数量去报单）"""
        pass


# the __all__ is generated
__all__ = ["TradingSignalType", "TradingListener", "OrderType", "AccountService", "trading_signal_type_to_order_type"]

# __init__.py structure:
# common code of the package
# export interface in __all__ which contains __all__ of its sub modules

# import all from submodule trader
from .trader import *
from .trader import __all__ as _trader_all

__all__ += _trader_all

# import all from submodule errors
from .errors import *
from .errors import __all__ as _errors_all

__all__ += _errors_all

# import all from submodule account
from .sim_account import *
from .sim_account import __all__ as _account_all

__all__ += _account_all
