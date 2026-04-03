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
    """将策略意图（TradingSignalType）映射为柜台报单动作（OrderType）。
    open_long → order_long，close_long → order_close_long，以此类推。
    keep_long / keep_short 没有对应的报单动作，返回 None。
    """
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
    """交易信号：策略大脑向账户服务传递买卖意图的核心数据包。

    一个信号描述了"对哪个标的、在什么时间窗口内、做什么操作、用多少仓位/资金/股数"。
    下单规模三选一：position_pct / order_money / order_amount，构造时断言只能传其中一个。
    默认路径（Trader.buy/sell）始终使用 position_pct。
    """

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
        :param entity_id:           交易标的 ID，如 stock_sz_000338
        :param due_timestamp:       信号过期时间（= happen_timestamp + 一个周期），过期后该信号不再有效
        :param happen_timestamp:    信号产生时间；账户服务据此时间取对应 K 线的 close 价撮合
        :param trading_level:       信号所在的 K 线级别（日线/周线/分钟线…）
        :param trading_signal_type: 策略意图（open_long / close_long 等）
        :param position_pct:        【三选一】目标仓位占账户总资金的比例（0~1），最常用
        :param order_money:         【三选一】本次下单使用的固定资金金额（元）
        :param order_amount:        【三选一】本次下单精确的股数/手数
        """
        self.entity_id = entity_id
        self.due_timestamp = due_timestamp
        self.happen_timestamp = happen_timestamp
        self.trading_level = trading_level
        self.trading_signal_type = trading_signal_type

        if len([x for x in (position_pct, order_money, order_amount) if x is not None]) != 1:
            assert False
        self.position_pct = position_pct
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
