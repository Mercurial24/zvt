# -*- coding: utf-8 -*-
import logging
import time
from typing import List

from zvt.broker.qmt.errors import QmtError, PositionOverflowError
from zvt.broker.qmt.qmt_quote import _to_qmt_code
from zvt.common.trading_models import BuyParameter, PositionType, SellParameter
from zvt.trader import AccountService, TradingSignal, OrderType, trading_signal_type_to_order_type
from zvt.utils.time_utils import now_pd_timestamp, to_pd_timestamp

from zvt.broker.qmt.qmt_remote import xtdata, QMTForwardClient

logger = logging.getLogger(__name__)


def _to_qmt_order_type(order_type: OrderType):
    # These are standard constants in QMT
    if order_type == OrderType.order_long:
        return 23 # STOCK_BUY
    elif order_type == OrderType.order_close_long:
        return 24 # STOCK_SELL


class QmtStockAccount(AccountService):
    def __init__(self, path, account_id, trader_name, session_id=None, base_url="http://192.168.48.207:8000", token=None) -> None:
        if not session_id:
            session_id = int(time.time())
        self.trader_name = trader_name
        self.account_id = account_id
        logger.info(f"path: {path}, account: {account_id}, trader_name: {trader_name}, session: {session_id}")

        client = QMTForwardClient(base_url, token)
        self.xt_trader = client.call("xtquant.xttrader.XtQuantTrader", path, session_id)
        self.account = client.call("xtquant.xttype.StockAccount", account_id, "STOCK")

        # Start and connect
        self.xt_trader.start()
        connect_result = self.xt_trader.connect()
        if connect_result != 0:
            logger.error(f"qmt trader 连接失败: {connect_result}")
            raise QmtError(f"qmt trader 连接失败: {connect_result}")
        logger.info("qmt trader 建立交易连接成功！")

        # Remote subscription requires a different mechanism as per README.md (Redis/MQ)
        # For now we just allow the trader to be used for active orders (Polling)
        pass

    def get_positions(self):
        return self.xt_trader.query_stock_positions(self.account)

    def get_current_position(self, entity_id, create_if_not_exist=False):
        stock_code = _to_qmt_code(entity_id=entity_id)
        return self.xt_trader.query_stock_position(self.account, stock_code)

    def get_current_account(self):
        return self.xt_trader.query_stock_asset(self.account)

    def order_by_amount(self, entity_id, order_price, order_timestamp, order_type, order_amount):
        stock_code = _to_qmt_code(entity_id=entity_id)
        # 23 = STOCK_BUY, 24 = STOCK_SELL, 11 = FIX_PRICE
        qmt_order_type = 23 if order_type == OrderType.order_long else 24
        
        fix_result_order_id = self.xt_trader.order_stock(
            account=self.account,
            stock_code=stock_code,
            order_type=qmt_order_type,
            order_volume=order_amount,
            price_type=11, # FIX_PRICE
            price=order_price,
            strategy_name=self.trader_name,
            order_remark="order from zvt",
        )
        logger.info(f"order result id: {fix_result_order_id}")
        return fix_result_order_id

    def on_trading_signals(self, trading_signals: List[TradingSignal]):
        for trading_signal in trading_signals:
            try:
                self.handle_trading_signal(trading_signal)
            except Exception as e:
                logger.exception(e)

    def handle_trading_signal(self, trading_signal: TradingSignal):
        entity_id = trading_signal.entity_id
        happen_timestamp = trading_signal.happen_timestamp
        order_type = trading_signal_type_to_order_type(trading_signal.trading_signal_type)
        
        if now_pd_timestamp() > to_pd_timestamp(trading_signal.due_timestamp):
            logger.warning(f"the signal is expired: {trading_signal.due_timestamp}")
            return
            
        quote = xtdata.get_full_tick([_to_qmt_code(entity_id=entity_id)])
        tick = quote.get(_to_qmt_code(entity_id=entity_id))
        if not tick:
            logger.error(f"Could not get quote for {entity_id}")
            return

        if order_type == OrderType.order_long:
            price = tick["askPrice"][0]
        elif order_type == OrderType.order_close_long:
            price = tick["bidPrice"][0]
        else:
            return

        self.order_by_amount(
            entity_id=entity_id,
            order_price=price,
            order_timestamp=happen_timestamp,
            order_type=order_type,
            order_amount=trading_signal.order_amount,
        )

    def sell(self, position_strategy: SellParameter):
        stock_codes = [_to_qmt_code(entity_id) for entity_id in position_strategy.entity_ids]
        for i, stock_code in enumerate(stock_codes):
            pct = position_strategy.sell_pcts[i]
            position = self.xt_trader.query_stock_position(self.account, stock_code)
            if position:
                # 24 = STOCK_SELL, 5 = MARKET_SH_CONVERT_5_CANCEL
                self.xt_trader.order_stock(
                    account=self.account,
                    stock_code=stock_code,
                    order_type=24,
                    order_volume=int(position.can_use_volume * pct),
                    price_type=5,
                    price=0,
                    strategy_name=self.trader_name,
                    order_remark="order from zvt",
                )

    def buy(self, buy_parameter: BuyParameter):
        acc = self.get_current_account()
        money_to_use = 0
        if buy_parameter.money_to_use:
            money_to_use = buy_parameter.money_to_use
        else:
            if buy_parameter.position_type == PositionType.cash:
                money_to_use = acc.cash * buy_parameter.position_pct
        
        stock_codes = [_to_qmt_code(entity_id) for entity_id in buy_parameter.entity_ids]
        ticks = xtdata.get_full_tick(code_list=stock_codes)

        if not buy_parameter.weights:
            stocks_count = len(stock_codes)
            money_for_stocks = [round(money_to_use / stocks_count)] * stocks_count
        else:
            weights_sum = sum(buy_parameter.weights)
            money_for_stocks = [round(money_to_use * (w / weights_sum)) for w in buy_parameter.weights]

        for i, stock_code in enumerate(stock_codes):
            tick = ticks.get(stock_code)
            if tick:
                try_price = tick["askPrice"][2]
                volume = int(money_for_stocks[i] / try_price / 100) * 100
                if volume > 0:
                    # 23 = STOCK_BUY, 5 = MARKET_SH_CONVERT_5_CANCEL
                    self.xt_trader.order_stock(
                        account=self.account,
                        stock_code=stock_code,
                        order_type=23,
                        order_volume=volume,
                        price_type=5,
                        price=0,
                        strategy_name=self.trader_name,
                        order_remark="order from zvt",
                    )


if __name__ == "__main__":
    # Test remote connection
    account = QmtStockAccount(path=r"D:\qmt\userdata_mini", account_id="8885660116", trader_name="test")
    print(account.get_positions())

__all__ = ["QmtStockAccount"]
