# -*- coding: utf-8 -*-
import logging
import pandas as pd

from zvt.contract import IntervalLevel, AdjustType
from zvt.contract.api import decode_entity_id, get_db_session
from zvt.domain import StockQuote
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import (
    to_date_time_str,
    to_pd_timestamp,
    current_date,
)

from zvt.broker.qmt.qmt_remote import xtdata

logger = logging.getLogger(__name__)


def _to_qmt_code(entity_id):
    _, exchange, code = decode_entity_id(entity_id=entity_id)
    return f"{code}.{exchange.upper()}"


def _to_zvt_entity_id(qmt_code):
    code, exchange = qmt_code.split(".")
    exchange = exchange.lower()
    return f"stock_{exchange}_{code}"


def _to_qmt_dividend_type(adjust_type: AdjustType):
    if adjust_type == AdjustType.qfq:
        return "front"
    elif adjust_type == AdjustType.hfq:
        return "back"
    else:
        return "none"


def _qmt_instrument_detail_to_stock(stock_detail):
    exchange = stock_detail["ExchangeID"].lower()
    code = stock_detail["InstrumentID"]
    name = stock_detail["InstrumentName"]
    try:
        list_date = to_pd_timestamp(stock_detail["OpenDate"])
    except:
        list_date = pd.Timestamp("1970-01-01")  # fallback to epoch
        
    try:
        end_date = to_pd_timestamp(stock_detail["ExpireDate"])
    except:
        end_date = None

    pre_close = stock_detail["PreClose"]
    limit_up_price = stock_detail["UpStopPrice"]
    limit_down_price = stock_detail["DownStopPrice"]
    float_volume = stock_detail["FloatVolume"]
    total_volume = stock_detail["TotalVolume"]

    entity_id = f"stock_{exchange}_{code}"

    return {
        "id": entity_id,
        "entity_id": entity_id,
        "timestamp": list_date,
        "entity_type": "stock",
        "exchange": exchange,
        "code": code,
        "name": name,
        "list_date": list_date,
        "end_date": end_date,
        "pre_close": pre_close,
        "limit_up_price": limit_up_price,
        "near_limit_up_price": limit_up_price * 0.98,
        "limit_down_price": limit_down_price,
        "float_volume": float_volume,
        "total_volume": total_volume,
    }


def get_qmt_stocks(include_bj=True):
    stock_list = xtdata.get_stock_list_in_sector("沪深A股")
    if not stock_list:
        stock_list = []

    if include_bj:
        bj_stock_list = xtdata.get_stock_list_in_sector("京市A股")
        if bj_stock_list:
            stock_list += bj_stock_list
            
    # QMT的板块分类有时不够严谨，可能会混入债券或指数（如899050北证50、821016债券等），这里统一过滤
    # 沪深: 60, 68, 00, 30. 北交所: 43, 83, 87, 92
    valid_prefixes = ('60', '68', '00', '30', '43', '83', '87', '92')
    stock_list = [s for s in stock_list if s.split('.')[0].startswith(valid_prefixes)]
    
    return stock_list


def _build_entity_list(qmt_stocks):
    entity_list = []
    for stock in qmt_stocks:
        stock_detail = xtdata.get_instrument_detail(stock, False)
        if stock_detail:
            entity_list.append(_qmt_instrument_detail_to_stock(stock_detail))
        else:
            code, exchange = stock.split(".")
            exchange = exchange.lower()
            entity_id = f"stock_{exchange}_{code}"
            entity = {
                "id": _to_zvt_entity_id(stock),
                "entity_id": entity_id,
                "entity_type": "stock",
                "exchange": exchange,
                "code": code,
                "name": "未获取",
            }

            capital_datas = xtdata.get_financial_data(
                [stock],
                table_list=["Capital"],
                report_type="report_time",
            )
            df = capital_datas.get(stock, {}).get("Capital")
            if pd_is_not_null(df):
                latest_data = df.iloc[-1]
                entity["float_volume"] = latest_data["circulating_capital"]
                entity["total_volume"] = latest_data["total_capital"]

            entity_list.append(entity)

    return pd.DataFrame.from_records(data=entity_list)


def get_entity_list(include_bj=True):
    stocks = get_qmt_stocks(include_bj=include_bj)
    return _build_entity_list(qmt_stocks=stocks)


def get_kdata(
    entity_id,
    start_timestamp,
    end_timestamp,
    level=IntervalLevel.LEVEL_1DAY,
    adjust_type=AdjustType.qfq,
    download_history=False,
):
    code = _to_qmt_code(entity_id=entity_id)
    period = level.value
    start_time = to_date_time_str(start_timestamp, fmt="YYYYMMDD")
    end_time = to_date_time_str(end_timestamp, fmt="YYYYMMDD")

    # On Linux, we assume data is downloaded on Windows server side
    # download_history is ignored or handled on Windows side beforehand
    
    records = xtdata.get_market_data(
        stock_list=[code],
        period=period,
        start_time=start_time,
        end_time=end_time,
        dividend_type=_to_qmt_dividend_type(adjust_type=adjust_type),
        fill_data=False,
    )

    if not records:
        return None

    dfs = []
    for col in records:
        df = records[col].T
        df.columns = [col]
        dfs.append(df)
    
    if not dfs:
        return None

    df = pd.concat(dfs, axis=1)
    if not df.empty and "volume" in df.columns:
        df["volume"] = df["volume"] * 100
    
    return df


def clear_history_quote(target_date=current_date()):
    session = get_db_session("qmt", data_schema=StockQuote)
    session.query(StockQuote).filter(StockQuote.timestamp < target_date).delete()
    session.commit()
    logger.info(f"clear stock quote data before: {target_date}")


# the __all__ is generated
__all__ = [
    "get_qmt_stocks",
    "get_entity_list",
    "get_kdata",
    "clear_history_quote",
]
