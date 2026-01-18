# -*- coding: utf-8 -*-
from sqlalchemy import Column, String, Float
from sqlalchemy.orm import declarative_base

from zvt.contract import Mixin
from zvt.contract.register import register_schema

ChinaMoneySupplyBase = declarative_base()


class ChinaMoneySupply(ChinaMoneySupplyBase, Mixin):
    """中国货币供应量 (M0/M1/M2)"""

    __tablename__ = "china_money_supply"

    code = Column(String(length=32))

    #: 货币和准货币(M2)数量(亿元)
    m2 = Column(Float, comment="货币和准货币(M2)数量(亿元)")
    #: M2同比增长(%)
    m2_yoy = Column(Float, comment="货币和准货币(M2)同比增长(%)")
    #: 货币(M1)数量(亿元)
    m1 = Column(Float, comment="货币(M1)数量(亿元)")
    #: M1同比增长(%)
    m1_yoy = Column(Float, comment="货币(M1)同比增长(%)")
    #: 流通中的现金(M0)数量(亿元)
    m0 = Column(Float, comment="流通中的现金(M0)数量(亿元)")
    #: M0同比增长(%)
    m0_yoy = Column(Float, comment="流通中的现金(M0)同比增长(%)")
    #: M1-M2剪刀差(%)
    m1_m2_scissors = Column(Float, comment="M1-M2剪刀差(%)")


register_schema(providers=["akshare"], db_name="china_money_supply", schema_base=ChinaMoneySupplyBase)


# the __all__ is generated
__all__ = ["ChinaMoneySupply"]
