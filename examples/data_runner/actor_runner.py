# -*- coding: utf-8 -*-
"""
市场参与者数据同步器
主要功能：同步上市公司持仓机构、前十大流通股东以及市场参与者概况数据。
数据源：默认 East Money (EM)
"""
import logging

from apscheduler.schedulers.background import BackgroundScheduler

from zvt import init_log
from zvt.domain import (
    StockInstitutionalInvestorHolder,
    StockTopTenFreeHolder,
    StockActorSummary,
)
from zvt.utils.recorder_utils import run_data_recorder

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()


@sched.scheduled_job("cron", hour=1, minute=00, day_of_week=2)
def record_actor_data(data_provider="em", entity_provider="em"):
    run_data_recorder(
        domain=StockInstitutionalInvestorHolder,
        data_provider=data_provider,
        entity_provider=entity_provider,
        day_data=True,
    )
    run_data_recorder(
        domain=StockTopTenFreeHolder, data_provider=data_provider, entity_provider=entity_provider, day_data=True
    )
    run_data_recorder(
        domain=StockActorSummary, data_provider=data_provider, entity_provider=entity_provider, day_data=True
    )


if __name__ == "__main__":
    init_log("actor_runner.log")

    record_actor_data()

    sched.start()

    sched._thread.join()
