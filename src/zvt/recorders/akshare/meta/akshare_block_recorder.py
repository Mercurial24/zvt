# -*- coding: utf-8 -*-
import logging

import pandas as pd

from zvt.contract.api import df_to_db
from zvt.contract.recorder import Recorder
from zvt.domain import Block, BlockCategory

logger = logging.getLogger(__name__)


class AkshareBlockRecorder(Recorder):
    """
    基于 akshare 的板块数据录入器。

    使用 akshare 的东方财富接口获取行业板块和概念板块列表。
    用法:
        recorder = AkshareBlockRecorder()
        recorder.run()
    """

    provider = "akshare"
    data_schema = Block

    def run(self):
        try:
            import akshare as ak
        except ImportError:
            logger.error("akshare is not installed. Please run: pip install akshare")
            return

        # 行业板块
        try:
            logger.info("Fetching industry blocks from akshare...")
            df_industry = ak.stock_board_industry_name_em()
            if df_industry is not None and not df_industry.empty:
                records = []
                for _, row in df_industry.iterrows():
                    name = row["板块名称"]
                    entity_id = f"block_cn_{name}"
                    records.append(
                        {
                            "id": entity_id,
                            "entity_id": entity_id,
                            "entity_type": "block",
                            "exchange": "cn",
                            "code": name,
                            "name": name,
                            "category": BlockCategory.industry.value,
                        }
                    )
                df = pd.DataFrame.from_records(records)
                df["timestamp"] = pd.Timestamp.now()
                df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=True)
                logger.info(f"Saved {len(records)} industry blocks.")
        except Exception as e:
            logger.error(f"Failed to fetch industry blocks: {e}")

        # 概念板块
        try:
            logger.info("Fetching concept blocks from akshare...")
            df_concept = ak.stock_board_concept_name_em()
            if df_concept is not None and not df_concept.empty:
                records = []
                for _, row in df_concept.iterrows():
                    name = row["板块名称"]
                    entity_id = f"block_cn_{name}"
                    records.append(
                        {
                            "id": entity_id,
                            "entity_id": entity_id,
                            "entity_type": "block",
                            "exchange": "cn",
                            "code": name,
                            "name": name,
                            "category": BlockCategory.concept.value,
                        }
                    )
                df = pd.DataFrame.from_records(records)
                df["timestamp"] = pd.Timestamp.now()
                df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=True)
                logger.info(f"Saved {len(records)} concept blocks.")
        except Exception as e:
            logger.error(f"Failed to fetch concept blocks: {e}")


if __name__ == "__main__":
    recorder = AkshareBlockRecorder()
    recorder.run()


# the __all__ is generated
__all__ = ["AkshareBlockRecorder"]
