# -*- coding: utf-8 -*-
import logging

import pandas as pd

from zvt.contract.api import df_to_db
from zvt.contract.recorder import Recorder
from zvt.domain.macro.china_money_supply import ChinaMoneySupply

logger = logging.getLogger(__name__)


class ChinaMoneySupplyRecorder(Recorder):
    """
    基于 akshare 的中国货币供应量(M0/M1/M2)录入器。

    数据来源: akshare -> macro_china_money_supply()
    用法:
        recorder = ChinaMoneySupplyRecorder()
        recorder.run()
    """

    provider = "akshare"
    data_schema = ChinaMoneySupply

    def run(self):
        try:
            import akshare as ak
        except ImportError:
            logger.error("akshare is not installed. Please run: pip install akshare")
            return

        try:
            logger.info("Fetching China money supply data from akshare...")
            df_raw = ak.macro_china_money_supply()

            if df_raw is None or df_raw.empty:
                logger.warning("No money supply data returned from akshare.")
                return

            # 转换为 ZVT schema 格式
            records = []
            for _, row in df_raw.iterrows():
                try:
                    date_str = str(row["统计时间"])
                    ts = pd.to_datetime(date_str)

                    m2 = pd.to_numeric(row.get("货币和准货币(M2)数量(亿元)", None), errors="coerce")
                    m2_yoy = pd.to_numeric(row.get("货币和准货币(M2)同比增长(%)", None), errors="coerce")
                    m1 = pd.to_numeric(row.get("货币(M1)数量(亿元)", None), errors="coerce")
                    m1_yoy = pd.to_numeric(row.get("货币(M1)同比增长(%)", None), errors="coerce")
                    m0 = pd.to_numeric(row.get("流通中的现金(M0)数量(亿元)", None), errors="coerce")
                    m0_yoy = pd.to_numeric(row.get("流通中的现金(M0)同比增长(%)", None), errors="coerce")

                    # 剪刀差
                    scissors = None
                    if pd.notna(m1_yoy) and pd.notna(m2_yoy):
                        scissors = m1_yoy - m2_yoy

                    record_id = f"china_money_supply_{ts.strftime('%Y%m')}"
                    records.append(
                        {
                            "id": record_id,
                            "entity_id": "china_money_supply",
                            "timestamp": ts,
                            "code": "china_money_supply",
                            "m2": m2,
                            "m2_yoy": m2_yoy,
                            "m1": m1,
                            "m1_yoy": m1_yoy,
                            "m0": m0,
                            "m0_yoy": m0_yoy,
                            "m1_m2_scissors": scissors,
                        }
                    )
                except Exception as e:
                    logger.debug(f"Skipping row due to error: {e}")
                    continue

            if records:
                df = pd.DataFrame.from_records(records)
                df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=True)
                logger.info(f"Saved {len(records)} months of money supply data.")
            else:
                logger.warning("No valid records after parsing.")

        except Exception as e:
            logger.error(f"Failed to fetch money supply data: {e}")


if __name__ == "__main__":
    recorder = ChinaMoneySupplyRecorder()
    recorder.run()


# the __all__ is generated
__all__ = ["ChinaMoneySupplyRecorder"]
