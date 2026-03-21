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
            
            # AkShare 返回的列名经常变化，这里做一个映射
            # 旧版: 统计时间, 货币和准货币(M2)数量(亿元), 货币和准货币(M2)同比增长(%) ...
            # 新版: 月份, M2-数量(亿元), M2-同比(%) ...
            col_map = {
                "date": ["月份", "统计时间"],
                "m2": ["M2-数量(亿元)", "货币和准货币(M2)数量(亿元)"],
                "m2_yoy": ["M2-同比(%)", "货币和准货币(M2)同比增长(%)"],
                "m1": ["M1-数量(亿元)", "货币(M1)数量(亿元)"],
                "m1_yoy": ["M1-同比(%)", "货币(M1)同比增长(%)"],
                "m0": ["M0-数量(亿元)", "流通中的现金(M0)数量(亿元)"],
                "m0_yoy": ["M0-同比(%)", "流通中的现金(M0)同比增长(%)"],
            }

            def get_val(row, keys):
                for k in keys:
                    if k in row:
                        return row[k]
                return None

            for _, row in df_raw.iterrows():
                try:
                    date_val = get_val(row, col_map["date"])
                    if not date_val:
                        continue
                    
                    # 处理 "2024年01月份" 这种格式
                    date_str = str(date_val).replace("年", "-").replace("月份", "").replace("月", "")
                    ts = pd.to_datetime(date_str)
                    
                    # 货币供应量数据通常是月末数据，统一设为该月最后一天或 1 号
                    # ZVT 习惯用 1 号（按月），或者实际发生日期
                    # 这里保持 pd.to_datetime 默认的 1 号
                    
                    m2 = pd.to_numeric(get_val(row, col_map["m2"]), errors="coerce")
                    m2_yoy = pd.to_numeric(get_val(row, col_map["m2_yoy"]), errors="coerce")
                    m1 = pd.to_numeric(get_val(row, col_map["m1"]), errors="coerce")
                    m1_yoy = pd.to_numeric(get_val(row, col_map["m1_yoy"]), errors="coerce")
                    m0 = pd.to_numeric(get_val(row, col_map["m0"]), errors="coerce")
                    m0_yoy = pd.to_numeric(get_val(row, col_map["m0_yoy"]), errors="coerce")

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
                    logger.warning(f"Skipping row {row.to_dict()} due to error: {e}")
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
