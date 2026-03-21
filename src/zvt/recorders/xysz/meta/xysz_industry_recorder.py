# -*- coding: utf-8 -*-
"""
xysz (星耀数智) 行业指数数据录入器

基于 xysz API 的行业指数接口，获取：
1. 行业板块列表 (get_industry_base_info) → Block entity
2. 行业成分股 (get_industry_constituent) → BlockStock entity
3. 行业日行情数据（含PE/PB/市值等）暂存于 Block entity 的扩展字段中

用法:
    recorder = xyszIndustryBlockRecorder()
    recorder.run()
"""
import logging

import pandas as pd

from zvt.contract.api import df_to_db
from zvt.contract.recorder import Recorder
from zvt.domain import Block, BlockCategory, BlockStock
from zvt.recorders.xysz.xysz_api import get_xysz_client

logger = logging.getLogger(__name__)


class xyszIndustryBlockRecorder(Recorder):
    """
    使用 xysz API 录入申万行业板块列表。
    数据来源：get_industry_base_info()
    """

    provider = "xysz"
    data_schema = Block

    def run(self):
        client = get_xysz_client()
        if not client:
            logger.error("xysz client not available. Please set AMAZING_DATA_USER/PASSWORD env vars.")
            return

        try:
            logger.info("Fetching industry base info from xysz...")
            result = client.get_industry_base_info(is_local=False)

            if result is None:
                logger.warning("No industry base info returned from xysz.")
                return

            # result 是一个 dict，key 为 code，value 为 DataFrame
            # 也可能直接是一个 DataFrame，取决于 API 版本
            records = []

            if isinstance(result, dict):
                for code, df in result.items():
                    if df is None or (hasattr(df, 'empty') and df.empty):
                        continue
                    if isinstance(df, pd.DataFrame):
                        for _, row in df.iterrows():
                            record = self._parse_industry_row(row, code)
                            if record:
                                records.append(record)
                    else:
                        # 可能是单行数据
                        record = self._parse_industry_row(df, code)
                        if record:
                            records.append(record)
            elif isinstance(result, pd.DataFrame):
                for _, row in result.iterrows():
                    index_code = str(row.get("INDEX_CODE", ""))
                    record = self._parse_industry_row(row, index_code)
                    if record:
                        records.append(record)

            if records:
                df = pd.DataFrame.from_records(records)
                # 去重
                df = df.drop_duplicates(subset=["id"])
                df["timestamp"] = pd.Timestamp.now()
                df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=True)
                logger.info(f"Saved {len(records)} industry blocks from xysz.")
            else:
                logger.warning("No valid industry records parsed from xysz.")

        except Exception as e:
            logger.error(f"Failed to fetch industry blocks from xysz: {e}")

    def _parse_industry_row(self, row, code):
        """从一行行业数据中提取 Block 记录"""
        try:
            # 优先根据 LEVEL_TYPE 选取对应的名称
            level_type = row.get("LEVEL_TYPE") if isinstance(row, (dict, pd.Series)) else getattr(row, "LEVEL_TYPE", None)
            name = None
            if level_type:
                field = f"LEVEL{level_type}_NAME"
                name = row.get(field) if isinstance(row, (dict, pd.Series)) else getattr(row, field, None)
            
            if not name or not str(name).strip():
                # 备选列表，按从细到粗尝试 (LEVEL3 -> LEVEL2 -> LEVEL1 -> INDUSTRY_CODE)
                for field in ["LEVEL3_NAME", "LEVEL2_NAME", "LEVEL1_NAME", "INDUSTRY_CODE"]:
                    val = row.get(field) if isinstance(row, (dict, pd.Series)) else getattr(row, field, None)
                    if val and str(val).strip():
                        name = str(val).strip()
                        break

            if not name:
                name = str(code)

            index_code = str(row.get("INDEX_CODE", code) if isinstance(row, (dict, pd.Series)) else code)
            entity_id = f"block_cn_{index_code}"

            return {
                "id": entity_id,
                "entity_id": entity_id,
                "entity_type": "block",
                "exchange": "cn",
                "code": index_code,
                "name": name,
                "category": BlockCategory.industry.value,
            }
        except Exception as e:
            logger.debug(f"Skipping industry row: {e}")
            return None


class xyszIndustryBlockStockRecorder(Recorder):
    """
    使用 xysz API 录入行业成分股数据。
    数据来源：get_industry_constituent()
    """

    provider = "xysz"
    data_schema = BlockStock

    def run(self):
        client = get_xysz_client()
        if not client:
            logger.error("xysz client not available.")
            return

        try:
            # 先获取行业列表
            logger.info("Fetching industry base info for constituent lookup...")
            base_info = client.get_industry_base_info(is_local=False)

            if base_info is None:
                logger.warning("No industry base info returned.")
                return

            # 提取所有行业指数代码
            code_list = []
            if isinstance(base_info, dict):
                code_list = list(base_info.keys())
            elif isinstance(base_info, pd.DataFrame) and "INDEX_CODE" in base_info.columns:
                code_list = base_info["INDEX_CODE"].unique().tolist()

            if not code_list:
                logger.warning("No industry codes found.")
                return

            logger.info(f"Fetching constituents for {len(code_list)} industry indices...")
            constituents = client.get_industry_constituent(code_list=code_list, is_local=False)

            if not constituents:
                logger.warning("No constituent data returned.")
                return

            records = []
            if isinstance(constituents, dict):
                for idx_code, df in constituents.items():
                    if df is None or (hasattr(df, 'empty') and df.empty):
                        continue
                    if not isinstance(df, pd.DataFrame):
                        continue

                    for _, row in df.iterrows():
                        stock_code = str(row.get("CON_CODE", "")).strip()
                        index_name = str(row.get("INDEX_NAME", idx_code)).strip()

                        if not stock_code:
                            continue

                        # 判断交易所
                        if stock_code.startswith("6"):
                            exchange = "sh"
                        elif stock_code.startswith(("0", "3")):
                            exchange = "sz"
                        elif stock_code.startswith(("4", "8")):
                            exchange = "bj"
                        else:
                            exchange = "cn"

                        stock_entity_id = f"stock_{exchange}_{stock_code}"
                        block_entity_id = f"block_cn_{idx_code}"

                        records.append({
                            "id": f"{block_entity_id}_{stock_entity_id}",
                            "entity_id": block_entity_id,
                            "entity_type": "block",
                            "exchange": "cn",
                            "code": idx_code,
                            "name": index_name,
                            "stock_id": stock_entity_id,
                            "stock_code": stock_code,
                            "stock_name": "",
                            "timestamp": pd.Timestamp.now(),
                        })

            if records:
                df = pd.DataFrame.from_records(records)
                df = df.drop_duplicates(subset=["id"])
                df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=True)
                logger.info(f"Saved {len(records)} block-stock mappings from xysz.")
            else:
                logger.warning("No valid constituent records.")

        except Exception as e:
            logger.error(f"Failed to fetch industry constituents from xysz: {e}")


if __name__ == "__main__":
    recorder = xyszIndustryBlockRecorder()
    recorder.run()


# the __all__ is generated
__all__ = ["xyszIndustryBlockRecorder", "xyszIndustryBlockStockRecorder"]
