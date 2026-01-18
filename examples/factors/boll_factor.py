# -*- coding: utf-8 -*-
from typing import Optional, List

import pandas as pd
from ta.volatility import BollingerBands

from zvt.contract.factor import Transformer
from zvt.factors.technical_factor import TechnicalFactor


class BollTransformer(Transformer):
    def transform_one(self, entity_id, df: pd.DataFrame) -> pd.DataFrame:
        indicator_bb = BollingerBands(close=df["close"], window=20, window_dev=2)

        # Add Bollinger Bands features
        df["bb_bbm"] = indicator_bb.bollinger_mavg()
        df["bb_bbh"] = indicator_bb.bollinger_hband()
        df["bb_bbl"] = indicator_bb.bollinger_lband()

        # Add Bollinger Band high indicator
        df["bb_bbhi"] = indicator_bb.bollinger_hband_indicator()

        # Add Bollinger Band low indicator
        df["bb_bbli"] = indicator_bb.bollinger_lband_indicator()

        # Add Width Size Bollinger Bands
        df["bb_bbw"] = indicator_bb.bollinger_wband()

        # Add Percentage Bollinger Bands
        df["bb_bbp"] = indicator_bb.bollinger_pband()
        return df


class BollFactor(TechnicalFactor):
    transformer = BollTransformer()

    def drawer_factor_df_list(self) -> Optional[List[pd.DataFrame]]:
        return [self.factor_df[["bb_bbm", "bb_bbh", "bb_bbl"]]]

    def compute_result(self):
        super().compute_result()
        self.result_df = (self.factor_df["bb_bbli"] - self.factor_df["bb_bbhi"]).to_frame(name="filter_result")
        self.result_df[self.result_df == 0] = None
        self.result_df[self.result_df == 1] = True
        self.result_df[self.result_df == -1] = False


if __name__ == "__main__":
    from zvt.domain import Stock1dKdata
    from zvt.contract import IntervalLevel
    from zvt.contract.api import get_entity_ids
    from zvt.contract import AdjustType

    # r = xyszStockKdataRecorder(force_update=True)
    # r.run()

    entity_ids = get_entity_ids(provider="xysz", entity_type="stock", codes=["002594", "002581"])
    print(f"Entity IDs found: {entity_ids}")
    
    if not entity_ids:
        print("No entities found for xysz source. Please run recorders first.")
    else:
        factor = BollFactor(
            entity_ids=entity_ids,
            provider="xysz",
            level=IntervalLevel.LEVEL_1DAY,
            start_timestamp="2022-01-01",
            end_timestamp="2023-01-01",
            adjust_type=AdjustType.qfq
        )
        if factor.factor_df is not None and not factor.factor_df.empty:
            factor.draw(show=True)
        else:
            print("No data found for the specified period and provider.")
