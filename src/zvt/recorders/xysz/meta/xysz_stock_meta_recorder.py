# -*- coding: utf-8 -*-
import logging
import os
import pandas as pd
from zvt.contract.recorder import Recorder
from zvt.domain import Stock
from zvt.contract.api import df_to_db
from zvt.recorders.xysz.xysz_api import get_xysz_client
from zvt.utils.time_utils import to_pd_timestamp


def _default_equity_cache_path() -> str:
    """股本结构 API 需要的本地缓存路径。"""
    return os.environ.get("XYSZ_EQUITY_CACHE_DIR", "/tmp/xysz_equity_cache")


def _build_cap_map_from_equity_structure(client, code_list: list, local_path: str) -> dict:
    """
    通过 get_equity_structure 获取每只股票最新一期股本（TOT_SHARE / FLOAT_SHARE），
    返回 {MARKET_CODE: {"float_share", "tot_share"}}。
    个股 query_kline 不包含 A_FLOAT_CAP/TOTAL_CAP，只有行业日线才有，故改用股本结构 API。
    """
    cap_map = {}
    batch_size = 500
    for i in range(0, len(code_list), batch_size):
        batch = code_list[i : i + batch_size]
        try:
            eq_df = client.get_equity_structure(
                code_list=batch,
                local_path=local_path,
                is_local=False,
            )
        except Exception as e:
            # 单批失败不阻断，记录后继续
            logging.getLogger(__name__).warning("get_equity_structure batch failed: %s", e)
            continue

        if eq_df is None:
            continue
        if isinstance(eq_df, pd.DataFrame) and eq_df.empty:
            continue
        if isinstance(eq_df, dict):
            frames = [v for v in eq_df.values() if v is not None and not (isinstance(v, pd.DataFrame) and v.empty)]
            if not frames:
                continue
            eq_df = pd.concat(frames, ignore_index=True)

        cols_upper = {str(c).upper(): c for c in eq_df.columns}
        mc_col = cols_upper.get("MARKET_CODE")
        cd_col = cols_upper.get("CHANGE_DATE")
        ts_col = cols_upper.get("TOT_SHARE")
        fs_col = cols_upper.get("FLOAT_SHARE")
        if not mc_col or not (ts_col or fs_col):
            continue

        for mc, grp in eq_df.groupby(mc_col):
            if cd_col and cd_col in grp.columns:
                grp = grp.sort_values(cd_col, ascending=False)
            latest = grp.iloc[0]
            cap_map[mc] = {
                "float_share": latest.get(fs_col) if fs_col else None,
                "tot_share": latest.get(ts_col) if ts_col else None,
            }
    return cap_map


def _build_close_map_from_kline(client, code_list: list) -> dict:
    """拉最近几日 K 线取每只股票最新收盘价，返回 {MARKET_CODE: close}，用于 市值 = close × 股本。"""
    try:
        import AmazingData as ad
    except ImportError:
        return {}
    end = pd.Timestamp.now()
    begin = end - pd.Timedelta(days=10)
    begin_int = int(begin.strftime("%Y%m%d"))
    end_int = int(end.strftime("%Y%m%d"))
    close_map = {}
    batch_size = 500
    for i in range(0, len(code_list), batch_size):
        batch = code_list[i : i + batch_size]
        try:
            kline_dict = client.query_kline(
                code_list=batch,
                begin_date=begin_int,
                end_date=end_int,
                period=ad.constant.Period.day.value,
            )
        except Exception as e:
            logging.getLogger(__name__).warning("query_kline for latest close failed: %s", e)
            continue
        for mc, kdf in (kline_dict or {}).items():
            if kdf is None or kdf.empty:
                continue
            raw = kdf.copy()
            if isinstance(raw.index, pd.DatetimeIndex):
                raw = raw.sort_index()
            else:
                raw = raw.reset_index(drop=True)
            cols_upper = {str(c).upper(): c for c in raw.columns}
            close_col = cols_upper.get("CLOSE") or cols_upper.get("CLOSE_PRICE")
            if close_col is None and "close" in raw.columns:
                close_col = "close"
            if close_col is None:
                continue
            last = raw.iloc[-1]
            try:
                close_val = last.get(close_col)
                if close_val is not None and isinstance(close_val, (int, float)) and close_val == close_val:
                    close_map[mc] = float(close_val)
            except (TypeError, ValueError):
                pass
    return close_map


class xyszStockMetaRecorder(Recorder):
    provider = "xysz"
    data_schema = Stock

    def __init__(self, force_update=False, sleeping_time=0):
        super().__init__(force_update, sleeping_time)
        self.client = get_xysz_client()

    def run(self):
        # 1. Get all A-share codes
        codes = self.client.get_code_list(security_type="EXTRA_STOCK_A")
        if not codes:
            self.logger.warning("No A-share codes returned")
            return

        # 2. 股本结构（TOT_SHARE / FLOAT_SHARE）
        cache_path = _default_equity_cache_path()
        os.makedirs(cache_path, exist_ok=True)
        cap_map = _build_cap_map_from_equity_structure(self.client, codes, cache_path)
        self.logger.info("Equity structure loaded for %d stocks", len(cap_map))

        # 2.5 最新收盘价，用于 市值 = close × 股本（符合 Stock schema）
        close_map = _build_close_map_from_kline(self.client, codes)
        self.logger.info("Latest close loaded for %d stocks", len(close_map))

        # 3. Get stock basic in batches and merge cap / 市值
        BATCH_SIZE = 500
        for i in range(0, len(codes), BATCH_SIZE):
            batch = codes[i : i + BATCH_SIZE]
            df = self.client.get_stock_basic(batch)

            if df is None or df.empty:
                continue

            df = df.rename(columns={
                "SECURITY_NAME": "name",
                "LISTDATE": "list_date",
                "DELISTDATE": "end_date",
            })

            records = []
            for _, row in df.iterrows():
                market_code = row.get("MARKET_CODE")
                if not market_code:
                    continue

                parts = str(market_code).split(".")
                code = parts[0]
                exchange = "unknown"
                if len(parts) > 1:
                    suffix = parts[1].upper()
                    if suffix == "SH":
                        exchange = "sh"
                    elif suffix == "SZ":
                        exchange = "sz"
                    elif suffix == "BJ":
                        exchange = "bj"
                if exchange == "unknown":
                    if code.startswith("6"):
                        exchange = "sh"
                    elif code.startswith("0") or code.startswith("3"):
                        exchange = "sz"
                    elif code.startswith("4") or code.startswith("8"):
                        exchange = "bj"

                entity_id = f"stock_{exchange}_{code}"
                cap_info = cap_map.get(market_code) or {}
                close = close_map.get(market_code)
                float_share = cap_info.get("float_share")
                tot_share = cap_info.get("tot_share")
                if close is not None and isinstance(close, (int, float)) and close == close:
                    record_float_cap = float(close) * float(float_share) if float_share is not None else None
                    record_total_cap = float(close) * float(tot_share) if tot_share is not None else None
                else:
                    record_float_cap = None
                    record_total_cap = None

                record = {
                    "id": entity_id,
                    "entity_id": entity_id,
                    "entity_type": "stock",
                    "exchange": exchange,
                    "code": code,
                    "name": row.get("name"),
                    "list_date": to_pd_timestamp(str(row.get("list_date"))),
                    "end_date": to_pd_timestamp(str(row.get("end_date"))) if row.get("end_date") else None,
                    "timestamp": to_pd_timestamp(str(row.get("list_date"))),
                }
                if record_float_cap is not None:
                    record["float_cap"] = record_float_cap
                if record_total_cap is not None:
                    record["total_cap"] = record_total_cap
                records.append(record)

            if records:
                df_to_db(
                    df=pd.DataFrame(records),
                    data_schema=self.data_schema,
                    provider=self.provider,
                    force_update=self.force_update,
                )
                self.logger.info("Persisted %d stocks (batch %d/%d)", len(records), i // BATCH_SIZE + 1, (len(codes) + BATCH_SIZE - 1) // BATCH_SIZE)

    def __init_client(self):
        pass

if __name__ == "__main__":
    recorder = xyszStockMetaRecorder()
    recorder.run()
