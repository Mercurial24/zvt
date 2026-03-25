# -*- coding: utf-8 -*-
"""收盘后每日增量更新：日线、财报、股东/股本/分红/配股、复权因子、行业。

使用方式：
  python daily_update_xysz.py              # 更新全部（子进程模式）
  python daily_update_xysz.py --klines     # 仅日线
  python daily_update_xysz.py --financial  # 仅财报
  python daily_update_xysz.py --equity     # 仅股东/股本/分红/配股
  python daily_update_xysz.py --adj-factor # 仅复权因子
  python daily_update_xysz.py --industry   # 仅行业

建议：用 cron 或任务计划在每日收盘后（如 16:30）执行。
"""
from __future__ import annotations

import argparse
import os
import sys
import typing

if typing.TYPE_CHECKING:
    import pandas as pd
    from amazing_data_down import AmazingDataClient

# ============================================================================
#  配置常量（可通过环境变量覆盖）
# ============================================================================

# 登录
LOGIN_USER = os.environ.get("AMAZING_DATA_USER", "10100223966")
LOGIN_PASSWORD = os.environ.get("AMAZING_DATA_PASSWORD", "10100223966@2026")
LOGIN_HOST = os.environ.get("AMAZING_DATA_HOST", "140.206.44.234")
LOGIN_PORT = int(os.environ.get("AMAZING_DATA_PORT", "8600"))

# 挂载盘保存路径
KLINE_DAILY_PATH = "/mnt/point/stock_data/xysz_data/base_data/klines_daily_dir"
FINANCIAL_DIR = "/mnt/point/stock_data/xysz_data/base_data/"
EQUITY_SAVE_DIR = "/mnt/point/stock_data/xysz_data/base_data/"
ADJ_FACTOR_SAVE_DIR = "/mnt/point/stock_data/xysz_data/base_data/"

# 本地 SSD 缓存路径（HDF5 与挂载盘不兼容，必须用本地磁盘）
FINANCIAL_CACHE_DIR = "/data/stock_data/xysz_data/base_data/"
EQUITY_CACHE_DIR = "/data/stock_data/xysz_data/base_data/"
ADJ_FACTOR_CACHE_DIR = "/data/stock_data/xysz_data/base_data/"

# 增量范围
KLINE_LAST_TRADING_DAYS = 10
FINANCIAL_BEGIN_OFFSET_DAYS = 365 * 2
EQUITY_BEGIN_OFFSET_DAYS = 365 * 2

# 批次大小
BATCH_SIZE = 1000
_FINANCIAL_BATCH_SIZE = 1000
_ADJ_FACTOR_BATCH_SIZE = int(os.environ.get("ADJ_FACTOR_BATCH_SIZE", "1000"))


# ============================================================================
#  工具函数
# ============================================================================

def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _date_int_from_str(s: str | int) -> int:
    """'20260215' -> 20260215"""
    try:
        if isinstance(s, int):
            return s
        return int(s.replace("-", "")[:8])
    except Exception:
        return 0


def _release_memory() -> None:
    """强制回收内存并归还给 OS（解决 Python 不释放 RSS 的问题）。"""
    import gc
    import ctypes

    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


def _close_all_hdf5_handles() -> None:
    """强制关闭所有 pytables 打开的 HDF5 文件句柄。

    AmazingData SDK 内部用 pytables 读写 HDF5 缓存但不一定正确关闭，
    导致后续批次报 'already opened' 或 'block0_items_variety' 错误。
    """
    try:
        import tables
        tables.file._open_files.close_all()
    except Exception:
        pass


def _check_memory() -> None:
    try:
        import psutil
        mem = psutil.virtual_memory()
        print(
            f"[Memory] Total: {mem.total / 1024**3:.1f}GB, "
            f"Available: {mem.available / 1024**3:.1f}GB, "
            f"Percent: {mem.percent}%"
        )
    except ImportError:
        pass


def _get_code_list_with_retry(
    client, security_type: str = "EXTRA_STOCK_A", retries: int = 3
) -> list[str]:
    """稳健获取代码列表：重试并校验返回值。"""
    import time

    last_err = None
    for i in range(retries):
        try:
            codes = client.get_code_list(security_type=security_type)
            if isinstance(codes, list) and codes:
                return codes
            last_err = RuntimeError(f"invalid code list: {type(codes)}")
        except Exception as e:
            last_err = e
        if i < retries - 1:
            sleep_s = 1 + i
            print(f"[代码表] 第 {i+1}/{retries} 次获取失败，{sleep_s}s 后重试: {last_err}")
            time.sleep(sleep_s)

    print(f"[代码表] 获取失败，已重试 {retries} 次: {last_err}")
    return []


def _write_parquet_fast_compatible(df, path: str) -> None:
    """Parquet 写入，兼容对象存储挂载盘（pyarrow → polars → pandas 降级）。"""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, path)
        return
    except Exception:
        pass

    try:
        import polars as pl

        pl.from_pandas(df).write_parquet(path)
        return
    except Exception:
        pass

    df.to_parquet(path, index=False)


def _process_batch_dict(batch_dict: dict, period_col: str | None) -> typing.Any:
    """将 API 返回的 dict[code, DataFrame] 转为长表 DataFrame。"""
    import pandas as pd

    parts = []
    for code in list(batch_dict.keys()):
        df = batch_dict.pop(code)
        if df is None or (hasattr(df, "empty") and df.empty):
            continue
        df = df.copy()
        df["code"] = code
        col = period_col
        if col and col in df.columns:
            df["report_period"] = df[col]
        else:
            df = df.reset_index()
            if "index" in df.columns:
                df = df.rename(columns={"index": "report_period"})
        parts.append(df)
    if not parts:
        return None
    return pd.concat(parts, axis=0, ignore_index=True)


def _save_api_result_as_parquet(
    result, path: str, dedup_cols: list[str], name: str
) -> None:
    """将 API 返回的 dict 或 DataFrame 保存为 parquet，自动处理两种返回类型。"""
    import pandas as pd

    if isinstance(result, dict) and result:
        parts = []
        for code, df in result.items():
            if df is None or (hasattr(df, "empty") and df.empty):
                continue
            df = df.copy()
            key_col = dedup_cols[0] if dedup_cols else None
            if key_col and key_col not in df.columns:
                df[key_col] = code
            parts.append(df)
        if not parts:
            print(f"[{name}] 无数据，跳过")
            return
        out = pd.concat(parts, axis=0, ignore_index=True)
    elif isinstance(result, pd.DataFrame) and not result.empty:
        out = result.copy()
    else:
        print(f"[{name}] 返回为空，跳过")
        return

    valid_dedup = [c for c in dedup_cols if c in out.columns]
    if valid_dedup:
        out = out.drop_duplicates(subset=valid_dedup, keep="last")
    _write_parquet_fast_compatible(out, path)
    print(f"[{name}] 已保存: {path} ({len(out)} 行)")


# ============================================================================
#  Parquet 合并辅助函数（流式低内存）
# ============================================================================

def _merge_financial_dict_to_path(
    client_get_fn,
    path: str,
    period_col: str | None,
    code_list: list[str],
    begin_date: int,
    end_date: int,
    local_path: str,
    name: str,
) -> None:
    """分批拉取财报/股东类 dict 数据，流式合并到 parquet 文件。"""
    import tempfile

    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    _ensure_dir(path)
    n_batches = (len(code_list) + _FINANCIAL_BATCH_SIZE - 1) // _FINANCIAL_BATCH_SIZE
    temp_files: list[str] = []

    # ---- 阶段 1：分批下载，每批写临时 parquet ----
    for batch_idx in range(n_batches):
        batch_codes = code_list[
            batch_idx * _FINANCIAL_BATCH_SIZE : (batch_idx + 1) * _FINANCIAL_BATCH_SIZE
        ]
        batch_dict = client_get_fn(
            code_list=batch_codes,
            local_path=local_path,
            is_local=False,
            begin_date=begin_date,
            end_date=end_date,
        )
        if not batch_dict:
            continue

        batch_df = _process_batch_dict(batch_dict, period_col)
        del batch_dict

        if batch_df is None or batch_df.empty:
            _release_memory()
            continue

        if "report_period" not in batch_df.columns:
            print(f"[{name}] 批次 {batch_idx+1}/{n_batches}: 无 report_period，跳过")
            del batch_df
            _release_memory()
            continue

        tmp = tempfile.NamedTemporaryFile(
            suffix=".parquet", dir=os.path.dirname(path) or ".", delete=False
        )
        tmp.close()
        _write_parquet_fast_compatible(batch_df, tmp.name)
        temp_files.append(tmp.name)
        print(f"[{name}] 批次 {batch_idx+1}/{n_batches} 已下载 ({len(batch_df)} 条)")
        del batch_df
        _release_memory()

    if not temp_files:
        print(f"[{name}] 所有批次均无新数据，跳过")
        return

    # ---- 阶段 2：全程流式合并 ----
    print(f"[{name}] 开始流式合并 {len(temp_files)} 个批次...")

    # 2a) 扫描临时文件提取 report_period 集合（不加载全量数据）
    all_periods: set = set()
    valid_temp_files: list[str] = []
    for tf in temp_files:
        try:
            tf_pf = pq.ParquetFile(tf)
            for batch in tf_pf.iter_batches(batch_size=65536, columns=["report_period"]):
                col = batch.column("report_period")
                all_periods.update(v.as_py() for v in col if v.as_py() is not None)
            valid_temp_files.append(tf)
            del tf_pf
        except Exception:
            try:
                os.unlink(tf)
            except Exception:
                pass
    _release_memory()

    if not valid_temp_files:
        print(f"[{name}] 新批次读取后为空，跳过")
        return

    periods = sorted(all_periods)
    print(f"[{name}] 新数据涉及 {len(periods)} 个报告期")

    # 2b) 无历史文件 → 直接合并临时文件
    if not os.path.isfile(path):
        _concat_temp_files_to_path(valid_temp_files, path, name)
        return

    # 2c) 有历史文件 → 流式过滤旧数据 + 追加新数据
    base_pf = pq.ParquetFile(path)
    base_schema = base_pf.schema_arrow
    base_cols = list(base_schema.names)

    if "report_period" not in base_cols:
        print(f"[{name}] 历史文件缺少 report_period，回退为仅保存新数据")
        del base_pf
        _concat_temp_files_to_path(valid_temp_files, path, name)
        return

    try:
        rp_type = base_schema.field("report_period").type
    except Exception:
        rp_type = pa.string()
    value_set = pa.array(periods, type=rp_type)

    tmp_path = f"{path}.tmp"
    writer = None
    kept_old_rows = 0

    try:
        for batch in base_pf.iter_batches(batch_size=8192):
            table = pa.Table.from_batches([batch], schema=base_schema)
            mask = pc.is_in(table.column("report_period"), value_set=value_set)
            unaffected = table.filter(pc.invert(mask))
            if unaffected.num_rows > 0:
                if writer is None:
                    writer = pq.ParquetWriter(tmp_path, base_schema)
                writer.write_table(unaffected)
                kept_old_rows += unaffected.num_rows
            del table, mask, unaffected
    except Exception as e:
        print(f"[{name}] 读取旧文件失败 ({e})，将仅保存新数据")
        kept_old_rows = 0

    try:
        if hasattr(base_pf, "reader") and hasattr(base_pf.reader, "close"):
            base_pf.reader.close()
    except Exception:
        pass
    del base_pf
    _release_memory()

    # 逐个追加临时文件
    new_total_rows = 0
    for tf in valid_temp_files:
        try:
            tf_pf = pq.ParquetFile(tf)
            for batch in tf_pf.iter_batches(batch_size=8192):
                aligned = pa.RecordBatch.from_pydict(
                    {
                        col: (
                            batch.column(col)
                            if col in batch.schema.names
                            else pa.nulls(batch.num_rows, type=base_schema.field(col).type)
                        )
                        for col in base_cols
                    },
                    schema=base_schema,
                )
                tbl = pa.Table.from_batches([aligned], schema=base_schema)
                if writer is None:
                    writer = pq.ParquetWriter(tmp_path, base_schema)
                writer.write_table(tbl)
                new_total_rows += tbl.num_rows
                del tbl, aligned
            del tf_pf
        except Exception as e:
            print(f"[{name}] 追加临时文件 {tf} 失败: {e}")
        finally:
            try:
                os.unlink(tf)
            except Exception:
                pass
        _release_memory()

    if writer is not None:
        writer.close()

    if os.path.isfile(tmp_path):
        os.replace(tmp_path, path)
        print(f"[{name}] 流式合并完成: 保留旧数据 {kept_old_rows} 行 + 新数据 {new_total_rows} 行")
    else:
        print(f"[{name}] 合并后无数据写入")


def _concat_temp_files_to_path(temp_files: list[str], path: str, name: str) -> None:
    """将临时 parquet 文件合并写入目标路径，逐文件流式处理。"""
    import pyarrow.parquet as pq

    writer = None
    try:
        for tf in temp_files:
            tbl = pq.read_table(tf)
            if writer is None:
                writer = pq.ParquetWriter(path, tbl.schema)
            writer.write_table(tbl)
            del tbl
            _release_memory()
    finally:
        if writer is not None:
            writer.close()
    for tf in temp_files:
        try:
            os.unlink(tf)
        except Exception:
            pass
    print(f"[{name}] 已从临时文件写入")


def _merge_dataframe_to_path(
    new_df: pd.DataFrame,
    path: str,
    subset_keys: list[str],
    name: str,
) -> None:
    """整表类（如 profit_express、dividend）：按 key 低内存增量合并。"""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    if new_df is None or (hasattr(new_df, "empty") and new_df.empty):
        print(f"[{name}] 无新数据，跳过")
        return
    if not isinstance(new_df, pd.DataFrame):
        return
    _ensure_dir(path)
    new_df = new_df.copy()
    keys = [k for k in subset_keys if k in new_df.columns]
    if not keys:
        keys = list(new_df.columns[:2])

    new_df = new_df.drop_duplicates(subset=keys, keep="last")
    if not os.path.isfile(path):
        _write_parquet_fast_compatible(new_df, path)
        print(f"[{name}] 本地无历史文件，已保存 {len(new_df)} 行")
        return

    tmp_path = f"{path}.tmp"
    base_pf = pq.ParquetFile(path)
    base_schema = base_pf.schema_arrow
    base_cols = list(base_schema.names)

    for col in base_cols:
        if col not in new_df.columns:
            new_df[col] = pd.NA
    new_df = new_df[base_cols]

    missing_keys = [k for k in keys if k not in base_cols]
    if missing_keys:
        print(f"[{name}] 历史文件缺少 key({missing_keys})，回退为追加模式")
        writer = None
        try:
            for batch in base_pf.iter_batches(batch_size=32768):
                table = pa.Table.from_batches([batch], schema=base_schema)
                if writer is None:
                    writer = pq.ParquetWriter(tmp_path, base_schema)
                writer.write_table(table)
            writer.write_table(
                pa.Table.from_pandas(new_df, schema=base_schema, preserve_index=False)
            )
        finally:
            if writer is not None:
                writer.close()
        os.replace(tmp_path, path)
        print(f"[{name}] 已追加保存，共 {len(new_df)} 行新增")
        return

    key_specs = {}
    for k in keys:
        vals = [v for v in new_df[k].dropna().unique().tolist()]
        if not vals:
            key_specs[k] = ("empty", None)
            continue
        try:
            key_specs[k] = ("native", pa.array(vals, type=base_schema.field(k).type))
        except Exception:
            key_specs[k] = ("string", pa.array([str(v) for v in vals], type=pa.string()))

    writer = None
    affected_parts: list[pd.DataFrame] = []
    try:
        for batch in base_pf.iter_batches(batch_size=32768):
            table = pa.Table.from_batches([batch], schema=base_schema)

            mask = None
            for k in keys:
                mode, valset = key_specs[k]
                if mode == "empty":
                    col_mask = pa.array([False] * table.num_rows)
                elif mode == "native":
                    col_mask = pc.is_in(table.column(k), value_set=valset)
                else:
                    col_mask = pc.is_in(pc.cast(table.column(k), pa.string()), value_set=valset)
                mask = col_mask if mask is None else pc.and_(mask, col_mask)

            unaffected = table.filter(pc.invert(mask))
            if unaffected.num_rows > 0:
                if writer is None:
                    writer = pq.ParquetWriter(tmp_path, base_schema)
                writer.write_table(unaffected)

            affected = table.filter(mask)
            if affected.num_rows > 0:
                affected_parts.append(affected.to_pandas())

        if affected_parts:
            affected_old_df = pd.concat(affected_parts, axis=0, ignore_index=True)
            merged_df = pd.concat([affected_old_df, new_df], axis=0, ignore_index=True)
            del affected_old_df
        else:
            merged_df = new_df

        merged_df = merged_df.drop_duplicates(subset=keys, keep="last")
        merged_tbl = pa.Table.from_pandas(
            merged_df[base_cols], schema=base_schema, preserve_index=False
        )
        if writer is None:
            writer = pq.ParquetWriter(tmp_path, base_schema)
        writer.write_table(merged_tbl)
    finally:
        if writer is not None:
            writer.close()

    try:
        if hasattr(base_pf, "reader") and hasattr(base_pf.reader, "close"):
            base_pf.reader.close()
    except Exception:
        pass
    del base_pf
    _release_memory()

    os.replace(tmp_path, path)
    print(f"[{name}] 已按 key 低内存合并保存，共 {len(new_df)} 行新增")


# ============================================================================
#  业务更新函数
# ============================================================================

KLINE_YEARLY_PATH = "/mnt/point/stock_data/xysz_data/base_data/klines_yearly"


def _update_yearly_kline_file(year: int, new_df: "pd.DataFrame") -> None:
    """
    将新的 K 线数据合并写入对应年度的 Parquet 文件。
    - 文件内按 (code, kline_time) 排序，确保行组统计有意义（支持跳过）
    - 以 (code, date) 为唯一键，新数据覆盖旧数据
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    os.makedirs(KLINE_YEARLY_PATH, exist_ok=True)
    dst_path = os.path.join(KLINE_YEARLY_PATH, f"year={year}.parquet")
    tmp_path = dst_path + ".tmp"

    if os.path.exists(dst_path):
        try:
            old_df = pq.read_table(dst_path).to_pandas()
            combined = pd.concat([old_df, new_df], axis=0, ignore_index=True)
            del old_df
        except Exception as e:
            print(f"  [日线][警告] 读取旧文件失败({e})，仅保留新数据")
            combined = new_df
    else:
        combined = new_df

    key_cols = ["code", "date"] if "date" in combined.columns else ["code", "kline_time"]
    combined = combined.drop_duplicates(subset=key_cols, keep="last")
    # 按 code 排序是关键：使得 Parquet 行组统计(min/max code)有效，查单股时能跳过无关行组
    combined = combined.sort_values(["code", "kline_time"]).reset_index(drop=True)

    # 统一列类型，确保跨年份文件 schema 一致
    if "date" in combined.columns:
        combined["date"] = combined["date"].astype("int32")
    if "kline_time" in combined.columns:
        combined["kline_time"] = pd.to_datetime(combined["kline_time"]).astype("datetime64[us]")

    table = pa.Table.from_pandas(combined, preserve_index=False)
    pq.write_table(table, tmp_path, row_group_size=100_000, compression="snappy")
    os.replace(tmp_path, dst_path)
    print(f"  [日线] year={year}.parquet 已更新: {len(combined)} 行")
    del combined, table


def update_klines_daily(client) -> list[str]:
    """增量更新日线：拉最近 N 个交易日，按年写入 Parquet（klines_yearly/）。"""
    import AmazingData as ad
    import pandas as pd
    from tqdm import tqdm

    calendar = client.get_calendar(data_type="str", market="SH")
    if not calendar:
        print("[日线] 未获取到交易日历，跳过")
        return []

    recent = sorted(
        [_date_int_from_str(d) for d in calendar if _date_int_from_str(d) > 0]
    )[-KLINE_LAST_TRADING_DAYS:]
    if not recent:
        print("[日线] 无有效交易日，跳过")
        return []
    begin_date, end_date = min(recent), max(recent)

    all_codes = _get_code_list_with_retry(client, security_type="EXTRA_STOCK_A")
    if not all_codes:
        print("[日线] 未获取到股票代码列表，跳过")
        return []

    kline_dict = {}
    for i in range(0, len(all_codes), BATCH_SIZE):
        batch = all_codes[i : i + BATCH_SIZE]
        batch_dict = client.query_kline(
            code_list=batch,
            begin_date=begin_date,
            end_date=end_date,
            period=ad.constant.Period.day.value,
        )
        kline_dict.update(batch_dict)
        print(f"[日线] 已拉取 {len(kline_dict)} 只...")

    if not kline_dict:
        print("[日线] 本区间无新数据，跳过保存")
        return all_codes

    parts = []
    for code, df in kline_dict.items():
        if df is None or df.empty:
            continue
        df = df.copy().reset_index(drop=True)
        if "kline_time" not in df.columns:
            print(f"[日线][警告] {code}: 未找到 kline_time 列，跳过")
            continue
        df["date"] = pd.to_datetime(df["kline_time"]).dt.strftime("%Y%m%d").astype("int32")
        df["kline_time"] = pd.to_datetime(df["kline_time"]).astype("datetime64[us]")
        df["code"] = code
        parts.append(df)

    if not parts:
        print("[日线] 所有批次数据处理后为空")
        return all_codes

    out = pd.concat(parts, axis=0, ignore_index=True)
    out = out.drop_duplicates(subset=["code", "date"], keep="last")
    out["code"] = out["code"].astype("string")
    out["year"] = pd.to_datetime(out["kline_time"]).dt.year

    # 按年分组，逐年写入（通常只涉及当年，偶尔涉及跨年更新的两年）
    for year, year_df in out.groupby("year"):
        year_df = year_df.drop(columns=["year"]).reset_index(drop=True)
        _update_yearly_kline_file(int(year), year_df)

    print(f"[日线] 已更新年度 Parquet，本次涉及 {out['year'].nunique()} 个年份，共 {len(out)} 条")
    return all_codes



def update_financial(client, codes: list[str] | None = None) -> None:
    """增量更新财报（资产负债表、现金流量表、利润表、业绩快报、业绩预告）。"""
    from datetime import datetime, timedelta

    if not codes:
        codes = _get_code_list_with_retry(client, security_type="EXTRA_STOCK_A")
    if not codes:
        print("[财报] 未获取到股票代码列表，跳过")
        return

    end_date = int(datetime.now().strftime("%Y%m%d"))
    begin_date = int((datetime.now() - timedelta(days=FINANCIAL_BEGIN_OFFSET_DAYS)).strftime("%Y%m%d"))
    if begin_date < 20000101:
        begin_date = 20000101

    for get_fn, filename, period_col, label in [
        (client.get_balance_sheet, "balance_sheet.parquet", "REPORTING_PERIOD", "资产负债表"),
        (client.get_cash_flow, "cash_flow.parquet", "REPORTING_PERIOD", "现金流量表"),
        (client.get_income, "income.parquet", "REPORTING_PERIOD", "利润表"),
    ]:
        _merge_financial_dict_to_path(
            get_fn,
            os.path.join(FINANCIAL_DIR, filename),
            period_col,
            codes,
            begin_date,
            end_date,
            FINANCIAL_CACHE_DIR,
            label,
        )

    for get_fn, filename, keys, label in [
        (client.get_profit_express, "profit_express.parquet", ["MARKET_CODE", "REPORTING_PERIOD"], "业绩快报"),
        (client.get_profit_notice, "profit_notice.parquet", ["MARKET_CODE", "REPORTING_PERIOD"], "业绩预告"),
    ]:
        df = get_fn(
            code_list=codes,
            local_path=FINANCIAL_CACHE_DIR,
            is_local=False,
            begin_date=begin_date,
            end_date=end_date,
        )
        _merge_dataframe_to_path(df, os.path.join(FINANCIAL_DIR, filename), keys, label)
        del df
        _release_memory()


def update_adj_factor(client, codes: list[str] | None = None) -> None:
    """增量更新复权因子：分批拉取，每只股票一个 parquet 文件。

    输出结构：
      backward_factor/<CODE>.parquet  — 后复权因子
      adj_factor/<CODE>.parquet       — 单次复权因子
    每个文件两列：date（日期）, factor（因子值）
    """
    if not codes:
        codes = _get_code_list_with_retry(client, security_type="EXTRA_STOCK_A")
    if not codes:
        print("[复权因子] 未获取到股票代码列表，跳过")
        return

    os.makedirs(ADJ_FACTOR_CACHE_DIR, exist_ok=True)

    batch_size = _ADJ_FACTOR_BATCH_SIZE
    n_batches = (len(codes) + batch_size - 1) // batch_size

    for factor_name, get_fn, dirname in [
        ("后复权因子", client.get_backward_factor, "backward_factor"),
        ("单次复权因子", client.get_adj_factor, "adj_factor"),
    ]:
        save_dir = os.path.join(ADJ_FACTOR_SAVE_DIR, dirname)
        os.makedirs(save_dir, exist_ok=True)
        total_codes = 0
        total_errors = 0

        print(
            f"[复权因子] 开始下载 {factor_name}，共 {len(codes)} 只，"
            f"分 {n_batches} 批（每批 {batch_size} 只）..."
        )

        for batch_idx in range(n_batches):
            batch_codes = codes[batch_idx * batch_size : (batch_idx + 1) * batch_size]
            batch_df = None

            _close_all_hdf5_handles()

            try:
                batch_df = get_fn(
                    code_list=batch_codes,
                    local_path=ADJ_FACTOR_CACHE_DIR,
                    is_local=False,
                )
            except Exception as e:
                print(
                    f"[复权因子] {factor_name} 批次 {batch_idx+1}/{n_batches} "
                    f"读取失败: {e}"
                )
                continue

            if batch_df is None or batch_df.empty:
                print(f"[复权因子] {factor_name} 批次 {batch_idx+1}/{n_batches} 无数据")
                continue

            n_codes = batch_df.shape[1]
            for code in batch_df.columns:
                series = batch_df[code].dropna()
                if series.empty:
                    continue
                per_stock_df = series.to_frame("factor")
                per_stock_df.index.name = "date"
                per_stock_df = per_stock_df.reset_index()
                try:
                    _write_parquet_fast_compatible(
                        per_stock_df, os.path.join(save_dir, f"{code}.parquet")
                    )
                except Exception as e:
                    print(f"[复权因子] {factor_name} 写入 {code} 失败: {e}")
                    total_errors += 1

            total_codes += n_codes
            print(
                f"[复权因子] {factor_name} 批次 {batch_idx+1}/{n_batches}: "
                f"{n_codes} 只股票 (累计 {total_codes})"
            )
            del batch_df
            _release_memory()

        print(
            f"[复权因子] {factor_name} 完成: {total_codes} 只股票 -> {save_dir}"
            + (f" (写入失败 {total_errors} 只)" if total_errors else "")
        )


def _build_close_map_from_kline(client, codes: list[str]) -> dict:
    """拉最近几日 K 线，取每只股票最新收盘价，返回 {MARKET_CODE: close}。用于 市值 = close * 股本。"""
    import pandas as pd

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
    for i in range(0, len(codes), batch_size):
        batch = codes[i : i + batch_size]
        try:
            kline_dict = client.query_kline(
                code_list=batch,
                begin_date=begin_int,
                end_date=end_int,
                period=ad.constant.Period.day.value,
            )
        except Exception as e:
            print(f"[股票基础信息] 拉取最新价批次失败: {e}")
            _close_all_hdf5_handles()
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
                if close_val is not None and (isinstance(close_val, (int, float)) and close_val == close_val):
                    close_map[mc] = float(close_val)
            except (TypeError, ValueError):
                pass
        _close_all_hdf5_handles()
        _release_memory()
    print(f"[股票基础信息] 最新价覆盖 {len(close_map)} 只")
    return close_map


def _build_cap_map_from_equity_structure(client, codes: list[str]) -> dict:
    """通过 get_equity_structure 获取最新一期股本，返回 {MARKET_CODE: {tot_share, float_share}}。"""
    import pandas as pd

    cap_map: dict = {}
    batch_size = 500
    n_batches = (len(codes) + batch_size - 1) // batch_size
    for i in range(0, len(codes), batch_size):
        batch = codes[i : i + batch_size]
        batch_idx = i // batch_size + 1
        try:
            eq_df = client.get_equity_structure(
                code_list=batch,
                local_path=EQUITY_CACHE_DIR,
                is_local=False,
            )
        except Exception as e:
            print(f"[股票基础信息] 股本结构批次 {batch_idx}/{n_batches} 获取失败: {e}")
            _close_all_hdf5_handles()
            continue

        if eq_df is None or (isinstance(eq_df, pd.DataFrame) and eq_df.empty):
            _close_all_hdf5_handles()
            continue

        if isinstance(eq_df, dict):
            frames = []
            for _code, _df in eq_df.items():
                if _df is not None and not _df.empty:
                    frames.append(_df)
            if not frames:
                continue
            eq_df = pd.concat(frames, ignore_index=True)

        cols_upper = {str(c).upper(): c for c in eq_df.columns}
        mc_col = cols_upper.get("MARKET_CODE")
        cd_col = cols_upper.get("CHANGE_DATE")
        ts_col = cols_upper.get("TOT_SHARE")
        fs_col = cols_upper.get("FLOAT_SHARE")

        if not mc_col or not (ts_col or fs_col):
            print(f"[股票基础信息] 股本结构批次 {batch_idx}/{n_batches}: 缺少关键列，跳过 (列: {list(eq_df.columns)})")
            continue

        for mc, grp in eq_df.groupby(mc_col):
            if cd_col and cd_col in grp.columns:
                grp = grp.sort_values(cd_col, ascending=False)
            latest = grp.iloc[0]
            cap_map[mc] = {
                "tot_share": latest.get(ts_col) if ts_col else None,
                "float_share": latest.get(fs_col) if fs_col else None,
            }
        print(f"[股票基础信息] 股本结构批次 {batch_idx}/{n_batches}: 获取 {len(batch)} 只")
        del eq_df
        _close_all_hdf5_handles()
        _release_memory()

    return cap_map


def update_stock_meta(client) -> None:
    """全量更新股票基础信息（代码表）落盘到数据湖，与 xyszStockMetaRecorder 逻辑一致。"""
    import pandas as pd

    codes = _get_code_list_with_retry(client, security_type="EXTRA_STOCK_A")
    if not codes:
        print("[股票基础信息] 未获取到代码列表，跳过")
        return
    print(f"[股票基础信息] 开始下载 {len(codes)} 只股票...")

    # ---- 阶段 1: 批量获取股本结构（TOT_SHARE / FLOAT_SHARE）----
    print("[股票基础信息] 正在获取股本结构...")
    cap_map = _build_cap_map_from_equity_structure(client, codes)
    print(f"[股票基础信息] 股本结构获取完成，覆盖 {len(cap_map)} 只股票")

    # ---- 阶段 1.5: 拉最近 K 线取最新收盘价，用于 市值 = close × 股本（符合 Stock schema 流通/总市值）----
    print("[股票基础信息] 正在获取最新收盘价...")
    close_map = _build_close_map_from_kline(client, codes)

    # ---- 阶段 2: 获取基础信息 ----
    all_records = []
    batch_size = 500
    n_batches = (len(codes) + batch_size - 1) // batch_size
    for i in range(0, len(codes), batch_size):
        batch = codes[i : i + batch_size]
        batch_idx = i // batch_size + 1
        df = None
        try:
            df = client.get_stock_basic(batch)
            if df is None or df.empty:
                print(f"[股票基础信息] 批次 {batch_idx}/{n_batches}: get_stock_basic 返回空，跳过")
                continue

            float_caps = []
            total_caps = []
            for _, row in df.iterrows():
                market_code = row.get("MARKET_CODE")
                cap = cap_map.get(market_code) or {}
                close = close_map.get(market_code) if close_map else None
                float_share = cap.get("float_share")
                tot_share = cap.get("tot_share")
                if close is not None and isinstance(close, (int, float)) and close == close:
                    float_caps.append(float(close) * float(float_share) if float_share is not None else None)
                    total_caps.append(float(close) * float(tot_share) if tot_share is not None else None)
                else:
                    float_caps.append(None)
                    total_caps.append(None)
            
            df["FLOAT_CAP"] = float_caps
            df["TOTAL_CAP"] = total_caps

            all_records.append(df.copy())
            print(f"[股票基础信息] 批次 {batch_idx}/{n_batches}: 获取了 {len(df)} 只")
        except Exception as e:
            print(f"[股票基础信息] 批次 {batch_idx}/{n_batches}: get_stock_basic 调用异常: {e}")
        finally:
            if df is not None:
                del df
            _close_all_hdf5_handles()
            _release_memory()

    if not all_records:
        print("[股票基础信息] 无有效记录，跳过")
        return

    out = pd.concat(all_records, ignore_index=True)
    cap_valid = out["TOTAL_CAP"].notna().sum()
    print(f"[股票基础信息] 总计 {len(out)} 条，其中 {cap_valid} 条有市值数据（close×股本）")
    path = os.path.join(FINANCIAL_DIR, "stock_meta.parquet")
    _ensure_dir(path)
    _write_parquet_fast_compatible(out, path)
    print(f"[股票基础信息] 已保存 {len(out)} 条 -> {path}")


def update_industry(client) -> None:
    """全量更新行业基础信息与行业成分股。"""
    import pandas as pd

    _ensure_dir(EQUITY_SAVE_DIR)

    print("[行业] 开始下载 industry_base_info...")
    base_info = client.get_industry_base_info(local_path=EQUITY_CACHE_DIR, is_local=False)
    _save_api_result_as_parquet(
        base_info,
        os.path.join(EQUITY_SAVE_DIR, "industry_base_info.parquet"),
        ["INDEX_CODE"],
        "行业基础信息",
    )

    code_list = []
    if isinstance(base_info, dict) and base_info:
        code_list = list(base_info.keys())
    elif isinstance(base_info, pd.DataFrame) and not base_info.empty and "INDEX_CODE" in base_info.columns:
        code_list = base_info["INDEX_CODE"].unique().tolist()
    if not code_list:
        print("[行业] 无行业代码，跳过 industry_constituent")
        return

    print(f"[行业] 开始下载 industry_constituent（{len(code_list)} 个行业）...")
    constituent = client.get_industry_constituent(
        code_list=code_list, local_path=EQUITY_CACHE_DIR, is_local=False
    )
    _save_api_result_as_parquet(
        constituent,
        os.path.join(EQUITY_SAVE_DIR, "industry_constituent.parquet"),
        ["INDEX_CODE", "CON_CODE"],
        "行业成分股",
    )


def update_equity(client, codes: list[str] | None = None) -> None:
    """增量更新股东/股本/分红/配股。"""
    from datetime import datetime, timedelta

    if not codes:
        codes = _get_code_list_with_retry(client, security_type="EXTRA_STOCK_A")
    if not codes:
        print("[股东股本] 未获取到股票代码列表，跳过")
        return

    end_date = int(datetime.now().strftime("%Y%m%d"))
    begin_date = int((datetime.now() - timedelta(days=EQUITY_BEGIN_OFFSET_DAYS)).strftime("%Y%m%d"))
    if begin_date < 20000101:
        begin_date = 20000101

    for get_fn, filename, period_col, label in [
        (client.get_equity_pledge_freeze, "equity_pledge_freeze.parquet", "ANN_DATE", "股权冻结质押"),
        (client.get_equity_restricted, "equity_restricted.parquet", "LIST_DATE", "限售股解禁"),
    ]:
        _merge_financial_dict_to_path(
            get_fn,
            os.path.join(EQUITY_SAVE_DIR, filename),
            period_col,
            codes,
            begin_date,
            end_date,
            EQUITY_CACHE_DIR,
            label,
        )

    for get_fn, filename, keys, label in [
        (client.get_share_holder, "share_holder.parquet", ["MARKET_CODE", "HOLDER_ENDDATE"], "十大股东"),
        (client.get_holder_num, "holder_num.parquet", ["MARKET_CODE", "HOLDER_ENDDATE"], "股东户数"),
        (client.get_equity_structure, "equity_structure.parquet", ["MARKET_CODE", "CHANGE_DATE"], "股本结构"),
        (client.get_dividend, "dividend.parquet", ["MARKET_CODE", "DATE_EX"], "分红"),
        (client.get_right_issue, "right_issue.parquet", ["MARKET_CODE", "EX_DIVIDEND_DATE"], "配股"),
    ]:
        df = get_fn(
            code_list=codes,
            local_path=EQUITY_CACHE_DIR,
            is_local=False,
            begin_date=begin_date,
            end_date=end_date,
        )
        _merge_dataframe_to_path(df, os.path.join(EQUITY_SAVE_DIR, filename), keys, label)
        del df
        _release_memory()


# ============================================================================
#  入口
# ============================================================================

def _create_client():
    """登录并创建 AmazingDataClient 实例。"""
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    if SCRIPT_DIR not in sys.path:
        sys.path.insert(0, SCRIPT_DIR)
    from amazing_data_down import AmazingDataClient

    AmazingDataClient.login(
        username=LOGIN_USER,
        password=LOGIN_PASSWORD,
        host=LOGIN_HOST,
        port=LOGIN_PORT,
    )
    return AmazingDataClient()


def run_full_update() -> None:
    """执行全部增量更新，每个步骤用独立子进程运行。

    AmazingData SDK 的 C++ 层（SWIG 包装）在长时间运行后会累积内部状态，
    导致 Swig::DirectorMethodException 崩溃。子进程隔离可彻底规避此问题。

    子进程异常退出时会打印该步骤的 stdout/stderr，并在最后汇总失败步骤。
    """
    import subprocess

    script = os.path.abspath(__file__)
    python = sys.executable

    steps = ["--stock-meta", "--klines", "--financial", "--equity", "--adj-factor", "--industry"]
    failed_steps = []

    for flag in steps:
        step_name = flag.lstrip("-")
        print(f"\n{'='*60}")
        print(f"[run_full_update] 启动子进程: {step_name}")
        print(f"{'='*60}", flush=True)
        try:
            ret = subprocess.call(
                [python, "-u", script, flag],
                timeout=3600 * 6,
            )
        except subprocess.TimeoutExpired:
            ret = -1
            print(f"[run_full_update] {step_name} 子进程超时")
        except Exception as e:
            ret = -1
            print(f"[run_full_update] {step_name} 子进程启动或执行异常: {e}")

        if ret != 0:
            failed_steps.append((step_name, ret))
            print(f"[run_full_update] {step_name} 子进程退出码 {ret}，继续下一步...")
        else:
            print(f"[run_full_update] {step_name} 完成")

    print("\n" + "=" * 60)
    if failed_steps:
        print("[run_full_update] 以下步骤异常退出:")
        for name, code in failed_steps:
            print(f"  - {name}: 退出码 {code}")
        print("=" * 60)
        _notify_xysz_update_failed(failed_steps)
    else:
        print("daily_update 全部完成")
    print("=" * 60)


def _notify_xysz_update_failed(failed_steps: list) -> None:
    """子进程有失败时尝试发送通知（如微信），无配置则静默跳过。"""
    try:
        from zvt.informer.wechat_webhook import WechatWebhookInformer
        inf = WechatWebhookInformer()
        msg = "xysz 数据湖更新有步骤失败: " + ", ".join(f"{n}({c})" for n, c in failed_steps)
        inf.send_message(content=msg)
    except Exception:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="收盘后增量更新：股票基础信息、日线、财报、股东/股本/分红/配股、复权因子、行业"
    )
    parser.add_argument("--stock-meta", action="store_true", dest="stock_meta", help="仅更新股票基础信息（代码表）")
    parser.add_argument("--klines", action="store_true", help="仅更新日线")
    parser.add_argument("--financial", action="store_true", help="仅更新财报")
    parser.add_argument("--equity", action="store_true", help="仅更新股东/股本/分红/配股")
    parser.add_argument("--adj-factor", action="store_true", dest="adj_factor", help="仅更新复权因子")
    parser.add_argument("--industry", action="store_true", help="仅更新行业")
    args = parser.parse_args()

    if not (getattr(args, "stock_meta", False) or args.klines or args.financial or args.equity or args.adj_factor or args.industry):
        run_full_update()
        return

    _check_memory()
    client = _create_client()

    try:
        if args.stock_meta:
            update_stock_meta(client)
        if args.klines:
            update_klines_daily(client)
        if args.financial:
            update_financial(client)
        if args.equity:
            update_equity(client)
        if args.adj_factor:
            update_adj_factor(client)
        if args.industry:
            update_industry(client)
    finally:
        try:
            import AmazingData as ad
            ad.logout(username=LOGIN_USER)
            print("[logout] 已注销登录")
        except Exception:
            pass

    print("daily_update 完成")


if __name__ == "__main__":
    main()
