# -*- coding: utf-8 -*-
import logging
import os
from typing import List, Union, Type, Dict

import pandas as pd
import polars as pl
from sqlalchemy.ext.declarative import DeclarativeMeta

from zvt.contract import IntervalLevel
from zvt.contract.schema import Mixin
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_pd_timestamp

logger = logging.getLogger(__name__)

# Parquet 存储基准路径，应从环境变量读取或在系统初始化时配置
# 可由使用者覆盖，默认同 zvt_daily_job.py 中保持一致
XYSZ_PARQUET_BASE_DIR = os.environ.get("XYSZ_PARQUET_BASE_DIR", "/mnt/point/stock_data/xysz_data/base_data")
QMT_PARQUET_BASE_DIR = os.environ.get("QMT_PARQUET_BASE_DIR", "/mnt/point/stock_data/qmt_data/base_data")

def get_parquet_dir(provider: str) -> str:
    if provider == "xysz":
        return XYSZ_PARQUET_BASE_DIR
    elif provider == "qmt":
        return QMT_PARQUET_BASE_DIR
    else:
        # Default fallback
        return os.path.join(XYSZ_PARQUET_BASE_DIR, f"{provider}_data")

class ParquetReader:
    """
    提供和数据库 SQLAlchemy get_data 类似的接口，直接从 Parquet 数据湖中读取数据。
    基于 Polars 实现高性能谓词下推与列修剪。
    """
    
    @classmethod
    def get_data(
        cls,
        data_schema: Type[Mixin],
        ids: List[str] = None,
        entity_ids: List[str] = None,
        entity_id: str = None,
        codes: List[str] = None,
        code: str = None,
        level: Union[IntervalLevel, str] = None,
        provider: str = None,
        columns: List = None,
        col_label: dict = None,
        return_type: str = "df",
        start_timestamp: Union[pd.Timestamp, str] = None,
        end_timestamp: Union[pd.Timestamp, str] = None,
        filters: List = None,  # SQLAlchemy 过滤器，在 Parquet 中不完全支持，如需过滤推荐用 pandas 读出来后操作
        order=None, # 同上
        limit: int = None, # 支持基础 limit 对抗 OOM
        distinct=None, # 同上
        index: Union[str, list] = None,
        drop_index_col=False,
        time_field: str = "timestamp",
    ):
        """
        通过 Polars 从 Parquet 文件获取数据。支持单一文件与分区目录。
        """
        if "providers" not in data_schema.__dict__:
            logger.error(f"no provider registered for: {data_schema}")
        if not provider:
            provider = data_schema.providers[0]

        # 根据 schema 的 __tablename__ 或者某种映射决定具体的 parquet 文件/目录名
        # ZVT 中 table 名通常是小写带下划线，例如 stock_1d_kdata, xysz_balance_sheet 等
        table_name = data_schema.__tablename__
        
        # 定义 Schema 到 Parquet 文件/目录的映射体系 (数据湖规范)
        # 这里需要适配 data_engine_xysz/qmt 的 daily_update.py 以及 `factor` 保存目录
        # 这里是一套硬编码的约定，你也可以将其抽象到 data_schema.parquet_path 中
        parquet_base = get_parquet_dir(provider)
        
        target_path = None
        # 特例映射：
        if table_name == "stock_1d_kdata":
            # 优先使用新的年度格式目录，不存在时回退到旧的日期分区目录
            yearly_path = os.path.join(parquet_base, "klines_yearly")
            daily_path  = os.path.join(parquet_base, "klines_daily_dir")
            target_path = yearly_path if os.path.isdir(yearly_path) else daily_path
        elif table_name == "stock_1d_hfq_kdata":
            yearly_path = os.path.join(parquet_base, "klines_yearly_hfq")
            daily_path  = os.path.join(parquet_base, "klines_daily_hfq_dir")
            target_path = yearly_path if os.path.isdir(yearly_path) else daily_path
        else:
            # 默认映射规则，例如 xysz_balance_sheet -> balance_sheet.parquet
            # 移除 schema 表名前缀，如 xysz_, qmt_ 等
            clean_name = table_name.replace(f"{provider}_", "", 1)
            target_path = os.path.join(parquet_base, f"{clean_name}.parquet")
            if not os.path.exists(target_path): # 判断是否有直接以此命名的全称
                 target_path = os.path.join(parquet_base, f"{table_name}.parquet")
                 
            # 如果是 Factor 计算产生的结果，约定保存在 factors 子目录下 (例如 src/zvt/factors)
            if not os.path.exists(target_path) and hasattr(data_schema, "factor_name"):
                target_path = os.path.join(parquet_base, "factors", f"{data_schema.__tablename__}")
                if not os.path.exists(target_path) and not os.path.isdir(target_path):
                     target_path = os.path.join(parquet_base, "factors", f"{data_schema.__tablename__}.parquet")

        if not os.path.exists(target_path):
            logger.error(f"Parquet source not found for schema {table_name}: {target_path}")
            return None if return_type == "df" else []

        try:
            if os.path.isdir(target_path):
                entries = os.listdir(target_path)
                if not entries:
                    logger.warning(f"Empty directory: {target_path}")
                    return None if return_type == "df" else []

                first = sorted(entries)[0]

                # ----------------------------------------------------------
                # 年度文件格式：year=YYYY.parquet（新格式，推荐）
                # 只加载日期范围命中的年份文件，文件数极少（~13 个），
                # 文件内按 code 排序，Polars 谓词下推能跳过无关行组。
                # ----------------------------------------------------------
                if first.startswith("year="):
                    year_start = to_pd_timestamp(start_timestamp).year if start_timestamp else 1990
                    year_end   = to_pd_timestamp(end_timestamp).year   if end_timestamp   else 9999
                    selected_files = []
                    for e in entries:
                        if not e.startswith("year=") or not e.endswith(".parquet"):
                            continue
                        try:
                            y = int(e[5:-8])  # "year=2024.parquet" -> 2024
                        except ValueError:
                            continue
                        if year_start <= y <= year_end:
                            selected_files.append(os.path.join(target_path, e))

                    if not selected_files:
                        logger.warning(f"No yearly files match years [{year_start}, {year_end}] in {target_path}")
                        return None if return_type == "df" else []

                    selected_files.sort()
                    logger.debug(f"Yearly pruning: {len(selected_files)}/{len(entries)} files selected")
                    # 年度文件内 kline_time 存的是 μs，但 Polars 多文件合并 schema
                    # 时有时推断成 ns，导致精度冲突。按 Polars 提示传入 cast_options。
                    try:
                        lazy_df = pl.scan_parquet(
                            selected_files,
                            cast_options=pl.ScanCastOptions(datetime_cast="nanosecond-downcast"),
                        )
                    except TypeError:
                        # 旧版本 Polars 不支持 cast_options，直接扫描
                        lazy_df = pl.scan_parquet(selected_files)

                # ----------------------------------------------------------
                # 旧日期分区格式：date=YYYYMMDD/（兼容保留）
                # 先通过 os.listdir 过滤分区目录，减少 PyArrow 遍历次数。
                # ----------------------------------------------------------
                elif first.startswith("date="):
                    import pyarrow.dataset as ds
                    date_start_int = int(to_pd_timestamp(start_timestamp).strftime('%Y%m%d')) if start_timestamp else 0
                    date_end_int   = int(to_pd_timestamp(end_timestamp).strftime('%Y%m%d'))   if end_timestamp   else 99999999
                    subdirs = []
                    for e in entries:
                        try:
                            d = int(e.split("=", 1)[1])
                        except (ValueError, IndexError):
                            continue
                        if date_start_int <= d <= date_end_int:
                            subdirs.append(os.path.join(target_path, e))
                    if not subdirs:
                        logger.warning(f"No date partitions match [{date_start_int}, {date_end_int}]")
                        return None if return_type == "df" else []
                    logger.debug(f"Date partition pruning: {len(subdirs)}/{len(entries)} selected")
                    dataset = ds.dataset(subdirs, format="parquet", partitioning="hive")
                    lazy_df = pl.scan_pyarrow_dataset(dataset)

                else:
                    # 未知分区类型，全量扫描
                    import pyarrow.dataset as ds
                    dataset = ds.dataset(target_path, format="parquet", partitioning="hive")
                    lazy_df = pl.scan_pyarrow_dataset(dataset)
            else:
                lazy_df = pl.scan_parquet(target_path)

        except Exception as e:
            logger.error(f"Error scanning parquet {target_path}: {e}")
            return None if return_type == "df" else []

        # 获取 Schema 实际存在的列，做交集防止选取不存在的列爆错
        available_cols = lazy_df.collect_schema().names()

        # -----------------------------
        # 列修剪 (Column Pruning)
        # -----------------------------
        select_cols = []
        if columns:
            # columns 可能包含 SQLAlchemy Column 对象，转为 str
            for col in columns:
                if isinstance(col, str):
                    select_cols.append(col)
                elif hasattr(col, "name"):
                    select_cols.append(col.name)
            
            # 必须带上 time_field, id, entity_id 这些核心列以便作为 index
            mandatory_cols = ["id", "entity_id", time_field, "code"]
            for mc in mandatory_cols:
                 if mc in available_cols and mc not in select_cols:
                     select_cols.append(mc)
            final_select_cols = [c for c in select_cols if c in available_cols]
            
            # 为了后面归一化 logic (合成 timestamp)，必须确保 date 或 kline_time 被保留
            for extra in ["date", "kline_time"]:
                if extra in available_cols and extra not in final_select_cols:
                    final_select_cols.append(extra)
        else:
            final_select_cols = None

        # -----------------------------
        # 谓词下推 (Predicate Pushdown)
        # -----------------------------
        filter_exprs = []

        # 时间过滤
        if start_timestamp:
            st = to_pd_timestamp(start_timestamp)
            # 在某些表中，时间字段在 parquet 里面存的是 int64 的 YYYYMMDD，比如 K 线里的 'date'
            if time_field == "timestamp" and "timestamp" not in available_cols and "date" in available_cols:
                # 兼容 qmt/xysz_daily 跑出来的分区的 date 字段
                date_int = int(st.strftime('%Y%m%d'))
                filter_exprs.append(pl.col("date") >= date_int)
            else:
                 if time_field in available_cols:
                     # 假设是 datetime 
                     filter_exprs.append(pl.col(time_field) >= st)
                 elif "kline_time" in available_cols:
                     filter_exprs.append(pl.col("kline_time") >= st)

        if end_timestamp:
            et = to_pd_timestamp(end_timestamp)
            if time_field == "timestamp" and "timestamp" not in available_cols and "date" in available_cols:
                date_int = int(et.strftime('%Y%m%d'))
                filter_exprs.append(pl.col("date") <= date_int)
            else:
                 if time_field in available_cols:
                     filter_exprs.append(pl.col(time_field) <= et)
                 elif "kline_time" in available_cols:
                     filter_exprs.append(pl.col("kline_time") <= et)

        # 实体过滤
        target_entities = []
        target_codes = []
        if entity_id: target_entities.append(entity_id)
        if entity_ids: target_entities.extend(entity_ids)
        if code: target_codes.append(code)
        if codes: target_codes.extend(codes)

        if target_entities and "entity_id" in available_cols:
            filter_exprs.append(pl.col("entity_id").is_in(target_entities))
        elif target_codes and "code" in available_cols:
            filter_exprs.append(pl.col("code").is_in([str(c) for c in target_codes]))
        elif target_entities and "code" in available_cols:
            # map entity_id (e.g. stock_sz_000001) to code (e.g. 000001.SZ or 000001)
            mapped_codes = []
            for eid in target_entities:
                 parts = eid.split("_")
                 if len(parts) >= 3:
                      # stock_sz_000001 -> 000001.SZ
                      c = parts[2]
                      ex = parts[1].upper()
                      mapped_codes.append(f"{c}.{ex}")
                 else:
                      mapped_codes.append(eid)
            filter_exprs.append(pl.col("code").is_in(mapped_codes))

        # IDs 过滤
        target_ids_list = []
        if ids: target_ids_list.extend(ids)
        if target_ids_list and "id" in available_cols:
            filter_exprs.append(pl.col("id").is_in(target_ids_list))

        # 执行过滤
        if filter_exprs:
            # 将所有的 filter expressions 用 AND 连接起来
            combined_filter = filter_exprs[0]
            for expr in filter_exprs[1:]:
                combined_filter = combined_filter & expr
            lazy_df = lazy_df.filter(combined_filter)
            
        # -----------------------------
        # Collect 数据
        # -----------------------------
        try:
            if final_select_cols:
                lazy_df = lazy_df.select(final_select_cols)

            if limit is not None:
                lazy_df = lazy_df.limit(limit)
            
            # Streaming 模式 collect 来减少内存膨胀
            # 较新版本的 polars 没有 collect(streaming=True) 而是直接 collect
            df = lazy_df.collect().to_pandas()
        except Exception as e:
            logger.error(f"Failed to collect parquet data from {target_path} : {e}")
            return None if return_type == "df" else []

        if df.empty:
            return None if return_type == "df" else []

        # -----------------------------
        # 格式归一化 (匹配 SQLAlchemy get_data)
        # -----------------------------
        
        # 数据湖出来的数据，如果没有 entity_id 但有 code
        if "entity_id" not in df.columns and "code" in df.columns:
            # 简单转换
            def code_to_entity(c):
                s = str(c).strip()
                ex = "sz"
                if s.startswith(("6", "SH")): ex = "sh"
                if s.endswith("SH"): ex = "sh"
                if s.endswith("SZ"): ex = "sz"
                if s.endswith("BJ"): ex = "bj"
                _c = s.split(".")[0] if "." in s else s
                return f"stock_{ex}_{_c}"
            df["entity_id"] = df["code"].apply(code_to_entity)

        # 还原时间列
        if time_field == "timestamp" and "timestamp" not in df.columns:
             if "kline_time" in df.columns:
                 df["timestamp"] = pd.to_datetime(df["kline_time"])
             elif "date" in df.columns:
                 df["timestamp"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d", errors="coerce")

        # 如果没有 id 有 entity_id 和 time_field
        if "id" not in df.columns and "entity_id" in df.columns:
             if time_field in df.columns:
                 df["id"] = df["entity_id"] + "_" + df[time_field].astype(str)
             elif "timestamp" in df.columns:
                 df["id"] = df["entity_id"] + "_" + df["timestamp"].astype(str)
             elif "date" in df.columns:
                 df["id"] = df["entity_id"] + "_" + df["date"].astype(str)

        if pd_is_not_null(df):
            # Sort 按 sqlalchemy 默认行为
            sort_cols = [c for c in ["entity_id", time_field] if c in df.columns]
            if sort_cols:
                df = df.sort_values(by=sort_cols)
                
            if index:
                from zvt.utils.pd_utils import index_df
                df = index_df(df, index=index, drop=drop_index_col, time_field=time_field)

        if return_type == "df":
            return df
        elif return_type == "domain":
             # NOT RECOMMENDED FOR PARQUET, very slow and memory intensive, just fake it with dict records for now
             # We should avoid returning SQLAlchemy domain objects from Parquet.
             # Return dict instead.
             return df.to_dict("records")
        elif return_type == "dict":
            return df.to_dict("records")

        return df
