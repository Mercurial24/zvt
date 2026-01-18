import logging
import os
from typing import List
import pandas as pd
from zvt.domain import Stock1dKdata, Stock1dHfqKdata
from zvt.contract.api import df_to_db, get_db_session

logger = logging.getLogger(__name__)

# 数据湖目录
XYSZ_PARQUET_BASE_DIR = os.environ.get("XYSZ_PARQUET_BASE_DIR", "/data/stock_data/xysz_data/base_data")

def _normalize_code(c: str) -> str:
    s = str(c).strip()
    if "." in s:
        return s
    if not s or not s.isdigit():
        return s
    first = s[0]
    if first == "6":
        return f"{s}.SH"
    if first in ("0", "3"):
        return f"{s}.SZ"
    if first in ("4", "8"):
        return f"{s}.BJ"
    return f"{s}.SZ"

def xysz_code_to_entity_id(market_code: str) -> str:
    if not isinstance(market_code, str) or "." not in market_code:
        return f"stock_cn_{market_code}"
    code, exchange = market_code.strip().upper().rsplit(".", 1)
    ex = "sh" if exchange == "SH" else ("sz" if exchange == "SZ" else "bj")
    return f"stock_{ex}_{code}"

def compute_and_save_xysz_hfq(codes: List[str] = None, start_timestamp=None):
    """
    基于 Parquet 中的后复权因子和数据库中的日线，计算并保存后复权 K 线。
    """
    logger.info("开始计算 xysz 后复权 K 线...")

    factor_path = os.path.join(XYSZ_PARQUET_BASE_DIR, "backward_factor.parquet")
    if not os.path.isfile(factor_path):
        logger.error(f"未找到复权因子文件: {factor_path}")
        return

    # 加载复权因子 parquet
    logger.info("正在加载 Parquet 复权因子...")
    try:
        df_factor_wide = pd.read_parquet(factor_path)
    except Exception as e:
        logger.error(f"加载复权因子失败: {e}")
        return

    # 规范化列名和时间
    if df_factor_wide.index.names and "index" in (df_factor_wide.index.names or []) or isinstance(df_factor_wide.index, pd.DatetimeIndex):
        df_factor_wide = df_factor_wide.reset_index()
    
    date_col = df_factor_wide.columns[0]
    if date_col == "index":
        df_factor_wide = df_factor_wide.rename(columns={"index": "timestamp"})
        date_col = "timestamp"

    if pd.api.types.is_numeric_dtype(df_factor_wide[date_col]):
        df_factor_wide = df_factor_wide[df_factor_wide[date_col] > 19000101]
        df_factor_wide["timestamp"] = pd.to_datetime(df_factor_wide[date_col].astype(int).astype(str), format="%Y%m%d", errors="coerce")
    else:
        df_factor_wide["timestamp"] = pd.to_datetime(df_factor_wide[date_col], errors="coerce")

    # 构建 code 到 entity_id 的映射
    code_cols = [c for c in df_factor_wide.columns if c not in [date_col, "timestamp"]]
    code_to_entity = {c: xysz_code_to_entity_id(_normalize_code(c)) for c in code_cols}
    # 反向查找：根据 entity_id 找 code_col
    entity_to_code = {v: k for k, v in code_to_entity.items()}

    session = get_db_session(provider="xysz", data_schema=Stock1dKdata)
    
    if codes:
        df_entities = Stock1dKdata.query_data(
            provider="xysz", 
            columns=[Stock1dKdata.entity_id, Stock1dKdata.code], 
            limit=1000000 
        )
        if df_entities is not None and not df_entities.empty:
            mapping = df_entities.drop_duplicates(subset=['code'])
            target_ids = mapping[mapping['code'].isin(codes)]['entity_id'].tolist()
        else:
            target_ids = []
    else:
        entities = session.query(Stock1dKdata.entity_id).distinct().all()
        target_ids = [e[0] for e in entities]

    if not target_ids:
        logger.error("未找到任何 entity_id，计算终止。")
        return

    logger.info(f"共获取到 {len(target_ids)} 只股票准备进行计算入库...")

    batch_size = 50
    import tqdm
    for i in tqdm.tqdm(range(0, len(target_ids), batch_size), desc="批处理进度"):
        batch_ids = target_ids[i:i + batch_size]
        
        # 增量更新条件
        filters = [Stock1dKdata.entity_id.in_(batch_ids)]
        if start_timestamp:
            filters.append(Stock1dKdata.timestamp >= start_timestamp)

        df_kdata = Stock1dKdata.query_data(provider="xysz", filters=filters)
        if df_kdata is None or df_kdata.empty:
            continue
            
        # 从宽表中提取当前 batch 的因子
        batch_cols = [entity_to_code.get(e) for e in batch_ids if entity_to_code.get(e) in df_factor_wide.columns]
        if not batch_cols:
            continue
            
        df_adj = df_factor_wide[["timestamp"] + batch_cols].copy()
        df_adj = df_adj.melt(id_vars=["timestamp"], value_vars=batch_cols, var_name="code", value_name="hfq_factor")
        df_adj = df_adj.dropna(subset=["hfq_factor"])
        df_adj["entity_id"] = df_adj["code"].map(code_to_entity)

        df_merged = pd.merge(
            df_kdata, 
            df_adj[['entity_id', 'timestamp', 'hfq_factor']], 
            on=['entity_id', 'timestamp'], 
            how='left'
        )
        
        # 针对每只股票分别做 ffill() 和 bfill()
        df_merged['hfq_factor'] = df_merged.groupby('entity_id')['hfq_factor'].transform(lambda x: x.ffill().bfill())
        df_merged['hfq_factor'] = df_merged['hfq_factor'].fillna(1.0)
        
        df_hfq = df_merged.copy()
        for col in ['open', 'close', 'high', 'low']:
            df_hfq[col] = df_hfq[col] * df_hfq['hfq_factor']
        
        df_hfq['volume'] = df_hfq['volume'] / df_hfq['hfq_factor']
        
        df_hfq = df_hfq.drop(columns=['hfq_factor', 'id'], errors='ignore')
        df_hfq['id'] = df_hfq['entity_id'] + '_' + df_hfq['timestamp'].dt.strftime('%Y-%m-%d')
        df_hfq['level'] = '1d'
        
        df_to_db(df=df_hfq, data_schema=Stock1dHfqKdata, provider="xysz", force_update=True)
        # 清理内存
        del df_kdata, df_adj, df_merged, df_hfq

    logger.info("后复权 K线数据计算并建库完毕。")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    compute_and_save_xysz_hfq(codes=None)
