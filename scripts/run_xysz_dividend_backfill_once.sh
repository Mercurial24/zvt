#!/usr/bin/env bash
set -euo pipefail

# 一次性回填 xysz 股息率所需原料并重算估值
# 用法:
#   bash scripts/run_xysz_dividend_backfill_once.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

echo "==> [1/2] 回填 xysz 分红原料到 DB (dividend.parquet -> dividend_detail)"
conda run -n quant python scripts/import_xysz_parquet_to_zvt.py \
  --provider xysz \
  --only dividend

echo "==> [2/2] 全量重算 xysz 估值（含 dividend_ps_ttm/dividend_yield_ttm）"
conda run -n quant python -c "from zvt.recorders.xysz.finance.xysz_valuation_recorder import xyszValuationRecorder; xyszValuationRecorder(force_update=True, sleeping_time=0).run()"

echo "==> 完成"
