#!/usr/bin/env bash
set -euo pipefail

# 用法:
#   bash scripts/backfill_xysz_dividend_yield.sh
#
# 功能:
#   1) 从 xysz parquet 导入分红原料到 ZVT DB
#   2) 重算 xysz 估值并写入 dividend_ps_ttm / dividend_yield_ttm

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

echo "==> [1/2] 导入 xysz 分红原料 (dividend.parquet -> xysz_dividend_financing.db)"
conda run -n quant python scripts/import_xysz_parquet_to_zvt.py \
  --provider xysz \
  --only dividend

echo "==> [2/2] 重算 xysz 估值并写入股息率字段"
conda run -n quant python -c "from zvt.recorders.xysz.finance.xysz_valuation_recorder import xyszValuationRecorder; xyszValuationRecorder(force_update=True, sleeping_time=0).run()"

echo "==> 完成: xysz 分红原料已入库，股息率字段已回填。"
