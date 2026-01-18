#!/bin/bash
# 使用 conda 环境 quant 运行复权因子诊断（需 pyarrow、zvt）
cd "$(dirname "$0")/.."
source /root/miniconda3/etc/profile.d/conda.sh 2>/dev/null || true
conda activate quant
exec python scripts/diagnose_backward_factor.py
