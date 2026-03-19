# 数据存储策略：设计初衷与当前实现对照

本文档对照「SQLite 存什么、Parquet 存什么」的设计初衷，说明当前 ZVT 实现是否符合、以及差异与可改进点。

---

## 一、设计初衷简述

| 存储 | 适用数据特点 | 应放入的数据 |
|------|--------------|--------------|
| **SQLite** | 高频修改、低频读取、关系复杂 | 代码表、交易日历、除权除息表、财务报表原始数据、个人交易数据 |
| **Parquet** | 海量、结构整齐、回测时整块读取 | K 线、Level-2/Tick、预计算因子库 |

---

## 二、当前实现：读取路由（`get_data`）

在 `src/zvt/contract/api.py` 中，**读取**时的路由规则为：

- 若 Schema 带 `storage_type`，则按其值选引擎。
- 否则按 `__tablename__` 自动路由：
  - 表名包含 **`kdata`、`tick`、`factor`** → **Parquet**
  - 其余 → **SQLite**

因此：

- **K 线**（`stock_1d_kdata`、`stock_1d_hfq_kdata` 等）→ 读 Parquet ✅  
- **预计算因子**（表名含 `factor` 或 Schema 显式设 `storage_type="parquet"`）→ 读 Parquet ✅  
- **代码表、财报、除权除息、日历、估值、板块等** → 读 SQLite ✅  

与「K 线 / 因子走 Parquet，其余走 SQLite」的初衷一致。

---

## 三、逐类数据对照

### 1. 应该放进 SQLite 的数据

| 类型 | 设计初衷 | 当前实现 | 是否符合 |
|------|----------|----------|----------|
| **代码表 (Security Master)** | 股票代码、名称、上市/退市、行业、板块等 | `stock`、`stock_detail`、`block`、`block_stock` 等仅由 Recorder 写 SQLite；表名不含 kdata/tick/factor，读走 SQLite | ✅ 符合 |
| **交易日历 (Calendar)** | 交易日、调休等 | `stock_trade_day` 等存在 domain，读走 SQLite | ✅ 符合 |
| **除权除息表 (Adjustments)** | 送转、派息、拆股等 | `dividend_detail`、`rights_issue_detail` 等；数据湖有 dividend/right_issue.parquet 经 import 入 ZVT 后进 SQLite，读走 SQLite | ✅ 符合（Parquet 为数据源，查询以 SQLite 为主） |
| **财务报表原始数据** | 三大报表原始科目 | 数据湖有 balance_sheet/income/cash_flow.parquet，`import_xysz_parquet_to_zvt` 导入到 ZVT 的 SQLite；`balance_sheet`、`income_statement`、`cash_flow_statement` 表名不含 kdata/tick/factor，读走 SQLite | ✅ 符合（Parquet 作归档/ETL 源，查询用 SQLite） |
| **个人交易数据** | 资金流、成交、委托、持仓 | 若存在 Trades/Orders 等表，按当前规则会走 SQLite；当前 daily_job 未体现此类 Recorder | ⚠️ 未在每日任务中体现，路由规则符合 |

### 2. 必须放进 Parquet 的数据

| 类型 | 设计初衷 | 当前实现 | 是否符合 |
|------|----------|----------|----------|
| **K 线 (OHLCV)** | 海量、回测整块读 | 数据湖：`klines_daily_dir`、`klines_daily_hfq_dir` 等；`get_data` 对含 `kdata` 的 Schema 走 Parquet；`ParquetReader` 映射 `stock_1d_kdata` → `klines_daily_dir` 等 | ✅ 符合 |
| **Level-2 / Tick** | 高频、海量 | 表名含 `tick` 会走 Parquet；具体表与数据湖需按实装核对 | ✅ 路由符合 |
| **预计算因子库 (Factor Store)** | 回测时直接读全市场序列，不现场算 | `factor.py` 支持 `storage_type="parquet"`，写入 `{parquet_base}/factors/{tablename}`；表名含 `factor` 时读走 Parquet | ✅ 符合 |

---

## 四、与初衷的差异与注意点

1. **财务报表「双写」**  
   设计是「财务报表原始数据放 SQLite」。当前是：数据湖先落 Parquet，再导入 SQLite；读时走 SQLite。即 SQLite 为查询主存储，Parquet 为上游/归档，与初衷不冲突，只是多了一层 Parquet 数据源。

2. **K 线写入路径**  
   日线等由 `daily_update_*` 写入数据湖 Parquet；部分 Recorder（如 xyszStockKdataRecorder、QMTStockKdataRecorder）仍会写 SQLite。读时已统一走 Parquet（表名含 kdata），因此回测与批量读已符合「K 线必须 Parquet」；若希望完全统一，可后续改为 Recorder 只写 Parquet 或仅用 SQLite 做增量/补数。

3. **交易日历**  
   有 `stock_trade_day` 等 Schema，读走 SQLite，符合「日历放 SQLite」。

4. **个人交易数据**  
   设计上应只放 SQLite；当前路由下此类表会走 SQLite。若后续有 Trades/Orders 等，只要表名不含 kdata/tick/factor 即可保持符合。

---

## 五、结论

- **读取侧**：按表名/`storage_type` 的规则已实现「K 线 / Tick / 因子 → Parquet，其余（代码表、日历、除权除息、财报、估值、板块等）→ SQLite」，与设计初衷一致。  
- **写入侧**：代码表、日历、除权除息、财报、估值等以 SQLite 为主（财报与除权除息另有 Parquet 数据源）；K 线与因子以 Parquet 为主且读路径已走 Parquet。  
- 若需严格「只存一份」，可后续收敛：例如财报/除权除息仅保留 SQLite 或明确 Parquet 仅作归档；K 线可考虑 Recorder 只写 Parquet 或明确 SQLite 仅作补数用途。
