# ZVT 数据层看板

本文档描述本项目的**数据库全貌**：数据来源、落盘情况、每日更新方式及相关文档索引，作为数据层统一看板维护。

---

## 1. 数据存储结构

- **根路径**：由 `ZVT_HOME` 或默认 `zvt_env["data_path"]` 决定（见 `src/zvt/consts.py`，默认示例：`/data/code/zvt/zvt-home`）。
- **库文件规则**：按 `{data_path}/{provider}/{provider}_{db_name}.db` 存放 SQLite。
  - 例：xysz 日 K → `{data_path}/xysz/xysz_stock_1d_kdata.db`
  - 例：qmt 财务 → `{data_path}/qmt/qmt_finance.db`
- **查询时**：需指定 `provider`，同一张逻辑表（如 `BalanceSheet`）下不同 provider 存在各自 db 文件中。

---

## 2. 数据源总览

| 数据源             | 说明                                                       | 相关文档 / 脚本                                                                                |
| ------------------ | ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| **xysz**           | 星河数智 / AmazingData，A 股行情、财务、股东、行业等       | [docs/XYSZ_PARQUET_IMPORT.md](XYSZ_PARQUET_IMPORT.md)、`scripts/import_xysz_parquet_to_zvt.py` |
| **qmt**            | 迅投 QMT（Windows），通过 RPC 在 Linux 拉行情与财务        | [docs/AGENT_GUIDE_QMT_LINUX.md](AGENT_GUIDE_QMT_LINUX.md)、`scripts/update_qmt_data.py`        |
| **akshare**        | AkShare 接口，板块、宏观等                                 | —                                                                                              |
| **em / eastmoney** | 东方财富，港股/美股/指数/新闻等（本看板重点为 xysz / qmt） | —                                                                                              |
| **joinquant**      | 聚宽（本项目中未在每日任务中启用）                         | —                                                                                              |

---

## 3. 各数据源：数据从哪儿来、哪些已落盘

### 3.1 xysz（星河数智 / AmazingData）

**数据从哪儿来**

- **方式 A**：上游脚本（如 `my_quant_begin/data_engine_xysz/amazing_data_down.py`、`daily_update.py`）下载到 **Parquet 数据湖**，路径默认：`/data/stock_data/xysz_data/base_data`（可用环境变量 `XYSZ_PARQUET_BASE_DIR` 覆盖）。
- **方式 B**：ZVT 内 **xysz Recorder** 直接调 AmazingData API，按实体增量拉取并写入 ZVT SQLite。

**相关文档**

- Parquet → ZVT 导入说明：[docs/XYSZ_PARQUET_IMPORT.md](XYSZ_PARQUET_IMPORT.md)
- 导入脚本（分批、列名映射）：`scripts/import_xysz_parquet_to_zvt.py`

**已落盘数据（ZVT 表 + provider）**

| 逻辑表 / db_name                                             | 说明                                 | 备注                          |
| ------------------------------------------------------------ | ------------------------------------ | ----------------------------- |
| Stock (stock_meta)                                           | 股票列表、基础信息                   | xysz 股票列表                 |
| Block / BlockStock (block_meta)                              | 行业板块、成分股                     | 行业基础信息 + 成分股映射     |
| Stock1dKdata (stock_1d_kdata)                                | 日 K 线（不复权）                    | 可来自 Parquet 或 Recorder    |
| StockAdjFactor (stock_adj_factor)                            | 后复权因子                           | 仅 xysz 使用该 schema         |
| Stock1dHfqKdata (stock_1d_hfq_kdata)                         | 日 K 后复权                          | 由脚本根据日 K + 复权因子计算 |
| BalanceSheet / IncomeStatement / CashFlowStatement (finance) | 资产负债表、利润表、现金流量表       | 三表同一 db_name              |
| StockValuation (valuation)                                   | 估值（PE/PB/PS/PCF、市值、换手率等） | 由 xysz 财务 + K 线计算       |
| HolderNum (holder)                                           | 股东户数                             |                               |
| TopTenHolder (holder)                                        | 十大股东                             |                               |
| DividendDetail / RightsIssueDetail (dividend_financing)      | 分红、配股                           | 可由 Parquet 导入             |

**Parquet 与 Recorder 对应**

- 导入脚本支持的 Parquet：`klines_daily`、`balance_sheet`、`income`、`cash_flow`、`holder_num`、`share_holder`、`dividend`、`right_issue`、`industry_base_info`、`industry_constituent`、复权因子等（见 `import_xysz_parquet_to_zvt.py` 内 `--only` 说明）。
- 列名映射与 xysz 的 Recorder 一致（见 `src/zvt/recorders/xysz/` 下各 `_get_column_map()`）。

---

### 3.2 QMT（迅投）

**数据从哪儿来**

- **运行环境**：QMT（xtquant）仅支持 Windows；Linux 通过 **HTTP RPC** 访问 Windows 上的 `data_engine_qmt/server_windows.py`，由 Windows 执行 xtdata 等接口。
- **入口脚本**：`scripts/update_qmt_data.py`（建议使用 conda 环境 `quant`）。

**相关文档**

- Linux 侧接入说明、RPC 架构、常见问题：[docs/AGENT_GUIDE_QMT_LINUX.md](AGENT_GUIDE_QMT_LINUX.md)
- QMT 转发服务（Windows ↔ Ubuntu）：[docs/QMT_FORWARD_SERVICE.md](QMT_FORWARD_SERVICE.md)

**已落盘数据（ZVT 表 + provider）**

| 逻辑表 / db_name                                             | 说明                            | 备注                            |
| ------------------------------------------------------------ | ------------------------------- | ------------------------------- |
| Stock (stock_meta)                                           | 股票列表                        | QMT 全市场列表                  |
| Index1dKdata (index_1d_kdata)                                | 重要指数日 K                    | 需在 schema 中注册 provider qmt |
| BalanceSheet / IncomeStatement / CashFlowStatement (finance) | 资产负债表、利润表、现金流量表  | 列名与 QMT 财务字段全量映射     |
| Stock1dKdata (stock_1d_kdata)                                | 日 K（不复权 bfq / 后复权 hfq） | 按 adjust_type 分表写入         |

**说明**

- 财务三表：列映射与落盘逻辑见 `src/zvt/recorders/qmt/finance/qmt_finance_recorder.py`（含列名归一化与 QMT 文档字段映射）。
- 指数日 K：`Index1dKdata` 的 `register_schema` 需包含 `"qmt"`（见 `src/zvt/domain/quotes/index/index_1d_kdata.py`）。

---

### 3.3 Akshare

**数据从哪儿来**

- 直接调用 AkShare 接口。

**已落盘数据（本每日任务涉及）**

| 逻辑表 / db_name                      | 说明           |
| ------------------------------------- | -------------- |
| Block (block_meta)                    | 板块基础信息   |
| ChinaMoneySupply (china_money_supply) | 中国货币供应量 |

---

## 4. 每日数据更新流程

**主入口**：`zvt_daily_job.py`（根目录）。

**执行方式**

- 立即执行一次：`python zvt_daily_job.py --now`
- 定时执行：默认每天 16:30（周一至周五），可通过 `--time HH:MM` 修改。

**当前每日任务列表（顺序执行）**

| 序号 | 任务名称                 | 数据源  | 说明                                     |
| ---- | ------------------------ | ------- | ---------------------------------------- |
| 1    | 股票基础信息 (xysz)      | xysz    | xyszStockMetaRecorder                    |
| 2    | 板块基础信息 (Akshare)   | akshare | AkshareBlockRecorder                     |
| 3    | 行业板块信息 (xysz)      | xysz    | 行业 Block + BlockStock                  |
| 4    | 行业成分股映射 (xysz)    | xysz    |                                          |
| 5    | 中国货币供应量 (Akshare) | akshare | ChinaMoneySupplyRecorder                 |
| 6    | 复权因子 (xysz)          | xysz    | xyszStockAdjFactorRecorder               |
| 7    | 日线 K 线数据 (xysz)     | xysz    | xyszStockKdataRecorder, level='1d'       |
| 8    | 后复权日线 (xysz)        | xysz    | compute_and_save_xysz_hfq（近约 15 日）  |
| 9    | 资产负债表 (xysz)        | xysz    | xyszBalanceSheetRecorder                 |
| 10   | 利润表 (xysz)            | xysz    | xyszIncomeStatementRecorder              |
| 11   | 现金流量表 (xysz)        | xysz    | xyszCashFlowRecorder                     |
| 12   | 估值/市盈率 (xysz)       | xysz    | xyszValuationRecorder（PE/PB/PS/PCF 等） |
| 13   | QMT 股票列表             | qmt     | QMTStockRecorder                         |
| 14   | QMT 指数日线             | qmt     | QmtIndexRecorder（IMPORTANT_INDEX）      |
| 15   | QMT 资产负债表           | qmt     | QmtBalanceSheetRecorder                  |
| 16   | QMT 利润表               | qmt     | QmtIncomeStatementRecorder               |
| 17   | QMT 现金流量表           | qmt     | QmtCashFlowRecorder                      |
| 18   | QMT 日线 (不复权)        | qmt     | QMTStockKdataRecorder, bfq               |
| 19   | QMT 日线 (后复权)        | qmt     | QMTStockKdataRecorder, hfq               |

**说明**

- 阶段 1（Parquet 数据湖更新）、阶段 2（先 Parquet 导入再 Recorder 补缺）在 `zvt_daily_job.py` 中当前为注释状态；若启用，需保证数据湖路径与 `run_import_xysz_parquet_to_zvt` 的 `--only` 与脚本一致。
- QMT 数据也可单独用 `scripts/update_qmt_data.py` 更新（不依赖每日任务）。

---

## 5. 按库（db_name）与 provider 速查

下表仅列出**本项目实际使用**的 provider 与 db_name 组合，便于排查“某类数据在哪个库、哪个 provider”。

| db_name            | 表/用途                          | 已使用的 provider                             |
| ------------------ | -------------------------------- | --------------------------------------------- |
| stock_meta         | 股票列表                         | xysz, qmt, em, exchange, joinquant, eastmoney |
| block_meta         | 板块/行业                        | xysz, akshare, em, eastmoney, sina            |
| stock_1d_kdata     | 股票日 K                         | xysz, qmt, em, joinquant                      |
| stock_1d_hfq_kdata | 股票日 K 后复权                  | xysz, qmt, em, joinquant                      |
| stock_adj_factor   | 复权因子                         | xysz（仅 xysz 使用此 schema）                 |
| index_1d_kdata     | 指数日 K                         | em, sina, qmt                                 |
| finance            | 资产负债表 / 利润表 / 现金流量表 | eastmoney, xysz, qmt                          |
| valuation          | 股票估值（PE/PB/PS 等）          | joinquant, xysz                               |
| holder             | 股东户数、十大股东等             | eastmoney, joinquant, xysz                    |
| dividend_financing | 分红、配股等                     | eastmoney, xysz                               |
| china_money_supply | 中国货币供应量                   | akshare                                       |

---

## 6. 其他说明

- **xysz 估值**：`xyszValuationRecorder` 用 xysz 的日 K、利润表、资产负债表、现金流量表与 Stock 的 float_cap 计算 PE/PE_TTM、PB、PS、PCF、市值、流通市值、换手率等并写入 `StockValuation`（provider=xysz）。详见 `src/zvt/recorders/xysz/finance/xysz_valuation_recorder.py`。
- **xysz 日 K 去重**：日线在“非实时”且库中已有当日数据时，会通过 `evaluate_start_end_size_timestamps` 跳过拉取，避免重复请求（见 xysz_stock_kdata_recorder）。
- **列名映射**：xysz 财务、QMT 财务的列映射分别在 `xysz_finance_recorder.py`、`qmt_finance_recorder.py` 中维护；新增字段时需同时核对 schema 与映射表。
- **环境**：日常更新与脚本推荐使用 conda 环境 `quant`，且 QMT 需保证 Windows 端 RPC 服务已启动。

---

## 7. 脚本与代码位置速查

| 用途                                   | 路径                                    |
| -------------------------------------- | --------------------------------------- |
| 每日任务入口                           | `zvt_daily_job.py`（根目录）            |
| QMT 数据更新（独立于每日任务）         | `scripts/update_qmt_data.py`            |
| xysz Parquet → ZVT 导入                | `scripts/import_xysz_parquet_to_zvt.py` |
| xysz 后复权日线计算                    | `scripts/compute_xysz_hfq_kdata.py`     |
| xysz Recorder（行情/财务/估值/股东等） | `src/zvt/recorders/xysz/`               |
| QMT Recorder（行情/财务/指数）         | `src/zvt/recorders/qmt/`                |
| Akshare Recorder（板块/宏观）          | `src/zvt/recorders/akshare/`            |
| 财务 / 估值 / 分红等 Domain 定义       | `src/zvt/domain/fundamental/`           |
| 重要指数代码列表                       | `src/zvt/consts.py` → `IMPORTANT_INDEX` |

---

*文档维护于 docs 目录，作为数据层看板；有新增数据源或表时请同步更新本文件。*
