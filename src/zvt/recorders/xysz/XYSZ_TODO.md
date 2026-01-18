# 星河数智 (xysz) 数据接口接入进度

本文档对照 `data_engine_xysz`（中国银河证券星河数智 AmazingData）的接口说明，汇总已在 ZVT 中注册的接口与**尚未接入**的接口，便于后续按需补全。

---

## 一、已接入 ZVT 的接口

| 星河数智接口 | ZVT Recorder | 说明 |
|-------------|--------------|------|
| `get_code_list` + `get_stock_basic` | `xyszStockRecorder` | 股票列表/基础信息 (meta) |
| `query_kline` | `xyszStockKdataRecorder` | 日/周/月 K 线 |
| `get_backward_factor` | `xyszStockAdjFactorRecorder` | 后复权因子 |
| `get_balance_sheet` | `xyszBalanceSheetRecorder` | 资产负债表 |
| `get_income` | `xyszIncomeStatementRecorder` | 利润表 |
| `get_cash_flow` | `xyszCashFlowRecorder` | 现金流量表 |
| `get_dividend` | `xyszDividendDetailRecorder` | 分红 |
| `get_right_issue` | `xyszRightsIssueDetailRecorder` | 配股 |
| `get_share_holder` (HOLDER_TYPE=10/20) | `xyszTopTenHolderRecorder` / `xyszTopTenTradableHolderRecorder` | 十大股东 / 流通股前十 |
| `get_holder_num` | `xyszHolderNumRecorder` | **股东户数**（已补） |
| `get_long_hu_bang` | `xyszDragonAndTigerRecorder` | 龙虎榜 |
| `get_block_trading` | `xyszBigDealTradingRecorder` | 大宗交易 |
| `get_margin_detail` | `xyszMarginTradingRecorder` | 融资融券明细 |

---

## 二、尚未接入的接口（按优先级建议）

### 1. 财务类（与现有 data_engine_xysz 管线一致）

| 接口 | 说明 | 待办 |
|------|------|------|
| `get_profit_express` | 业绩快报 | xysz_api 未实现；需补 API + domain（如 ProfitExpress）+ Recorder |
| `get_profit_notice` | 业绩预告 | 同上，需补 API + domain（如 ProfitNotice）+ Recorder |

### 2. 股东/股本类（API 已有，仅缺 domain + Recorder）

| 接口 | 说明 | 待办 |
|------|------|------|
| `get_equity_structure` | 股本结构 | 新增 EquityStructure schema + xyszEquityStructureRecorder |
| `get_equity_pledge_freeze` | 股权冻结/质押 | 新增 PledgeFreeze 类 schema + Recorder |
| `get_equity_restricted` | 限售股解禁 | 新增 EquityRestricted 类 schema + Recorder |

### 3. 基础/行情与复权

| 接口 | 说明 | 待办 |
|------|------|------|
| `get_adj_factor` | 单次复权因子 | 当前仅用后复权；若需前复权可在 adj_factor Recorder 或新表中接入 |
| `get_history_stock_status` | 历史证券状态（ST、涨跌停、除权除息等） | 可选，做状态因子时再补 schema + Recorder |

### 4. 融资融券汇总（与现有 per-stock 不同）

| 接口 | 说明 | 待办 |
|------|------|------|
| `get_margin_summary` | 全市场融资融券汇总 | 非 per-stock；需要时可单独建 MarginSummary schema + Recorder |

---

## 三、当前未接入且与 A 股股票管线无关的接口

以下接口在星河数智手册中存在，但当前 data_engine_xysz 全量/增量脚本未使用，ZVT 也未接。若后续做 ETF/指数/可转债等再考虑。

- **基础**：`get_code_info`、`get_hist_code_list`、`get_bj_code_mapping`、`get_etf_pcf`、期货/期权代码表等  
- **行情**：`query_snapshot`（快照）  
- **期权**：`get_option_basic_info`、`get_option_std_ctr_specs`、`get_option_mon_ctr_specs`  
- **ETF**：`get_fund_share`、`get_fund_iopv`  
- **指数**：`get_index_constituent`、`get_index_weight`  
- **行业**：`get_industry_base_info`、`get_industry_constituent`、`get_industry_weight`、`get_industry_daily`  
- **可转债**：`get_kzz_issuance`、`get_kzz_share`、`get_kzz_conv` 等系列  
- **国债**：`get_treasury_yield`  

---

## 四、补全时注意事项

1. **xysz_api.py**：若接口在手册中有但 API 封装里没有（如 `get_profit_express`、`get_profit_notice`），需先在 `xysz_api.py` 中补对应方法（含 `local_path` 等参数与底层 `info_data`/`base_data` 调用）。  
2. **local_path**：星河数智多数接口需要 `local_path`；ZVT 中部分调用使用 `is_local=False` 且未传 `local_path`，若底层 SDK 报错，可在 `xysz_api` 内对 `local_path` 做默认值（如临时目录）或从配置读取。  
3. **Domain**：ZVT 若没有对应表（如业绩快报、股本结构），需在 `zvt.domain` 中新增 schema 并 `register_schema`，再写 Recorder 做字段映射与入库。

---

*文档维护：与 data_engine_xysz 及星河数智接口说明同步更新。*
