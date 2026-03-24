# ZVT 框架数据接入现状及具体字段一览

本文档记录了目前系统中基于 ZVT 框架的数据对接情况，**并重点列出了每张表具体包含的数据项**，以方便在编写策略时快速确认哪些指标已可以直接获取（无需自己重新计算）。

> **提醒**：目前系统中的 K 线数据（各级别 `BFQ`, `HFQ` 等）均使用 Parquet 格式进行高效挂载和存取，`zvt-home/data/` 目录残存的 sqlite K线库属于废弃或过渡文件，实际读取已路由至 Parquet 数据湖。

---

## 1. 已经有数据并且 ZVT 已定义的数据模型 (Data Existing & Defined)

### 1.1 估值数据 (`xysz_valuation.db` / `qmt_valuation.db`)
这是量化选股和基本面分析最常取用的每日计算指标库。
*   **`stock_valuation` (股票估值)**
    *   `capitalization`：总股本
    *   `circulating_cap`：流通股本
    *   `market_cap`：总市值
    *   `circulating_market_cap`：流通市值
    *   `turnover_ratio`：换手率
    *   `pe`：市盈率（静）
    *   `pe_ttm`：滚动市盈率 (TTM)
    *   `pb`：市净率
    *   `ps`：市销率
    *   `pcf`：市现率
*   **`etf_valuation` (ETF估值)**
    *   包含上述 PE / PB / PS / PCF 等指标及其对应衍生组（`pe1`, `pe_ttm1`, `pb1`, `ps1`, `pcf1`），用于穿透评估。

### 1.2 财务数据 (`xysz_finance.db`)
由于原生财务三表极其庞大，这里仅列出大家最常使用的核心主流项，策略撰写时可直接调取。
*   **`balance_sheet` (资产负债表 - 核心项)**: `cash_and_cash_equivalents` (现金及等价物), `accounts_receivable` (应收账款), `inventories` (存货), `total_current_assets` (流动资产合计), `fixed_assets` (固定资产), **`total_assets` (总资产)**, `short_term_borrowing` (短期借款), `total_current_liabilities` (流动负债合计), **`total_liabilities` (总负债)**, **`total_equity` (所有者权益合计)**。
*   **`income_statement` (利润表 - 核心项)**: **`operating_income` (营业收入)**, `operating_costs` (营业成本), `rd_costs` (研发费用), `operating_profit` (营业利润), `total_profits` (利润总额), **`net_profit` (净利润)**, **`net_profit_as_parent` (归母净利润)**, **`deducted_net_profit` (扣非归母净利润)**, `eps` (每股收益)。
*   **`cash_flow_statement` (现金流量表 - 核心项)**: `total_op_cash_inflows` (经营现金流入), `total_op_cash_outflows` (经营现金流出), **`net_op_cash_flows` (经营活动产生的现金流量净额)**, `net_investing_cash_flows` (投资活动产生净额), `net_financing_cash_flows` (筹资活动产生净额), `net_cash_increase` (现金及等价物净增加额)。
*   **`finance_factor` (衍生财务因子)**: 非常适合选股直接取用，包含：`basic_eps` (基本每股收益), `op_cash_flow_ps` (每股经营现金流), `gross_profit_margin` (销售毛利率), `net_margin` (销售净利率), **`roe` (净资产收益率)**, `rota` (总资产回报率), `debt_asset_ratio` (资产负债率), `current_ratio` (流动比率), `total_assets_turnover` (总资产周转率), 等增速指标 (`op_income_growth_yoy`, `net_profit_growth_yoy`)。

### 1.3 股东数据 (`xysz_holder.db` / `qmt_holder.db`)
*   **`holder_num` (股东人数)**
    *   `holder_num`：当期 A 股股东人数。
    *   `total_holder_num`：当期总股东人数。
*   **`top_ten_tradable_holder` (前十大流通股东) / `top_ten_holder` (前十大股东)**
    *   `holder_name` / `holder_code`：股东名称/代码。
    *   `shareholding_numbers`：持股数量。
    *   `shareholding_ratio`：总股本持股比例。
    *   `change`：较上一期持股数量的变动。
    *   `change_ratio`：较上一期持股变动比例。

### 1.4 交易衍生数据 (`xysz_trading.db`)
*   **`margin_trading` (融资融券)**:
    *   `rz_balance`：融资余额。
    *   `rz_buy_amount` / `rz_repay_amount`：融资买入额 / 偿还额。
    *   `rq_balance` / `rq_volume`：融券余额 / 融券余量。
    *   `rq_sell_volume` / `rq_repay_volume`：融券卖出量 / 偿还量。
*   **`dragon_and_tiger` (龙虎榜)**:
    *   `buy_amount` / `sell_amount`：上榜买入/卖出总额。
    *   `net_amount`：净买入额。
    *   `turnover_rate`：换手率。
    *   `net_amount_rate`：净买入占比。
    *   `reason`：上榜原因说明。

### 1.5 分红融资数据 (`xysz_dividend_financing.db`)
*   **`dividend_detail` (分红/送转明细)**:
    *   `cash_tax_free` / `cash_tax_bearing`：派息方案金额 (税前预案/税后)。
    *   `share_bonus` / `share_transfer`：送红股 / 资本公积金转增股本方案。
    *   关键日期项：`dividend_record_date`（股权登记日）、`exclude_dividend_date`（除权除息日）、`dividend_payment_date`（派息日）等。
*   **`rights_issue_detail` (配股明细)**: `rights_issue_price` (配股价), `rights_issue_ratio` (配股比例), `rights_issue_numbers` / `rights_issue_amount` 等。

### 1.6 行情数据 (数据湖 Parquet 存储)
*   **`Stock1dKdata` / `Stock1dHfqKdata` (及其它周期级 K 线)**: `open`, `high`, `low`, `close`, `volume`, `amount`。目前日线及内部高频均直接依赖 Parquet 高性能加载。本地 SQLite 中对应的 `.db` 文件请忽略（后续将直接废弃或删除）。

---

## 2. ZVT 已定义，但目前暂无或未拉取入库的数据 (Defined but No Data Yet)
*前一版我们只列出了几个典型的大类，为了实现“绝对排查全”，我刚才通过脚本跑通了 ZVT 底层的 `global_schemas` 注册表，比对你本地的数据湖与 SQLite 后的**全量未空置明细表**如下：*

### 2.1 高级行情、量价与特定衍生信息 (衍生因子流)
*   **涨跌停与市场情绪**: `LimitUpInfo` (涨停分析), `LimitDownInfo` (跌停分析), `Emotion` (市场情绪表), `StockHotTopic` (股票热门概念)。
*   **特殊指标计算面 (ZVT原生因子)**: `Stock1dMaFactor` (各种 MA 均线因子特征), `Stock1dZenFactor` (基于“缠论”体系的 K 线分型、笔、线段等因子特征表), `Index1dZenFactor`。
*   **资金流与异动流**: `BigDealTrading` (大宗交易数据), `StockMoneyFlow`, `IndexMoneyFlow` (逐笔级别大单或中单净流入模型), `MarginTradingSummary` (除了现有的个股融资融券外，缺少大盘汇总版汇总结构)。

### 2.2 特殊主体持仓明细 (核心人物与机构持仓)
*   **核心资金异动表**: `HkHolder` (互联互通详情), `InstitutionalInvestorHolder` (机构明细)。
*   **董监高与实控人买卖**: `ManagerTrading` (高管交易), `HolderTrading` (重要股东交易记录表)。

### 2.3 其他大类金融衍生品 (泛资产)
*   **公募基金类**: `Fund` (公募基金列表信息), `FundStock` (公募基金的底层持仓股照明细)。
*   **期权与期货**: `Future` (期货品种基础信息), `Future1dKdata`, `Option*` (期权类)。
*   **可转债**: `CBond` (转债品种表，注意：ZVT 缺失转债深度的衍生特征定义，此表仅包含发债代码或基础信息)。
*   **全球市场/海外类**: 美股 (`Stockus`, `StockusKdata`, `StockusQuote` 等), 港股 (`Stockhk`, `StockhkKdata` 等), 全球核心指数 (`Indexus`, `Indexhk`), 汇率及数字货币 (`Currency`, `Currency1dKdata`)。
*   **宏观经济数据库**: `Economy` (各类PMI、GDP), `Country` (国家基础表), `ChinaMoneySupply` (中国货币供应量 M0/M1/M2 数据等)。

### 2.4 回测基建与系统内部状态表 (非数据源对象)
*ZVT 把一些回测运行时的数据也通过 ORM 的形式建了表，通常它们在你本地测试框架后才会产生，目前你的底层是没有该历史的：*
*   `FactorState`, `RecorderState`, `TaggerState` (各类下载器、因子计算的历史游标偏移和状态)。
*   `TraderInfo`, `AccountStats`, `Order`, `Position` (ZVT 的模拟盘交易系统、订单/持仓流的历史账本)。

---

## 3. ZVT 尚未定义且当前系统缺失的数据 (Undefined / Custom Needed)
*   **Level-2 逐笔/Tick 数据**：极其高频的盘口明细数据（需要用专门的大数据列式引擎 DolphinDB / 原生 Parquet）。
*   **Snapshot 每 3 秒实时切片**：实时推流（Orderbook）。
*   **自定义多因子中间态（Alpha 101/191 等）**：基于因子的中间面挂载，推荐作为计算后 Parquet 面板数据存储。
*   **可转债业务强相关因子**：ZVT 的 `CBond` 只记录债券基础特征，如转股溢价率、百元溢价率等未被专门设计。
*   **舆情/另类文本数据**：新闻研报情绪提取值等。
