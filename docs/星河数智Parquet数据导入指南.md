# 星河数智 base_data Parquet 导入 ZVT 说明

## 数据来源

`/data/stock_data/xysz_data/base_data` 下是由 `my_quant_begin/data_engine_xysz/amazing_data_down.py` 下载的 parquet 文件，包括：

| 文件 | 对应 ZVT 表/用途 |
|------|------------------|
| `balance_sheet.parquet` | BalanceSheet（资产负债表） |
| `income.parquet` | IncomeStatement（利润表） |
| `cash_flow.parquet` | CashFlowStatement（现金流量表） |
| `holder_num.parquet` | HolderNum（股东户数） |
| `share_holder.parquet` | TopTenHolder（十大股东，仅 HOLDER_TYPE=10） |
| `klines_daily.parquet` | 日 K 线（stock_1d_kdata，provider=xysz） |
| `dividend.parquet` / `right_issue.parquet` / `equity_*.parquet` | 暂未做导入脚本，可按需扩展 |

## 能否直接用 / 转换？

- **能**。ZVT 里已有 xysz 的 recorder（财务、股东、K 线等），列名映射已做好；你本地的 parquet 与这些 recorder 从 API 拉到的结构一致（同源 AmazingData），因此可以**离线导入**到 ZVT 的 SQLite，无需再打 API。
- 转换方式：用脚本**按批读取 parquet（按 row_group）**，做与 recorder 相同的列名映射与 `entity_id` 生成，再调用 `df_to_db` 写入。脚本**不会一次性加载整个大文件**，避免小内存机器爆掉。

## 使用方法

1. **环境**：在 **conda 的 quant 环境**下运行，且该环境需已安装 zvt 及其依赖（如 sqlalchemy、pandas、pyarrow 等）。若 zvt 是源码运行，脚本会自动把 `zvt/src` 加入 `sys.path`。
2. **数据目录**：默认 parquet 目录为 `/data/stock_data/xysz_data/base_data`，可通过 `--base-dir` 指定。
3. **（可选）指定 ZVT 数据目录**：  
   `export ZVT_HOME=/path/to/your/zvt-home`  
   不设置则使用 zvt 默认目录（如 `~/zvt-home`）。
4. **全量导入**（按表分批，每批约 5 万行，可调）：
   ```bash
   cd /data/code/zvt
   conda activate quant
   python scripts/import_xysz_parquet_to_zvt.py --base-dir /data/stock_data/xysz_data/base_data
   ```
5. **只导入部分表**（例如只导入 K 线和股东户数）：
   ```bash
   python scripts/import_xysz_parquet_to_zvt.py --base-dir /data/stock_data/xysz_data/base_data --only klines_daily,holder_num
   ```
6. **测试（限制行数）**：
   ```bash
   python scripts/import_xysz_parquet_to_zvt.py --base-dir /data/stock_data/xysz_data/base_data --only holder_num --max-rows 5000
   ```

## 参数说明

- `--base-dir`：parquet 所在目录，默认 `/data/stock_data/xysz_data/base_data`。
- `--only`：只导入的表，逗号分隔。可选：`klines_daily`、`balance_sheet`、`income`、`cash_flow`、`holder_num`、`share_holder`。不传则全部导入。
- `--max-rows`：每种表最多导入行数，用于试跑或限流。
- `--batch-size`：每批行数（默认 50000），越大单次内存越高。
- `--provider`：写入 ZVT 的 provider，默认 `xysz`。

## 注意事项

- **entity_id**：脚本从 parquet 中的 `MARKET_CODE`（如 `600104.SH`）推导 `entity_id`（如 `stock_sh_600104`）。若你希望用 ZVT 的 Stock 元数据做查询，需先有 xysz 的股票列表（例如先跑 xyszStockMetaRecorder），否则仅能通过已写入的 entity_id 在对应表中查数据。
- **K 线**：已把 xysz 加入 `stock_1d_kdata` 的 providers，导入后可用 `get_kdata(..., provider="xysz")` 查询。
- **内存**：大表（如 balance_sheet、income、klines_daily）按 row_group 分批读、写后即释放，避免整表进内存。

## 脚本位置

- 导入脚本：`zvt/scripts/import_xysz_parquet_to_zvt.py`
- 列名映射与 xysz recorder 一致：`zvt/recorders/xysz/finance/xysz_finance_recorder.py`、`zvt/recorders/xysz/holder/xysz_holder_recorder.py` 等。

---

## 数据流与项目设计：是否还需要 amazing_data_down.py？

### 两条数据来源

| 方式 | 脚本/组件 | 特点 |
|------|------------|------|
| **A. 下载到 parquet** | `my_quant_begin/data_engine_xysz/amazing_data_down.py` | 按批拉全量、落盘 parquet、内存可控（BATCH_SIZE）；适合全量/历史一次性拉取。 |
| **B. 直写 ZVT** | zvt 的 `xysz*Recorder`（如 `xyszBalanceSheetRecorder`） | 按实体（单只股票）从 API 拉取并直接 `df_to_db`；适合按实体增量、与 ZVT 调度集成。 |

两者都调用同一套 AmazingData API，只是落库位置不同（parquet vs ZVT SQLite）。

### 推荐做法（从项目设计角度）

**建议：以「是否要保留 parquet 原始存档」来选型。**

1. **需要保留 parquet 作为原始存档时（推荐）**
   - **全量/定期重拉**：继续用 `amazing_data_down.py` 下载到 `base_data`（或你指定的目录），作为**唯一原始数据源**。
   - **入 ZVT**：用 `import_xysz_parquet_to_zvt.py` 把 parquet 导入到 ZVT，ZVT 只作为**消费者**。
   - **增量更新**：二选一或组合使用：
     - 再跑一遍 `amazing_data_down.py`（例如只拉最近日期/增量），然后对**有变化的 parquet** 再跑一次导入脚本（可后续为脚本加「按日期/文件增量」支持）；或
     - 日常增量只用 ZVT 的 `xysz*Recorder` 按实体更新，不再依赖 parquet。
   - **好处**：parquet 与工具解耦，可给 pandas、备份、其他系统用；ZVT 与 my_quant_begin 职责清晰（一个管下载与存档，一个管入库与使用）。

2. **不关心 parquet，只用 ZVT 时**
   - **可以不再用** `amazing_data_down.py`，全部通过 ZVT 的 xysz recorders 拉数并写入 ZVT。
   - 首次全量：用 ZVT 调度跑全量实体（可能较慢、需注意 API 限频与内存）。
   - 日常：用 ZVT 的定时任务跑 xysz recorders 做增量即可。
   - 之前已下载的 parquet 可当作**一次性历史迁移**：用导入脚本灌进 ZVT 后，后续只靠 recorders 更新。

3. **折中（推荐多数场景）**
   - **历史/全量**：用 `amazing_data_down.py` 拉一次（或偶尔全量刷新）→ parquet → `import_xysz_parquet_to_zvt.py` 导入 ZVT。
   - **日常增量**：用 ZVT 的 xysz recorders 做按日/按实体的增量，不再每天跑 amazing_data_down。
   - 这样既保留 parquet 的备份与复用价值，又避免重复维护两套全量下载逻辑。

### 小结

- **后续是否还要用 amazing_data_down.py**：  
  - 要保留 parquet 存档或做全量/大范围更新 → **要**，继续用；  
  - 只关心 ZVT、且全量/增量都打算用 ZVT 解决 → **可以不用**。
- **项目设计上更稳妥的做法**：把 `amazing_data_down.py` 定为「星河数智原始数据下载与 parquet 落盘」的唯一入口，ZVT 通过导入脚本消费 parquet，日常增量用 ZVT recorders 补数即可。
