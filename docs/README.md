# 项目文档

本目录存放本项目的主要说明文档，供日常开发与 AI 协作参考。

## 文档列表

| 文档 | 说明 |
|------|------|
| [DATA_LAYER_DASHBOARD.md](DATA_LAYER_DASHBOARD.md) | **数据层看板**：数据库全貌、各数据源（xysz / QMT / Akshare）数据从哪儿来、哪些已落盘、每日更新流程、脚本与代码位置速查 |
| [XYSZ_PARQUET_IMPORT.md](XYSZ_PARQUET_IMPORT.md) | 星河数智 Parquet 数据湖导入 ZVT 的说明与使用方法 |
| [CRON_SCHEDULED_TASKS.md](CRON_SCHEDULED_TASKS.md) | **定时任务配置**：cron 调度说明、日志查看、任务管理、维护方法 |
| [AGENT_GUIDE_QMT_LINUX.md](AGENT_GUIDE_QMT_LINUX.md) | QMT 在 Linux 下的 RPC 接入说明（面向 AI Agent 的上下文） |
| [QMT_FORWARD_SERVICE.md](QMT_FORWARD_SERVICE.md) | QMT 转发服务（Windows ↔ Ubuntu）：协议、使用步骤、Redis 双通道等 |

## 其他相关位置

- QMT 目录入口（指向上述文档）：`data_engine_qmt/README.md`
- 每日任务入口：根目录 `zvt_daily_job.py`
- 导入脚本：`scripts/import_xysz_parquet_to_zvt.py`、`scripts/update_qmt_data.py`
