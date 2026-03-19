# 定时任务配置说明

本项目使用系统 **cron** 调度每日/每周定时任务，取代之前的 Python 常驻进程方案。

## 为什么选择 cron 而非 Python 定时库

| 对比项 | cron | Python 常驻（schedule/APScheduler） |
|--------|------|--------------------------------------|
| 可靠性 | 系统级服务，不依赖 Python 进程存活 | 进程挂了就没人调度 |
| 资源占用 | 跑完即释放内存 | 24h 常驻占内存 |
| 重启恢复 | 随系统自动生效 | 需额外配 systemd/supervisor 保活 |
| 日志 | 输出重定向到文件，永久保存 | 仅 tmux 缓冲区或需自行配日志 |

## 当前任务配置

通过 `crontab -l` 可查看：

```
# ZVT 每日数据更新：周一到周五 16:30
30 16 * * 1-5 /root/miniconda3/envs/quant/bin/python -u /data/code/zvt/zvt_daily_job.py >> /data/code/zvt/logs/daily_job.log 2>&1

# 格雷厄姆指数：每周五 17:00
0 17 * * 5 cd /data/code/my_quant_begin && /root/miniconda3/envs/quant/bin/python -u /data/code/my_quant_begin/graham_index.py >> /data/code/zvt/logs/graham_index.log 2>&1
```

| 任务 | 脚本 | 执行时间 | 日志文件 |
|------|------|----------|----------|
| ZVT 每日数据更新 | `zvt_daily_job.py` | 周一至周五 16:30 | `/data/code/zvt/logs/daily_job.log` |
| 格雷厄姆指数 | `graham_index.py` | 每周五 17:00 | `/data/code/zvt/logs/graham_index.log` |

## 日常操作

### 查看日志

```bash
# 实时跟踪 ZVT 日志
tail -f /data/code/zvt/logs/daily_job.log

# 实时跟踪格雷厄姆日志
tail -f /data/code/zvt/logs/graham_index.log

# 查看最近 100 行
tail -100 /data/code/zvt/logs/daily_job.log
```

### 管理定时任务

```bash
# 查看所有定时任务
crontab -l

# 编辑定时任务（打开编辑器，逐行增删改）
crontab -e

# 清空所有定时任务（慎用）
crontab -r
```

### 暂停某个任务

`crontab -e` 打开后，在对应行前加 `#` 注释掉即可：

```
# 30 16 * * 1-5 /root/miniconda3/envs/quant/bin/python -u ...
```

恢复时去掉 `#` 保存退出。

### 手动立即执行

无需等 cron 触发，直接命令行运行：

```bash
# ZVT 每日更新
/root/miniconda3/envs/quant/bin/python -u /data/code/zvt/zvt_daily_job.py

# 格雷厄姆指数
cd /data/code/my_quant_begin && /root/miniconda3/envs/quant/bin/python -u /data/code/my_quant_begin/graham_index.py
```

### 修改执行时间

`crontab -e` 打开后修改时间字段，cron 时间格式为：

```
分 时 日 月 星期几
```

常用示例：
- `30 16 * * 1-5` → 周一到周五 16:30
- `0 17 * * 5` → 每周五 17:00
- `0 9 * * *` → 每天 9:00
- `*/30 * * * *` → 每 30 分钟

## 日志维护

日志文件会持续增长，建议定期清理或配置 logrotate。手动清理：

```bash
# 清空日志（保留文件）
> /data/code/zvt/logs/daily_job.log

# 或只保留最近 1000 行
tail -1000 /data/code/zvt/logs/daily_job.log > /tmp/tmp.log && mv /tmp/tmp.log /data/code/zvt/logs/daily_job.log
```

## 重启服务器后

**不需要任何操作**。cron 是系统服务，定时任务持久化在 `/var/spool/cron/` 中，开机自动加载。

## 历史变更

- **2026-03-19**：从 Python 常驻进程（`run_auto_task.sh` + `schedule` 库）迁移到 cron 方案
  - `zvt_daily_job.py`：移除了未使用的 `--now`/`--time` 参数
  - `graham_index.py`：移除了 `import schedule` 和 `while True` 常驻循环
  - 旧脚本 `run_auto_task.sh` 中的 zvt_daily_job 和 graham_index 部分不再需要
