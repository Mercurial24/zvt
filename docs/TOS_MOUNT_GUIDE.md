# 火山引擎 TOS 对象存储自动挂载配置

## 背景

服务器每次重启后，TOS 对象存储挂载盘 `/mnt/point` 会丢失，需要手动重新挂载。
通过 systemd mount unit 实现开机自动挂载，且保证网络就绪后再执行挂载。

## 挂载信息

| 项目 | 值 |
|------|-----|
| 对象存储桶 | `/tos/wx-data-storage` |
| 挂载点 | `/mnt/point` |
| 文件系统类型 | `fsx`（实际为 `hpvs_fs`） |
| mount helper | `/usr/sbin/mount.fsx` |
| 鉴权方式 | IMDS（ECS 实例角色） |
| 角色 TRN | `trn:iam::2104894747:role/role-for-ecs` |
| Region | `cn-beijing` |
| Endpoint | `tos-cn-beijing.ivolces.com` |

## 原始挂载命令（手动）

```bash
mount -t fsx /tos/wx-data-storage /mnt/point \
  -o region="cn-beijing",endpoint="tos-cn-beijing.ivolces.com",tos_auth_role_trn=trn:iam::2104894747:role/role-for-ecs,tos_auth_type=IMDS,no_writeback_cache,tos_incremental_upload=true,tos_allow_delete=true
```

## 自动挂载配置（systemd mount unit）

配置文件路径：`/etc/systemd/system/mnt-point.mount`

```ini
[Unit]
Description=Mount TOS object storage via fsx
After=network-online.target
Wants=network-online.target
Before=remote-fs.target
DefaultDependencies=no

[Mount]
What=/tos/wx-data-storage
Where=/mnt/point
Type=fsx
Options=region=cn-beijing,endpoint=tos-cn-beijing.ivolces.com,tos_auth_role_trn=trn:iam::2104894747:role/role-for-ecs,tos_auth_type=IMDS,no_writeback_cache,tos_incremental_upload=true,tos_allow_delete=true
TimeoutSec=30

[Install]
WantedBy=remote-fs.target
```

### 关键配置说明

- **`After=network-online.target`** — 等网络完全就绪后再挂载，防止 IMDS 鉴权失败
- **`Wants=network-online.target`** — 拉起网络等待服务
- **`DefaultDependencies=no`** — 禁用默认依赖，避免与 `local-fs.target` 产生 ordering cycle
- **`Before=remote-fs.target`** — 归入远程文件系统组，在其之前完成挂载
- **`WantedBy=remote-fs.target`** — 开机时由远程文件系统目标拉起（而非 local-fs）
- **`TimeoutSec=30`** — 挂载超时 30 秒，避免网络异常时无限等待

> **注意**：不要在 Options 里加 `_netdev`，`fsx` 不认该参数会报 `Invalid Argument`。

## 常用管理命令

```bash
# 查看挂载状态
systemctl status mnt-point.mount

# 手动挂载
systemctl start mnt-point.mount

# 手动卸载
systemctl stop mnt-point.mount

# 启用开机自动挂载
systemctl enable mnt-point.mount

# 禁用开机自动挂载
systemctl disable mnt-point.mount

# 修改配置后重新加载
systemctl daemon-reload
```

## 故障排查

```bash
# 查看挂载日志
journalctl -u mnt-point.mount -e

# 确认挂载点是否可用
df -h /mnt/point
ls /mnt/point/

# 确认 fsx mount helper 存在
ls -l /usr/sbin/mount.fsx

# 确认 IMDS 鉴权可用（ECS 实例角色）
curl -s http://100.96.0.96/latest/meta-data/
```

## 涉及此挂载点的项目路径

项目中以下脚本依赖 `/mnt/point`：

- `scripts/daily_update_xysz.py` — 日线、财报等数据写入 `/mnt/point/stock_data/xysz_data/`
- `scripts/daily_update_qmt.py` — QMT 数据同步到挂载点
- `scripts/import_xysz_parquet_to_zvt.py` — 从挂载点读取 Parquet 导入 ZVT

---

*配置时间：2026-03-19*
