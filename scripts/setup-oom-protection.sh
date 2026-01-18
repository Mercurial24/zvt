#!/bin/bash
# 系统级 OOM 防护：一次执行，对所有脚本/进程生效
# 需 root 执行: sudo bash scripts/setup-oom-protection.sh

set -e

echo "=== 1. 写入 sysctl 配置 ==="
cat > /etc/sysctl.d/99-oom.conf << 'EOF'
# OOM 时优先杀掉触发分配的那个进程（吃内存的脚本/进程）
vm.oom_kill_allocating_task = 1
# 关闭内存过承诺，申请超量时先失败而不是拖死整机
vm.overcommit_memory = 2
vm.overcommit_ratio = 100
EOF

echo "=== 2. 应用 sysctl ==="
sysctl -p /etc/sysctl.d/99-oom.conf

echo "=== 3. 保护 SSH 服务（OOM 时尽量不杀 sshd）==="
mkdir -p /etc/systemd/system/ssh.service.d
cat > /etc/systemd/system/ssh.service.d/oom-protect.conf << 'EOF'
[Service]
OOMScoreAdjust=-1000
EOF

echo "=== 4. 重载 systemd 并重启 SSH ==="
systemctl daemon-reload
# 重启 SSH 会短暂断连，若当前是 SSH 登录请确认可接受
systemctl restart ssh

echo "=== 完成 ==="
echo "当前 sysctl:"
sysctl vm.oom_kill_allocating_task vm.overcommit_memory vm.overcommit_ratio
echo "SSH OOM 保护:"
systemctl show ssh -p OOMScoreAdjust
