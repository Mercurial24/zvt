#!/bin/bash
# 智能守护脚本：进程存活则跳过，进程已死（崩溃/退出）则自动重建
# 可随时执行，不会打断正在运行的任务
# 若需 crontab 开机拉起，可加：@reboot /data/code/zvt/run_auto_task.sh

# ── 辅助函数：检查进程是否存活 ────────────────────────────────────────────────
is_running() {
    pgrep -f "$1" > /dev/null 2>&1
}

start_session() {
    local session="$1"
    local workdir="$2"
    local cmd="$3"
    tmux has-session -t "$session" 2>/dev/null && tmux kill-session -t "$session"
    tmux new-session -d -s "$session" -c "$workdir"
    tmux send-keys -t "$session" "source /root/miniconda3/etc/profile.d/conda.sh && conda activate quant && $cmd" Enter
}

# ── 会话 1: graham_index ──────────────────────────────────────────────────────
if is_running "graham_index.py"; then
    echo "✅ graham_index.py 正在运行，跳过"
else
    echo "🔄 graham_index.py 未运行，重建会话..."
    start_session graham /data/code/my_quant_begin "python /data/code/my_quant_begin/graham_index.py"
    echo "   已创建 tmux 会话: graham"
fi

# ── 会话 2: zvt_daily_job ────────────────────────────────────────────────────
# 策略：先用 --now 立即执行一次全量更新，完成后再以定时模式（16:30）常驻
if is_running "zvt_daily_job.py"; then
    echo "✅ zvt_daily_job.py 正在运行，跳过"
else
    echo "🔄 zvt_daily_job.py 未运行，重建会话..."
    start_session zvt_daily /data/code/zvt \
        "python /data/code/zvt/zvt_daily_job.py --now && python /data/code/zvt/zvt_daily_job.py --time 16:30"
    echo "   已创建 tmux 会话: zvt_daily（先立即执行一次，再常驻等待每日 16:30）"
fi

