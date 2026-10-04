#!/bin/bash
# stockdata 服务管理脚本：独立于后端 FastAPI 进程运行。
# 用法: ./stockdata_ctl.sh {start|stop|restart|status}
# 数据落盘: data/ (共享)  |  端口: 3322  |  日志: data/stockdata.log
set -euo pipefail

BACKEND_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="$BACKEND_DIR/.venv/bin/python3"
SCRIPT="$BACKEND_DIR/scripts/run_stockdata_service.py"
PID_FILE="$BACKEND_DIR/../data/stockdata.pid"
LOG_FILE="$BACKEND_DIR/../data/stockdata.log"
PORT="${STOCKDATA_PORT:-3322}"

_is_running() {
    [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

_start() {
    if _is_running; then
        echo "stockdata 已在运行 (pid=$(cat "$PID_FILE"))"
        return 0
    fi
    rm -f "$PID_FILE"
    echo "启动 stockdata 服务 (端口 $PORT)…"
    cd "$BACKEND_DIR"
    nohup "$PYTHON" "$SCRIPT" >> "$LOG_FILE" 2>&1 &
    local pid=$!
    echo "$pid" > "$PID_FILE"
    sleep 2
    if kill -0 "$pid" 2>/dev/null; then
        echo "✓ stockdata 已启动 (pid=$pid, log=$LOG_FILE)"
    else
        echo "✗ 启动失败，查看日志: tail $LOG_FILE"
        rm -f "$PID_FILE"
        return 1
    fi
}

_stop() {
    if ! _is_running; then
        echo "stockdata 未在运行"
        rm -f "$PID_FILE"
        return 0
    fi
    local pid=$(cat "$PID_FILE")
    echo "停止 stockdata (pid=$pid)…"
    kill "$pid" 2>/dev/null
    # 等待退出（最多 10s）
    for i in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.5
    done
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
    rm -f "$PID_FILE"
    echo "✓ 已停止"
}

_status() {
    if _is_running; then
        local pid=$(cat "$PID_FILE")
        echo "stockdata: running (pid=$pid, port=$PORT)"
        # 显示调度器状态
        if [ -f "$LOG_FILE" ]; then
            echo "最近日志:"
            tail -3 "$LOG_FILE" | sed 's/^/  /'
        fi
    else
        echo "stockdata: stopped"
    fi
}

case "${1:-}" in
    start)   _start ;;
    stop)    _stop ;;
    restart) _stop; sleep 1; _start ;;
    status)  _status ;;
    *)       echo "用法: $0 {start|stop|restart|status}"; exit 1 ;;
esac
