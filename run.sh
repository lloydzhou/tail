#!/usr/bin/env bash
# 统一启停脚本:Kvrocks(L2 硬盘缓存)+ OpenResty 网关 + 可选 Python 模拟后端。
#
# 用法:
#   ./run.sh start    # 起全部(Kvrocks + 网关 + 模拟后端)
#   ./run.sh stop     # 停全部
#   ./run.sh status   # 查看状态
#   ./run.sh restart  # 重启
#   ./run.sh logs gateway|kvrocks|backend
set -euo pipefail

PROJECT="/home/lloyd/ZCodeProject"
OR_BIN="$PROJECT/runtime/openresty/bin/openresty"
OR_CONF="$PROJECT/openresty/conf/nginx.conf"
OR_PREFIX="$PROJECT/openresty"
KV_BIN="$PROJECT/runtime/kvrocks/bin/kvrocks"   # x.py 安装后的位置(见下)
KV_CONF="$PROJECT/openresty/conf/kvrocks.conf"
KV_DATA="$PROJECT/runtime/kvrocks-data"
LOG_DIR="$PROJECT/openresty/logs"
BE_PID="$PROJECT/runtime/backend.pid"
KV_PID_FILE="$LOG_DIR/kvrocks.pid"

# 兼容:x.py 默认只编译不 install,二进制在 build 目录
KV_BUILD_BIN="$PROJECT/build/kvrocks-2.16.0/kvbuild/kvrocks"
if [ ! -x "$KV_BIN" ] && [ -x "$KV_BUILD_BIN" ]; then
    KV_BIN="$KV_BUILD_BIN"
fi

mkdir -p "$LOG_DIR" "$KV_DATA"

log()  { echo "[run.sh] $*"; }
die()  { echo "[run.sh] ERROR: $*" >&2; exit 1; }

start_kvrocks() {
    if [ ! -x "$KV_BIN" ]; then
        die "kvrocks 二进制不存在: $KV_BIN (是否还没编译完成?)"
    fi
    if [ -f "$KV_PID_FILE" ] && kill -0 "$(cat "$KV_PID_FILE")" 2>/dev/null; then
        log "kvrocks 已在运行 (pid $(cat "$KV_PID_FILE"))"
        return
    fi
    log "启动 Kvrocks (硬盘缓存, 端口 6666)..."
    nohup "$KV_BIN" -c "$KV_CONF" > "$LOG_DIR/kvrocks.stdout" 2>&1 &
    echo $! > "$KV_PID_FILE"
    sleep 1
    # 等待端口就绪
    for _ in $(seq 1 20); do
        if redis-cli -p 6666 PING 2>/dev/null | grep -q PONG; then
            log "kvrocks 就绪 (pid $(cat "$KV_PID_FILE"))"
            return
        fi
        sleep 0.3
    done
    die "kvrocks 启动超时,查看 $LOG_DIR/kvrocks.stdout"
}

start_backend() {
    if [ -f "$BE_PID" ] && kill -0 "$(cat "$BE_PID")" 2>/dev/null; then
        log "mock backend 已在运行 (pid $(cat "$BE_PID"))"
        return
    fi
    log "启动 Python 模拟后端 (端口 8080)..."
    nohup python3 -m uvicorn tests.mock_backend:app_factory --factory \
        --host 127.0.0.1 --port 8080 > "$LOG_DIR/backend.log" 2>&1 &
    echo $! > "$BE_PID"
    sleep 1
    log "mock backend 就绪 (pid $(cat "$BE_PID"))"
}

start_gateway() {
    log "启动 OpenResty 网关 (端口 8765)..."
    "$OR_BIN" -p "$OR_PREFIX/" -c "$OR_CONF" 2>/dev/null \
        || "$OR_BIN" -p "$OR_PREFIX/" -c "$OR_CONF"   # 第二次输出真实错误
    log "openresty 已启动"
}

stop_gateway()   { "$OR_BIN" -p "$OR_PREFIX/" -s stop 2>/dev/null && log "openresty 已停止" || true; }
stop_kvrocks()   { [ -f "$KV_PID_FILE" ] && kill "$(cat "$KV_PID_FILE")" 2>/dev/null && log "kvrocks 已停止" || true; rm -f "$KV_PID_FILE"; }
stop_backend()   { [ -f "$BE_PID" ] && kill "$(cat "$BE_PID")" 2>/dev/null && log "backend 已停止" || true; rm -f "$BE_PID"; }

cmd_status() {
    echo "== OpenResty 网关 (8765) =="
    curl -s http://127.0.0.1:8765/__kvcache/health 2>/dev/null && echo "" || echo "  DOWN"
    echo "== Kvrocks (6666) =="
    redis-cli -p 6666 PING 2>/dev/null || echo "  DOWN"
    echo "== Mock backend (8080) =="
    curl -s http://127.0.0.1:8080/v1/chat/completions -X POST -H 'Content-Type: application/json' \
        -d '{"model":"x","messages":[]}' 2>/dev/null | head -c 60 && echo "" || echo "  DOWN"
}

case "${1:-}" in
    start)   start_kvrocks; start_backend; start_gateway ;;
    stop)    stop_gateway; stop_backend; stop_kvrocks ;;
    restart) "$0" stop; sleep 1; "$0" start ;;
    status)  cmd_status ;;
    logs)    shift; tail -F "$LOG_DIR/${1:-gateway}.log" 2>/dev/null || die "无 $1 日志(可选: gateway/kvrocks/backend)";;
    *) echo "用法: $0 {start|stop|restart|status|logs [gateway|kvrocks|backend]}"; exit 1 ;;
esac
