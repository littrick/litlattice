#!/usr/bin/env bash
# Gray-rollout helper for the symbols pilot (M8.5).
#
# Topology:
#   frontend --SYMBOLS_URL--> 127.0.0.1:13185 (proxy) --> Go :3184 or Rust :13184
#
# Usage:
#   scripts/symbols/gray.sh start          # start the Rust side + proxy on Go
#   scripts/symbols/gray.sh switch rust    # cut traffic to Rust
#   scripts/symbols/gray.sh switch go      # roll back to Go
#   scripts/symbols/gray.sh status
#   scripts/symbols/gray.sh stop
#
# Environment (defaults shown):
#   RUN_DIR=/home/developer/ws/test_ws/run
#   LOG_DIR=/home/developer/ws/test_ws/logs
#   GIT_SERVERS=127.0.0.1:3178
#   CTAGS=/usr/local/bin/universal-ctags-sg
#   RUST_BIN=<repo>/target/debug/symbols

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUN_DIR="${RUN_DIR:-/home/developer/ws/test_ws/run}"
LOG_DIR="${LOG_DIR:-/home/developer/ws/test_ws/logs}"
GIT_SERVERS="${GIT_SERVERS:-127.0.0.1:3178}"
CTAGS="${CTAGS:-/usr/local/bin/universal-ctags-sg}"
RUST_BIN="${RUST_BIN:-$REPO_DIR/target/debug/symbols}"

GO_ADDR="127.0.0.1:3184"
RUST_ADDR="127.0.0.1:13184"
PROXY_ADDR="127.0.0.1:13185"
# Rust debug/metrics port; the test_ws stack already uses 6060/6063/6064.
RUST_PROF_ADDR="${RUST_PROF_ADDR:-127.0.0.1:6074}"

mkdir -p "$RUN_DIR" "$LOG_DIR"

proxy_pid_file="$RUN_DIR/symbols-proxy.pid"
rust_pid_file="$RUN_DIR/rust-symbols.pid"

start_rust() {
    if [[ -f "$rust_pid_file" ]] && kill -0 "$(cat "$rust_pid_file")" 2>/dev/null; then
        echo "rust symbols already running (pid $(cat "$rust_pid_file"))"
        return
    fi
    if [[ ! -x "$RUST_BIN" ]]; then
        echo "missing $RUST_BIN; run: cargo build -p sg-bin-symbols" >&2
        exit 1
    fi
    local cache="$RUN_DIR/rust-symbols-cache"
    mkdir -p "$cache"
    SG_SYMBOLS_ADDR="$RUST_ADDR" \
    SRC_PROF_HTTP="$RUST_PROF_ADDR" \
    SRC_GIT_SERVERS="$GIT_SERVERS" \
    SYMBOLS_CACHE_DIR="$cache" \
    CTAGS_COMMAND="$CTAGS" \
    SRC_LOG_LEVEL=info \
        nohup "$RUST_BIN" >"$LOG_DIR/rust-symbols.log" 2>&1 &
    echo $! >"$rust_pid_file"
    echo "started rust symbols on $RUST_ADDR (pid $(cat "$rust_pid_file"))"
}

start_proxy() {
    local upstream="$1"
    stop_proxy
    nohup python3 "$SCRIPT_DIR/proxy.py" \
        --listen "$PROXY_ADDR" --upstream "$upstream" \
        >"$LOG_DIR/symbols-proxy.log" 2>&1 &
    echo $! >"$proxy_pid_file"
    echo "proxy $PROXY_ADDR -> $upstream (pid $(cat "$proxy_pid_file"))"
}

stop_proxy() {
    if [[ -f "$proxy_pid_file" ]] && kill -0 "$(cat "$proxy_pid_file")" 2>/dev/null; then
        kill "$(cat "$proxy_pid_file")"
        sleep 0.2
    fi
    rm -f "$proxy_pid_file"
}

status() {
    local proxy_target="-"
    if [[ -f "$proxy_pid_file" ]] && kill -0 "$(cat "$proxy_pid_file")" 2>/dev/null; then
        proxy_target="up (pid $(cat "$proxy_pid_file"))"
    fi
    local rust_state="down"
    if [[ -f "$rust_pid_file" ]] && kill -0 "$(cat "$rust_pid_file")" 2>/dev/null; then
        rust_state="up (pid $(cat "$rust_pid_file"))"
    fi
    echo "go     : $GO_ADDR"
    echo "rust   : $RUST_ADDR $rust_state"
    echo "proxy  : $PROXY_ADDR $proxy_target"
    echo "frontend must use SYMBOLS_URL=http://$PROXY_ADDR"
}

case "${1:-}" in
    start)
        start_rust
        start_proxy "$GO_ADDR"   # safe default: keep Go in front
        ;;
    switch)
        case "${2:-}" in
            go) start_proxy "$GO_ADDR" ;;
            rust) start_proxy "$RUST_ADDR" ;;
            *) echo "usage: $0 switch go|rust" >&2; exit 2 ;;
        esac
        ;;
    status) status ;;
    stop)
        stop_proxy
        if [[ -f "$rust_pid_file" ]]; then
            kill "$(cat "$rust_pid_file")" 2>/dev/null || true
            rm -f "$rust_pid_file"
        fi
        ;;
    *)
        echo "usage: $0 {start|switch go|switch rust|status|stop}" >&2
        exit 2
        ;;
esac
