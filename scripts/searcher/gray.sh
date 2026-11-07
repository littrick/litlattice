#!/usr/bin/env bash
# Gray-rollout helper for the searcher pilot (M8.8).
#
# Topology:
#   frontend --SEARCHER_URL--> 127.0.0.1:13186 (proxy) --> Go :3181 or Rust :13181
#
# Usage:
#   scripts/searcher/gray.sh start          # start the Rust side + proxy on Go
#   scripts/searcher/gray.sh switch rust    # cut traffic to Rust
#   scripts/searcher/gray.sh switch go      # roll back to Go
#   scripts/searcher/gray.sh status
#   scripts/searcher/gray.sh stop
#
# Rollback is a proxy restart: existing gRPC streams close and clients
# reconnect to Go. The Go and Rust zip caches are isolated (cache/searcher vs
# rust-searcher-cache) so rollback never depends on shared mutable state.
#
# Environment (defaults shown):
#   RUN_DIR=/home/developer/ws/test_ws/run
#   LOG_DIR=/home/developer/ws/test_ws/logs
#   GIT_SERVERS=127.0.0.1:3178
#   RUST_BIN=<repo>/target/debug/searcher

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUN_DIR="${RUN_DIR:-/home/developer/ws/test_ws/run}"
LOG_DIR="${LOG_DIR:-/home/developer/ws/test_ws/logs}"
GIT_SERVERS="${GIT_SERVERS:-127.0.0.1:3178}"
RUST_BIN="${RUST_BIN:-$REPO_DIR/target/debug/searcher}"

GO_ADDR="127.0.0.1:3181"
RUST_ADDR="127.0.0.1:13181"
PROXY_ADDR="127.0.0.1:13186"
# Rust debug/metrics port; the test_ws stack already uses 6060/6062/6063/6064/6074.
RUST_PROF_ADDR="${RUST_PROF_ADDR:-127.0.0.1:6075}"

mkdir -p "$RUN_DIR" "$LOG_DIR"

proxy_pid_file="$RUN_DIR/searcher-proxy.pid"
rust_pid_file="$RUN_DIR/rust-searcher.pid"

start_rust() {
    if [[ -f "$rust_pid_file" ]] && kill -0 "$(cat "$rust_pid_file")" 2>/dev/null; then
        echo "rust searcher already running (pid $(cat "$rust_pid_file"))"
        return
    fi
    if [[ ! -x "$RUST_BIN" ]]; then
        echo "missing $RUST_BIN; run: cargo build -p sg-bin-searcher" >&2
        exit 1
    fi
    local cache="$RUN_DIR/rust-searcher-cache"
    mkdir -p "$cache"
    SEARCHER_ADDR="$RUST_ADDR" \
    SRC_PROF_HTTP="$RUST_PROF_ADDR" \
    SRC_GIT_SERVERS="$GIT_SERVERS" \
    SEARCHER_CACHE_DIR="$cache" \
    SEARCHER_CACHE_SIZE_MB=5000 \
    SRC_LOG_LEVEL=info \
        nohup "$RUST_BIN" >"$LOG_DIR/rust-searcher.log" 2>&1 &
    echo $! >"$rust_pid_file"
    echo "started rust searcher on $RUST_ADDR (pid $(cat "$rust_pid_file"))"
}

start_proxy() {
    local upstream="$1"
    stop_proxy
    nohup python3 "$SCRIPT_DIR/proxy.py" \
        --listen "$PROXY_ADDR" --upstream "$upstream" \
        >"$LOG_DIR/searcher-proxy.log" 2>&1 &
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
    echo "frontend must use SEARCHER_URL=http://$PROXY_ADDR"
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
