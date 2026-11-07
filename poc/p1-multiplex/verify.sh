#!/usr/bin/env bash
# P1 acceptance checks: one cleartext port serves gRPC + HTTP.
set -euo pipefail

BIN=target/debug/p1-multiplex
ADDR=127.0.0.1:18080
BASE=http://$ADDR

. "$HOME/.cargo/env"
cargo build --quiet

$BIN "$ADDR" >/tmp/p1-multiplex.log 2>&1 &
pid=$!
trap 'kill $pid 2>/dev/null || true' EXIT
sleep 0.5

fail=0
check() { # name expected actual
  if [[ "$3" == *"$2"* ]]; then
    echo "PASS  $1"
  else
    echo "FAIL  $1: expected to contain '$2', got '$3'"
    fail=1
  fi
}

out=$($BIN client "$BASE")
check "grpc unary over h2c + metadata" "echo: hello (x-test=via-multiplex)" "$out"

out=$(curl -s "$BASE/foo")
check "http/1.1 routed to HTTP" '"version":"HTTP/1.1"' "$out"

out=$(curl -s --http2-prior-knowledge -H 'Content-Type: application/json' "$BASE/bar")
check "h2c prior-knowledge JSON routed to HTTP" '"version":"HTTP/2.0"' "$out"

out=$(curl -s -i --http2-prior-knowledge -H 'Content-Type: application/grpc' -H 'TE: trailers' \
  --data-binary '' "$BASE/nope.Svc/Method")
check "h2c + application/grpc routed to tonic" "grpc-status: 12" "$out"

out=$(curl -s -H 'Content-Type: application/grpc' "$BASE/foo")
check "http/1.1 + application/grpc stays HTTP (Go predicate requires h2)" '"version":"HTTP/1.1"' "$out"

out=$(curl -s --http2 -H 'Content-Type: application/json' \
  -H 'Connection: Upgrade, HTTP2-Settings' -H 'Upgrade: h2c' \
  -H 'HTTP2-Settings: AAMAAABkAAQAAP__' "$BASE/upgrade")
check "h2c upgrade served as HTTP/1.1 (known hyper gap)" '"version":"HTTP/1.1"' "$out"

if [[ $fail -ne 0 ]]; then
  echo "P1 FAILED"
  exit 1
fi
echo "P1 OK"
