#!/usr/bin/env bash
# P4 acceptance checks: session cookie (gorilla/securecookie) and Redis gob
# payload (boj/redistore) compatibility with the Go baseline.
#
#   ./verify.sh
#
# Requires: cargo, python3, and (unless SKIP_GO=1) go. Redis checks run when a
# server is reachable at REDIS_ADDR (default 127.0.0.1:6379); otherwise they are
# skipped.
set -euo pipefail
cd "$(dirname "$0")"

export PATH=/usr/local/go/bin:$PATH
. "$HOME/.cargo/env"

BIN=target/debug/p4-session
GOFIX=target/go-fixture
FIX=../../testdata/sessions
REDIS_ADDR=${REDIS_ADDR:-127.0.0.1:6379}
SKIP_GO=${SKIP_GO:-0}

cargo build --quiet
cargo test --quiet

jget() { python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"; }

fail=0
pass() { echo "PASS  $1"; }
bad() { echo "FAIL  $1: $2"; fail=1; }

check_eq() { # name expected actual
  if [[ "$3" == "$2" ]]; then pass "$1"; else bad "$1" "expected '$2', got '$3'"; fi
}

check_fails() { # name command...
  local name=$1; shift
  if "$@" >/dev/null 2>&1; then bad "$name" "command unexpectedly succeeded"; else pass "$name"; fi
}

check_succeeds() { # name command...
  local name=$1; shift
  if "$@" >/tmp/p4-out 2>&1; then pass "$name"; else bad "$name" "$(cat /tmp/p4-out)"; fi
}

entries_equal() { # name file1 file2
  python3 - "$2" "$3" >/tmp/p4-cmp 2>&1 <<'PY' && pass "$1" || bad "$1" "$(cat /tmp/p4-cmp)"
import json, sys
def entries(p):
    d = json.load(open(p))
    return sorted((e["key"], e["base64"]) for e in d["entries"])
a, b = entries(sys.argv[1]), entries(sys.argv[2])
if a != b:
    print("entries differ:\n  %r\n  %r" % (a, b))
    sys.exit(1)
PY
}

# ---------------------------------------------------------------- cookie ----
for name in cookie-empty-key cookie-test-key; do
  f=$FIX/$name.json
  cookie=$(jget "$f" cookie)
  key=$(jget "$f" key)
  id=$(jget "$f" id)
  ts=$(jget "$f" timestamp)

  out=$($BIN decode-cookie "$cookie" --key "$key" --now "$ts")
  check_eq "$name decode id" "$id" "$(python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])' <<<"$out")"
  check_eq "$name decode ts" "$ts" "$(python3 -c 'import json,sys;print(json.load(sys.stdin)["timestamp"])' <<<"$out")"

  re=$($BIN encode-cookie "$id" --key "$key" --ts "$ts")
  check_eq "$name re-encode byte-identical" "$cookie" "$re"

  check_fails "$name wrong key rejected" "$BIN" decode-cookie "$cookie" --key "wrong-key" --now "$ts"
  check_fails "$name expired rejected" "$BIN" decode-cookie "$cookie" --key "$key" --now "$((ts + 30 * 86400 + 1))"
  check_succeeds "$name within max age accepted" "$BIN" decode-cookie "$cookie" --key "$key" --now "$((ts + 29 * 86400))"

  if [[ $SKIP_GO != 1 ]]; then
    go build -C go-fixture -o "$(pwd)/$GOFIX" .
    out=$($GOFIX decode-cookie -cookie "$cookie" -key "$key")
    check_eq "$name Go decodes" "$id" "$(python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])' <<<"$out")"
  fi
done

# --------------------------------------------------------------- gob map ----
hexof() { od -An -v -tx1 "$1" | tr -d ' \n'; }

for name in redis-empty redis-single redis-multi; do
  gob=$FIX/$name.gob
  exp=$FIX/$name.expected.json
  $BIN decode-map "$(hexof "$gob")" > /tmp/p4-decoded.json
  entries_equal "$name Rust decodes Go payload" /tmp/p4-decoded.json "$exp"
done

$BIN encode-map "{\"entries\":[]}" > /tmp/p4-empty.hex
check_eq "empty map re-encode byte-identical" "$(hexof "$FIX/redis-empty.gob")" "$(cat /tmp/p4-empty.hex)"

actor_b64=$(python3 -c 'import base64,sys;print(base64.b64encode(open(sys.argv[1],"rb").read()).decode())' "$FIX/actor.json")
$BIN encode-map "{\"actor\":\"$actor_b64\"}" > /tmp/p4-single.hex
check_eq "single-entry map re-encode byte-identical" "$(hexof "$FIX/redis-single.gob")" "$(cat /tmp/p4-single.hex)"

# Rust-encoded multi-entry map must be readable by Go.
$BIN encode-map "$(cat "$FIX/redis-multi.expected.json")" > /tmp/p4-multi.hex
if [[ $SKIP_GO != 1 ]]; then
  go build -C go-fixture -o "$(pwd)/$GOFIX" .
  $GOFIX decode-gob -hex "$(cat /tmp/p4-multi.hex)" > /tmp/p4-multi-go.json
  entries_equal "multi-entry map Go decodes Rust payload" /tmp/p4-multi-go.json "$FIX/redis-multi.expected.json"
fi

# Rust-encoded map must also decode back to the same entries.
$BIN decode-map "$(cat /tmp/p4-multi.hex)" > /tmp/p4-multi-rt.json
entries_equal "multi-entry map Rust round-trip" /tmp/p4-multi-rt.json "$FIX/redis-multi.expected.json"

# Actor JSON document must round-trip byte-identically through the typed model.
out=$($BIN parse-actor "\"$actor_b64\"")
check_eq "actor sessionInfo re-encode byte-identical" "True" \
  "$(python3 -c 'import json,sys;print(json.load(sys.stdin)["reencode_equal"])' <<<"$out")"

# ------------------------------------------------------------ session id ----
id1=$($BIN gen-id | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')
id2=$($BIN gen-id | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')
if [[ ${#id1} == 52 && "$id1" =~ ^[A-Z2-7]+$ && "$id1" != "$id2" ]]; then
  pass "gen-id base32 52 chars"
else
  bad "gen-id" "got '$id1' / '$id2'"
fi

# ---------------------------------------------------------------- redis ----
if redis-cli -h "${REDIS_ADDR%:*}" -p "${REDIS_ADDR##*:}" ping >/dev/null 2>&1; then
  rid="P4RUST${RANDOM}${RANDOM}"
  $BIN redis-set "$rid" "{\"actor\":\"$actor_b64\"}" --redis "$REDIS_ADDR" >/dev/null
  cookie=$($BIN encode-cookie "$rid")
  out=$($BIN redis-get "$cookie" --redis "$REDIS_ADDR")
  check_eq "redis-get reads Rust payload" "1" \
    "$(python3 -c 'import json,sys;print(json.load(sys.stdin)["actor"]["actor"]["UID"])' <<<"$out")"

  if [[ $SKIP_GO != 1 ]]; then
    go build -C go-fixture -o "$(pwd)/$GOFIX" .
    out=$($GOFIX verify-redis -cookie "$cookie" -key "" -redis "$REDIS_ADDR")
    check_eq "Go reads Rust cookie + payload" "1" \
      "$(python3 -c 'import json,sys;print(json.load(sys.stdin)["actor"]["actor"]["UID"])' <<<"$out")"
  fi
  redis-cli -h "${REDIS_ADDR%:*}" -p "${REDIS_ADDR##*:}" del "session_$rid" >/dev/null

  # Go-produced payload read by Rust.
  rid2="P4GO${RANDOM}${RANDOM}"
  redis-cli -h "${REDIS_ADDR%:*}" -p "${REDIS_ADDR##*:}" -x setex "session_$rid2" 2592000 < "$FIX/redis-single.gob" >/dev/null
  cookie2=$($BIN encode-cookie "$rid2")
  out=$($BIN redis-get "$cookie2" --redis "$REDIS_ADDR")
  check_eq "redis-get reads Go payload written by redis-cli" "1" \
    "$(python3 -c 'import json,sys;print(json.load(sys.stdin)["actor"]["actor"]["UID"])' <<<"$out")"
  redis-cli -h "${REDIS_ADDR%:*}" -p "${REDIS_ADDR##*:}" del "session_$rid2" >/dev/null
else
  echo "SKIP  redis checks (no server at $REDIS_ADDR)"
fi

if [[ $fail == 0 ]]; then
  echo "P4 ALL CHECKS PASSED"
else
  echo "P4 CHECKS FAILED"
  exit 1
fi
