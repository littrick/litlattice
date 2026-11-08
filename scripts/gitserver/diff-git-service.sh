#!/usr/bin/env bash
# Differential check for the `/git/` smart HTTP transport and `/ping` (M9.2).
#
#   scripts/gitserver/diff-git-service.sh [repo]
#
# Both gitservers must run against the same repositories:
#
#   Go:   /home/developer/ws/test_ws/bin/gitserver            (:3178)
#   Rust: SRC_REPOS_DIR=... SG_GITSERVER_ADDR=127.0.0.1:13178 target/debug/gitserver
#
# `git` is invoked with `http.proxy=` so the host's global proxy config does
# not intercept localhost. The pack payload is compared after packet-line
# decoding, because sideband packet boundaries depend on git's write timing
# and differ between runs even for the same server.

set -euo pipefail

GO="${GO:-http://127.0.0.1:3178}"
RUST="${RUST:-http://127.0.0.1:13178}"
REPO="${1:-test/git-refs-fixture}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

failures=0

pass() { printf 'PASS %s\n' "$*"; }
fail() {
    printf 'FAIL %s\n' "$*" >&2
    failures=$((failures + 1))
}

# Normalized subset of response headers. Volatile ones (Date, X-Trace,
# Content-Length) are dropped; these are the ones the mux/handlers set.
header_dump() {
    python3 -I - "$1" <<'EOF'
import sys

wanted = {"location", "content-type", "x-content-type-options"}
lines = []
with open(sys.argv[1], encoding="latin-1") as handle:
    for line in handle.read().splitlines():
        name, sep, value = line.partition(":")
        if sep and name.strip().lower() in wanted:
            lines.append(f"{name.strip().lower()}: {value.strip()}")
print("\n".join(sorted(lines)))
EOF
}

# Compare status, mux-relevant headers and body of a request against both
# servers. `--path-as-is` keeps curl from cleaning dot segments before the
# request reaches the mux.
compare_request() {
    local label=$1 path=$2
    shift 2
    local go_status rust_status
    go_status=$(curl -s --path-as-is -D "$WORK/go.head" -o "$WORK/go.body" -w '%{http_code}' "$@" "$GO$path")
    rust_status=$(curl -s --path-as-is -D "$WORK/rust.head" -o "$WORK/rust.body" -w '%{http_code}' "$@" "$RUST$path")
    header_dump "$WORK/go.head" >"$WORK/go.headers"
    header_dump "$WORK/rust.head" >"$WORK/rust.headers"
    if [ "$go_status" != "$rust_status" ]; then
        fail "$label: status Go=$go_status Rust=$rust_status"
    elif ! cmp -s "$WORK/go.headers" "$WORK/rust.headers"; then
        fail "$label: headers differ (Go: $(tr '\n' ';' <"$WORK/go.headers") Rust: $(tr '\n' ';' <"$WORK/rust.headers"))"
    elif ! cmp -s "$WORK/go.body" "$WORK/rust.body"; then
        fail "$label: body differs"
    else
        pass "$label (status $go_status)"
    fi
}

# Extract the packfile (sideband band 1) from an upload-pack response.
pack_sha() {
    python3 -I - "$1" <<'EOF'
import hashlib, sys
data = open(sys.argv[1], "rb").read()
pack = bytearray()
index = 0
while index < len(data):
    length = int(data[index:index + 4], 16)
    index += 4
    if length == 0:
        continue
    payload = data[index:index + length - 4]
    index += length - 4
    if payload[:1] == b"\x01":
        pack += payload[1:]
print(hashlib.sha256(pack).hexdigest())
EOF
}

# A protocol v0 `want` request for one commit.
upload_request() {
    python3 -I - "$1" <<'EOF'
import sys
sha = sys.argv[1].encode()
content = b"want " + sha + b" multi_ack_detailed side-band-64k ofs-delta\n"
sys.stdout.buffer.write((b"%04x" % (len(content) + 4)) + content + b"0000" + b"0009done\n")
EOF
}

# ---------------------------------------------------------------- /ping
go_ping=$(curl -s -o "$WORK/go.ping" -w '%{http_code}' "$GO/ping")
rust_ping=$(curl -s -o "$WORK/rust.ping" -w '%{http_code}' "$RUST/ping")
if [ "$go_ping" = "$rust_ping" ] && cmp -s "$WORK/go.ping" "$WORK/rust.ping"; then
    pass "/ping (status $go_ping)"
else
    fail "/ping: status Go=$go_ping Rust=$rust_ping"
fi

# ------------------------------------------------------- info/refs advertisement
curl -s -o "$WORK/go.refs" "$GO/git/$REPO/info/refs?service=git-upload-pack"
curl -s -o "$WORK/rust.refs" "$RUST/git/$REPO/info/refs?service=git-upload-pack"
if cmp -s "$WORK/go.refs" "$WORK/rust.refs"; then
    pass "info/refs advertisement"
else
    fail "info/refs advertisement differs"
fi

# ---------------------------------------------------------------- clones
head=$(git --git-dir="/home/developer/ws/test_ws/run/repos/$REPO/.git" rev-parse HEAD 2>/dev/null || true)
if [ -z "$head" ]; then
    fail "cannot resolve $REPO; run scripts/gitserver/fixture-repos.sh"
else
    for protocol in 2 0; do
        rm -rf "$WORK/go.clone" "$WORK/rust.clone"
        if git -c http.proxy= -c protocol.version=$protocol clone -q "$GO/git/$REPO" "$WORK/go.clone" 2>"$WORK/go.clone.err" &&
            git -c http.proxy= -c protocol.version=$protocol clone -q "$RUST/git/$REPO" "$WORK/rust.clone" 2>"$WORK/rust.clone.err"; then
            go_state=$(git -C "$WORK/go.clone" for-each-ref --format='%(refname) %(objectname)'; git -C "$WORK/go.clone" cat-file --batch-all-objects --batch-check='%(objectname) %(objecttype)' | sort)
            rust_state=$(git -C "$WORK/rust.clone" for-each-ref --format='%(refname) %(objectname)'; git -C "$WORK/rust.clone" cat-file --batch-all-objects --batch-check='%(objectname) %(objecttype)' | sort)
            if [ "$go_state" = "$rust_state" ]; then
                pass "clone (protocol v$protocol)"
            else
                fail "clone (protocol v$protocol): refs/objects differ"
            fi
        else
            fail "clone (protocol v$protocol) failed"
        fi
    done

    upload_request "$head" >"$WORK/request.bin"
    gzip -n -c "$WORK/request.bin" >"$WORK/request.bin.gz"
    for encoding in plain gzip; do
        case $encoding in
            plain)
                extra=()
                body="$WORK/request.bin"
                ;;
            gzip)
                extra=(-H 'Content-Encoding: gzip')
                body="$WORK/request.bin.gz"
                ;;
        esac
        curl -s -o "$WORK/go.pack" -X POST -H 'Content-Type: application/x-git-upload-pack-request' \
            "${extra[@]}" --data-binary @"$body" "$GO/git/$REPO/git-upload-pack"
        curl -s -o "$WORK/rust.pack" -X POST -H 'Content-Type: application/x-git-upload-pack-request' \
            "${extra[@]}" --data-binary @"$body" "$RUST/git/$REPO/git-upload-pack"
        go_sha=$(pack_sha "$WORK/go.pack")
        rust_sha=$(pack_sha "$WORK/rust.pack")
        if [ -n "$go_sha" ] && [ "$go_sha" = "$rust_sha" ]; then
            pass "upload-pack ($encoding body): pack ${go_sha:0:16}"
        else
            fail "upload-pack ($encoding body): pack Go=${go_sha:0:16} Rust=${rust_sha:0:16}"
        fi
    done
fi

# ---------------------------------------------------------------- errors
compare_request "receive-pack rejected" "/git/$REPO/info/refs?service=git-receive-pack"
compare_request "missing repository" "/git/test/missing-git-service-fixture/info/refs?service=git-upload-pack"
compare_request "unknown subpath" "/git/$REPO/nope"

printf 'malformed' >"$WORK/bad.gz"
compare_request "malformed gzip" "/git/$REPO/git-upload-pack" \
    -X POST -H 'Content-Encoding: gzip' --data-binary @"$WORK/bad.gz"

# -------------------------------------------------- HTTP mux behavior
# `ServeMux` cleans the escaped path and redirects, and redirects the `/git`
# subtree root to `/git/`; `StripPrefix` then decodes the path.
compare_request "exact /git redirects" "/git"
compare_request "exact /git redirects with query" "/git?service=git-upload-pack"
compare_request "path cleaning: repeated slash" "/git//$REPO/info/refs?service=git-upload-pack"
compare_request "path cleaning: dot segment" "/git/./$REPO/info/refs?service=git-upload-pack"
compare_request "path cleaning: dot-dot segment" "/git/../git/ping"
compare_request "path cleaning: escaped space" "/git//a%20b/info/refs"
compare_request "unknown path" "/nope"
compare_request "ping with trailing slash" "/ping/"
compare_request "deleted repo alias" "/git/DELETED-1.2-$REPO/info/refs?service=git-upload-pack"
compare_request "percent-encoded repo separator" "/git/test%2Fgit-refs-fixture/info/refs?service=git-upload-pack"

# HEAD gets the redirect headers (no HTML body).
curl -s --path-as-is -I -o "$WORK/go.head" -w '%{http_code}' "$GO/git" >"$WORK/go.status"
curl -s --path-as-is -I -o "$WORK/rust.head" -w '%{http_code}' "$RUST/git" >"$WORK/rust.status"
header_dump "$WORK/go.head" >"$WORK/go.headers"
header_dump "$WORK/rust.head" >"$WORK/rust.headers"
if [ "$(cat "$WORK/go.status")" = "$(cat "$WORK/rust.status")" ] && cmp -s "$WORK/go.headers" "$WORK/rust.headers"; then
    pass "HEAD /git redirect (status $(cat "$WORK/go.status"))"
else
    fail "HEAD /git redirect: status Go=$(cat "$WORK/go.status") Rust=$(cat "$WORK/rust.status") headers Go=$(tr '\n' ';' <"$WORK/go.headers") Rust=$(tr '\n' ';' <"$WORK/rust.headers")"
fi

# X-Trace headers (instrumentation.HTTPMiddleware + trace.HTTPMiddleware).
# Values are random per request, so compare formats, the empty X-Trace-URL,
# and the W3C traceparent continuation.
header_value() {
    awk -v key="$1" '{
        line = $0
        sub(/\r$/, "", line)
        pos = index(line, ":")
        if (pos == 0) next
        name = substr(line, 1, pos - 1)
        value = substr(line, pos + 1)
        gsub(/^[ \t]+/, "", value)
        gsub(/[ \t]+$/, "", value)
        if (tolower(name) == tolower(key)) { print value; exit }
    }' "$2"
}

trace_head() { # out, curl args..., url
    local out=$1
    shift
    curl -s -D "$out" -o /dev/null "$@"
}

for pair in "go:$GO" "rust:$RUST"; do
    name=${pair%%:*}
    base=${pair#*:}
    trace_head "$WORK/$name.trace" "$base/ping"
    trace=$(header_value "X-Trace" "$WORK/$name.trace")
    span=$(header_value "X-Trace-Span" "$WORK/$name.trace")
    url=$(header_value "X-Trace-Url" "$WORK/$name.trace")
    if [[ "$trace" =~ ^[0-9a-f]{32}$ ]] && [ "$trace" != "00000000000000000000000000000000" ] \
        && [[ "$span" =~ ^[0-9a-f]{16}$ ]] && [ "$url" = "" ]; then
        pass "$name X-Trace headers (32/16 hex, empty URL)"
    else
        fail "$name X-Trace headers: trace='$trace' span='$span' url='$url'"
    fi
done

parent=11111111111111111111111111111111
for pair in "go:$GO" "rust:$RUST"; do
    name=${pair%%:*}
    base=${pair#*:}
    trace_head "$WORK/$name.parent" -H "traceparent: 00-$parent-2222222222222222-01" "$base/ping"
    trace=$(header_value "X-Trace" "$WORK/$name.parent")
    if [ "$trace" = "$parent" ]; then
        pass "$name continues a valid traceparent"
    else
        fail "$name traceparent: got '$trace', want '$parent'"
    fi
    trace_head "$WORK/$name.invalid" -H "traceparent: 00-00000000000000000000000000000000-2222222222222222-01" "$base/ping"
    trace=$(header_value "X-Trace" "$WORK/$name.invalid")
    if [[ "$trace" =~ ^[0-9a-f]{32}$ ]] && [ "$trace" != "00000000000000000000000000000000" ]; then
        pass "$name ignores an invalid traceparent"
    else
        fail "$name invalid traceparent: got '$trace'"
    fi
done

# Redirects run through the same middleware chain.
for pair in "go:$GO" "rust:$RUST"; do
    name=${pair%%:*}
    base=${pair#*:}
    trace_head "$WORK/$name.redirect" "$base/git"
    trace=$(header_value "X-Trace" "$WORK/$name.redirect")
    if [[ "$trace" =~ ^[0-9a-f]{32}$ ]]; then
        pass "$name redirect carries X-Trace"
    else
        fail "$name redirect X-Trace: got '$trace'"
    fi
done

if [ "$failures" -ne 0 ]; then
    printf 'diff-git-service: %d failure(s)\n' "$failures" >&2
    exit 1
fi
printf 'diff-git-service: all checks match\n'
