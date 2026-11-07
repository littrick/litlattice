#!/usr/bin/env bash
# Differential replay for the gitserver read path (M9.2).
#
#   scripts/gitserver/diff.sh record   # capture normalized responses from Go (:3178)
#   scripts/gitserver/diff.sh rust     # compare Rust (:13178) with the fixtures
#   scripts/gitserver/diff.sh go       # sanity check: Go against its own fixtures
#
# Fixtures live in testdata/grpc/gitserver. Archive responses are compared by
# sha256 of the concatenated bytes (chunk boundaries vary), so both services
# must read the same repositories: point the Rust binary at the same
# SRC_REPOS_DIR (read-only use is fine).
#
# Rust side, for example:
#   SRC_REPOS_DIR=/home/developer/ws/test_ws/run/repos \
#   SG_GITSERVER_ADDR=127.0.0.1:13178 target/debug/gitserver

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
FIXTURES="${FIXTURES:-$REPO_DIR/testdata/grpc/gitserver}"

if ! command -v cargo >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
fi

cd "$REPO_DIR"

case "${1:-}" in
    record)
        cargo run -q -p xtask -- record \
            --service gitserver --fixture "$FIXTURES" --addr http://127.0.0.1:3178
        ;;
    go)
        cargo run -q -p xtask -- diff \
            --service gitserver --fixture "$FIXTURES" --addr http://127.0.0.1:3178
        ;;
    rust)
        cargo run -q -p xtask -- diff \
            --service gitserver --fixture "$FIXTURES" --addr http://127.0.0.1:13178
        ;;
    *)
        echo "usage: $0 {record|go|rust}" >&2
        exit 2
        ;;
esac
