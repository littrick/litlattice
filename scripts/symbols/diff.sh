#!/usr/bin/env bash
# Differential replay for the symbols pilot (M8.5).
#
#   scripts/symbols/diff.sh record   # capture normalized responses from Go (:3184)
#   scripts/symbols/diff.sh rust     # compare Rust (:13184) with the fixtures
#   scripts/symbols/diff.sh go       # sanity check: Go against its own fixtures
#
# Fixtures live in testdata/grpc/symbols and carry the recorded baseline
# response; `record` overwrites `expected` from the live Go service.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
FIXTURES="${FIXTURES:-$REPO_DIR/testdata/grpc/symbols}"

if ! command -v cargo >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
fi

cd "$REPO_DIR"

case "${1:-}" in
    record)
        cargo run -q -p xtask -- record \
            --service symbols --fixture "$FIXTURES" --addr http://127.0.0.1:3184
        ;;
    go)
        cargo run -q -p xtask -- diff \
            --service symbols --fixture "$FIXTURES" --addr http://127.0.0.1:3184
        ;;
    rust)
        cargo run -q -p xtask -- diff \
            --service symbols --fixture "$FIXTURES" --addr http://127.0.0.1:13184
        ;;
    *)
        echo "usage: $0 {record|go|rust}" >&2
        exit 2
        ;;
esac
