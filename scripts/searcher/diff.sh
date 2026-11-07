#!/usr/bin/env bash
# Differential replay for the searcher pilot (M8.8).
#
#   scripts/searcher/diff.sh record   # capture normalized responses from Go (:3181)
#   scripts/searcher/diff.sh rust     # compare Rust (:13181) with the fixtures
#   scripts/searcher/diff.sh go       # sanity check: Go against its own fixtures
#
# Fixtures live in testdata/grpc/searcher and carry the recorded baseline
# response; `record` overwrites `expected` from the live Go service. Events are
# normalized (sorted file matches/chunks/ranges, separate Done) because worker
# scheduling makes emission order unstable.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
FIXTURES="${FIXTURES:-$REPO_DIR/testdata/grpc/searcher}"

if ! command -v cargo >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
fi

cd "$REPO_DIR"

case "${1:-}" in
    record)
        cargo run -q -p xtask -- record \
            --service searcher --fixture "$FIXTURES" --addr http://127.0.0.1:3181
        ;;
    go)
        cargo run -q -p xtask -- diff \
            --service searcher --fixture "$FIXTURES" --addr http://127.0.0.1:3181
        ;;
    rust)
        cargo run -q -p xtask -- diff \
            --service searcher --fixture "$FIXTURES" --addr http://127.0.0.1:13181
        ;;
    *)
        echo "usage: $0 {record|go|rust}" >&2
        exit 2
        ;;
esac
