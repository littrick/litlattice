#!/usr/bin/env bash
# Differential replay for the repo-updater gRPC API (M9.6).
#
#   scripts/repo-updater/diff.sh record   # capture normalized responses from Go (:3182)
#   scripts/repo-updater/diff.sh go       # sanity check: Go against its own fixtures
#   scripts/repo-updater/diff.sh rust     # compare Rust (:13182) with the fixtures
#
# Fixtures live in testdata/grpc/repoupdater_diff (the five cases the pilot
# reproduces; the batches-only EnqueueChangesetSync cases stay in
# testdata/grpc/repoupdater as baseline). Both processes must share the same
# Postgres: the `enqueue_known` case resolves repo id 999001
# (`rustdiff.example.com/owner/repo`) from the store, and the scheduler RPCs
# are forwarded to a live gitserver.
#
# Rust side, for example:
#   SRC_FRONTEND_INTERNAL=127.0.0.1:3090 SG_REPO_UPDATER_ADDR=127.0.0.1:13182 \
#   SRC_PROF_HTTP=127.0.0.1:6166 target/debug/repo-updater
#
# The `enqueue_known` canary is a write (the repo is queued for update), which
# is the same side effect the frontend causes routinely; run the suite against
# only one instance at a time when the scheduler mutates shared rows.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
FIXTURES="${FIXTURES:-$REPO_DIR/testdata/grpc/repoupdater_diff}"

if ! command -v cargo >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
fi

cd "$REPO_DIR"

case "${1:-}" in
    record)
        cargo run -q -p xtask -- record \
            --service repoupdater --fixture "$FIXTURES" --addr "${GO_ADDR:-http://127.0.0.1:3182}"
        ;;
    go)
        cargo run -q -p xtask -- diff \
            --service repoupdater --fixture "$FIXTURES" --addr "${GO_ADDR:-http://127.0.0.1:3182}"
        ;;
    rust)
        cargo run -q -p xtask -- diff \
            --service repoupdater --fixture "$FIXTURES" --addr "${RUST_ADDR:-http://127.0.0.1:13182}"
        ;;
    *)
        echo "usage: $0 {record|go|rust}" >&2
        exit 2
        ;;
esac
