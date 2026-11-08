#!/usr/bin/env bash
# Traffic switch / rollback drill for repo-updater (M9.6).
#
#   scripts/repo-updater/switch.sh canary <grpc-addr> <debug-addr>
#   scripts/repo-updater/switch.sh switch <new-grpc> <new-debug> <old-grpc> <old-debug>
#
# `canary` checks one instance the way the frontend and operators use it:
#   - the five differential fixtures pass over gRPC
#     (`scripts/repo-updater/diff.sh rust`, including EnqueueRepoUpdate,
#     RecloneRepository validation and RepoUpdateSchedulerInfo);
#   - `/repo-updater-state?format=json` is valid JSON with the expected
#     top-level keys;
#   - `/metrics` exposes the scrape-time scheduler gauge.
#
# `switch` runs the canary against both the instance that will take over and
# the one it replaces (the rollback target), then prints the deployment
# change. The frontend selects repo-updater with `REPO_UPDATER_URL`
# (`internal/repoupdater/client.go`; default `http://repo-updater:3182`).
#
# repo-updater is a singleton: two instances scheduling and purging the same
# Postgres will both fetch repositories and race on sync-job rows. During a
# switch, stop the old instance before (or at the same time as) starting the
# new one; in development the side-port rollout avoids downtime by pointing
# `REPO_UPDATER_URL` at the new port. Rollback is the reverse env change and
# must complete within one minute (M9 exit condition).
#
# Before switching, run the differential gates:
#   scripts/repo-updater/diff.sh go && scripts/repo-updater/diff.sh rust
#   scripts/repo-updater/diff-debug.sh
#   scripts/repo-updater/diff-metrics.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
FIXTURES="${FIXTURES:-$REPO_DIR/testdata/grpc/repoupdater_diff}"

if ! command -v cargo >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
fi

usage() {
    sed -n '2,9p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
    exit 2
}

# REPO_UPDATER_URL is a URL; accept `host:port` too.
url_of() {
    case "$1" in
        http://*|https://*) printf '%s' "${1%/}" ;;
        *) printf 'http://%s' "$1" ;;
    esac
}

canary() { # grpc-addr debug-addr
    local grpc_addr debug_addr
    grpc_addr=$(url_of "$1")
    debug_addr=$(url_of "$2")

    if (cd "$REPO_DIR" && cargo run -q -p xtask -- diff \
        --service repoupdater --fixture "$FIXTURES" --addr "$grpc_addr"); then
        printf 'PASS %s differential fixtures\n' "$grpc_addr"
    else
        printf 'FAIL %s differential fixtures\n' "$grpc_addr" >&2
        return 1
    fi

    if curl -fsS --max-time 5 "$debug_addr/repo-updater-state?format=json" \
        | jq -e 'type == "array" and (.[0] | has("Name") and has("UpdateQueue") and has("Schedule"))' \
        >/dev/null; then
        printf 'PASS %s /repo-updater-state\n' "$debug_addr"
    else
        printf 'FAIL %s /repo-updater-state\n' "$debug_addr" >&2
        return 1
    fi

    if curl -fsS --max-time 5 "$debug_addr/metrics" \
        | grep -q '^src_repoupdater_sched_known_repos '; then
        printf 'PASS %s /metrics\n' "$debug_addr"
    else
        printf 'FAIL %s /metrics\n' "$debug_addr" >&2
        return 1
    fi
}

case "${1:-}" in
    canary)
        [ $# -ge 3 ] || usage
        canary "$2" "$3"
        ;;
    switch)
        [ $# -ge 5 ] || usage
        new_grpc=$2
        new_debug=$3
        old_grpc=$4
        old_debug=$5
        rc=0
        canary "$new_grpc" "$new_debug" || rc=1
        canary "$old_grpc" "$old_debug" || rc=1
        if [ "$rc" -ne 0 ]; then
            echo "switch: canary failed; do not switch (or roll back first)" >&2
            exit 1
        fi
        cat <<EOF
switch: both instances are healthy.
  1. start $(url_of "$new_grpc") (Rust) and stop the old repo-updater $(url_of "$old_grpc") (Go)
     so that only one scheduler/purger writes to Postgres.
  2. set the frontend's REPO_UPDATER_URL to $(url_of "$new_grpc") and restart the frontend.
     In production the Rust binary should bind :3182 and REPO_UPDATER_URL can stay
     at the default http://repo-updater:3182.
  3. watch Internal/Unavailable rates, sync-job success and
     src_repoupdater_queued_sync_jobs_total for the rollout window.
  4. rollback (target: <= 1 minute): set REPO_UPDATER_URL back to
     $(url_of "$old_grpc"), restart the Go repo-updater, stop the Rust one.
     Sync jobs left in 'queued' are reclaimed by the heartbeat/resetter after
     the new instance starts, so no manual database cleanup is needed.
EOF
        ;;
    *)
        usage
        ;;
esac
