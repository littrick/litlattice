#!/usr/bin/env bash
# Traffic switch / rollback drill for gitserver (M9.4).
#
#   scripts/gitserver/switch.sh canary <addr> [repo]
#   scripts/gitserver/switch.sh switch <new-addr> <old-addr> [repo]
#
# `canary` checks one instance the way the frontend will use it: `/ping` must
# answer 200, the repository must report `cloned`, and `ResolveRevision(HEAD)`
# must succeed (scripts/xtask `canary` also lists the shard).
#
# `switch` runs the canary against both the instance that will take over and
# the one it replaces (the rollback target), then prints the deployment change.
# gitserver traffic is selected by the frontend's `SRC_GIT_SERVERS`
# (ServiceConnections.GitServers); switching and rolling back are env changes
# plus a frontend restart, not something this script can do from outside. The
# order and thresholds are in
# docs/architecture/10_compatibility_and_rollout.md §3 and §6.
#
# Before switching, run the differential gate:
#   scripts/gitserver/diff.sh go && scripts/gitserver/diff.sh rust
#   scripts/gitserver/diff-git-service.sh
#   scripts/gitserver/diff-metrics.sh
#   scripts/gitserver/diff-write.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

if ! command -v cargo >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
fi

usage() {
    sed -n '2,10p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
    exit 2
}

# ServiceConnections.GitServers entries are `host:port`, not URLs.
host_of() {
    printf '%s' "$1" | sed -E 's#^[A-Za-z]+://##; s#/$##'
}

canary() { # addr repo
    local addr=$1 repo=$2 status
    status=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$addr/ping") || status=000
    if [ "$status" = "200" ]; then
        printf 'PASS %s /ping 200\n' "$addr"
    else
        printf 'FAIL %s /ping %s\n' "$addr" "$status" >&2
        return 1
    fi
    (cd "$REPO_DIR" && cargo run -q -p xtask -- canary --addr "$addr" --repo "$repo")
}

case "${1:-}" in
    canary)
        [ $# -ge 2 ] || usage
        canary "${2%/}" "${3:-test/git-refs-fixture}"
        ;;
    switch)
        [ $# -ge 3 ] || usage
        new="${2%/}"
        old="${3%/}"
        repo="${4:-test/git-refs-fixture}"
        rc=0
        canary "$new" "$repo" || rc=1
        canary "$old" "$repo" || rc=1
        if [ "$rc" -ne 0 ]; then
            echo "switch: canary failed; do not switch (or roll back first)" >&2
            exit 1
        fi
        cat <<EOF
switch: both instances are healthy.
  1. set the frontend's SRC_GIT_SERVERS (ServiceConnections.GitServers) to $(host_of "$new")
     and restart the frontend (repo-updater keeps the same database rows).
  2. watch 5xx/Internal/Unavailable and p99 for the window in §6.
  3. rollback: set SRC_GIT_SERVERS back to $(host_of "$old") and restart the frontend.
EOF
        ;;
    *)
        usage
        ;;
esac
