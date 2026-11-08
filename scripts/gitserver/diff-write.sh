#!/usr/bin/env bash
# Write-path differential for gitserver (M9.4).
#
#   scripts/gitserver/diff-write.sh [--keep]
#
# Both gitservers must run against the same Postgres database but *separate*
# repository directories (each instance keeps its own volume; see
# docs/architecture/10_compatibility_and_rollout.md §2.2):
#
#   Go:   SRC_REPOS_DIR=$GO_REPOS    gitserver (:3178)
#   Rust: SRC_REPOS_DIR=$RUST_REPOS  gitserver (:13178)
#
# The script creates a local `file://` remote, points a repository row at it
# (`repo` + `external_services` + `external_service_repos`, the same shape
# repo-updater writes), then replays three phases against both instances:
#
#   clone  — FetchRepository, RepoCloneProgress, ResolveRevision, RevAtTime,
#            an idempotent second fetch, and ListRepositories
#   fetch  — FetchRepository after the remote gained a commit, then resolve
#   delete — DeleteRepository, then progress/list/resolve afterwards
#
# Every phase must produce identical normalized JSON. After `clone` the two
# on-disk repositories must also be identical (HEAD, refs, sourcegraph.type,
# info/attributes, sg_refhash_v2): that is the disk coexistence check which
# allows either instance to take over a repository directory during a rolling
# switch. RevAtTime additionally goes through the Go process's LRU cache, so
# cache and disk must agree.
#
# Environment:
#   GO / RUST            endpoints (default http://127.0.0.1:3178 / :13178)
#   GO_REPOS / RUST_REPOS  data dirs (default test_ws/run/repos and
#                          /tmp/gitserver-write-diff/repos)
#   PG_DSN               frontend database (devenv default)
#   REPO                 repository name to create and replay (default
#                        test/write-fixture)
#   SERVICE              external service display name (default
#                        gitserver-write-diff)
#
# The repository rows and directories are removed on exit unless --keep is
# passed (a failed run keeps them too, for inspection).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

GO="${GO:-http://127.0.0.1:3178}"
RUST="${RUST:-http://127.0.0.1:13178}"
GO_REPOS="${GO_REPOS:-/home/developer/ws/test_ws/run/repos}"
RUST_REPOS="${RUST_REPOS:-/tmp/gitserver-write-diff/repos}"
PG_DSN="${PG_DSN:-postgres://sourcegraph:sourcegraph@127.0.0.1:5432/sourcegraph?sslmode=disable}"
REPO="${REPO:-test/write-fixture}"
SERVICE="${SERVICE:-gitserver-write-diff}"

KEEP=0
if [ "${1:-}" = "--keep" ]; then
    KEEP=1
fi

if ! command -v cargo >/dev/null 2>&1; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
fi
for tool in psql git jq cargo; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "diff-write: missing $tool" >&2
        exit 2
    fi
done

if [ "$GO_REPOS" = "$RUST_REPOS" ]; then
    echo "diff-write: GO_REPOS and RUST_REPOS must differ (each instance needs its own volume)" >&2
    exit 2
fi

WORK="$(mktemp -d)"
REMOTE="$WORK/remote.git"
SOURCE="$WORK/src"
failures=0
keep_work=0

pass() { printf 'PASS %s\n' "$*"; }
fail() {
    printf 'FAIL %s\n' "$*" >&2
    failures=$((failures + 1))
}

psql_q() {
    psql "$PG_DSN" -q -v ON_ERROR_STOP=1 -v repo="$REPO" -v service="$SERVICE" -v url="file://$REMOTE" "$@"
}

cleanup() {
    if [ "$KEEP" = 1 ] || [ "$keep_work" = 1 ]; then
        echo "diff-write: kept $WORK and repository rows" >&2
        return
    fi
    psql_q >/dev/null 2>&1 <<'SQL' || true
DELETE FROM repo WHERE name = :'repo';
DELETE FROM external_services WHERE display_name = :'service';
SQL
    rm -rf "${GO_REPOS:?}/$REPO" "${RUST_REPOS:?}/$REPO"
    rm -rf "$WORK"
}
trap cleanup EXIT

# --------------------------------------------------------------- remote + DB
mkdir -p "$SOURCE" "$GO_REPOS" "$RUST_REPOS"
git init -q --bare "$REMOTE"
git init -q -b main "$SOURCE"
git -C "$SOURCE" config user.name "Write Diff"
git -C "$SOURCE" config user.email "write-diff@example.com"

commit() {
    local message=$1 date=$2
    git -C "$SOURCE" add -A
    GIT_AUTHOR_DATE="$date" GIT_COMMITTER_DATE="$date" \
        git -C "$SOURCE" commit -q -m "$message"
}

printf 'one\n' >"$SOURCE/a.txt"
commit "first commit" "2024-05-01T00:00:00Z"
printf 'two\n' >>"$SOURCE/a.txt"
printf 'two\n' >"$SOURCE/b.txt"
commit "second commit" "2024-05-02T00:00:00Z"
GIT_COMMITTER_DATE="2024-05-03T00:00:00Z" \
    git -C "$SOURCE" tag -a v1 -m "annotated tag" HEAD~1
git -C "$SOURCE" branch dev HEAD~1

sync_remote() {
    git --git-dir="$REMOTE" fetch -q "$SOURCE" \
        'refs/heads/*:refs/heads/*' 'refs/tags/*:refs/tags/*'
}

sync_remote
git --git-dir="$REMOTE" symbolic-ref HEAD refs/heads/main

psql_q >/dev/null <<'SQL'
DELETE FROM repo WHERE name = :'repo';
DELETE FROM external_services WHERE display_name = :'service';
WITH svc AS (
    INSERT INTO external_services (kind, display_name, config)
    VALUES ('OTHER', :'service', json_build_object('url', :'url')::text)
    RETURNING id
), r AS (
    INSERT INTO repo (name, external_service_type, external_service_id, metadata)
    VALUES (:'repo', 'other', :'url', json_build_object('RelativePath', '')::jsonb)
    RETURNING id
)
INSERT INTO external_service_repos (external_service_id, repo_id, clone_url)
SELECT svc.id, r.id, :'url' FROM svc, r;
SQL
if [ $? -ne 0 ]; then
    echo "diff-write: failed to create the repository rows" >&2
    exit 1
fi

rm -rf "${GO_REPOS:?}/$REPO" "${RUST_REPOS:?}/$REPO"

# ------------------------------------------------------------------- helpers
run_phase() { # outfile endpoint phase
    local out=$1 endpoint=$2 phase=$3
    if ! (cd "$REPO_DIR" && cargo run -q -p xtask -- replay-write \
        --addr "$endpoint" --repo "$REPO" --phase "$phase") \
        >"$out" 2>"$out.err"; then
        cat "$out.err" >&2
        return 1
    fi
}

compare_phase() { # phase
    local phase=$1
    if diff -u <(jq -S . "$WORK/go-$phase.json") <(jq -S . "$WORK/rust-$phase.json") \
        >"$WORK/$phase.diff"; then
        pass "$phase phase: normalized reports match"
    else
        fail "$phase phase: normalized reports differ"
        cat "$WORK/$phase.diff" >&2
        keep_work=1
    fi
}

# Repository state either instance would rely on after a takeover. Volatile
# metadata (FETCH_HEAD, pack order, mtimes) is deliberately excluded.
snapshot() { # git_dir
    local dir=$1
    {
        printf 'HEAD=%s\n' "$(cat "$dir/HEAD")"
        printf 'head_tree=%s\n' "$(git --git-dir="$dir" rev-parse 'HEAD^{tree}')"
        git --git-dir="$dir" for-each-ref \
            --format='ref %(refname) %(objectname) %(objecttype)' | sort
        printf 'sourcegraph.type=%s\n' "$(git --git-dir="$dir" config --get sourcegraph.type)"
        if [ -f "$dir/info/attributes" ]; then
            printf 'attributes=%s\n' "$(sha256sum "$dir/info/attributes" | cut -d' ' -f1)"
        else
            printf 'attributes=missing\n'
        fi
        if [ -f "$dir/sg_refhash_v2" ]; then
            printf 'sg_refhash_v2=%s\n' "$(cat "$dir/sg_refhash_v2")"
        else
            printf 'sg_refhash_v2=missing\n'
        fi
    }
}

# --------------------------------------------------------------------- clone
if run_phase "$WORK/go-clone.json" "$GO" clone &&
    run_phase "$WORK/rust-clone.json" "$RUST" clone; then
    compare_phase clone
    snapshot "$GO_REPOS/$REPO/.git" >"$WORK/go-disk.txt"
    snapshot "$RUST_REPOS/$REPO/.git" >"$WORK/rust-disk.txt"
    if diff -u "$WORK/go-disk.txt" "$WORK/rust-disk.txt" >"$WORK/disk.diff"; then
        pass "clone phase: on-disk repository layout matches"
    else
        fail "clone phase: on-disk repository layout differs"
        cat "$WORK/disk.diff" >&2
        keep_work=1
    fi
else
    fail "clone phase: replay failed"
    keep_work=1
fi

# --------------------------------------------------------------------- fetch
printf 'three\n' >>"$SOURCE/a.txt"
commit "third commit" "2024-05-04T00:00:00Z"
sync_remote
if run_phase "$WORK/go-fetch.json" "$GO" fetch &&
    run_phase "$WORK/rust-fetch.json" "$RUST" fetch; then
    compare_phase fetch
    snapshot "$GO_REPOS/$REPO/.git" >"$WORK/go-disk-fetch.txt"
    snapshot "$RUST_REPOS/$REPO/.git" >"$WORK/rust-disk-fetch.txt"
    if diff -u "$WORK/go-disk-fetch.txt" "$WORK/rust-disk-fetch.txt" >"$WORK/disk-fetch.diff"; then
        pass "fetch phase: on-disk repository layout matches"
    else
        fail "fetch phase: on-disk repository layout differs"
        cat "$WORK/disk-fetch.diff" >&2
        keep_work=1
    fi
else
    fail "fetch phase: replay failed"
    keep_work=1
fi

# -------------------------------------------------------------------- delete
if run_phase "$WORK/go-delete.json" "$GO" delete &&
    run_phase "$WORK/rust-delete.json" "$RUST" delete; then
    compare_phase delete
    for dir in "$GO_REPOS/$REPO" "$RUST_REPOS/$REPO"; do
        if [ -e "$dir" ]; then
            fail "delete phase: $dir still exists"
            keep_work=1
        fi
    done
    pass "delete phase: repository directories removed"
else
    fail "delete phase: replay failed"
    keep_work=1
fi

if [ "$failures" -gt 0 ]; then
    printf 'diff-write: %d failure(s)\n' "$failures" >&2
    exit 1
fi
echo "diff-write: all write-path phases match"
