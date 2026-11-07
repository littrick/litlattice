#!/usr/bin/env bash
# Create (or recreate) the repositories the gitserver fixtures replay against.
#
#   scripts/gitserver/fixture-repos.sh [repos_dir]
#
# Default repos_dir is $SRC_REPOS_DIR, else /home/developer/ws/test_ws/run/repos.
# Both the Go gitserver (:3178) and the Rust binary must point at the same
# directory. Commits and annotated tags use fixed identities and dates, so the
# resulting SHAs match the fixture expectations in testdata/grpc/gitserver/.
#
# `test/symbols-fixture` is not created here: it predates the gitserver
# fixtures and is also used by the searcher/symbols suites.

set -euo pipefail

REPOS_DIR="${1:-${SRC_REPOS_DIR:-/home/developer/ws/test_ws/run/repos}}"
REPO="$REPOS_DIR/test/git-refs-fixture/.git"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

git init -q --bare "$REPO"
git -C "$WORK" init -q -b main
git -C "$WORK" config user.name "Fixture Author"
git -C "$WORK" config user.email "fixture@example.com"

commit() {
    local message=$1 date=$2
    shift 2
    git -C "$WORK" add -A
    GIT_AUTHOR_DATE="$date" GIT_COMMITTER_DATE="$date" \
        git -C "$WORK" commit -q -m "$message"
}

printf 'a\n' >"$WORK/a.txt"
commit "root commit" "2024-01-01T00:00:00Z"

printf 'b\n' >"$WORK/a.txt"
printf 'b\n' >"$WORK/b.txt"
commit "second commit" "2024-02-02T00:00:00Z"

git -C "$WORK" branch feature
git -C "$WORK" branch dev HEAD~1
# Annotated and lightweight tags exercise both ref types; dates are fixed so
# tag object IDs stay stable.
GIT_COMMITTER_DATE="2024-03-03T00:00:00Z" \
    git -C "$WORK" tag -a v1.0 -m "annotated tag" HEAD~1
GIT_COMMITTER_DATE="2024-03-03T00:00:00Z" \
    git -C "$WORK" tag -a v2.0 -m "annotated tag two" HEAD
git -C "$WORK" tag lightweight HEAD~1

git --git-dir="$REPO" fetch -q "$WORK" \
    'refs/heads/*:refs/heads/*' 'refs/tags/*:refs/tags/*'
git --git-dir="$REPO" symbolic-ref HEAD refs/heads/main

git --git-dir="$REPO" log --format='%H %s' main
