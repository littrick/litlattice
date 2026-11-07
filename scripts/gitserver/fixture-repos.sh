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

# ---------------------------------------------------------------- blame fixture
# Three commits change `file.txt` in ways that produce several blame hunks:
# `c2` rewrites lines 1 and 4, `c3` makes a whitespace-only change to line 2,
# so `-w` reassigns that line to `c1`. `héllo.txt` exercises git's quoted
# (octal-escaped) porcelain filenames.
BLAME_REPO="$REPOS_DIR/test/blame-fixture/.git"
BLAME_WORK="$WORK/blame"
git init -q --bare "$BLAME_REPO"
git init -q -b main "$BLAME_WORK"

blame_commit() {
    local message=$1 date=$2 name=$3 email=$4
    git -C "$BLAME_WORK" add -A
    GIT_AUTHOR_DATE="$date" GIT_COMMITTER_DATE="$date" \
    GIT_AUTHOR_NAME="$name" GIT_AUTHOR_EMAIL="$email" \
    GIT_COMMITTER_NAME="$name" GIT_COMMITTER_EMAIL="$email" \
        git -C "$BLAME_WORK" commit -q -m "$message"
}

printf 'alpha\nbeta\ngamma\ndelta\n' >"$BLAME_WORK/file.txt"
printf 'other\n' >"$BLAME_WORK/other.txt"
printf 'unicode\n' >"$BLAME_WORK/héllo.txt"
blame_commit "root commit" "2024-04-01T00:00:00Z" \
    "Alpha Writer" "alpha@example.com"

printf 'ALPHA\nbeta\ngamma\nDELTA\n' >"$BLAME_WORK/file.txt"
blame_commit "rewrite first and last lines" "2024-05-02T00:00:00Z" \
    "Bravo Writer" "bravo@example.com"

printf 'ALPHA\n beta\ngamma\nDELTA\n' >"$BLAME_WORK/file.txt"
blame_commit "whitespace only change" "2024-06-03T00:00:00Z" \
    "Charlie Writer" "charlie@example.com"

git --git-dir="$BLAME_REPO" fetch -q "$BLAME_WORK" \
    'refs/heads/*:refs/heads/*'
git --git-dir="$BLAME_REPO" symbolic-ref HEAD refs/heads/main

git --git-dir="$BLAME_REPO" log --format='%H %s' main
