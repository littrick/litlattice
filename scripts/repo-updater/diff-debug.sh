#!/usr/bin/env bash
# Black-box differential for repo-updater's debug HTTP surface (M9.6).
#
#   scripts/repo-updater/diff-debug.sh
#
# Both processes serve the same Postgres, so:
#  - /manual-purge validation responses must match status, content type and
#    body byte for byte, for GET query params and urlencoded POST bodies;
#  - /repo-updater-state?format=json must use the same JSON shape (field
#    order, `null` for nil slices, no trailing newline), and the
#    database-derived SyncJobs entries must be identical for common IDs.
# Process-local content (UpdateQueue/Schedule) is only checked for shape.
#
# Go listens on SRC_PROF_HTTP=127.0.0.1:6064 in the reference environment,
# Rust on 127.0.0.1:6166; override with GO_DEBUG / RUST_DEBUG.
#
# Debug endpoints are not the traffic path: this gate complements
# scripts/repo-updater/diff.sh (gRPC) and diff-metrics.sh (Prometheus).

set -euo pipefail

GO=${GO_DEBUG:-http://127.0.0.1:6064}
RUST=${RUST_DEBUG:-http://127.0.0.1:6166}
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

fail=0

# --- /manual-purge validation matrix -------------------------------------
echo "manual-purge validation (status/content-type/body)"
for query in \
    '' 'limit=' 'limit=abc' 'limit=-1' 'limit=0' 'limit=10001' \
    'limit=1&perSecond=abc' 'limit=1&perSecond=0.1' 'limit=1&perSecond=0.05'
do
    go_out=$(curl -s -o "$TMP/g.body" -w '%{http_code}|%{content_type}' "$GO/manual-purge?$query")
    rust_out=$(curl -s -o "$TMP/r.body" -w '%{http_code}|%{content_type}' "$RUST/manual-purge?$query")
    if [[ "$go_out" == "$rust_out" ]] && cmp -s "$TMP/g.body" "$TMP/r.body"; then
        echo "  ok ?$query -> $go_out $(cat "$TMP/g.body")"
    else
        echo "  FAIL ?$query go=$go_out rust=$rust_out"
        echo "    go  : $(cat "$TMP/g.body")"
        echo "    rust: $(cat "$TMP/r.body")"
        fail=1
    fi
done

echo "manual-purge POST form body"
for body in 'limit=abc' 'limit=0&perSecond=x'; do
    go_out=$(curl -s -o "$TMP/g.body" -w '%{http_code}|%{content_type}' \
        -X POST -H 'Content-Type: application/x-www-form-urlencoded' --data "$body" "$GO/manual-purge")
    rust_out=$(curl -s -o "$TMP/r.body" -w '%{http_code}|%{content_type}' \
        -X POST -H 'Content-Type: application/x-www-form-urlencoded' --data "$body" "$RUST/manual-purge")
    if [[ "$go_out" == "$rust_out" ]] && cmp -s "$TMP/g.body" "$TMP/r.body"; then
        echo "  ok POST $body -> $go_out $(cat "$TMP/g.body")"
    else
        echo "  FAIL POST $body go=$go_out rust=$rust_out"
        echo "    go  : $(cat "$TMP/g.body")"
        echo "    rust: $(cat "$TMP/r.body")"
        fail=1
    fi
done

# `http.Error` sets nosniff; `http.NotFound` is the same helper.
for path in manual-purge 'nope'; do
    go_nosniff=$(curl -s -D- -o /dev/null "$GO/$path" | grep -ci '^x-content-type-options: nosniff' || true)
    rust_nosniff=$(curl -s -D- -o /dev/null "$RUST/$path" | grep -ci '^x-content-type-options: nosniff' || true)
    if [[ "$go_nosniff" == "$rust_nosniff" ]]; then
        echo "  ok /$path nosniff=$go_nosniff"
    else
        echo "  FAIL /$path nosniff go=$go_nosniff rust=$rust_nosniff"
        fail=1
    fi
done

# --- /repo-updater-state shape -------------------------------------------
echo "repo-updater-state JSON shape"
for query in 'dumper=nope&format=json' 'format=json'; do
    curl -fsS "$GO/repo-updater-state?$query" > "$TMP/go.json"
    curl -fsS "$RUST/repo-updater-state?$query" > "$TMP/rust.json"

    if [[ "$query" == dumper=nope* ]]; then
        if cmp -s "$TMP/go.json" "$TMP/rust.json"; then
            echo "  ok ?$query -> $(cat "$TMP/go.json")"
        else
            echo "  FAIL ?$query go=$(cat "$TMP/go.json") rust=$(cat "$TMP/rust.json")"
            fail=1
        fi
        continue
    fi

    go_key_order=$(jq -r '.[0] | to_entries | map(.key) | join(",")' "$TMP/go.json")
    rust_key_order=$(jq -r '.[0] | to_entries | map(.key) | join(",")' "$TMP/rust.json")
    if [[ "$go_key_order" == "$rust_key_order" ]]; then
        echo "  ok top-level field order: $go_key_order"
    else
        echo "  FAIL top-level field order go=$go_key_order rust=$rust_key_order"
        fail=1
    fi

    for entry in '.[0].Schedule[0]' '.[0].Schedule[0].Repo' '.[0].SyncJobs[0]'; do
        go_order=$(jq -r "($entry // {}) | to_entries | map(.key) | join(\",\")" "$TMP/go.json")
        rust_order=$(jq -r "($entry // {}) | to_entries | map(.key) | join(\",\")" "$TMP/rust.json")
        if [[ "$go_order" == "$rust_order" ]]; then
            echo "  ok $entry field order"
        else
            echo "  FAIL $entry field order go=$go_order rust=$rust_order"
            fail=1
        fi
    done

    # `json.MarshalIndent` writes no trailing newline.
    for side in go rust; do
        if [[ "$(tail -c1 "$TMP/$side.json" | od -An -c | tr -d ' ')" == '\n' ]]; then
            echo "  FAIL $side JSON has a trailing newline"
            fail=1
        fi
    done

    go_jobs=$(jq -r '(.[0].SyncJobs // []) | length' "$TMP/go.json")
    rust_jobs=$(jq -r '(.[0].SyncJobs // []) | length' "$TMP/rust.json")
    echo "  SyncJobs rows go=$go_jobs rust=$rust_jobs (new jobs may appear between the two requests)"

    mismatches=$(jq -n --slurpfile g "$TMP/go.json" --slurpfile r "$TMP/rust.json" '
        ($g[0][0].SyncJobs // []) as $gs
        | ($r[0][0].SyncJobs // []) as $rs
        | ($gs | map({key: (.ID | tostring), value: .}) | from_entries) as $gm
        | ($rs | map({key: (.ID | tostring), value: .}) | from_entries) as $rm
        | [ $gm | keys[] as $k
            | select($rm[$k] != null)
            | select($gm[$k] != $rm[$k])
            | {id: $k, go: $gm[$k], rust: $rm[$k]} ]
        | length')
    if [[ "$mismatches" == 0 ]]; then
        echo "  ok common SyncJobs entries are identical"
    else
        echo "  FAIL $mismatches common SyncJobs entries differ"
        jq -n --slurpfile g "$TMP/go.json" --slurpfile r "$TMP/rust.json" '
            ($g[0][0].SyncJobs // []) as $gs
            | ($r[0][0].SyncJobs // []) as $rs
            | ($gs | map({key: (.ID | tostring), value: .}) | from_entries) as $gm
            | ($rs | map({key: (.ID | tostring), value: .}) | from_entries) as $rm
            | [ $gm | keys[] as $k
                | select($rm[$k] != null)
                | select($gm[$k] != $rm[$k])
                | {id: $k, go: $gm[$k], rust: $rm[$k]} ]'
        fail=1
    fi
done

if [[ $fail -ne 0 ]]; then
    echo "DEBUG_HTTP_DIFF_FAILED"
    exit 1
fi
echo "DEBUG_HTTP_DIFF_OK"
