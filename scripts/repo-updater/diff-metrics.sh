#!/usr/bin/env bash
# Live Go/Rust repo-updater /metrics differential (M9.6).
#
#   scripts/repo-updater/diff-metrics.sh
#
# Both processes scrape the same Postgres, so the six state gauges must agree
# (max_sync_backoff advances with wall clock; allow a 60s tolerance). The
# gitserver client families must match on HELP/TYPE; values are process-local
# counters. `src_repoupdater_*` families are compared in both directions:
# a Rust-only family must be declared in the Go source (set GO_SRC to the
# baseline checkout to check that; without it, Rust-only families fail).
#
# Reference environment: Go SRC_PROF_HTTP=127.0.0.1:6064, Rust
# SRC_PROF_HTTP=127.0.0.1:6166. Override with GO_METRICS / RUST_METRICS / GO_SRC.

set -euo pipefail

GO=${GO_METRICS:-http://127.0.0.1:6064/metrics}
RUST=${RUST_METRICS:-http://127.0.0.1:6166/metrics}
GO_SRC=${GO_SRC:-}
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

curl -fsS --max-time 10 "$GO" > "$TMP/go.txt"
curl -fsS --max-time 10 "$RUST" > "$TMP/rust.txt"

names() {
    grep -E '^# TYPE src_repoupdater' "$1" | awk '{print $3}' | sort
}
value() {
    awk -v family="$2" '$1 == family {print $2}' "$1"
}

fail=0
echo "state gauges (same database)"
for family in \
    external_services_total queued_sync_jobs_total completed_sync_jobs_total \
    errored_sync_jobs_percentage max_sync_backoff stale_repos
do
    name="src_repoupdater_$family"
    go_value=$(value "$TMP/go.txt" "$name")
    rust_value=$(value "$TMP/rust.txt" "$name")
    if [[ -z "$go_value" || -z "$rust_value" ]]; then
        echo "  FAIL $family go=${go_value:-<missing>} rust=${rust_value:-<missing>}"
        fail=1
        continue
    fi
    if [[ "$family" == max_sync_backoff ]]; then
        ok=$(awk -v a="$go_value" -v b="$rust_value" 'BEGIN { d = a - b; if (d < 0) d = -d; print (d < 60) ? "ok" : "mismatch" }')
    else
        ok=$([[ "$go_value" == "$rust_value" ]] && echo ok || echo mismatch)
    fi
    echo "  $ok $family go=$go_value rust=$rust_value"
    [[ "$ok" == ok ]] || fail=1
done

# Compare `# HELP`/`# TYPE` for one family. Lazy vectors (histograms and
# counters with labels) only render once a label value has been observed, so
# a family that is absent on the Rust side is informational for them; plain
# gauges/counters must exist on both sides.
# A lazy vector (histogram or counter with labels) only renders once a label
# value has been observed, so absence in one process is informational.
is_lazy() {
    case "$1" in
        *_duration_seconds|*_errors_total|*_client_total|*_store_total|*_syncer_total) return 0 ;;
        # `NewStoreMetrics` families (`src_repoupdater_store_*`,
        # `src_repocreater_store_*`) are empty-label vectors that only render
        # after the operation has been observed once.
        *_store_*) return 0 ;;
        *) return 1 ;;
    esac
}

check_family() {
    local family=$1 go_meta rust_meta
    go_meta=$(grep -E "^# (TYPE|HELP) $family " "$TMP/go.txt" | sort || true)
    rust_meta=$(grep -E "^# (TYPE|HELP) $family " "$TMP/rust.txt" | sort || true)
    if [[ -z "$go_meta" && -z "$rust_meta" ]]; then
        # Neither process has touched the family yet (empty vectors render
        # only after the first observation).
        echo "  info $family not observed by either process"
    elif [[ -z "$go_meta" ]]; then
        if is_lazy "$family"; then
            echo "  info $family observed by Rust only (Go has not used it since start)"
        else
            echo "  FAIL $family is exposed by Rust but not by Go"
            fail=1
        fi
    elif [[ -z "$rust_meta" ]]; then
        if is_lazy "$family"; then
            echo "  info $family not yet observed by Rust"
        else
            echo "  FAIL $family missing from Rust"
            fail=1
        fi
    elif [[ "$go_meta" == "$rust_meta" ]]; then
        echo "  ok $family"
    else
        echo "  FAIL $family metadata differs"
        diff <(echo "$go_meta") <(echo "$rust_meta") | sed 's/^/    /'
        fail=1
    fi
}

# `internal/gitserver/connection` and the repository-service RED metrics are
# registered by every process that imports the gitserver client.
echo "gitserver client family metadata"
for family in \
    src_gitserver_addr_for_repo_invoked \
    src_gitserver_client_concurrent_requests \
    src_gitserver_repositoryservice_client_duration_seconds \
    src_gitserver_repositoryservice_client_total \
    src_gitserver_repositoryservice_client_errors_total
do
    check_family "$family"
done

# `internal/repos/sync_worker.go` builds the worker metrics with
# `workerutil.NewMetrics` and the store metrics with the
# `repo_sync_worker_store` name; the resetter uses its own three counters.
echo "sync worker family metadata"
for family in \
    src_repo_updater_external_service_syncer_handlers \
    src_repo_updater_external_service_syncer_duration_seconds \
    src_repo_updater_external_service_syncer_total \
    src_repo_updater_external_service_syncer_errors_total \
    src_external_service_queue_resets_total \
    src_external_service_queue_max_resets_total \
    src_external_service_queue_reset_errors_total \
    src_workerutil_dbworker_store_repo_sync_worker_store_duration_seconds \
    src_workerutil_dbworker_store_repo_sync_worker_store_total \
    src_workerutil_dbworker_store_repo_sync_worker_store_errors_total
do
    check_family "$family"
done

# The op label values are the process-local operation names. Rust must only
# ever use operations the Go baseline declares.
echo "dbworker store op vocabulary"
go_ops=$(grep -oE '^src_workerutil_dbworker_store_repo_sync_worker_store_total\{op="[^"]+"' "$TMP/go.txt" \
    | sed -E 's/.*op="([^"]+)".*/\1/' | sort -u || true)
known_ops='AddExecutionLogEntry
CanceledJobs
CountByState
Dequeue
Exists
Heartbeat
MarkComplete
MarkErrored
MarkFailed
MaxDurationInQueue
Requeue
ResetStalled
UpdateExecutionLogEntry'
for op in $(grep -oE '^src_workerutil_dbworker_store_repo_sync_worker_store_total\{op="[^"]+"' "$TMP/rust.txt" \
    | sed -E 's/.*op="([^"]+)".*/\1/' | sort -u || true)
do
    if grep -qxF "$op" <<<"$known_ops"; then
        echo "  ok op=$op"
    else
        echo "  FAIL op=$op is not declared by the Go baseline"
        fail=1
    fi
done
echo "  info Go ops observed: ${go_ops:-<none>}"

# `internal/repos/observability.go` `NewStoreMetrics`: the syncer observes
# `Transact`, `Done` and the four repo-write operations. The create families
# use the baseline's `src_repocreater_` prefix and the delete-not-in families
# keep its `exernal` typo; both must match exactly.
echo "repos store family metadata"
for family in \
    src_repoupdater_store_transact_duration_seconds \
    src_repoupdater_store_transact_total \
    src_repoupdater_store_transact_errors_total \
    src_repoupdater_store_done_duration_seconds \
    src_repoupdater_store_done_total \
    src_repoupdater_store_done_errors_total \
    src_repocreater_store_create_external_service_repo_duration_seconds \
    src_repocreater_store_create_external_service_repo_total \
    src_repocreater_store_create_external_service_repo_errors_total \
    src_repoupdater_store_update_external_service_repo_duration_seconds \
    src_repoupdater_store_update_external_service_repo_total \
    src_repoupdater_store_update_external_service_repo_errors_total \
    src_repoupdater_store_delete_external_service_repo_duration_seconds \
    src_repoupdater_store_delete_external_service_repo_total \
    src_repoupdater_store_delete_external_service_repo_errors_total \
    src_repoupdater_store_delete_exernal_service_repos_not_in_duration_seconds \
    src_repoupdater_store_delete_exernal_service_repos_not_in_total \
    src_repoupdater_store_delete_exernal_service_repos_not_in_errors_total
do
    check_family "$family"
done

for family in \
    src_external_service_queue_resets_total \
    src_external_service_queue_max_resets_total \
    src_external_service_queue_reset_errors_total \
    src_repo_updater_external_service_syncer_handlers \
    src_repoupdater_store_transact_total \
    src_repoupdater_store_done_total \
    src_repocreater_store_create_external_service_repo_total \
    src_repoupdater_store_delete_exernal_service_repos_not_in_total
do
    echo "  info $family go=$(value "$TMP/go.txt" "$family") rust=$(value "$TMP/rust.txt" "$family") (process-local)"
done

names "$TMP/go.txt" > "$TMP/go.names"
names "$TMP/rust.txt" > "$TMP/rust.names"
echo "families only in Go (informational)"
sed 's/^/  /' <(comm -23 "$TMP/go.names" "$TMP/rust.names")
echo "families only in Rust (must all be declared in the Go source)"
unknown=0
if [[ -n "$GO_SRC" ]]; then
    grep -rhoE 'src_repoupdater_[a-z_]+' "$GO_SRC/cmd/repo-updater" "$GO_SRC/internal" \
        --include='*.go' 2>/dev/null | sort -u > "$TMP/go.src.names"
fi
while read -r family; do
    [[ -n "$family" ]] || continue
    if [[ -n "$GO_SRC" ]] && grep -qxF "$family" "$TMP/go.src.names"; then
        echo "  ok $family (declared in Go source, series only observed in Rust)"
    else
        echo "  FAIL $family is not declared anywhere in the Go baseline"
        unknown=1
    fi
done < <(comm -13 "$TMP/go.names" "$TMP/rust.names")
if [[ $unknown -ne 0 ]]; then
    fail=1
fi

if [[ $fail -ne 0 ]]; then
    echo "METRICS_DIFF_FAILED"
    exit 1
fi
echo "METRICS_DIFF_OK"
