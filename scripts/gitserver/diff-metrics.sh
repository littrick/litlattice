#!/usr/bin/env bash
# Differential check for the gitserver debug `/metrics` surface (M9.2).
#
#   scripts/gitserver/diff-metrics.sh
#
# Both gitservers expose their Prometheus endpoint on `SRC_PROF_HTTP`:
#
#   Go:   test_ws/bin/gitserver   SRC_PROF_HTTP=127.0.0.1:6061
#   Rust: target/debug/gitserver  SRC_PROF_HTTP=127.0.0.1:6075
#
# Only the families the Rust server registers are compared: the Go process
# also exports families from packages it links but the read path never
# touches (client-side counters, `go_*`/`process_*`). Families whose children
# are created lazily by traffic (`exec_*`, `gitservice_*`, `search_*`) are
# checked structurally (type, help, label names, bucket boundaries) after the
# fixture and git-service suites have run against both servers. Values are
# compared only for the scrape-time disk gauges; counters legitimately differ
# because the two processes have served different amounts of traffic.

set -euo pipefail

GO="${GO:-http://127.0.0.1:6061}"
RUST="${RUST:-http://127.0.0.1:6075}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

failures=0

pass() { printf 'PASS %s\n' "$*"; }
fail() {
    printf 'FAIL %s\n' "$*" >&2
    failures=$((failures + 1))
}

for pair in "go:$GO" "rust:$RUST"; do
    name=${pair%%:*}
    base=${pair#*:}
    status=$(curl -s -o "$WORK/$name.metrics" -D "$WORK/$name.headers" -w '%{http_code}' "$base/metrics")
    if [ "$status" = 200 ]; then
        pass "$name /metrics (status 200)"
    else
        fail "$name /metrics: status $status"
    fi
    content_type=$(grep -i '^content-type:' "$WORK/$name.headers" | head -1 | cut -d' ' -f2- | tr -d '\r')
    printf '%s\n' "$content_type" >"$WORK/$name.content-type"
done

if [ "$(cat "$WORK/go.content-type")" = "$(cat "$WORK/rust.content-type")" ]; then
    pass "content-type: $(cat "$WORK/go.content-type")"
else
    fail "content-type Go='$(cat "$WORK/go.content-type")' Rust='$(cat "$WORK/rust.content-type")'"
fi

go_404=$(curl -s -o /dev/null -w '%{http_code}' "$GO/nope")
rust_404=$(curl -s -o /dev/null -w '%{http_code}' "$RUST/nope")
if [ "$go_404" = "$rust_404" ]; then
    pass "unknown path (status $go_404)"
else
    fail "unknown path: status Go=$go_404 Rust=$rust_404"
fi

set +e
python3 -I - "$WORK/go.metrics" "$WORK/rust.metrics" <<'EOF'
import sys

# Families the Rust server must expose from startup: plain gauges/counters/
# histograms (a series at 0) plus the scrape-time disk gauges.
EXPECTED = {
    "src_service_metadata",
    "src_gitserver_echo_duration_seconds",
    "src_gitserver_clone_queue",
    "src_gitserver_repo_cloned",
    "src_gitserver_repo_cloned_failed",
    "src_gitserver_repo_corrupted",
    "src_gitserver_repo_wrong_shard",
    "src_gitserver_repo_wrong_shard_deleted",
    "src_gitserver_repos_recloned",
    "src_gitserver_janitor_running",
    "src_gitserver_janitor_duration_seconds",
    "src_gitserver_non_existing_repos_removed",
    "src_gitserver_search_running",
    "src_gitserver_search_latency_seconds",
    "src_gitserver_exec_blocked_command_received",
    "src_gitserver_bad_commit_error",
    "src_gitserver_disk_space_available",
    "src_gitserver_disk_space_total",
    "src_disk_space_available_bytes",
    "src_disk_space_total_bytes",
}

# Families that only exist once the process has served matching traffic.
TRAFFIC = {
    "src_gitserver_exec_running",
    "src_gitserver_exec_duration_seconds",
    "src_gitserver_gitservice_running",
    "src_gitserver_gitservice_duration_seconds",
    "src_gitserver_search_duration_seconds",
}

# Gauges whose label set is compared verbatim.
DISK = {
    "src_gitserver_disk_space_available",
    "src_gitserver_disk_space_total",
    "src_disk_space_available_bytes",
    "src_disk_space_total_bytes",
}


def parse(path):
    families = {}
    current = None
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.rstrip("\n")
            if line.startswith("# HELP "):
                _, _, name, help_text = line.split(" ", 3)
                families.setdefault(name, {"help": help_text, "type": None, "samples": {}})
                families[name]["help"] = help_text
            elif line.startswith("# TYPE "):
                _, _, name, kind = line.split(" ", 3)
                families.setdefault(name, {"help": None, "type": None, "samples": {}})
                families[name]["type"] = kind
            elif line and not line.startswith("#"):
                name = line.split("{", 1)[0].split(" ", 1)[0]
                # Histograms expose _bucket/_sum/_count; attribute them to the
                # family they belong to when present.
                base = name
                for suffix in ("_bucket", "_sum", "_count"):
                    if base.endswith(suffix):
                        base = base[: -len(suffix)]
                        break
                if base in families:
                    families[base]["samples"][line] = True
    return families


go = parse(sys.argv[1])
rust = parse(sys.argv[2])

failures = 0


def fail(message):
    global failures
    failures += 1
    print(f"FAIL {message}", file=sys.stderr)


missing = EXPECTED - rust.keys()
if missing:
    fail(f"rust is missing families: {sorted(missing)}")
else:
    print(f"PASS rust exposes all {len(EXPECTED)} expected families")

extra = rust.keys() - EXPECTED - TRAFFIC
if extra:
    fail(f"rust exposes unexpected families: {sorted(extra)}")

absent = TRAFFIC - rust.keys()
if absent:
    print(f"SKIP traffic-only families not yet exercised: {sorted(absent)}")

compared = (EXPECTED | TRAFFIC) & rust.keys()
for name in sorted(compared):
    if name not in go:
        fail(f"{name}: not registered by the Go server")
        continue
    if rust[name]["type"] != go[name]["type"]:
        fail(f"{name}: type Rust={rust[name]['type']} Go={go[name]['type']}")
    elif rust[name]["help"] != go[name]["help"]:
        fail(f"{name}: help Rust={rust[name]['help']!r} Go={go[name]['help']!r}")
    else:
        print(f"PASS {name} ({rust[name]['type']}, help matches)")


def sample_key(sample):
    return sample.rsplit(" ", 1)[0]


def label_names(samples):
    names = set()
    for sample in samples:
        key = sample_key(sample)
        if "{" not in key:
            continue
        for pair in key.split("{", 1)[1].rstrip("}").split(","):
            names.add(pair.split("=", 1)[0])
    return names


def bucket_bounds(samples):
    bounds = set()
    for sample in samples:
        key = sample_key(sample)
        if "_bucket{" not in key:
            continue
        for pair in key.split("{", 1)[1].rstrip("}").split(","):
            if pair.startswith("le="):
                bounds.add(pair[3:])
    return bounds


for name in sorted(compared & go.keys()):
    rust_labels = label_names(rust[name]["samples"])
    go_labels = label_names(go[name]["samples"])
    if rust_labels != go_labels:
        fail(f"{name}: labels Rust={sorted(rust_labels)} Go={sorted(go_labels)}")
        continue
    if rust[name]["type"] == "histogram":
        rust_bounds = bucket_bounds(rust[name]["samples"])
        go_bounds = bucket_bounds(go[name]["samples"])
        if rust_bounds != go_bounds:
            fail(
                f"{name}: buckets Rust={sorted(rust_bounds)} "
                f"Go={sorted(go_bounds)}"
            )
            continue
        print(f"PASS {name} (labels {sorted(rust_labels)}, "
              f"{len(rust_bounds)} buckets match)")
    else:
        print(f"PASS {name} (labels {sorted(rust_labels)})")


def series(samples):
    parsed = {}
    for sample in samples:
        key, value = sample.rsplit(" ", 1)
        parsed[key] = float(value)
    return parsed


for name in sorted(DISK & rust.keys() & go.keys()):
    rust_series = series(rust[name]["samples"])
    go_series = series(go[name]["samples"])
    if set(rust_series) != set(go_series):
        fail(f"{name}: series Rust={sorted(rust_series)} Go={sorted(go_series)}")
        continue
    for key in rust_series:
        tolerance = max(abs(go_series[key]) * 0.01, 64 * 1024 * 1024)
        if abs(rust_series[key] - go_series[key]) > tolerance:
            fail(f"{name}: value{key} Rust={rust_series[key]} Go={go_series[key]}")
            continue
        print(f"PASS {name}{key} value ~{rust_series[key]:.0f}")

sys.exit(1 if failures else 0)
EOF
python_status=$?
set -e
if [ "$python_status" -ne 0 ]; then
    failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
    printf 'diff-metrics: %d failure(s)\n' "$failures" >&2
    exit 1
fi
printf 'diff-metrics: all checks match\n'
