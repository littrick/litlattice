#!/usr/bin/env bash
# M9.49a: stage a `sourcegraph/server` root filesystem without a docker daemon.
#
# The contract mirrors `cmd/server/BUILD.bazel` in the Go repository:
#   * `server`                         -> /server
#   * `DEPS` (9 services)              -> /usr/local/bin/<name>
#   * `ZOEKT_DEPS` (4)                 -> /usr/local/bin/<name>
#   * syntect_server, scip-ctags       -> /usr/local/bin/<name>
#   * prom-wrapper                     -> /bin/prom-wrapper
#   * postgres-optimize.sh             -> /postgres-optimize.sh
#   * reindex.sh                       -> /reindex.sh
#   * image tests                      -> /opt/image_tests/
#   * /version.txt, /timestamp.txt, /assets-dist/web.manifest.json
#   * /var/lib/nginx/tmp, /var/run, /sg_grafana_additional_dashboards
#
# The staged tree is consumed by `verify-rootfs.sh` and can also be used as the
# docker build context (`docker/server/Dockerfile` copies this tree). External
# Go binaries (zoekt/syntect/scip-ctags/prom-wrapper) are copied from
# `--external-dir` when present; missing ones are recorded in
# `<stage>/var/lib/sg-rootfs/missing-external.txt` as ENV_GAP instead of being
# fabricated.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)
CARGO_LOCK=${CARGO_LOCK:-/tmp/m98_cargo.lock}
CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$REPO_ROOT/target}

# `DEPS` from cmd/server/BUILD.bazel:88-98 (9 binaries).
CORE_BINS=(
  frontend
  gitserver
  searcher
  symbols
  worker
  repo-updater
  migrator
  precise-code-intel-worker
  embeddings
)
# `ZOEKT_DEPS` from cmd/server/BUILD.bazel:100-105 plus the syntax-highlighter
# tars. These are external Go binaries; they are not built from this workspace.
EXTERNAL_BINS=(
  zoekt-archive-index
  zoekt-git-index
  zoekt-sourcegraph-indexserver
  zoekt-webserver
  syntect_server
  scip-ctags
  prom-wrapper
)

STAGE=""
BIN_DIR=""
EXTERNAL_DIR=""
BUILD=0
PROFILE=debug
CHOWN=0

usage() {
  cat <<'EOF'
Usage: build-rootfs.sh --stage DIR [options]

Options:
  --stage DIR          Required. Directory to populate with the image rootfs.
  --bin-dir DIR        Directory containing the built Rust binaries
                       (server, frontend, gitserver, ...). Defaults to
                       $CARGO_TARGET_DIR/debug when --build is used.
  --external-dir DIR   Directory containing external binaries (zoekt-*,
                       syntect_server, scip-ctags, prom-wrapper). Missing
                       binaries are recorded as ENV_GAP.
  --build              Build the 11 Rust binaries first (under the shared
                       cargo flock). Uses --bin-dir as CARGO_TARGET_DIR root.
  --release            Use $CARGO_TARGET_DIR/release for --build.
  --chown              chown staged files/directories when running as root.
                       Without it the staging tree keeps the builder's owner;
                       the Dockerfile's COPY --chown supplies image ownership.
  -h, --help           Show this help.

The script never invokes docker and never claims to build or run an image.
EOF
}

fail() {
  echo "build-rootfs: $*" >&2
  exit 1
}

while [ $# -gt 0 ]; do
  case "$1" in
    --stage) STAGE=${2:-}; shift 2 ;;
    --bin-dir) BIN_DIR=${2:-}; shift 2 ;;
    --external-dir) EXTERNAL_DIR=${2:-}; shift 2 ;;
    --build) BUILD=1; shift ;;
    --release) PROFILE=release; shift ;;
    --chown) CHOWN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[ -n "$STAGE" ] || { usage >&2; fail "--stage is required"; }

if [ "$BUILD" = 1 ]; then
  command -v cargo >/dev/null 2>&1 || fail "cargo not found in PATH"
  packages=()
  for name in "${CORE_BINS[@]}" server; do
    case "$name" in
      precise-code-intel-worker) packages+=(-p sg-bin-precise-worker) ;;
      *) packages+=(-p "sg-bin-$name") ;;
    esac
  done
  echo "build-rootfs: building Rust binaries ($PROFILE) under $CARGO_LOCK" >&2
  # All cargo invocations share the workspace lock (see repo memory rule).
  flock -w 3600 "$CARGO_LOCK" -c \
    "cd '$REPO_ROOT' && CARGO_INCREMENTAL=0 CARGO_TARGET_DIR='$CARGO_TARGET_DIR' CARGO_PROFILE_DEV_DEBUG=0 cargo build ${packages[*]}"
  BIN_DIR=$CARGO_TARGET_DIR/$PROFILE
fi

[ -n "$BIN_DIR" ] || { usage >&2; fail "--bin-dir is required unless --build is used"; }
[ -d "$BIN_DIR" ] || fail "--bin-dir does not exist: $BIN_DIR"

mkdir -p "$STAGE"
STAGE=$(cd -- "$STAGE" && pwd)

install_file() { # src dst mode
  local src=$1 dst=$2 mode=$3
  [ -f "$src" ] || fail "missing source file: $src"
  mkdir -p "$(dirname "$dst")"
  cp "$src" "$dst"
  chmod "$mode" "$dst"
}

install_bin() { # src dst
  install_file "$1" "$2" 0755
}

write_file() { # path mode contents
  local path=$1 mode=$2 contents=$3
  mkdir -p "$(dirname "$path")"
  printf '%s' "$contents" >"$path"
  chmod "$mode" "$path"
}

# ---------------------------------------------------------------------------
# Directories (wolfi-images/server.yaml:79-88 plus runtime data dirs).
# ---------------------------------------------------------------------------
dirs=(
  usr/local/bin bin sbin
  opt/image_tests
  etc/sourcegraph
  assets-dist
  sg_config_prometheus sg_config_grafana sg_prometheus_add_ons sg_config_docs
  var/lib/nginx/tmp
  var/run
  sg_grafana_additional_dashboards
  var/opt/sourcegraph/repos
  var/opt/sourcegraph/cache
  var/opt/sourcegraph/blobstore
  var/opt/sourcegraph/redis
  var/opt/sourcegraph/redis-cache
  var/opt/sourcegraph/zoekt/index
  var/opt/sourcegraph/postgresql
  var/opt/sourcegraph/postgresql-markers
  var/lib/sg-rootfs
)
for dir in "${dirs[@]}"; do
  mkdir -p "$STAGE/$dir"
  chmod 0755 "$STAGE/$dir"
done
write_file "$STAGE/var/lib/nginx/tmp/.keep" 0644 'keep
'
write_file "$STAGE/var/run/.keep" 0644 'keep
'

# ---------------------------------------------------------------------------
# Rust binaries.
# ---------------------------------------------------------------------------
install_bin "$BIN_DIR/server" "$STAGE/server"
for name in "${CORE_BINS[@]}"; do
  install_bin "$BIN_DIR/$name" "$STAGE/usr/local/bin/$name"
done

# ---------------------------------------------------------------------------
# Checked-in helper scripts (verbatim copies of the Go image inputs).
# ---------------------------------------------------------------------------
install_bin "$SCRIPT_DIR/rootfs/postgres-optimize.sh" "$STAGE/postgres-optimize.sh"
install_bin "$SCRIPT_DIR/rootfs/reindex.sh" "$STAGE/reindex.sh"
for script in "$SCRIPT_DIR"/image_tests/*.sh; do
  install_bin "$script" "$STAGE/opt/image_tests/$(basename "$script")"
done

# ---------------------------------------------------------------------------
# External binaries: copy what is available, record the rest as ENV_GAP.
# ---------------------------------------------------------------------------
find_external() { # name -> prints path
  local name=$1 dir
  for dir in "$EXTERNAL_DIR" "$BIN_DIR"; do
    if [ -n "$dir" ] && [ -x "$dir/$name" ]; then
      printf '%s\n' "$dir/$name"
      return 0
    fi
  done
  return 1
}

: >"$STAGE/var/lib/sg-rootfs/missing-external.txt"
for name in "${EXTERNAL_BINS[@]}"; do
  if src=$(find_external "$name"); then
    case "$name" in
      prom-wrapper) install_bin "$src" "$STAGE/bin/prom-wrapper" ;;
      *) install_bin "$src" "$STAGE/usr/local/bin/$name" ;;
    esac
  else
    printf '%s\n' "$name" >>"$STAGE/var/lib/sg-rootfs/missing-external.txt"
    echo "build-rootfs: ENV_GAP external binary not found: $name" >&2
  fi
done

# ---------------------------------------------------------------------------
# Version stamps (`internal/version/BUILD.bazel:23-54` defaults).
#
# The upstream release image bakes the release version into the Go binary with
# ldflags; the Rust frontend reads `/version.txt` instead. Keep the default in
# step with the bundled web assets: a dev stamp makes `/help` (and other
# version-gated UI) behave like an unreleased build, e.g. redirecting to the
# local docs server at http://localhost:3000.
# ---------------------------------------------------------------------------
write_file "$STAGE/version.txt" 0644 "${SOURCEGRAPH_VERSION:-5.5.0}"
write_file "$STAGE/timestamp.txt" 0644 "${SOURCEGRAPH_BUILD_TIMESTAMP:-}"

# ---------------------------------------------------------------------------
# Web assets: no `client/` in this workspace (`ENV_GAP`), so ship the same
# dummy manifest as `no_client_bundle.image` (`tar_dummy_manifest`, mode 0655).
# ---------------------------------------------------------------------------
write_file "$STAGE/assets-dist/web.manifest.json" 0655 '{}'

# ---------------------------------------------------------------------------
# Minimal monitoring assets. The Go image uses generated monitoring configs;
# these are placeholders that satisfy the file-existence contract and are
# marked KNOWN_DIFF in verify-rootfs.sh.
# ---------------------------------------------------------------------------
write_file "$STAGE/prometheus.sh" 0755 '#!/bin/sh
exec prometheus "$@"
'
write_file "$STAGE/alertmanager.sh" 0755 '#!/bin/sh
exec alertmanager "$@"
'
write_file "$STAGE/sg_config_prometheus/prometheus.yml" 0777 'global: {}
'
write_file "$STAGE/sg_config_prometheus/alertmanager.yml" 0777 'route: {}
'
write_file "$STAGE/postgres_exporter.yaml" 0644 ''

# ---------------------------------------------------------------------------
# Users/groups for the staging root (the Dockerfile creates the same IDs with
# addgroup/adduser; verify-rootfs.sh checks both).
# ---------------------------------------------------------------------------
if [ ! -e "$STAGE/etc/passwd" ]; then
  write_file "$STAGE/etc/passwd" 0644 'root:x:0:0:root:/root:/bin/sh
sourcegraph:x:100:101:Sourcegraph:/home/sourcegraph:/bin/nologin
postgres:x:70:70:Postgres:/var/lib/postgresql:/bin/nologin
nginx:x:101:102:nginx:/var/lib/nginx:/bin/nologin
redis:x:102:103:redis:/var/lib/redis:/bin/nologin
grafana:x:103:104:grafana:/var/lib/grafana:/bin/nologin
'
fi
if [ ! -e "$STAGE/etc/group" ]; then
  write_file "$STAGE/etc/group" 0644 'root:x:0:
sourcegraph:x:101:
postgres:x:70:
nginx:x:102:
redis:x:103:
grafana:x:104:
'
fi

# ---------------------------------------------------------------------------
# Expected ownership metadata (used by verify-rootfs.sh; actual chown only when
# root and --chown are requested).
# ---------------------------------------------------------------------------
ownership=$STAGE/var/lib/sg-rootfs/ownership.tsv
: >"$ownership"
owner_rows=(
  "server	100	101"
  "usr/local/bin	100	101"
  "bin/prom-wrapper	0	0"
  "postgres-optimize.sh	0	0"
  "reindex.sh	0	0"
  "opt/image_tests	0	0"
  "version.txt	0	0"
  "timestamp.txt	0	0"
  "assets-dist	100	101"
  "var/opt/sourcegraph/repos	100	101"
  "var/opt/sourcegraph/cache	100	101"
  "var/opt/sourcegraph/blobstore	100	101"
  "var/opt/sourcegraph/zoekt/index	100	101"
  "var/opt/sourcegraph/redis	102	103"
  "var/opt/sourcegraph/redis-cache	102	103"
  "var/opt/sourcegraph/postgresql	70	70"
  "var/opt/sourcegraph/postgresql-markers	70	70"
)
printf '%s\n' "${owner_rows[@]}" >"$ownership"
chmod 0644 "$ownership"

if [ "$CHOWN" = 1 ]; then
  [ "$(id -u)" = 0 ] || fail "--chown requires root"
  while IFS=$'\t' read -r path uid gid; do
    chown -R "$uid:$gid" "$STAGE/$path"
  done <"$ownership"
fi

echo "SERVER_IMAGE_ROOTFS_STAGED $STAGE"
