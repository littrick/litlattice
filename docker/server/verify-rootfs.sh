#!/usr/bin/env bash
# M9.49a: assert the `sourcegraph/server` rootfs layout without a docker daemon.
#
# Checks the staged tree produced by `build-rootfs.sh` against the Go image
# contract (`cmd/server/BUILD.bazel`, `cmd/server/image_test.yaml`,
# `wolfi-images/server.yaml`) and parses the Dockerfile for the metadata that is
# not visible in a plain directory tree: ENTRYPOINT, ENV and WORKDIR.
#
# Exit status is non-zero if any assertion fails. Output ends with
# `SERVER_IMAGE_ROOTFS_OK (N assertions)` on success.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
STAGE=""
DOCKERFILE=$SCRIPT_DIR/Dockerfile
WITH_EXTERNAL=0
STRICT_OWNER=0

usage() {
  cat <<'EOF'
Usage: verify-rootfs.sh --stage DIR [options]

Options:
  --stage DIR         Required. Rootfs tree produced by build-rootfs.sh.
  --dockerfile PATH   Dockerfile to check for entrypoint/env/workdir/users.
  --with-external     Also run `th --version` smoke tests for the available
                      zoekt binaries (`test_zoekt.sh` semantics).
  --strict-owner      Require the staged uid/gid to equal the expected image
                      owner. Use after `build-rootfs.sh --chown` (root).
  -h, --help          Show this help.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --stage) STAGE=${2:-}; shift 2 ;;
    --dockerfile) DOCKERFILE=${2:-}; shift 2 ;;
    --with-external) WITH_EXTERNAL=1; shift ;;
    --strict-owner) STRICT_OWNER=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "verify-rootfs: unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[ -n "$STAGE" ] || { usage >&2; exit 2; }
[ -d "$STAGE" ] || { echo "verify-rootfs: not a directory: $STAGE" >&2; exit 1; }
[ -f "$DOCKERFILE" ] || { echo "verify-rootfs: missing Dockerfile: $DOCKERFILE" >&2; exit 1; }
STAGE=$(cd -- "$STAGE" && pwd)

COUNT=0
FAILED=0
BUILDER_UID=$(id -u)
BUILDER_GID=$(id -g)
ENV_GAPS=()

ok() { COUNT=$((COUNT + 1)); printf 'ok %d - %s\n' "$COUNT" "$1"; }
bad() { COUNT=$((COUNT + 1)); FAILED=1; printf 'not ok %d - %s\n' "$COUNT" "$1"; }

check_exists() { # desc path
  if [ -e "$2" ]; then ok "$1"; else bad "$1 (missing $2)"; fi
}
check_absent() { # desc path
  if [ ! -e "$2" ]; then ok "$1"; else bad "$1 (unexpected $2)"; fi
}
check_exec() { # desc path
  if [ -x "$2" ]; then ok "$1"; else bad "$1 (not executable: $2)"; fi
}
check_mode() { # desc path expected
  local actual
  actual=$(stat -c '%a' "$2" 2>/dev/null || true)
  if [ "$actual" = "$3" ]; then ok "$1"; else bad "$1 (mode want $3 got ${actual:-missing})"; fi
}
check_content() { # desc path expected
  local actual
  actual=$(cat "$2" 2>/dev/null || true)
  if [ "$actual" = "$3" ]; then ok "$1"; else bad "$1 (content want [$3] got [$actual])"; fi
}
check_dir() { # desc path
  if [ -d "$2" ]; then ok "$1"; else bad "$1 (not a directory: $2)"; fi
}
check_grep() { # desc file fixed-string
  if grep -qF -- "$3" "$2" 2>/dev/null; then ok "$1"; else bad "$1 (missing [$3] in $2)"; fi
}
check_grep_re() { # desc file regex
  if grep -qE -- "$3" "$2" 2>/dev/null; then ok "$1"; else bad "$1 (missing /$3/ in $2)"; fi
}

echo "# rootfs: $STAGE"
echo "# dockerfile: $DOCKERFILE"

# ---------------------------------------------------------------------------
# Binary install paths.
# ---------------------------------------------------------------------------
check_exists '/server exists' "$STAGE/server"
check_exec '/server is executable' "$STAGE/server"
check_mode '/server mode 0755' "$STAGE/server" 755
check_absent '/usr/local/bin/server is not installed' "$STAGE/usr/local/bin/server"

CORE_BINS=(frontend gitserver searcher symbols worker repo-updater migrator precise-code-intel-worker embeddings)
for name in "${CORE_BINS[@]}"; do
  check_exists "DEPS binary /usr/local/bin/$name" "$STAGE/usr/local/bin/$name"
  check_exec "DEPS binary $name is executable" "$STAGE/usr/local/bin/$name"
done

missing_file=$STAGE/var/lib/sg-rootfs/missing-external.txt
EXTERNAL_BINS=(zoekt-archive-index zoekt-git-index zoekt-sourcegraph-indexserver zoekt-webserver syntect_server scip-ctags prom-wrapper)
for name in "${EXTERNAL_BINS[@]}"; do
  case "$name" in
    prom-wrapper) target=$STAGE/bin/prom-wrapper ;;
    *) target=$STAGE/usr/local/bin/$name ;;
  esac
  if [ -e "$target" ]; then
    check_exec "external binary $name is executable" "$target"
  elif grep -qxF -- "$name" "$missing_file" 2>/dev/null; then
    ENV_GAPS+=("$name")
    ok "external binary $name recorded as ENV_GAP (not fabricated)"
  else
    bad "external binary $name missing and not recorded in $missing_file"
  fi
done

# Shell helpers and image tests.
for path in postgres-optimize.sh reindex.sh; do
  check_exec "/$path is executable" "$STAGE/$path"
done
for script in test_services.sh test_packages.sh test_zoekt.sh; do
  check_exists "image test /opt/image_tests/$script" "$STAGE/opt/image_tests/$script"
  check_exec "image test $script is executable" "$STAGE/opt/image_tests/$script"
done

# ---------------------------------------------------------------------------
# Version stamps and web assets.
# ---------------------------------------------------------------------------
check_exists '/version.txt exists' "$STAGE/version.txt"
check_content '/version.txt default version' "$STAGE/version.txt" '0.0.0+dev'
check_exists '/timestamp.txt exists' "$STAGE/timestamp.txt"
if [ -e "$STAGE/timestamp.txt" ] && [ -s "$STAGE/timestamp.txt" ]; then
  bad '/timestamp.txt is empty by default'
else
  ok '/timestamp.txt is empty by default'
fi
check_exists '/assets-dist/web.manifest.json exists' "$STAGE/assets-dist/web.manifest.json"
if command -v jq >/dev/null 2>&1; then
  if jq -e '. == {}' "$STAGE/assets-dist/web.manifest.json" >/dev/null 2>&1; then
    ok '/assets-dist/web.manifest.json is the {} dummy manifest'
  else
    bad '/assets-dist/web.manifest.json is not the {} dummy manifest'
  fi
else
  check_content '/assets-dist/web.manifest.json content' "$STAGE/assets-dist/web.manifest.json" '{}'
fi
check_mode '/assets-dist/web.manifest.json mode 0655' "$STAGE/assets-dist/web.manifest.json" 655

# Monitoring file-existence contract (KNOWN_DIFF: placeholder contents).
check_exec '/prometheus.sh is executable' "$STAGE/prometheus.sh"
check_exec '/alertmanager.sh is executable' "$STAGE/alertmanager.sh"
for file in prometheus.yml alertmanager.yml; do
  check_exists "/sg_config_prometheus/$file exists" "$STAGE/sg_config_prometheus/$file"
done
check_exists '/postgres_exporter.yaml exists' "$STAGE/postgres_exporter.yaml"

# ---------------------------------------------------------------------------
# Static directories (wolfi-images/server.yaml:79-88).
# ---------------------------------------------------------------------------
for dir in var/lib/nginx/tmp var/run sg_grafana_additional_dashboards; do
  check_dir "/$dir is a directory" "$STAGE/$dir"
  check_mode "/$dir mode 0755" "$STAGE/$dir" 755
done

# ---------------------------------------------------------------------------
# Users/groups (image_test.yaml:49-60).
# ---------------------------------------------------------------------------
for entry in \
  'sourcegraph:x:100:101' \
  'postgres:x:70:70' \
  'nginx:x:101:102' \
  'redis:x:102:103' \
  'grafana:x:103:104'; do
  check_grep "staged /etc/passwd has $entry" "$STAGE/etc/passwd" "$entry"
  check_grep "Dockerfile creates user $entry" "$DOCKERFILE" "${entry%%:*}"
done
for entry in 'sourcegraph:x:101' 'postgres:x:70' 'nginx:x:102' 'redis:x:103' 'grafana:x:104'; do
  check_grep "staged /etc/group has $entry" "$STAGE/etc/group" "$entry"
done

# ---------------------------------------------------------------------------
# Owner metadata. `build-rootfs.sh --chown` (root) must match exactly; a
# non-root staging tree is accepted when it is owned by the builder, and the
# Dockerfile COPY --chown lines below assert the image ownership instead.
# ---------------------------------------------------------------------------
ownership=$STAGE/var/lib/sg-rootfs/ownership.tsv
if [ -f "$ownership" ]; then
  while IFS=$'\t' read -r path expected_uid expected_gid; do
    [ -n "${path:-}" ] || continue
    actual=$(stat -c '%u:%g' "$STAGE/$path" 2>/dev/null || true)
    if [ -z "$actual" ]; then
      if grep -qxF -- "$(basename "$path")" "$missing_file" 2>/dev/null; then
        ok "owner of $path skipped (ENV_GAP missing external)"
      else
        bad "owner of $path (path missing)"
      fi
    elif [ "$actual" = "$expected_uid:$expected_gid" ]; then
      ok "owner of $path is $expected_uid:$expected_gid"
    elif [ "$STRICT_OWNER" = 0 ] && [ "$actual" = "$BUILDER_UID:$BUILDER_GID" ]; then
      ok "owner of $path is builder $actual (image owner $expected_uid:$expected_gid via COPY --chown)"
    else
      bad "owner of $path (want $expected_uid:$expected_gid got $actual)"
    fi
  done <"$ownership"
else
  bad "missing $ownership (run build-rootfs.sh)"
fi

# ---------------------------------------------------------------------------
# Packaging description metadata: entrypoint/env/workdir and ownership.
# ---------------------------------------------------------------------------
check_grep 'Dockerfile ENTRYPOINT' "$DOCKERFILE" 'ENTRYPOINT ["/sbin/tini", "--", "/server"]'
check_grep 'Dockerfile GO111MODULES=on' "$DOCKERFILE" 'GO111MODULES=on'
check_grep 'Dockerfile LANG=en_US.utf8' "$DOCKERFILE" 'LANG=en_US.utf8'
check_grep 'Dockerfile LC_ALL=en_US.utf8' "$DOCKERFILE" 'LC_ALL=en_US.utf8'
check_grep_re 'Dockerfile WORKDIR /' "$DOCKERFILE" '^WORKDIR /$'
check_grep 'Dockerfile installs /server' "$DOCKERFILE" 'rootfs/server /server'
check_grep 'Dockerfile installs DEPS to /usr/local/bin' "$DOCKERFILE" 'rootfs/usr/local/bin/ /usr/local/bin/'
check_grep 'Dockerfile installs prom-wrapper to /bin' "$DOCKERFILE" 'rootfs/bin/prom-wrapper /bin/prom-wrapper'
check_grep 'Dockerfile installs image tests' "$DOCKERFILE" 'rootfs/opt/image_tests/ /opt/image_tests/'
check_grep 'Dockerfile chowns /server to 100:101' "$DOCKERFILE" 'COPY --chown=100:101 rootfs/server /server'
check_grep 'Dockerfile chowns binaries to 100:101' "$DOCKERFILE" 'COPY --chown=100:101 rootfs/usr/local/bin/ /usr/local/bin/'
check_grep 'Dockerfile creates sourcegraph group 101' "$DOCKERFILE" 'addgroup -g 101 sourcegraph'
check_grep 'Dockerfile creates postgres user 70' "$DOCKERFILE" 'adduser -D -u 70 -G postgres'
check_grep 'Dockerfile creates nginx user 101' "$DOCKERFILE" 'adduser -D -u 101 -G nginx'
check_grep 'Dockerfile creates redis user 102' "$DOCKERFILE" 'adduser -D -u 102 -G redis'
check_grep 'Dockerfile creates grafana user 103' "$DOCKERFILE" 'adduser -D -u 103 -G grafana'

# ---------------------------------------------------------------------------
# Optional external smoke tests (`test_zoekt.sh` only runs available binaries).
# ---------------------------------------------------------------------------
if [ "$WITH_EXTERNAL" = 1 ]; then
  for name in zoekt-webserver zoekt-git-index; do
    binary=$STAGE/usr/local/bin/$name
    if [ -x "$binary" ]; then
      if "$binary" --version >/dev/null 2>&1; then
        ok "$name --version runs"
      else
        bad "$name --version failed"
      fi
    else
      ENV_GAPS+=("$name")
      ok "$name --version skipped (ENV_GAP)"
    fi
  done
fi

if [ "$FAILED" -ne 0 ]; then
  echo "SERVER_IMAGE_ROOTFS_FAILED ($COUNT assertions, ENV_GAP: ${ENV_GAPS[*]:-none})"
  exit 1
fi
if [ "${#ENV_GAPS[@]}" -gt 0 ]; then
  echo "# ENV_GAP missing external binaries: ${ENV_GAPS[*]}"
fi
echo "SERVER_IMAGE_ROOTFS_OK ($COUNT assertions)"
