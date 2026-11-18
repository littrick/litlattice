#!/usr/bin/env bash
# Standalone `sourcegraph/server` (Rust) filesystem packager.
#
# Produces a root filesystem tarball that `docker import` accepts — no
# Dockerfile and no prebuilt artifacts are required. Everything external is
# fetched or built at packaging time:
#
#   * Sourcegraph source (pinned commit) — web bundle + Go sidecars
#   * Rust service binaries — this repository
#   * syntect_server / scip-ctags — Sourcegraph's syntax-highlighter crate
#   * Ubuntu 24.04 + Postgres/Redis/nginx/git/... — debootstrap + apt
#
# Usage:
#   ./docker/package/package.sh                 # all stages
#   ./docker/package/package.sh --only frontend
#   ./docker/package/package.sh --from rootfs
#   sudo ./docker/package/package.sh            # rootfs stages need root
#
# Important environment:
#   BUILD_DIR        build workspace (default <repo>/.package-build)
#   PACKAGE_PROXY    http(s)/all proxy for network fetches (e.g. host:port)
#   SUDO             command prefix for root operations (default: sudo -n)
#   CARGO_PROFILE    release (default) or debug
#   JOBS             parallel build jobs
#
# The rootfs stages require root (debootstrap, chown, tar --numeric-owner).
# Non-root builds are fine for `fetch`, `frontend`, `rust`, `go`, `syntect`.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=versions.env
. "$SCRIPT_DIR/versions.env"
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)

BUILD_DIR=${BUILD_DIR:-$REPO_ROOT/.package-build}
SRC_DIR=${SRC_DIR:-$BUILD_DIR/sourcegraph}
DIST_DIR=${DIST_DIR:-$BUILD_DIR/web-dist}
RUST_BIN=${RUST_BIN:-$BUILD_DIR/rust-bin}
GO_BIN=${GO_BIN:-$BUILD_DIR/go-bin}
EXT_DIR=${EXT_DIR:-$BUILD_DIR/ext}
ROOTFS=${ROOTFS:-$BUILD_DIR/rootfs}
OUT_DIR=${OUT_DIR:-$BUILD_DIR/out}
RUST_TARGET_DIR=${RUST_TARGET_DIR:-$REPO_ROOT/target}
OUT_NAME=${OUT_NAME:-sourcegraph-server-rust-${SOURCEGRAPH_VERSION}-fs.tar.gz}
CARGO_PROFILE=${CARGO_PROFILE:-release}
JOBS=${JOBS:-}

if [ "$(id -u)" = 0 ]; then
    SUDO=${SUDO:-}
else
    SUDO=${SUDO:-sudo -n}
fi

log() { printf '== %s\n' "$*"; }
fail() { printf 'package: %s\n' "$*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }
run_root() {
    if [ -z "$SUDO" ]; then
        "$@"
        return
    fi
    if [ -n "${PACKAGE_PROXY:-}" ]; then
        $SUDO env http_proxy="http://$PACKAGE_PROXY" https_proxy="http://$PACKAGE_PROXY" \
            all_proxy="$PACKAGE_PROXY" "$@"
    else
        $SUDO "$@"
    fi
}

proxy_env() {
    if [ -n "${PACKAGE_PROXY:-}" ]; then
        export all_proxy="$PACKAGE_PROXY" ALL_PROXY="$PACKAGE_PROXY" \
               http_proxy="http://$PACKAGE_PROXY" \
               https_proxy="http://$PACKAGE_PROXY" \
               HTTP_PROXY="http://$PACKAGE_PROXY" \
               HTTPS_PROXY="http://$PACKAGE_PROXY" \
               npm_config_proxy="http://$PACKAGE_PROXY" \
               npm_config_https_proxy="http://$PACKAGE_PROXY"
    fi
}

usage() {
    sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

ONLY=""
FROM=""
CLEAN=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        --only) ONLY=${2:?--only needs a stage list}; shift 2 ;;
        --from) FROM=${2:?--from needs a stage}; shift 2 ;;
        --clean) CLEAN=1; shift ;;
        -h|--help) usage 0 ;;
        *) usage 2 ;;
    esac
done

want() {
    local stage=$1
    if [ -n "$ONLY" ]; then
        case ",$ONLY," in *",$stage,"*) return 0 ;; *) return 1 ;; esac
    fi
    if [ -n "$FROM" ]; then
        case "$FROM" in
            "$stage") return 0 ;;
            fetch) [ "$stage" != fetch ] && return 0 || return 1 ;;
            frontend) case "$stage" in rust|go|syntect|rootfs|package) return 0 ;; esac; return 1 ;;
            rust) case "$stage" in rust|go|syntect|rootfs|package) return 0 ;; esac; return 1 ;;
            go) case "$stage" in go|syntect|rootfs|package) return 0 ;; esac; return 1 ;;
            syntect) case "$stage" in syntect|rootfs|package) return 0 ;; esac; return 1 ;;
            rootfs) case "$stage" in rootfs|package) return 0 ;; esac; return 1 ;;
            *) return 1 ;;
        esac
    fi
    return 0
}

# ---------------------------------------------------------------------------
# 1. Sourcegraph source
# ---------------------------------------------------------------------------
stage_fetch() {
    log "sourcegraph source $SOURCEGRAPH_COMMIT -> $SRC_DIR"
    mkdir -p "$BUILD_DIR"
    if [ -d "$SRC_DIR/.git" ]; then
        git -C "$SRC_DIR" fetch --quiet origin "$SOURCEGRAPH_COMMIT" ||
            git -C "$SRC_DIR" fetch --quiet origin
    else
        rm -rf "$SRC_DIR"
        git clone --filter=blob:none "$SOURCEGRAPH_REPO" "$SRC_DIR"
    fi
    git -C "$SRC_DIR" checkout --quiet --force "$SOURCEGRAPH_COMMIT"
    [ "$(git -C "$SRC_DIR" rev-parse HEAD)" = "$SOURCEGRAPH_COMMIT" ] ||
        fail "sourcegraph source is not at $SOURCEGRAPH_COMMIT"
    [ -f "$SRC_DIR/pnpm-lock.yaml" ] || fail "sourcegraph source has no pnpm-lock.yaml"
}

# ---------------------------------------------------------------------------
# 2. Web bundle (source build)
# ---------------------------------------------------------------------------
stage_frontend() {
    log "building the web bundle from source"
    command -v node >/dev/null || fail "node is required (>= 20.8)"
    local node_major node_minor
    node_major=$(node -p 'process.versions.node.split(".")[0]')
    node_minor=$(node -p 'process.versions.node.split(".")[1]')
    if [ "$node_major" -lt 20 ] || { [ "$node_major" -eq 20 ] && [ "$node_minor" -lt 8 ]; }; then
        fail "node >= 20.8 is required (found $(node --version))"
    fi
    if ! command -v pnpm >/dev/null; then
        command -v corepack >/dev/null || fail "pnpm/corepack is required"
        corepack enable
        corepack prepare pnpm@8.9.2 --activate
    fi
    local jobs_env=()
    [ -n "$JOBS" ] && jobs_env=(--workspace-concurrency="$JOBS")
    (
        cd "$SRC_DIR"
        export CI=true NODE_OPTIONS="${NODE_OPTIONS:---max-old-space-size=8192}"
        pnpm install --frozen-lockfile "${jobs_env[@]}"
        pnpm run generate
        pnpm --filter @sourcegraph/web run build
    )
    [ -f "$SRC_DIR/client/web/dist/web.manifest.json" ] ||
        fail "web build did not produce client/web/dist/web.manifest.json"
    rm -rf "$DIST_DIR"
    mkdir -p "$DIST_DIR"
    cp -a "$SRC_DIR/client/web/dist/." "$DIST_DIR/"
    log "web bundle: $(find "$DIST_DIR" -type f | wc -l) files"
}

# ---------------------------------------------------------------------------
# 3. Rust service binaries (this repository)
# ---------------------------------------------------------------------------
stage_rust() {
    log "building Rust binaries ($CARGO_PROFILE)"
    local profile_flag=(--release)
    [ "$CARGO_PROFILE" = debug ] && profile_flag=()
    (
        cd "$REPO_ROOT"
        export CARGO_TARGET_DIR="$RUST_TARGET_DIR"
        if [ -n "$JOBS" ]; then
            CARGO_BUILD_JOBS="$JOBS" cargo build "${profile_flag[@]}" --bins
        else
            cargo build "${profile_flag[@]}" --bins
        fi
    )
    rm -rf "$RUST_BIN"
    mkdir -p "$RUST_BIN"
    local dir="$RUST_TARGET_DIR/$CARGO_PROFILE"
    for bin in server frontend gitserver searcher symbols worker repo-updater \
               migrator precise-code-intel-worker embeddings; do
        [ -x "$dir/$bin" ] || fail "missing Rust binary: $dir/$bin"
        install -m 0755 "$dir/$bin" "$RUST_BIN/$bin"
        # The workspace builds release with debug = 1; the shipped image does
        # not need line tables.
        strip --strip-debug "$RUST_BIN/$bin" 2>/dev/null || true
    done
}

# ---------------------------------------------------------------------------
# 4. Go sidecars (from the pinned Sourcegraph source)
# ---------------------------------------------------------------------------
stage_go() {
    log "building Go sidecars"
    command -v go >/dev/null || fail "go is required (1.22.x)"
    mkdir -p "$GO_BIN"
    (
        cd "$SRC_DIR"
        export CGO_ENABLED=0
        go build -trimpath -buildvcs=false -ldflags="-s -w" -o "$GO_BIN/migrator" ./cmd/migrator
        go build -trimpath -buildvcs=false -ldflags="-s -w" -o "$GO_BIN/blobstore" ./cmd/blobstore
        go build -trimpath -buildvcs=false -ldflags="-s -w" -o "$GO_BIN/prom-wrapper" \
            ./docker-images/prometheus/cmd/prom-wrapper
        # The zoekt binaries live in the pinned module dependency (cmd/server/BUILD.bazel ZOEKT_DEPS).
        for zoekt in zoekt-archive-index zoekt-git-index zoekt-sourcegraph-indexserver zoekt-webserver; do
            go build -trimpath -buildvcs=false -ldflags="-s -w" -o "$GO_BIN/$zoekt" \
                "github.com/sourcegraph/zoekt/cmd/$zoekt"
        done
    )
}

# ---------------------------------------------------------------------------
# 5. Syntax highlighter (syntect_server + scip-ctags)
# ---------------------------------------------------------------------------
stage_syntect() {
    log "building syntect_server and scip-ctags from source"
    local src="$SRC_DIR/docker-images/syntax-highlighter"
    [ -f "$src/Cargo.toml" ] || fail "missing syntax-highlighter source: $src"
    (
        cd "$src"
        cargo build --release --locked --bin syntect_server --bin scip-ctags
    )
    mkdir -p "$EXT_DIR"
    install -m 0755 "$src/target/release/syntect_server" "$EXT_DIR/syntect_server"
    install -m 0755 "$src/target/release/scip-ctags" "$EXT_DIR/scip-ctags"
    strip --strip-debug "$EXT_DIR/syntect_server" "$EXT_DIR/scip-ctags" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# 6. Ubuntu root filesystem + application staging
# ---------------------------------------------------------------------------
stage_rootfs() {
    log "creating the Ubuntu $UBUNTU_SUITE root filesystem"
    command -v debootstrap >/dev/null ||
        fail "debootstrap is required (apt-get install debootstrap)"
    [ "$(id -u)" = 0 ] || [ -n "$SUDO" ] || fail "rootfs staging needs root"
    local packages="bash,ca-certificates,curl,git,git-lfs,gosu,openssh-client,nginx,postgresql,postgresql-contrib,redis-server,prometheus-postgres-exporter,locales,tini,python3,procps"
    if [ "${REUSE_ROOTFS:-0}" = 1 ] && [ -x "$ROOTFS/bin/bash" ]; then
        log "reusing existing rootfs at $ROOTFS"
    else
        rm -rf "$ROOTFS"
        mkdir -p "$ROOTFS"
        run_root debootstrap --variant=minbase --components=main,universe \
            --include="$packages" "$UBUNTU_SUITE" "$ROOTFS" "$UBUNTU_MIRROR"
    fi

    log "configuring users, locale and runtime directories"
    run_root chroot "$ROOTFS" localedef -i en_US -f UTF-8 en_US.UTF-8
    run_root chroot "$ROOTFS" sh -c '
        set -e
        # Ubuntu 24.04 prometheus-postgres-exporter creates a prometheus
        # user/group at exactly the image sourcegraph ids (100:101).
        # Keep the exporter binary but free the ids first.
        if getent passwd prometheus >/dev/null 2>&1 &&
           [ "$(id -u prometheus)" = 100 ]; then
            userdel prometheus
        fi
        if getent group prometheus >/dev/null 2>&1 &&
           [ "$(getent group prometheus | cut -d: -f3)" = 101 ]; then
            groupdel prometheus
        fi
        getent group sourcegraph >/dev/null || groupadd -g 101 sourcegraph
        id -u sourcegraph >/dev/null 2>&1 ||
          useradd -u 100 -g sourcegraph -m -d /home/sourcegraph -s /usr/sbin/nologin sourcegraph
        getent group redis >/dev/null || groupadd -g 103 redis
        ln -sf /usr/bin/tini /sbin/tini
        ln -sf /usr/sbin/gosu /usr/local/bin/su-exec
        ln -sf /usr/bin/prometheus-postgres-exporter /usr/local/bin/postgres_exporter
        mkdir -p /var/lib/nginx/tmp /root/.cache /sg_grafana_additional_dashboards \
                 /etc/sourcegraph /assets-dist /run/postgresql \
                 /var/opt/sourcegraph/repos /var/opt/sourcegraph/cache \
                 /var/opt/sourcegraph/blobstore /var/opt/sourcegraph/redis \
                 /var/opt/sourcegraph/redis-cache /var/opt/sourcegraph/zoekt/index \
                 /opt/image_tests /opt/s3proxy
        chmod 0755 /var/lib/nginx/tmp
        chown -R postgres:postgres /run/postgresql
        chown -R sourcegraph:sourcegraph /assets-dist \
                 /var/opt/sourcegraph/repos /var/opt/sourcegraph/cache \
                 /var/opt/sourcegraph/blobstore /var/opt/sourcegraph/zoekt
        chown -R redis:redis /var/opt/sourcegraph/redis /var/opt/sourcegraph/redis-cache
        apt-get clean
        rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
    '

    log "staging application files"
    # Rust binaries.
    run_root install -m 0755 "$RUST_BIN/server" "$ROOTFS/server"
    for bin in frontend gitserver searcher symbols worker repo-updater migrator \
               precise-code-intel-worker embeddings; do
        run_root install -m 0755 "$RUST_BIN/$bin" "$ROOTFS/usr/local/bin/$bin"
    done
    # Go sidecars.
    run_root install -m 0755 "$GO_BIN/migrator" "$ROOTFS/usr/local/bin/migrator-go"
    run_root install -m 0755 "$GO_BIN/blobstore" "$ROOTFS/usr/local/bin/blobstore"
    for zoekt in zoekt-archive-index zoekt-git-index zoekt-sourcegraph-indexserver zoekt-webserver; do
        run_root install -m 0755 "$GO_BIN/$zoekt" "$ROOTFS/usr/local/bin/$zoekt"
    done
    run_root install -m 0755 "$GO_BIN/prom-wrapper" "$ROOTFS/bin/prom-wrapper"
    # Syntax highlighter.
    run_root install -m 0755 "$EXT_DIR/syntect_server" "$ROOTFS/usr/local/bin/syntect_server"
    run_root install -m 0755 "$EXT_DIR/scip-ctags" "$ROOTFS/usr/local/bin/scip-ctags"
    # blobstore wrapper expected by the Procfile (/opt/s3proxy/run-docker-container.sh).
    run_root sh -c "cat >'$ROOTFS/opt/s3proxy/run-docker-container.sh' <<'EOF'
#!/bin/sh
export BLOBSTORE_DATA_DIR=\"\${JCLOUDS_FILESYSTEM_BASEDIR:-/var/opt/sourcegraph/blobstore}\"
exec /usr/local/bin/blobstore
EOF
chmod 0755 '$ROOTFS/opt/s3proxy/run-docker-container.sh'"

    # Helper scripts and image tests come from the Sourcegraph source tree.
    run_root install -m 0755 "$REPO_ROOT/docker/server/rootfs/postgres-optimize.sh" \
        "$ROOTFS/postgres-optimize.sh"
    run_root install -m 0755 "$REPO_ROOT/docker/server/rootfs/reindex.sh" "$ROOTFS/reindex.sh"
    local test_script
    for test_script in "$SRC_DIR"/cmd/server/image_tests/*.sh; do
        [ -f "$test_script" ] || continue
        run_root install -m 0755 "$test_script" "$ROOTFS/opt/image_tests/$(basename "$test_script")"
    done

    # Version stamps.
    printf '%s' "$SOURCEGRAPH_VERSION" >"$BUILD_DIR/version.txt"
    : >"$BUILD_DIR/timestamp.txt"
    run_root install -m 0644 "$BUILD_DIR/version.txt" "$ROOTFS/version.txt"
    run_root install -m 0644 "$BUILD_DIR/timestamp.txt" "$ROOTFS/timestamp.txt"

    # Monitoring file-existence contract (observability stays disabled).
    run_root sh -c "printf '#!/bin/sh\nexec prometheus \"\$@\"\n' >'$ROOTFS/prometheus.sh'"
    run_root sh -c "printf '#!/bin/sh\nexec alertmanager \"\$@\"\n' >'$ROOTFS/alertmanager.sh'"
    run_root mkdir -p "$ROOTFS/sg_config_prometheus"
    run_root sh -c "printf 'global: {}\n' >'$ROOTFS/sg_config_prometheus/prometheus.yml'"
    run_root sh -c "printf 'route: {}\n' >'$ROOTFS/sg_config_prometheus/alertmanager.yml'"
    run_root sh -c ": >'$ROOTFS/postgres_exporter.yaml'"
    run_root chmod 0755 "$ROOTFS/prometheus.sh" "$ROOTFS/alertmanager.sh"

    # Web bundle (stage-assets enforces a real web.manifest.json). It writes
    # into the root-owned rootfs, so run it through the root prefix.
    run_root bash "$REPO_ROOT/docker/server/stage-assets.sh" --dist-dir "$DIST_DIR" "$ROOTFS"
    run_root chroot "$ROOTFS" chown -R sourcegraph:sourcegraph /assets-dist

    # The image serves the front door on nginx :7080 and runs as root.
    # Only regular files: /usr/local/bin also holds symlinks into the rootfs
    # (postgres_exporter, su-exec) that are dangling from the host namespace,
    # and chmod would follow them.
    run_root find "$ROOTFS/usr/local/bin" -maxdepth 1 -type f -exec chmod 0755 {} +
    run_root chmod 0755 "$ROOTFS/server" "$ROOTFS/bin/prom-wrapper"
    run_root chmod 0755 "$ROOTFS/postgres-optimize.sh" "$ROOTFS/reindex.sh"
    run_root touch "$ROOTFS/var/lib/nginx/tmp/.keep" "$ROOTFS/var/run/.keep"
    run_root chmod 0644 "$ROOTFS/var/lib/nginx/tmp/.keep" "$ROOTFS/var/run/.keep"
}

# ---------------------------------------------------------------------------
# 7. Filesystem tarball
# ---------------------------------------------------------------------------
stage_package() {
    log "writing $OUT_NAME"
    mkdir -p "$OUT_DIR"
    local tar="$OUT_DIR/$OUT_NAME"
    rm -f "$tar" "$tar.sha256"
    run_root tar --numeric-owner -C "$ROOTFS" -cpf - . | gzip -1 >"$tar"
    ( cd "$OUT_DIR" && sha256sum "$OUT_NAME" >"$OUT_NAME.sha256" )
    ls -lh "$tar" "$tar.sha256"
    log "import with: $SCRIPT_DIR/import-image.sh $tar <tag>"
}

# ---------------------------------------------------------------------------
# Drive
# ---------------------------------------------------------------------------
if [ "$CLEAN" = 1 ]; then
    log "removing $BUILD_DIR"
    run_root rm -rf "$BUILD_DIR"
fi
proxy_env
mkdir -p "$BUILD_DIR"
want fetch && stage_fetch
want frontend && stage_frontend
want rust && stage_rust
want go && stage_go
want syntect && stage_syntect
want rootfs && stage_rootfs
want package && stage_package
log "done"
