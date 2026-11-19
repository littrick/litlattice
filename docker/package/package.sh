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
#   ./docker/package/package.sh --features observability
#   ./docker/package/package.sh --features docker
#   sudo ./docker/package/package.sh            # rootfs stages need root
#
# Optional, independent features (comma-separated, OFF by default):
#   observability  add Prometheus/Alertmanager/prom-wrapper, baseline configs,
#                  alert rules and the metrics target list; output names get a
#                  `-observability` suffix;
#   docker         after writing the rootfs tarball, import it as a Docker image
#                  and save an additional `<name>.docker.tar` artifact;
#   sveltekit      build and ship the SvelteKit web app (`_sk`) and expose the
#                  opt-in "Try a new, faster experience" toggle.
# Combine them as `--features observability,docker,sveltekit`.
#
# Important environment:
#   BUILD_DIR        build workspace (default <repo>/.package-build)
#   PACKAGE_PROXY    http(s)/all proxy for network fetches (e.g. host:port)
#   SUDO             command prefix for root operations (default: sudo -n)
#   CARGO_PROFILE    release (default) or debug
#   JOBS             parallel build jobs
#
# The web bundle needs Node 20.8–24. Sourcegraph 5.5 declares
# "node": "^v20.8.0", and the upper bound matters: Node 25 removed
# buffer.SlowBuffer, which its dependency tree (jwa ->
# buffer-equal-constant-time) still dereferences.
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
OUT_NAME=${OUT_NAME:-}
IMAGE_TAG=${IMAGE_TAG:-}
IMAGE_OUT_NAME=${IMAGE_OUT_NAME:-}
DOCKER_CMD=${DOCKER:-docker}
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

# Download Sourcegraph's patched universal-ctags (the symbols service spawns
# it for languages that scip-ctags does not cover, e.g. C/C++/Markdown).
fetch_universal_ctags() {
    local dest="$EXT_DIR/universal-ctags"
    [ -x "$dest" ] && return 0
    log "downloading universal-ctags $UNIVERSAL_CTAGS_VERSION"
    mkdir -p "$EXT_DIR"
    local tmp="$dest.tmp"
    rm -f "$tmp"
    curl -fsSL --retry 5 --retry-delay 2 -o "$tmp" "$UNIVERSAL_CTAGS_URL" ||
        fail "failed to download universal-ctags from $UNIVERSAL_CTAGS_URL"
    echo "$UNIVERSAL_CTAGS_SHA256  $tmp" | sha256sum -c - >/dev/null ||
        fail "universal-ctags checksum mismatch"
    chmod 0755 "$tmp"
    mv "$tmp" "$dest"
}

usage() {
    sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

ONLY=""
FROM=""
FEATURES=""
CLEAN=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        --only) ONLY=${2:?--only needs a stage list}; shift 2 ;;
        --from) FROM=${2:?--from needs a stage}; shift 2 ;;
        --features) FEATURES=${2:?--features needs a list}; shift 2 ;;
        --clean) CLEAN=1; shift ;;
        -h|--help) usage 0 ;;
        *) usage 2 ;;
    esac
done

# Comma-separated feature list, validated once.
FEATURES=${FEATURES// /}
for feature in ${FEATURES//,/ }; do
    case "$feature" in
        observability | docker | sveltekit) ;;
        *) fail "unknown feature: $feature (supported: observability, docker, sveltekit)" ;;
    esac
done

has_feature() {
    case ",$FEATURES," in *",$1,"*) return 0 ;; esac
    return 1
}

# Feature-specific output suffixes. Features are independent: `docker` adds an
# image artifact, `observability` changes the rootfs/image contents.
FEATURE_SUFFIX=""
if has_feature observability; then
    FEATURE_SUFFIX="$FEATURE_SUFFIX-observability"
fi
if has_feature sveltekit; then
    FEATURE_SUFFIX="$FEATURE_SUFFIX-sveltekit"
fi
OUT_NAME=${OUT_NAME:-sourcegraph-server-rust-${SOURCEGRAPH_VERSION}${FEATURE_SUFFIX}-fs.tar.gz}
IMAGE_TAG=${IMAGE_TAG:-sourcegraph/server:rust-${SOURCEGRAPH_VERSION}${FEATURE_SUFFIX}}
IMAGE_OUT_NAME=${IMAGE_OUT_NAME:-sourcegraph-server-rust-${SOURCEGRAPH_VERSION}${FEATURE_SUFFIX}.docker.tar}

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
    command -v node >/dev/null || fail "node 20.8–24 is required"
    local node_major node_minor node_patch
    node_major=$(node -p 'process.versions.node.split(".")[0]')
    node_minor=$(node -p 'process.versions.node.split(".")[1]')
    node_patch=$(node -p 'process.versions.node.split(".")[2]')
    # Sourcegraph's engines field pins "^v20.8.0". Do not relax the upper
    # bound: Node 25 removed buffer.SlowBuffer (Node 24.0.0 removed it by
    # accident; 24.0.1 restored it), and buffer-equal-constant-time@1.0.1,
    # pulled in by jwa/jws, dereferences it during `pnpm run generate`.
    if [ "$node_major" -lt 20 ] ||
       { [ "$node_major" -eq 20 ] && [ "$node_minor" -lt 8 ]; } ||
       [ "$node_major" -ge 25 ] ||
       { [ "$node_major" -eq 24 ] && [ "$node_minor" -eq 0 ] && [ "$node_patch" -lt 1 ]; }; then
        fail "node 20.8–24 is required (found $(node --version); buffer.SlowBuffer was removed in Node 25)"
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
        # The web esbuild config defaults NODE_ENV to "development" when it is
        # unset, which ships an unminified React dev build and leaves the
        # esbuild live-reload EventSource (/.assets/esbuild) in the bundle.
        # The Bazel production build sets NODE_ENV=production; do the same.
        NODE_ENV=production pnpm --filter @sourcegraph/web run build
        if has_feature sveltekit; then
            DEPLOY_TYPE=dev pnpm --filter @sourcegraph/web-sveltekit run build
        fi
    )
    if has_feature sveltekit; then
        [ -f "$SRC_DIR/client/web/dist/_sk/index.html" ] ||
            fail "sveltekit feature enabled but the build did not produce client/web/dist/_sk/index.html"
    else
        # The source tree is cached across builds; never leak a previous
        # SvelteKit build into a feature-off image.
        rm -rf "$SRC_DIR/client/web/dist/_sk"
    fi
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
    command -v cargo >/dev/null ||
        fail "cargo is required; run compile stages as your user (sudo resets PATH and drops ~/.cargo/bin)"
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
    local rust_bins="server frontend gitserver searcher symbols worker repo-updater
               migrator blobstore precise-code-intel-worker embeddings"
    if has_feature observability; then
        rust_bins="$rust_bins prom-wrapper"
    fi
    for bin in $rust_bins; do
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
    command -v cargo >/dev/null ||
        fail "cargo is required; run compile stages as your user (sudo resets PATH and drops ~/.cargo/bin)"
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
    fetch_universal_ctags
    local packages="bash,ca-certificates,curl,git,git-lfs,gosu,openssh-client,nginx,postgresql,postgresql-contrib,redis-server,locales,tini,python3,procps"
    if has_feature observability; then
        packages="$packages,prometheus,prometheus-alertmanager,prometheus-postgres-exporter"
    fi
    if [ "${REUSE_ROOTFS:-0}" = 1 ] && [ -x "$ROOTFS/bin/bash" ]; then
        # Feature sets change the installed packages; reusing a rootfs built
        # for the other set would silently ship the wrong image.
        if has_feature observability &&
           { [ ! -x "$ROOTFS/usr/bin/prometheus" ] ||
             [ ! -x "$ROOTFS/usr/bin/prometheus-postgres-exporter" ]; }; then
            fail "REUSE_ROOTFS=1 rootfs lacks observability packages; rebuild without reuse"
        fi
        if ! has_feature observability &&
           { [ -x "$ROOTFS/usr/bin/prometheus" ] ||
             [ -x "$ROOTFS/usr/bin/prometheus-postgres-exporter" ]; }; then
            fail "REUSE_ROOTFS=1 rootfs contains observability packages; rebuild without reuse"
        fi
        log "reusing existing rootfs at $ROOTFS"
    else
        run_root rm -rf "$ROOTFS"
        mkdir -p "$ROOTFS"
        run_root debootstrap --variant=minbase --components=main,universe \
            --include="$packages" "$UBUNTU_SUITE" "$ROOTFS" "$UBUNTU_MIRROR"
    fi

    log "configuring users, locale and runtime directories"
    run_root chroot "$ROOTFS" localedef -i en_US -f UTF-8 en_US.UTF-8
    run_root chroot "$ROOTFS" sh -c '
        set -e
        # The image contract fixes sourcegraph at 100:101. Which Ubuntu
        # packages occupy those ids depends on the feature set (redis,
        # ssl-cert, prometheus, ...), so relocate whatever owns them and
        # update the affected files.
        uid_owner=$(getent passwd 100 | cut -d: -f1 || true)
        if [ -n "$uid_owner" ] && [ "$uid_owner" != sourcegraph ]; then
            usermod -u 1999 "$uid_owner"
            find / -xdev -uid 100 -exec chown -h "$uid_owner" {} + 2>/dev/null || true
        fi
        gid_owner=$(getent group 101 | cut -d: -f1 || true)
        if [ -n "$gid_owner" ] && [ "$gid_owner" != sourcegraph ]; then
            groupmod -g 1999 "$gid_owner"
            find / -xdev -gid 101 -exec chgrp -h "$gid_owner" {} + 2>/dev/null || true
        fi
        getent group sourcegraph >/dev/null || groupadd -g 101 sourcegraph
        id -u sourcegraph >/dev/null 2>&1 ||
          useradd -u 100 -g sourcegraph -m -d /home/sourcegraph -s /usr/sbin/nologin sourcegraph
        getent group redis >/dev/null || groupadd -g 103 redis
        ln -sf /usr/bin/tini /sbin/tini
        ln -sf /usr/sbin/gosu /usr/local/bin/su-exec
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

    if has_feature observability; then
        run_root mkdir -p "$ROOTFS/var/opt/sourcegraph/prometheus"
        run_root chroot "$ROOTFS" chown -R sourcegraph:sourcegraph /var/opt/sourcegraph/prometheus
    fi

    log "staging application files"
    # Rust binaries.
    run_root install -m 0755 "$RUST_BIN/server" "$ROOTFS/server"
    for bin in frontend gitserver searcher symbols worker repo-updater migrator blobstore \
               precise-code-intel-worker embeddings; do
        run_root install -m 0755 "$RUST_BIN/$bin" "$ROOTFS/usr/local/bin/$bin"
    done
    # prom-wrapper is the observability helper installed at /bin, the path the
    # Procfile expects.
    if has_feature observability; then
        run_root install -m 0755 "$RUST_BIN/prom-wrapper" "$ROOTFS/bin/prom-wrapper"
    else
        run_root rm -f "$ROOTFS/bin/prom-wrapper"
    fi
    # Go sidecars. A reused rootfs may still carry binaries replaced by Rust
    # implementations (migrator, blobstore); remove those stale copies.
    run_root rm -f "$ROOTFS/usr/local/bin/migrator-go"
    for zoekt in zoekt-archive-index zoekt-git-index zoekt-sourcegraph-indexserver zoekt-webserver; do
        run_root install -m 0755 "$GO_BIN/$zoekt" "$ROOTFS/usr/local/bin/$zoekt"
    done
    # Syntax highlighter.
    run_root install -m 0755 "$EXT_DIR/syntect_server" "$ROOTFS/usr/local/bin/syntect_server"
    run_root install -m 0755 "$EXT_DIR/scip-ctags" "$ROOTFS/usr/local/bin/scip-ctags"
    # `type:symbol` search for languages outside scip-ctags' allowlist (C/C++,
    # Markdown, ...) spawns `universal-ctags` from PATH.
    run_root install -m 0755 "$EXT_DIR/universal-ctags" \
        "$ROOTFS/usr/local/bin/universal-ctags"
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

    if has_feature observability; then
        # Observability: prom-wrapper starts these scripts. They come from the
        # pinned Sourcegraph source and expect `prometheus`/`alertmanager` on PATH
        # (Ubuntu's Alertmanager binary is `prometheus-alertmanager`).
        run_root install -m 0755 "$SRC_DIR/docker-images/prometheus/prometheus.sh" \
            "$ROOTFS/prometheus.sh"
        run_root install -m 0755 "$SRC_DIR/docker-images/prometheus/alertmanager.sh" \
            "$ROOTFS/alertmanager.sh"
        run_root ln -sf /usr/bin/prometheus-alertmanager "$ROOTFS/usr/bin/alertmanager"
        run_root mkdir -p "$ROOTFS/sg_config_prometheus" "$ROOTFS/sg_prometheus_add_ons"
        # Baseline single-container configs + generated rule/target files.
        run_root cp "$SRC_DIR/docker-images/prometheus/config/prometheus.yml" \
            "$ROOTFS/sg_config_prometheus/prometheus.yml"
        run_root cp "$SRC_DIR/docker-images/prometheus/config/alertmanager.yml" \
            "$ROOTFS/sg_config_prometheus/alertmanager.yml"
        run_root cp "$REPO_ROOT/docker/package/monitoring/"*_alert_rules.yml \
            "$ROOTFS/sg_config_prometheus/"
        run_root cp "$REPO_ROOT/docker/package/monitoring/prometheus_targets.yml" \
            "$ROOTFS/sg_prometheus_add_ons/prometheus_targets.yml"
        run_root ln -sf /usr/bin/prometheus-postgres-exporter \
            "$ROOTFS/usr/local/bin/postgres_exporter"
        run_root sh -c ": >'$ROOTFS/postgres_exporter.yaml'"
    fi

    # Web bundle (stage-assets enforces a real web.manifest.json). It writes
    # into the root-owned rootfs, so run it through the root prefix.
    run_root bash "$REPO_ROOT/docker/server/stage-assets.sh" --dist-dir "$DIST_DIR" "$ROOTFS"
    run_root chroot "$ROOTFS" chown -R sourcegraph:sourcegraph /assets-dist

    # Migration SQL corpus for the Rust migrator (`MIGRATIONS_DIR`,
    # default /migrations). The Rust migrator reads
    # the same corpus from disk.
    run_root rm -rf "$ROOTFS/migrations"
    run_root cp -a "$SRC_DIR/migrations" "$ROOTFS/migrations"
    # Expected schema descriptions (`drift`) and frozen definitions
    # (`upgrade`/`downgrade` at older revisions).
    run_root cp "$SRC_DIR/internal/database/schema.json" "$ROOTFS/migrations/schema.json"
    run_root cp "$SRC_DIR/internal/database/schema.codeintel.json" \
        "$ROOTFS/migrations/schema.codeintel.json"
    run_root cp "$SRC_DIR/internal/database/schema.codeinsights.json" \
        "$ROOTFS/migrations/schema.codeinsights.json"
    run_root mkdir -p "$ROOTFS/migrations/frozen"
    run_root cp "$SRC_DIR/internal/database/migration/shared/data/frozen/4.5.0.json" \
        "$ROOTFS/migrations/frozen/4.5.0.json"

    # The image serves the front door on nginx :7080 and runs as root.
    # Only regular files: /usr/local/bin also holds symlinks into the rootfs
    # (postgres_exporter, su-exec) that are dangling from the host namespace,
    # and chmod would follow them.
    run_root find "$ROOTFS/usr/local/bin" -maxdepth 1 -type f -exec chmod 0755 {} +
    run_root chmod 0755 "$ROOTFS/server"
    if has_feature observability; then
        run_root chmod 0755 "$ROOTFS/bin/prom-wrapper"
    fi
    run_root chmod 0755 "$ROOTFS/postgres-optimize.sh" "$ROOTFS/reindex.sh"
    run_root touch "$ROOTFS/var/lib/nginx/tmp/.keep" "$ROOTFS/var/run/.keep"
    run_root chmod 0644 "$ROOTFS/var/lib/nginx/tmp/.keep" "$ROOTFS/var/run/.keep"
}

# ---------------------------------------------------------------------------
# 7. Filesystem tarball
# ---------------------------------------------------------------------------
stage_package() {
    # The rootfs must match the selected feature set, otherwise a feature-
    # suffixed artifact could contain a different image.
    if has_feature observability && [ ! -x "$ROOTFS/bin/prom-wrapper" ]; then
        fail "observability feature enabled but $ROOTFS lacks prom-wrapper; run the rootfs stage with --features observability"
    fi
    if ! has_feature observability && [ -e "$ROOTFS/bin/prom-wrapper" ]; then
        fail "observability content present in $ROOTFS but the feature is off; rebuild the rootfs"
    fi
    if has_feature sveltekit && [ ! -f "$ROOTFS/assets-dist/_sk/index.html" ]; then
        fail "sveltekit feature enabled but the rootfs lacks assets-dist/_sk/index.html; build the frontend stage with the feature"
    fi
    if ! has_feature sveltekit && [ -e "$ROOTFS/assets-dist/_sk/index.html" ]; then
        fail "SvelteKit assets present in the rootfs but the feature is off; rebuild the frontend/rootfs stages"
    fi

    log "writing $OUT_NAME"
    mkdir -p "$OUT_DIR"
    local tar="$OUT_DIR/$OUT_NAME"
    rm -f "$tar" "$tar.sha256"
    run_root tar --numeric-owner -C "$ROOTFS" -cpf - . | gzip -1 >"$tar"
    ( cd "$OUT_DIR" && sha256sum "$OUT_NAME" >"$OUT_NAME.sha256" )
    ls -lh "$tar" "$tar.sha256"
    log "import with: $SCRIPT_DIR/import-image.sh $tar <tag>"

    # `docker` is an independent feature: it consumes the rootfs tarball and
    # adds a Docker image plus a saved image tar artifact.
    if has_feature docker; then
        log "importing Docker image $IMAGE_TAG"
        if [ -n "$SUDO" ]; then
            $SUDO env DOCKER="$DOCKER_CMD" "$SCRIPT_DIR/import-image.sh" "$tar" "$IMAGE_TAG"
        else
            DOCKER="$DOCKER_CMD" "$SCRIPT_DIR/import-image.sh" "$tar" "$IMAGE_TAG"
        fi

        local image_tar="$OUT_DIR/$IMAGE_OUT_NAME"
        rm -f "$image_tar" "$image_tar.sha256"
        log "saving Docker image $IMAGE_TAG -> $IMAGE_OUT_NAME"
        # Redirect in the caller's shell so the artifact is owned by the
        # packaging user, not root/docker.
        if [ -n "$SUDO" ]; then
            $SUDO "$DOCKER_CMD" save "$IMAGE_TAG" >"$image_tar"
        else
            "$DOCKER_CMD" save "$IMAGE_TAG" >"$image_tar"
        fi
        ( cd "$OUT_DIR" && sha256sum "$IMAGE_OUT_NAME" >"$IMAGE_OUT_NAME.sha256" )
        ls -lh "$image_tar" "$image_tar.sha256"
    fi
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
