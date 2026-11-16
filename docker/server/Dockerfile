# syntax=docker/dockerfile:1.7
#
# M9.49a `sourcegraph/server` image (Rust binaries, external Go/sidecar tools).
#
# ENV_GAP: this environment has no docker daemon (`docker info` fails), so this
# file has NOT been built or run. The layout it consumes is produced and
# asserted offline by:
#
#   ./build-rootfs.sh --build --stage rootfs --external-dir <dir>
#   ./verify-rootfs.sh --stage rootfs
#
# Build (once a daemon is available):
#
#   cd docker/server
#   ./build-rootfs.sh --build --stage rootfs --external-dir /path/to/externals
#   docker build -t sourcegraph/server:rust .
#
# The Go contract is `cmd/server/BUILD.bazel:155-189` plus
# `wolfi-images/server.yaml:49-88`.

ARG WOLFI_BASE=cgr.dev/chainguard/wolfi-base:latest
FROM ${WOLFI_BASE}

# Mirrors the package set in wolfi-images/server.yaml. Package availability is
# not verified here (no daemon / no network); treat a failed build as an
# ENV_GAP until it can be exercised.
RUN apk add --no-cache \
      tini \
      bash ca-certificates git git-lfs glibc-locale-en libev libstdc++ \
      nginx openssh-client pcre posix-libc-utils \
      postgresql-12 postgresql-12-client postgresql-12-contrib \
      redis-6.2 redis-cli-6.2 sqlite-libs su-exec \
      python3 \
      prometheus prometheus-alertmanager prometheus-postgres-exporter \
      comby ctags coursier p4cli p4-fusion-sg s3proxy grafana-7

# Users/groups: wolfi-images/server.yaml:49-77 and image_test.yaml:49-60.
RUN addgroup -g 101 sourcegraph \
 && addgroup -g 70 postgres \
 && addgroup -g 102 nginx \
 && addgroup -g 103 redis \
 && addgroup -g 104 grafana \
 && adduser -D -u 100 -G sourcegraph -h /home/sourcegraph -s /bin/nologin sourcegraph \
 && adduser -D -u 70 -G postgres -h /var/lib/postgresql -s /bin/nologin postgres \
 && adduser -D -u 101 -G nginx -h /var/lib/nginx -s /bin/nologin nginx \
 && adduser -D -u 102 -G redis -h /var/lib/redis -s /bin/nologin redis \
 && adduser -D -u 103 -G grafana -h /var/lib/grafana -s /bin/nologin grafana

# Static directories (cmd/server/BUILD.bazel:50-60, wolfi-images/server.yaml:79-88)
# and the runtime data roots (crates/base/sg-entry/src/server.rs).
RUN mkdir -p \
      /var/lib/nginx/tmp /var/run /sg_grafana_additional_dashboards \
      /var/opt/sourcegraph/repos /var/opt/sourcegraph/cache \
      /var/opt/sourcegraph/blobstore /var/opt/sourcegraph/redis \
      /var/opt/sourcegraph/redis-cache /var/opt/sourcegraph/zoekt/index \
      /var/opt/sourcegraph/postgresql /var/opt/sourcegraph/postgresql-markers \
      /etc/sourcegraph /assets-dist /run/postgresql \
 && chmod 0755 /var/lib/nginx/tmp /var/run /sg_grafana_additional_dashboards \
 && touch /var/lib/nginx/tmp/.keep /var/run/.keep \
 && chown -R sourcegraph:sourcegraph /assets-dist \
      /var/opt/sourcegraph/repos /var/opt/sourcegraph/cache \
      /var/opt/sourcegraph/blobstore /var/opt/sourcegraph/zoekt \
 && chown -R redis:redis /var/opt/sourcegraph/redis /var/opt/sourcegraph/redis-cache \
 && chown -R postgres:postgres /var/opt/sourcegraph/postgresql \
      /var/opt/sourcegraph/postgresql-markers /run/postgresql

# /server (tar_server has no package_dir) and the DEPS/ZOEKT/syntax binaries.
COPY --chown=100:101 rootfs/server /server
COPY --chown=100:101 rootfs/usr/local/bin/ /usr/local/bin/
# Historical prom-wrapper path is fixed by the Procfile.
COPY --chown=0:0 rootfs/bin/prom-wrapper /bin/prom-wrapper
COPY --chown=0:0 rootfs/postgres-optimize.sh /postgres-optimize.sh
COPY --chown=0:0 rootfs/reindex.sh /reindex.sh
COPY --chown=0:0 rootfs/opt/image_tests/ /opt/image_tests/

# Version stamps (internal/version/BUILD.bazel:23-54) and web assets.
COPY --chown=0:0 rootfs/version.txt /version.txt
COPY --chown=0:0 rootfs/timestamp.txt /timestamp.txt
COPY --chown=100:101 rootfs/assets-dist/ /assets-dist/

# Minimal monitoring file-existence contract (KNOWN_DIFF: generated assets).
COPY --chown=0:0 rootfs/prometheus.sh /prometheus.sh
COPY --chown=0:0 rootfs/alertmanager.sh /alertmanager.sh
COPY --chown=0:0 rootfs/sg_config_prometheus/ /sg_config_prometheus/
COPY --chown=0:0 rootfs/postgres_exporter.yaml /postgres_exporter.yaml

RUN chmod 0755 /server /usr/local/bin/* /bin/prom-wrapper \
      /postgres-optimize.sh /reindex.sh /opt/image_tests/*.sh \
      /prometheus.sh /alertmanager.sh

# cmd/server/BUILD.bazel:159-184.
ENTRYPOINT ["/sbin/tini", "--", "/server"]
ENV GO111MODULES=on \
    LANG=en_US.utf8 \
    LC_ALL=en_US.utf8
WORKDIR /
