#!/usr/bin/env bash
# Import a packaged root filesystem tarball as a runnable Docker image.
#
#   docker/package/import-image.sh \
#     .package-build/out/sourcegraph-server-rust-5.5.0-fs.tar.gz \
#     litlattice:latest
#
# `docker import` does not read the filesystem's config, so the entrypoint,
# exposed port and runtime environment are supplied here (the same values the
# reference image bakes into its metadata).
set -euo pipefail

TAR=${1:?usage: import-image.sh <fs.tar.gz> <image:tag>}
TAG=${2:?usage: import-image.sh <fs.tar.gz> <image:tag>}
DOCKER=${DOCKER:-docker}
[ -f "$TAR" ] || { echo "import-image: no such file: $TAR" >&2; exit 1; }

"$DOCKER" import \
    --change 'ENTRYPOINT ["/sbin/tini","--","/server"]' \
    --change 'EXPOSE 7080' \
    --change 'WORKDIR /' \
    --change 'ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/postgresql/16/bin LANG=en_US.utf8 LC_ALL=en_US.utf8 DISABLE_OBSERVABILITY=1 DISABLE_HYBRID_SEARCH=true BAZEL_SKIP_OOB_INFER_VERSION=1' \
    "$TAR" "$TAG"

echo "imported $TAG"
echo "run: docker run -d --name sg-rust -p 7080:7080 -v sg-rust-data:/var/opt/sourcegraph $TAG"
