#!/usr/bin/env bash
# stage-assets.sh -- M9.49b: stage the web bundle into an image rootfs.
#
# The `sourcegraph/server` image installs `//client/web/dist:tar_bundle` at
# `/assets-dist` (see the Go `cmd/server/BUILD.bazel` tars list). That tree is
# what the frontend's `/.assets` handler serves and what
# `/assets-dist/web.manifest.json` names the hashed bundles from. This script
# is the Rust build's equivalent staging step for the A-slice Dockerfile /
# rootfs assembler.
#
# INTERFACE (stable; the A slice's Dockerfile/rootfs script calls this):
#
#   docker/server/stage-assets.sh [--dist-dir DIR] ROOTFS
#
#   ROOTFS           rootfs prefix of the image being assembled. Assets are
#                    staged into $ROOTFS/assets-dist (i.e. /assets-dist at
#                    runtime). ROOTFS is created if missing.
#   --dist-dir DIR   source bundle directory. Defaults to
#                    <repo>/client/web/dist (Go's `assets.DevAssetsPath`).
#   -h, --help       print this help and exit 0.
#
# Contract:
#   * Only a real directory is staged. If DIR does not exist, the script exits
#     non-zero with an explicit `ENV_GAP:` diagnostic and stages nothing; it
#     never fabricates `/assets-dist` or a manifest.
#   * After copying, a regular (not symlink) and non-empty
#     `$ROOTFS/assets-dist/web.manifest.json` must exist. The Go image test
#     asserts exactly this path (`cmd/server/image_test.yaml:115-117`).
#   * File entries are chmod 0655 and directories 0755, mirroring the Bazel
#     `pkg_tar(mode = "0655")` bundle. Override with ASSET_FILE_MODE /
#     ASSET_DIR_MODE when an OCI builder already controls modes.
#
# Exit codes:
#   0  staged successfully
#   2  usage error
#   3  ENV_GAP: dist directory missing (web assets were not built)
#   4  ENV_GAP: dist directory exists but carries no usable web.manifest.json
#
# The no-bundle variant of the image (`no_client_bundle.image`) is NOT handled
# here: Go builds it from `//client/web/dist:tar_dummy_manifest` (`{}`) rather
# than from a real bundle. The A-slice assembler must create that `{}` manifest
# explicitly when it intentionally ships without a web build.
set -euo pipefail

SELF_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SELF_DIR/../.." && pwd)

usage() {
    awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "${BASH_SOURCE[0]}"
}

DIST_DIR="$REPO_ROOT/client/web/dist"
ROOTFS=""
FILE_MODE="${ASSET_FILE_MODE:-0655}"
DIR_MODE="${ASSET_DIR_MODE:-0755}"

while [ "$#" -gt 0 ]; do
    case "$1" in
        --dist-dir)
            [ "$#" -ge 2 ] || { echo "stage-assets: --dist-dir needs a value" >&2; exit 2; }
            DIST_DIR="$2"
            shift 2
            ;;
        --dist-dir=*)
            DIST_DIR="${1#--dist-dir=}"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            while [ "$#" -gt 0 ]; do
                if [ -n "$ROOTFS" ]; then
                    echo "stage-assets: extra argument: $1" >&2
                    exit 2
                fi
                ROOTFS="$1"
                shift
            done
            ;;
        -*)
            echo "stage-assets: unknown option: $1" >&2
            exit 2
            ;;
        *)
            if [ -n "$ROOTFS" ]; then
                echo "stage-assets: extra argument: $1" >&2
                exit 2
            fi
            ROOTFS="$1"
            shift
            ;;
    esac
done

if [ -z "$ROOTFS" ]; then
    echo "stage-assets: missing ROOTFS argument" >&2
    usage >&2
    exit 2
fi

# Make relative --dist-dir values independent of the caller's cwd.
case "$DIST_DIR" in
    /*) ;;
    *) DIST_DIR="$PWD/$DIST_DIR" ;;
esac

if [ ! -d "$DIST_DIR" ]; then
    cat >&2 <<EOF
ENV_GAP: web assets were not built: $DIST_DIR is not a directory.
ENV_GAP: build client/web/dist (or pass --dist-dir DIR) and rerun.
ENV_GAP: refusing to fabricate /assets-dist; the image would serve no web UI.
EOF
    exit 3
fi

DEST="$ROOTFS/assets-dist"
rm -rf -- "$DEST"
mkdir -p -- "$DEST"
cp -a -- "$DIST_DIR"/. "$DEST"/

MANIFEST="$DEST/web.manifest.json"
if [ -L "$MANIFEST" ] || [ ! -f "$MANIFEST" ]; then
    cat >&2 <<EOF
ENV_GAP: $DIST_DIR does not contain a regular web.manifest.json.
ENV_GAP: the web build did not finish (Bazel //client/web/dist:copy_bundle writes it next to the bundles).
ENV_GAP: refusing to synthesize a manifest.
EOF
    exit 4
fi
if [ ! -s "$MANIFEST" ]; then
    echo "ENV_GAP: $MANIFEST is empty; a valid build manifest is required." >&2
    exit 4
fi

find "$DEST" -type d -exec chmod "$DIR_MODE" {} +
find "$DEST" -type f -exec chmod "$FILE_MODE" {} +

file_count=$(find "$DEST" -type f | wc -l | tr -d ' ')
manifest_bytes=$(wc -c <"$MANIFEST" | tr -d ' ')
printf 'stage-assets: staged %s files from %s to %s (web.manifest.json %s bytes, mode %s)\n' \
    "$file_count" "$DIST_DIR" "$DEST" "$manifest_bytes" "$FILE_MODE"
