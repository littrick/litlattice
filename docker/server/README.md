# `sourcegraph/server` image packaging (Rust build)

This directory is the Rust rewrite's packaging surface for the single-container
`sourcegraph/server` image. The A slice owns the Dockerfile / rootfs assembly
(9 service binaries, zoekt, syntect, Postgres bootstrap); this file documents
the web-assets contract that the B slice implements and that the A slice must
call.

## Web assets: `stage-assets.sh`

Go installs `//client/web/dist:tar_bundle` as `/assets-dist` and asserts
`/assets-dist/web.manifest.json` in `cmd/server/image_test.yaml`. The Rust
build has no Bazel `client/web/dist` target, so the A slice's rootfs assembler
invokes:

```sh
docker/server/stage-assets.sh [--dist-dir DIR] ROOTFS
```

* `ROOTFS/assets-dist` receives the bundle (`/assets-dist` at runtime).
* `--dist-dir` defaults to `<repo>/client/web/dist`.
* A missing source directory is an explicit `ENV_GAP` (exit 3); a source
  directory without a regular, non-empty `web.manifest.json` is exit 4. The
  script never fabricates assets or a manifest.
* Files are staged `0655` and directories `0755`, mirroring Bazel's
  `pkg_tar(mode = "0655")`; override with `ASSET_FILE_MODE` / `ASSET_DIR_MODE`.

The no-bundle image variant (`no_client_bundle.image`, manifest `{}`) must be
assembled explicitly by the A slice from
`//client/web/dist:tar_dummy_manifest`; it is intentionally not a mode of this
script (a `{}` manifest is a real, intentional build output, not a substitute
for a missing bundle).

The frontend serves these files from `/.assets` and renders the SPA shell from
the manifest (`sg_ui::init()` installs a `/assets-dist` `DirProvider`, exactly
like Go; missing manifest → HTTP 500). Set `SG_UI_EMBEDDED_ASSETS=1` to opt
into the compiled-in `EMBEDDED_ASSETS` tree for a source checkout without a
web build — that reproduces the `{}`-manifest shell (200, no bundles) and is
recorded as `KNOWN_DIFF` in the B slice overview.

nginx is a reverse proxy only (`listen 7080` → `127.0.0.1:8080`); it must not
gain a static `/assets-dist` or `/.assets` location. The generated
`nginx.conf`/redis config bytes and modes are already verified by
`run_server_entry_diff.sh`; do not re-implement those assertions.
