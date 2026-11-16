# `sourcegraph/server` image (Rust rewrite)

Packaging for the single-container image. The Go process set is replaced by the
Rust binaries under `bins/`; zoekt, syntect_server, scip-ctags, prom-wrapper,
nginx, Postgres and Redis remain external (Go binaries/packages). The A slice
owns the Dockerfile / rootfs assembly (9 service binaries, zoekt, syntect,
Postgres bootstrap); the B slice owns the web-assets staging contract.

Contract source of truth (`dev/origin/repo`):

| Go input | What it defines |
| --- | --- |
| `cmd/server/BUILD.bazel:88-152` | `DEPS`, `ZOEKT_DEPS`, binary install paths, helper scripts, image tests |
| `cmd/server/BUILD.bazel:155-189` | entrypoint, `ENV`, `WORKDIR`, tars |
| `cmd/server/image_test.yaml` | container-structure-test assertions |
| `cmd/server/image_tests/*.sh` | service/zoekt/package smoke tests |
| `wolfi-images/server.yaml:49-88` | users/groups and static directories |
| `cmd/server/shared/postgres.go:16-199` | local Postgres provisioning (ported into `crates/base/sg-entry`) |

## Layout produced by `build-rootfs.sh`

```
/server                              Rust `bins/server` (entrypoint: /sbin/tini -- /server)
/usr/local/bin/{frontend,gitserver,searcher,symbols,worker,repo-updater,
                migrator,precise-code-intel-worker,embeddings}   DEPS (9)
/usr/local/bin/{zoekt-archive-index,zoekt-git-index,
                zoekt-sourcegraph-indexserver,zoekt-webserver,
                syntect_server,scip-ctags}                       external
/bin/prom-wrapper
/postgres-optimize.sh
/reindex.sh
/opt/image_tests/{test_services.sh,test_packages.sh,test_zoekt.sh}
/version.txt        0.0.0+dev
/timestamp.txt      empty
/assets-dist/web.manifest.json   staged from client/web/dist (see below)
/var/lib/nginx/tmp, /var/run, /sg_grafana_additional_dashboards
/etc/passwd: sourcegraph 100:101, postgres 70:70, nginx 101:102,
             redis 102:103, grafana 103:104
```

`server` is installed at `/server`, **not** `/usr/local/bin/server`; the
`tar_server` pkg_tar in the Go build has no `package_dir`.

## Web assets: `stage-assets.sh`

Go installs `//client/web/dist:tar_bundle` as `/assets-dist` and asserts
`/assets-dist/web.manifest.json` in `cmd/server/image_test.yaml`. The Rust
build has no Bazel `client/web/dist` target, so the rootfs assembler invokes:

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
assembled explicitly from `//client/web/dist:tar_dummy_manifest`; it is
intentionally not a mode of this script (a `{}` manifest is a real, intentional
build output, not a substitute for a missing bundle). `build-rootfs.sh` stages a
`{}` dummy manifest only because this workspace has no `client/` tree.

The frontend serves these files from `/.assets` and renders the SPA shell from
the manifest (`sg_ui::init()` installs a `/assets-dist` `DirProvider`, exactly
like Go; missing manifest → HTTP 500). Set `SG_UI_EMBEDDED_ASSETS=1` to opt
into the compiled-in `EMBEDDED_ASSETS` tree for a source checkout without a web
build — that reproduces the `{}`-manifest shell (200, no bundles) and is
recorded as `KNOWN_DIFF` in the B slice overview.

nginx is a reverse proxy only (`listen 7080` → `127.0.0.1:8080`); it must not
gain a static `/assets-dist` or `/.assets` location. The generated
`nginx.conf`/redis config bytes and modes are already verified by
`run_server_entry_diff.sh`; do not re-implement those assertions.

## Usage (no docker daemon required)

```bash
# Build the 11 Rust binaries and stage the rootfs.
./build-rootfs.sh --build --stage rootfs

# If the binaries already exist:
./build-rootfs.sh --stage rootfs --bin-dir /path/to/target/debug

# Add external binaries when available (otherwise recorded as ENV_GAP).
./build-rootfs.sh --build --stage rootfs --external-dir /path/to/externals

# Offline layout assertions; prints SERVER_IMAGE_ROOTFS_OK (N assertions).
./verify-rootfs.sh --stage rootfs
./verify-rootfs.sh --stage rootfs --with-external   # zoekt --version smoke
```

`build-rootfs.sh --build` wraps cargo in the shared workspace flock and honours
`CARGO_TARGET_DIR`. `verify-rootfs.sh` never calls docker: it checks file
paths, modes, users/groups, ownership metadata from
`rootfs/var/lib/sg-rootfs/ownership.tsv`, and parses this directory's
`Dockerfile` for `ENTRYPOINT`, `ENV`, `WORKDIR` and `COPY --chown`.

## Rebuilding the image once a docker daemon is available

```bash
cd docker/server
./build-rootfs.sh --build --stage rootfs --external-dir /path/to/externals
./verify-rootfs.sh --stage rootfs --with-external
docker build -t sourcegraph/server:rust .
docker run --rm sourcegraph/server:rust /opt/image_tests/test_services.sh
```

`test_services.sh` runs every service with `SANITY_CHECK=true`. The Rust
`gitserver` and `repo-updater` now call `sg_core::sanity_check()` (M9.49a
prerequisite), matching `cmd/gitserver/main.go:11` and
`cmd/repo-updater/main.go:12`.

## Postgres provisioning

`crates/base/sg-entry/src/server.rs` now ports
`cmd/server/shared/postgres.go:16-199`:

* `mkdir -p /run/postgresql && chown -R postgres /run/postgresql`;
* markers directory and `sourcegraph`/`sourcegraph-codeintel` marker files;
* fresh `initdb --nosync`, `pg_ctl start`, `createdb` per database, marker
  `touch`, `pg_ctl stop`, and the `5.1-reindex.completed` marker;
* existing data directory: `chown -R postgres`, create only missing databases
  (ignoring `database "X" already exists`), then stop;
* the same `✱` log lines and the final `postgres:` Procfile line.

The plan is pure and unit-tested; `run_from_env` executes it before the 5.1
reindex gate. The Go oracle's deliberate omission of provisioning remains the
reason the M9.11c differential does not cover it; `test_ws` carries a stub-based
Go/Rust e2e for the command sequence.

## ENV_GAP / KNOWN_DIFF

- **ENV_GAP docker daemon**: `docker info` fails in this environment. The
  Dockerfile has not been built or run; only the offline rootfs layout was
  verified. Do not claim the image builds or starts until the steps above run
  on a host with a daemon.
- **ENV_GAP external binaries**: `nginx`, `tini`, `syntect_server`,
  `zoekt-archive-index`, `zoekt-sourcegraph-indexserver` and `prom-wrapper` are
  absent from this host. `test_ws/bin` provides `zoekt-webserver` and
  `zoekt-git-index` for `--with-external` smoke tests. Missing names are listed
  in `rootfs/var/lib/sg-rootfs/missing-external.txt`.
- **ENV_GAP web bundle**: this workspace has no `client/` tree and
  `/assets-dist` is not writable, so no real bundle was staged; the smoke test
  uses a fixture fake dist and the rootfs stages a `{}` dummy manifest.
- **KNOWN_DIFF monitoring assets**: `/prometheus.sh`, `/alertmanager.sh`,
  `/sg_config_prometheus/*.yml` are minimal placeholders, not the generated Go
  monitoring configs.
- **ENV_GAP local Postgres binaries**: `postgres`/`initdb`/`pg_ctl` are not on
  `PATH` here, so provisioning is asserted with stubs, not a real database
  server.
