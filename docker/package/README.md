# Standalone packaging (filesystem image)

Clone the `package` branch (it contains the latest Rust port plus this
packaging flow) and build a self-contained `sourcegraph/server` replacement as
a **root filesystem tarball** that `docker import` accepts:

```sh
git clone -b package <this-repository-url>
cd <repository>
```

This directory is the only packaging
entry point: a fresh clone of this repository can build the image without a
Dockerfile or any prebuilt artifacts.

Everything external is fetched/built during packaging:

| Component | Source at package time |
| --- | --- |
| Rust service binaries | this repository (`cargo build --release --bins`) |
| Web UI (`/assets-dist`) | `sourcegraph/sourcegraph-public-snapshot` @ pinned commit, `pnpm` build |
| Go sidecars (`migrator-go`, `blobstore`, `zoekt-*`, `prom-wrapper`) | same Sourcegraph source |
| `syntect_server`, `scip-ctags` | `docker-images/syntax-highlighter` in that source |
| Postgres, Redis, nginx, git, tini, gosu, … | Ubuntu 24.04 via `debootstrap`/apt |
| Root filesystem | assembled directly, no `docker build` |

## Requirements

- Linux host with root (or passwordless `sudo`); `debootstrap` installed
  (`apt-get install debootstrap`).
- `git`, `node >= 20.8`, `pnpm 8.9.2` (or `corepack`), `go 1.22.x`,
  `cargo`/`rustup` (the repository's `rust-toolchain.toml` pins 1.78.0).
- Network access for: the Sourcegraph clone, npm registry, Go modules,
  crates.io, the Ubuntu mirror. Set `PACKAGE_PROXY=host:port` when a proxy is
  required.
- Disk: roughly 8–12 GiB for the build workspace (node_modules + targets +
  rootfs), plus the output tarball (~450 MiB).

## Usage

```sh
# full build (rootfs/debootstrap need root; the rest can run as your user)
PACKAGE_PROXY=172.17.0.1:7890 sudo -E ./docker/package/package.sh

# or run the network/compile stages as your user, then the root stages
PACKAGE_PROXY=172.17.0.1:7890 ./docker/package/package.sh --only fetch,frontend,rust,go,syntect
sudo -E ./docker/package/package.sh --from rootfs
```

Artifacts land in `.package-build/out/`:

```
sourcegraph-server-rust-5.5.0-fs.tar.gz
sourcegraph-server-rust-5.5.0-fs.tar.gz.sha256
```

Import and run:

```sh
./docker/package/import-image.sh \
  .package-build/out/sourcegraph-server-rust-5.5.0-fs.tar.gz \
  sourcegraph/server:rust

docker run -d --name sg-rust -p 7080:7080 \
  -v sg-rust-data:/var/opt/sourcegraph sourcegraph/server:rust
```

`import-image.sh` applies the metadata `docker import` cannot infer:
`ENTRYPOINT ["/sbin/tini","--","/server"]`, `EXPOSE 7080`, the runtime `PATH`
and the image environment (`DISABLE_OBSERVABILITY=1`,
`DISABLE_HYBRID_SEARCH=true`, `BAZEL_SKIP_OOB_INFER_VERSION=1`).

## Stages

`package.sh --only <list>` / `--from <stage>` select stages; defaults are
driven by `docker/package/versions.env`:

1. `fetch` — clone/checkout `SOURCEGRAPH_COMMIT` (web UI + Go sidecars).
2. `frontend` — `pnpm install --frozen-lockfile`, `pnpm run generate`,
   `pnpm --filter @sourcegraph/web run build`; copies `client/web/dist`.
3. `rust` — `cargo build --release --bins`; stages the 10 service binaries.
4. `go` — builds `migrator`, `blobstore`, `prom-wrapper` and the four
   `zoekt-*` commands from the pinned module dependency.
5. `syntect` — builds `syntect_server`/`scip-ctags`, strips debug info.
6. `rootfs` — `debootstrap noble` with the stock packages, users/groups,
   runtime directories, application files, `/version.txt` = `SOURCEGRAPH_VERSION`,
   and the web bundle (`docker/server/stage-assets.sh`).
7. `package` — `tar --numeric-owner` + gzip and sha256.

## Fidelity notes

- Observability is disabled (no Grafana/Prometheus packages); the Procfile's
  monitoring lines resolve to the file-existence contract only.
- Hybrid/zoekt search is disabled (`DISABLE_HYBRID_SEARCH=true`); zoekt
  binaries are shipped for parity, but the index server is not run.
- `/var/opt/sourcegraph/postgresql{,-markers}` are intentionally **not**
  pre-created: `/server` uses their existence to decide whether to run
  `initdb`.
- The database schema/migrations are still owned by the Go `migrator`
  (`migrator-go`), matching the project's storage contract.
- The web bundle is built from the pinned Sourcegraph commit; changing
  `SOURCEGRAPH_COMMIT`/`SOURCEGRAPH_VERSION` in `versions.env` is the only
  switch needed for another release.
