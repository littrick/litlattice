# 独立打包（文件系统镜像）

克隆 `package` 分支（包含最新的 Rust 移植版以及本打包流程），构建一个自包含的
`sourcegraph/server` 替代品，形式为 `docker import` 可接受的**根文件系统 tarball**：

```sh
git clone -b package <this-repository-url>
cd <repository>
```

本目录是唯一的打包入口：全新克隆本仓库即可构建镜像，无需 Dockerfile，也无需任何
预构建产物。

所有外部内容均在打包期间获取/构建：

| 组件 | 打包时的来源 |
| --- | --- |
| Rust 服务二进制 | 本仓库（`cargo build --release --bins`） |
| Web UI（`/assets-dist`） | 固定提交的 `sourcegraph/sourcegraph-public-snapshot`，`pnpm` 构建 |
| Go sidecar（`migrator-go`、`blobstore`、`zoekt-*`、`prom-wrapper`） | 同一份 Sourcegraph 源码 |
| `syntect_server`、`scip-ctags` | 该源码中的 `docker-images/syntax-highlighter` |
| Postgres、Redis、nginx、git、tini、gosu 等 | 通过 `debootstrap`/apt 安装的 Ubuntu 24.04 |
| 根文件系统 | 直接组装，不使用 `docker build` |

## 要求

- Linux 主机，具有 root（或免密 `sudo`）；已安装 `debootstrap`
  （`apt-get install debootstrap`）。
- `git`、`node 20.8–24`（Sourcegraph 5.5 的 `engines` 为 `^v20.8`；Node 25 起
  移除了 `buffer.SlowBuffer`，`pnpm run generate` 会失败）、`pnpm 8.9.2`
  （或 `corepack`）、`go 1.22.x`、`cargo`/`rustup`
  （仓库的 `rust-toolchain.toml` 固定工具链版本）。
- 编译阶段（`frontend`/`rust`/`go`/`syntect`）建议以普通用户身份运行：
  `sudo` 会按 `secure_path` 重置 `PATH`，用户级安装的 `node`/`cargo`
  （如 `~/.cargo/bin`）在 root 下不可见。
- 需要网络访问：Sourcegraph 克隆、npm registry、Go modules、crates.io、
  Ubuntu 镜像源。需要代理时设置 `PACKAGE_PROXY=host:port`。
- 磁盘：构建工作区约需 8–12 GiB（node_modules + targets + rootfs），
  外加输出 tarball（约 450 MiB）。

## 用法

```sh
# 完整构建（rootfs/debootstrap 需要 root；其余步骤可以以你的用户身份运行）
PACKAGE_PROXY=172.17.0.1:7890 sudo -E ./docker/package/package.sh

# 或者以你的用户身份运行网络/编译阶段，然后再运行需要 root 的阶段
PACKAGE_PROXY=172.17.0.1:7890 ./docker/package/package.sh --only fetch,frontend,rust,go,syntect
sudo -E ./docker/package/package.sh --from rootfs
```

产物位于 `.package-build/out/`：

```
sourcegraph-server-rust-5.5.0-fs.tar.gz
sourcegraph-server-rust-5.5.0-fs.tar.gz.sha256
```

导入并运行：

```sh
./docker/package/import-image.sh \
  .package-build/out/sourcegraph-server-rust-5.5.0-fs.tar.gz \
  sourcegraph/server:rust

docker run -d --name sg-rust -p 7080:7080 \
  -v sg-rust-data:/var/opt/sourcegraph sourcegraph/server:rust
```

`import-image.sh` 会补上 `docker import` 无法推断的元数据：
`ENTRYPOINT ["/sbin/tini","--","/server"]`、`EXPOSE 7080`、运行时 `PATH`
以及镜像环境变量（`DISABLE_OBSERVABILITY=1`、
`DISABLE_HYBRID_SEARCH=true`、`BAZEL_SKIP_OOB_INFER_VERSION=1`）。

## 阶段

`package.sh --only <list>` / `--from <stage>` 用于选择阶段；默认流程由
`docker/package/versions.env` 驱动：

1. `fetch` — 克隆/检出 `SOURCEGRAPH_COMMIT`（Web UI + Go sidecar）。
2. `frontend` — `pnpm install --frozen-lockfile`、`pnpm run generate`、
   `pnpm --filter @sourcegraph/web run build`；复制 `client/web/dist`。
3. `rust` — `cargo build --release --bins`；暂存 10 个服务二进制。
4. `go` — 从固定的模块依赖构建 `migrator`、`blobstore`、`prom-wrapper` 以及
   四个 `zoekt-*` 命令。
5. `syntect` — 构建 `syntect_server`/`scip-ctags`，并去除调试信息。
6. `rootfs` — 使用标准软件包执行 `debootstrap noble`，配置用户/组、
   运行时目录、应用文件、`/version.txt` = `SOURCEGRAPH_VERSION`，
   以及 Web 包（`docker/server/stage-assets.sh`）。
7. `package` — `tar --numeric-owner` + gzip 和 sha256。

## 保真度说明

- 可观测性已禁用（无 Grafana/Prometheus 软件包）；Procfile 中的监控行仅满足
  文件存在性契约。
- 混合/zoekt 搜索已禁用（`DISABLE_HYBRID_SEARCH=true`）；为保持一致性仍随附
  zoekt 二进制，但不运行索引服务器。
- `/var/opt/sourcegraph/postgresql{,-markers}` 有意**不**预先创建：`/server`
  依据它们是否存在来决定是否运行 `initdb`。
- 数据库 schema/迁移仍由 Go `migrator`（`migrator-go`）负责，符合项目的
  存储契约。
- Web 包由固定的 Sourcegraph 提交构建；要切换到另一个发布版本，只需修改
  `versions.env` 中的 `SOURCEGRAPH_COMMIT`/`SOURCEGRAPH_VERSION`。
