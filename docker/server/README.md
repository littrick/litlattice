# `sourcegraph/server` 镜像（Rust 重写）

> **独立打包：** `docker/package/` 是规范的端到端流程。
> 它在打包期间获取 Sourcegraph 源码（Web UI + Go sidecar）和 Ubuntu 软件包，
> 并写出一个 `docker import` 可用的文件系统 tarball，无需 Dockerfile。
> 参见 [`../package/README.md`](../package/README.md)。


单容器镜像的打包流程。Go 进程集由 `bins/` 下的 Rust 二进制替换；zoekt、
syntect_server、scip-ctags、prom-wrapper、nginx、Postgres 和 Redis 仍为外部组件
（Go 二进制/软件包）。A 部分负责 Dockerfile / rootfs 组装（9 个服务二进制、
zoekt、syntect、Postgres 引导）；B 部分负责 Web 资源暂存契约。

契约的事实来源（`dev/origin/repo`）：

| Go 输入 | 定义的内容 |
| --- | --- |
| `cmd/server/BUILD.bazel:88-152` | `DEPS`、`ZOEKT_DEPS`、二进制安装路径、辅助脚本、镜像测试 |
| `cmd/server/BUILD.bazel:155-189` | entrypoint、`ENV`、`WORKDIR`、tars |
| `cmd/server/image_test.yaml` | container-structure-test 断言 |
| `cmd/server/image_tests/*.sh` | 服务/zoekt/打包冒烟测试 |
| `wolfi-images/server.yaml:49-88` | 用户/组和静态目录 |
| `cmd/server/shared/postgres.go:16-199` | 本地 Postgres 供给（已移植到 `crates/base/sg-entry`） |

## `build-rootfs.sh` 生成的布局

```
/server                              Rust `bins/server`（entrypoint: /sbin/tini -- /server）
/usr/local/bin/{frontend,gitserver,searcher,symbols,worker,repo-updater,
                migrator,precise-code-intel-worker,embeddings}   DEPS（9 个）
/usr/local/bin/{zoekt-archive-index,zoekt-git-index,
                zoekt-sourcegraph-indexserver,zoekt-webserver,
                syntect_server,scip-ctags}                       外部组件
/bin/prom-wrapper
/postgres-optimize.sh
/reindex.sh
/opt/image_tests/{test_services.sh,test_packages.sh,test_zoekt.sh}
/version.txt        5.5.0（可通过 SOURCEGRAPH_VERSION 覆盖）
/timestamp.txt      空
/assets-dist/web.manifest.json   从 client/web/dist 暂存（见下文）
/var/lib/nginx/tmp, /var/run, /sg_grafana_additional_dashboards
/etc/passwd: sourcegraph 100:101, postgres 70:70, nginx 101:102,
             redis 102:103, grafana 103:104
```

`server` 安装在 `/server`，**而不是** `/usr/local/bin/server`；Go 构建中的
`tar_server` pkg_tar 没有 `package_dir`。

## Web 资源：`stage-assets.sh`

Go 将 `//client/web/dist:tar_bundle` 安装为 `/assets-dist`，并在
`cmd/server/image_test.yaml` 中断言 `/assets-dist/web.manifest.json`。Rust
构建没有 Bazel 的 `client/web/dist` 目标，因此 rootfs 组装器会调用：

```sh
docker/server/stage-assets.sh [--dist-dir DIR] ROOTFS
```

* `ROOTFS/assets-dist` 接收该包（运行时为 `/assets-dist`）。
* `--dist-dir` 默认为 `<repo>/client/web/dist`。
* 缺少源目录是显式的 `ENV_GAP`（退出码 3）；源目录存在但没有常规且非空的
  `web.manifest.json` 则退出码 4。脚本绝不伪造资源或 manifest。
* 文件以 `0655`、目录以 `0755` 暂存，与 Bazel 的
  `pkg_tar(mode = "0655")` 一致；可通过 `ASSET_FILE_MODE` / `ASSET_DIR_MODE` 覆盖。

无 bundle 的镜像变体（`no_client_bundle.image`，manifest 为 `{}`）必须显式
从 `//client/web/dist:tar_dummy_manifest` 组装；它有意不作为本脚本的一种模式
（`{}` manifest 是真实且有意为之的构建输出，不能替代缺失的 bundle）。
`build-rootfs.sh` 之所以会暂存一个 `{}` 占位 manifest，仅仅是因为本工作区
没有 `client/` 目录树。

前端从 `/.assets` 提供这些文件，并根据 manifest 渲染 SPA 外壳
（`sg_ui::init()` 安装 `/assets-dist` 的 `DirProvider`，与 Go 完全一致；
缺少 manifest → HTTP 500）。设置 `SG_UI_EMBEDDED_ASSETS=1` 可在没有 Web 构建的
源码检出中选用编译内置的 `EMBEDDED_ASSETS` 树——这会复现 `{}` manifest 外壳
（200，无 bundle），并在 B 部分概览中记录为 `KNOWN_DIFF`。

nginx 仅作为反向代理（`listen 7080` → `127.0.0.1:8080`）；它不得增加静态的
`/assets-dist` 或 `/.assets` location。生成的 `nginx.conf`/redis 配置字节和
权限模式已由 `run_server_entry_diff.sh` 验证；不要重新实现这些断言。

## 用法（无需 docker daemon）

```bash
# 构建 11 个 Rust 二进制并暂存 rootfs。
./build-rootfs.sh --build --stage rootfs

# 如果二进制已经存在：
./build-rootfs.sh --stage rootfs --bin-dir /path/to/target/debug

# 在可用时加入外部二进制（否则记录为 ENV_GAP）。
./build-rootfs.sh --build --stage rootfs --external-dir /path/to/externals

# 离线布局断言；打印 SERVER_IMAGE_ROOTFS_OK（N 个断言）。
./verify-rootfs.sh --stage rootfs
./verify-rootfs.sh --stage rootfs --with-external   # zoekt --version 冒烟测试
```

`build-rootfs.sh --build` 将 cargo 包装在共享工作区的 flock 中，并遵循
`CARGO_TARGET_DIR`。`verify-rootfs.sh` 从不调用 docker：它检查文件路径、
权限模式、用户/组、来自 `rootfs/var/lib/sg-rootfs/ownership.tsv` 的属主元数据，
并解析本目录的 `Dockerfile` 中的 `ENTRYPOINT`、`ENV`、`WORKDIR` 和
`COPY --chown`。

## 在 docker daemon 可用后重建镜像

```bash
cd docker/server
./build-rootfs.sh --build --stage rootfs --external-dir /path/to/externals
./verify-rootfs.sh --stage rootfs --with-external
docker build -t sourcegraph/server:rust .
docker run --rm sourcegraph/server:rust /opt/image_tests/test_services.sh
```

`test_services.sh` 以 `SANITY_CHECK=true` 运行每个服务。Rust 的 `gitserver`
和 `repo-updater` 现在会调用 `sg_core::sanity_check()`（M9.49a 前置条件），
与 `cmd/gitserver/main.go:11` 和 `cmd/repo-updater/main.go:12` 一致。

## Postgres 供给

`crates/base/sg-entry/src/server.rs` 现在移植了
`cmd/server/shared/postgres.go:16-199`：

* `mkdir -p /run/postgresql && chown -R postgres /run/postgresql`；
* markers 目录以及 `sourcegraph`/`sourcegraph-codeintel` marker 文件；
* 全新的 `initdb --nosync`、`pg_ctl start`、为每个数据库执行 `createdb`、
  对 marker 执行 `touch`、`pg_ctl stop`，以及 `5.1-reindex.completed` marker；
* 已存在的数据目录：`chown -R postgres`，仅创建缺失的数据库
  （忽略 `database "X" already exists`），然后停止；
* 相同的 `✱` 日志行以及最后的 `postgres:` Procfile 行。

该计划是纯函数并有单元测试；`run_from_env` 在 5.1 重建索引门禁之前执行它。
Go oracle 有意省略供给，这仍是 M9.11c 差分测试未覆盖它的原因；`test_ws`
为命令序列提供了基于 stub 的 Go/Rust 端到端测试。

## ENV_GAP / KNOWN_DIFF

- **ENV_GAP docker daemon**：本环境中 `docker info` 失败。Dockerfile 尚未
  构建或运行；仅验证了离线 rootfs 布局。在有 daemon 的主机上完成上述步骤之前，
  不要声称镜像可以构建或启动。
- **ENV_GAP 外部二进制**：本主机缺少 `nginx`、`tini`、`syntect_server`、
  `zoekt-archive-index`、`zoekt-sourcegraph-indexserver` 和 `prom-wrapper`。
  `test_ws/bin` 为 `--with-external` 冒烟测试提供 `zoekt-webserver` 和
  `zoekt-git-index`。缺失的名称列于
  `rootfs/var/lib/sg-rootfs/missing-external.txt`。
- **ENV_GAP web bundle**：本工作区没有 `client/` 目录树，且 `/assets-dist`
  不可写，因此未暂存真实的 bundle；冒烟测试使用 fixture 假 dist，rootfs
  暂存一个 `{}` 占位 manifest。
- **KNOWN_DIFF 监控资源**：`/prometheus.sh`、`/alertmanager.sh`、
  `/sg_config_prometheus/*.yml` 是最小占位符，而非生成的 Go 监控配置。
- **ENV_GAP 本地 Postgres 二进制**：此处 `PATH` 中没有
  `postgres`/`initdb`/`pg_ctl`，因此供给仅用 stub 断言，而非真实数据库服务器。
