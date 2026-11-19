# 1. 可执行文件总览

本仓库的可执行文件分为三类：

1. **Rust 重写进程**：`bins/` 下的 11 个二进制，由 `cargo build --release --bins`
   产出，是 Sourcegraph 服务进程的 Rust 替代品；
2. **Go 侧车（sidecar）**：打包镜像时从
   `sourcegraph-public-snapshot@c864f15` 构建/复用的 Go 二进制（`blobstore`、
   `zoekt-*`、`prom-wrapper`）；
3. **系统/第三方进程**：Ubuntu 软件包或 syntax-highlighter 产物（`nginx`、
   `postgres`、`redis-server`、`postgres_exporter`、`syntect_server`、
   `scip-ctags`、`universal-ctags`、`tini`）。

> 镜像内的完整路径以 `docker/package/package.sh` 的 `stage_rootfs` 为准；
> 入口是 `/sbin/tini -- /server`（`docker/package/import-image.sh`）。

## 1.1 Rust 二进制（`bins/`）

| 可执行文件 | 源包（crate） | 镜像内路径 | 角色 | 默认监听 | 是否常驻 |
| --- | --- | --- | --- | --- | --- |
| `server` | `sg-bin-server` | `/server` | 单容器**父进程/监督器**：等价 Go `cmd/server`，准备环境、生成配置、初始化 Postgres、跑迁移、按 Procfile 拉起并监管全部子进程 | 无 | 是 |
| `frontend` | `sg-bin-frontend` | `/usr/local/bin/frontend` | Web/API/GraphQL 前端：UI 资源、`/.api/graphql`、`/.internal` 内部 API、会话与配置服务 | 外部 `:8080`（`SRC_HTTP_ADDR`），内部 `:3090`（`SRC_HTTP_ADDR_INTERNAL`） | 是 |
| `gitserver` | `sg-bin-gitserver` | `/usr/local/bin/gitserver` | 裸 Git 仓库托管与 Git 操作（archive/blame/commit/diff 等） | `:3178`（`GITSERVER_ADDR`） | 是 |
| `searcher` | `sg-bin-searcher` | `/usr/local/bin/searcher` | 未索引内容搜索（`index:no`），按需从 gitserver 取文件 | `:3181`（`SEARCHER_ADDR`） | 是 |
| `symbols` | `sg-bin-symbols` | `/usr/local/bin/symbols` | 符号搜索服务（`type:symbol`），驱动 ctags | `:3184`（常量 `DEFAULT_ADDR`） | 是 |
| `worker` | `sg-bin-worker` | `/usr/local/bin/worker` | 后台作业（56 个内置 job 组：索引、权限、批量变更、exhaustive、code monitors 等） | 健康检查 `:3189` | 是 |
| `repo-updater` | `sg-bin-repo-updater` | `/usr/local/bin/repo-updater` | 外部服务/仓库同步、clone 调度、仓库清理 | `:3182` | 是 |
| `precise-code-intel-worker` | `sg-bin-precise-worker` | `/usr/local/bin/precise-code-intel-worker` | LSIF/SCIP 上传处理、精确代码智能索引 | `:3188` | 是 |
| `migrator` | `sg-bin-migrator` | `/usr/local/bin/migrator` | 数据库迁移 CLI：`up`/`upto`/`downto`/`validate`/`describe`/`drift`/`add-log`/`upgrade`/`downgrade`/`run-out-of-band-migrations` 全部本地执行（`sg-migrator`） | 无 | 否（一次性） |
| `embeddings` | `sg-bin-embeddings` | `/usr/local/bin/embeddings` | 代码嵌入索引下载与 `POST /search` 接口 | `:9991` | 条件启用（`SRC_ENABLE_EMBEDDINGS=true`） |
| `sourcegraph` | `sg-bin-sourcegraph` | 不安装进镜像 | 开发用组合二进制：在**一个进程**内跑 frontend + gitserver + repo-updater + searcher + worker（对应 Go `cmd/sourcegraph`） | 取决于内部服务 | 否（镜像不使用） |

`server` 被特意安装在根目录 `/server`（而不是 `/usr/local/bin/server`），与 Go
镜像的 `tar_server` 布局一致；其余 9 个服务二进制都在 `/usr/local/bin`。

## 1.2 Go 侧车与外部组件

| 可执行文件 | 来源 | 镜像内路径 | 角色 | 由谁启动 |
| --- | --- | --- | --- | --- |
| `tini` | Ubuntu `tini` 包 | `/sbin/tini` | PID 1，负责回收僵尸进程与信号转发 | Docker `ENTRYPOINT` |
| `nginx` | Ubuntu `nginx` 包 | `/usr/sbin/nginx` | 单容器唯一对外入口，`7080 → 127.0.0.1:8080` | `/server` Procfile |
| `postgres` | Ubuntu `postgresql-16` | `/usr/lib/postgresql/16/bin/postgres` | 本地数据库（sourcegraph / sourcegraph-codeintel / 可选 codeinsights） | `/server` 的 `postgres:` Procfile 行 |
| `postgres_exporter` | Ubuntu 包 | `/usr/bin/prometheus-postgres-exporter` | 导出 Postgres 指标（观测组件禁用时仍会启动该进程） | `/server` Procfile |
| `redis-server` | Ubuntu `redis-server` 包 | `/usr/bin/redis-server` | 两个实例：session/持久存储 `:6379`、缓存 `:6380` | `/server` Procfile |
| `blobstore` | Sourcegraph Go 源码 | `/usr/local/bin/blobstore` | S3 兼容对象存储，保存精确代码智能上传（filesystem backend） | `/server` Procfile |
| `syntect_server` | syntax-highlighter 源码构建 | `/usr/local/bin/syntect_server` | 语法高亮服务（Rocket） | `/server` Procfile |
| `scip-ctags` | syntax-highlighter 源码构建 | `/usr/local/bin/scip-ctags` | 为 scip-ctags 支持语言生成符号/高亮数据 | symbols / syntax-analysis 子进程 |
| `universal-ctags` | Sourcegraph 补丁版（GCS 固定产物） | `/usr/local/bin/universal-ctags` | symbols 服务对 C/C++、Markdown 等非 scip 语言的解析器 | symbols 子进程 |
| `zoekt-sourcegraph-indexserver` | Sourcegraph Go 源码 | `/usr/local/bin/zoekt-sourcegraph-indexserver` | 周期性从 frontend 拉取仓库列表并构建 zoekt 索引 | `/server` Procfile |
| `zoekt-webserver` | 同上 | `/usr/local/bin/zoekt-webserver` | 提供索引搜索（gRPC/RPC）与索引器代理 | `/server` Procfile |
| `zoekt-git-index` | 同上 | `/usr/local/bin/zoekt-git-index` | 索引器按需调用的索引构建子命令 | indexserver 子进程 |
| `zoekt-archive-index` | 同上 | `/usr/local/bin/zoekt-archive-index` | 同上（归档索引） | indexserver 子进程 |
| `prom-wrapper` | Sourcegraph Go 源码 | `/bin/prom-wrapper` | 生成/托管 Prometheus 配置（默认被禁用） | Procfile（观测开启时） |
| `prometheus` / `alertmanager` / `grafana-server` | Ubuntu / 镜像包 | 见 `/prometheus.sh` 等 | 观测栈（默认 `DISABLE_OBSERVABILITY=1` 不启动） | Procfile（观测开启时） |
| `postgres-optimize.sh` / `reindex.sh` | Sourcegraph 源码 | `/postgres-optimize.sh`、`/reindex.sh` | Postgres 参数优化、5.1 版本数据库重建 | `server` / 运维手动 |

## 1.3 为什么有 Go 侧车

本仓库重写了 Go 服务进程与数据库迁移工具。对象存储、zoekt 搜索引擎与
语法高亮仍属于「保持不变的组件」，继续复用 Go 侧车；迁移谱系（schema、
frozen definitions、stitched graph、OOB migrations）已由 `sg-migrator` 原生
实现，镜像不再包含 `migrator-go`。详见 `docker/package/README.md` 与
`docker/server/README.md`。

## 1.4 构建与安装位置

- `cargo build --release --bins` 产出 `target/release/{server,frontend,...}`；
- `docker/package/package.sh` 的 `stage_rust` 把 10 个服务二进制（含 `server`、
  不含组合二进制 `sourcegraph`）`strip --strip-debug` 后放入
  `.package-build/rust-bin/`，再在 `stage_rootfs` 安装到镜像；
- `sourcegraph` 组合二进制只在开发/差分测试中使用，不随镜像发布。
