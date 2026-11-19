# 3. 进程参数与环境变量参考

本文逐个说明单容器镜像里的进程：如何被启动、监听什么、读取哪些环境变量、依赖谁。
默认值取自本仓库代码（`bins/`、`crates/`、`crates/base/sg-entry`）与
`docker/package/package.sh`；同名环境变量（含 `$CONFIG_DIR/env` 文件）可以覆盖。

---

## 0. 公共环境变量

### 0.1 镜像级 ENV（`import-image.sh` 写入镜像元数据）

| 变量 | 值 | 说明 |
| --- | --- | --- |
| `PATH` | `/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/postgresql/16/bin` | 让 `migrator`、`postgres` 等按路径找到 |
| `LANG` / `LC_ALL` | `en_US.utf8` | locale |
| `DISABLE_OBSERVABILITY` | `1` | 关闭 prometheus/grafana 进程 |
| `DISABLE_HYBRID_SEARCH` | `true` | 关闭 searcher 的 zoekt 混合搜索 |
| `BAZEL_SKIP_OOB_INFER_VERSION` | `1` | 兼容变量（无实际行为） |

### 0.2 目录与生命周期

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `/var/opt/sourcegraph` | 持久数据根目录（Postgres、repos、redis、zoekt、blobstore） |
| `CONFIG_DIR` | `/etc/sourcegraph` | 生成配置根目录（`env`、`nginx.conf`、`redis-*.conf`、`ssh/`） |
| `SRC_REPOS_DIR` | `$DATA_DIR/repos` | gitserver 的裸仓库目录 |
| `SYMBOLS_CACHE_DIR` / `SEARCHER_CACHE_DIR` | `$DATA_DIR/cache` | 符号/搜索缓存目录 |
| `SRC_LOG_LEVEL` | 空（`info` 语义） | `dbug`/`info`/`warn`/`error`/`crit`；影响版本/初始化提示与 postgres_exporter 日志级别 |
| `DEPLOY_TYPE` | `docker-container` | 决定 blobstore 默认端点（`http://127.0.0.1:9000`） |
| `INSECURE_DEV` | 空 | 为 `true` 时各服务默认只监听回环地址 |
| `IGNORE_PROCESS_DEATH` | 空 | 为 `true` 时子进程退出不触发整体关闭 |
| `PGRESTORE` | 空 | 为 `true` 时只启动 Postgres，不跑迁移/应用进程 |
| `SOURCEGRAPH_5_1_DB_MIGRATION` | 空 | 必须显式 `true` 才允许执行 5.1 数据库重建 |
| `ALLOW_SINGLE_DOCKER_CODE_INSIGHTS` | 空 | 为 `true` 时额外创建/迁移 `sourcegraph-codeinsights` 库 |
| `SRC_ENABLE_EMBEDDINGS` | 空 | 为 `true` 时追加 `embeddings` 进程与 pprof 条目 |
| `SRC_PROF_HTTP` | 空（服务默认 `:6060`） | 各服务的 pprof/debug 监听地址；为空表示关闭 |
| `SRC_PROF_SERVICES` | `sg-entry` 生成 | 前端 debug 代理的服务清单（JSON） |

### 0.3 数据库与服务发现

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PGHOST` | `127.0.0.1`（本地 Postgres 时） | 主库地址 |
| `PGPORT` | `5432` | 主库端口 |
| `PGUSER` | `postgres` | 主库用户 |
| `PGPASSWORD` | 空 | 主库密码（本地 trust 认证时可为空） |
| `PGDATABASE` | `sourcegraph` | 主库名 |
| `PGSSLMODE` | `disable` | 本地 Postgres 时 |
| `PGDATASOURCE` | 空 | 直接指定 DSN（优先于分散的 PG*） |
| `CODEINTEL_PGHOST` 等 | 同 `PG*` 规则 | 代码智能库（默认 `sourcegraph-codeintel`） |
| `CODEINSIGHTS_PGHOST` 等 | 同 `PG*` 规则 | Code Insights 库（条件启用） |
| `REDIS_ENDPOINT` | 空 | 同时作为 store/cache 的回退地址 |
| `REDIS_STORE_ENDPOINT` | `127.0.0.1:6379` | 会话/持久键值 |
| `REDIS_CACHE_ENDPOINT` | `127.0.0.1:6380` | 缓存/限流 |
| `SRC_GIT_SERVERS` | `127.0.0.1:3178` | gitserver 地址列表（逗号分隔） |
| `SEARCHER_URL` | `http://127.0.0.1:3181` | searcher 地址 |
| `REPO_UPDATER_URL` | `http://127.0.0.1:3182` | repo-updater 地址 |
| `SYMBOLS_URL` | `http://127.0.0.1:3184` | symbols 地址 |
| `SRC_SYNTECT_SERVER` | `http://127.0.0.1:9238` | 语法高亮服务 |
| `SRC_FRONTEND_INTERNAL` | `127.0.0.1:3090` | frontend 内部地址：HTTP GraphQL（`/graphql`，worker 使用）与 gRPC（`ConfigService`、`ZoektConfigurationService`）共用 |
| `INDEXED_SEARCH_SERVERS` / `ZOEKT_HOST` | `127.0.0.1:3070` | zoekt-webserver 地址 |
| `SRC_HTTP_ADDR` | `:8080` | frontend 外部 HTTP |
| `SRC_HTTPS_ADDR` | `:8443` | 基线遗留变量；当前镜像未启用 HTTPS 监听 |
| `SRC_HTTP_ADDR_INTERNAL` | `0.0.0.0:3090`（镜像内实际用于 `127.0.0.1` 访问） | frontend 内部 HTTP |
| `SRC_NGINX_HTTP_ADDR` | `:7080` | 仅作为环境默认值导出；生成的 `nginx.conf` 固定 `listen 7080`，改此变量不会改变实际监听端口 |
| `SRC_SESSION_COOKIE_KEY` | 空 | 会话 cookie 签名密钥（空表示使用基线默认行为） |
| `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` | 空 | 出站代理（拉镜像、访问 code host 等） |

> `sg-entry` 会把最终环境导出给子进程；`$CONFIG_DIR/env` 中的键不会覆盖已存在的
> 进程环境变量（`set_default` 语义）。

---

## 1. `tini`（PID 1）

- 路径：`/sbin/tini`
- 命令行：`/sbin/tini -- /server`（镜像 `ENTRYPOINT`）
- 作用：转发信号给 `/server`、回收孤儿进程。
- 参数/环境：无。

---

## 2. `/server`（Rust `bins/server`，监督器）

- 路径：`/server`
- 启动：`tini` exec 得到；自身拉起 Procfile 子进程。
- 监听：无（不监听端口）。
- 关键行为：
  1. 合并环境（见 0.2/0.3）；
  2. 拷贝 `/etc/sourcegraph/ssh` 与 `netrc`/`gitconfig`；
  3. 生成 `nginx.conf`、`nginx/*.conf`、`redis-store.conf`、`redis-cache.conf`；
  4. 首次启动初始化本地 Postgres（`initdb --nosync`、`pg_ctl`、三个 `createdb`）；
  5. 调 `migrator up -db frontend|codeintel[|codeinsights]`；
  6. 5.1 重建门禁（缺 marker 时要求 `SOURCEGRAPH_5_1_DB_MIGRATION=true`，否则退出码 101）；
  7. 启动 `postgres` → 等待就绪（`wait_for_postgres_ready`）→ 执行迁移 →
     启动应用 Procfile；用进程组监管，SIGINT/SIGTERM/SIGHUP → SIGINT 所有子进程，
     10 秒宽限后 SIGKILL。
- 生成的 Procfile（按顺序）：

  | 名称 | 命令 |
  | --- | --- |
  | `nginx` | `nginx -p . -g 'daemon off;' -c /etc/sourcegraph/nginx.conf ...` |
  | `frontend` | `env CONFIGURATION_MODE=server frontend` |
  | `gitserver` | `env HOSTNAME="<SRC_GIT_SERVERS>" gitserver` |
  | `symbols` | `symbols` |
  | `searcher` | `searcher` |
  | `worker` | `worker` |
  | `repo-updater` | `repo-updater` |
  | `precise-code-intel-worker` | `precise-code-intel-worker` |
  | `syntect-server` | `sh -c 'env QUIET=true ROCKET_ENV=production ROCKET_PORT=9238 ... syntect_server ...'` |
  | `postgres_exporter` | `env DATA_SOURCE_NAME="<dsn>" postgres_exporter --config.file=/postgres_exporter.yaml --log.level=<level>` |
  | `embeddings`（可选） | `embeddings`（`SRC_ENABLE_EMBEDDINGS=true`） |
  | `prometheus` / `grafana`（可选） | 见 0.1（默认关闭） |
  | `blobstore`（可选） | `/opt/s3proxy/run-docker-container.sh` |
  | `redis-store` / `redis-cache`（可选） | `redis-server <CONFIG_DIR>/redis-*.conf` |
  | `zoekt-indexserver` / `zoekt-webserver`（可选） | 见第 17 节 |

- 覆盖开关：`DISABLE_OBSERVABILITY`、`DISABLE_BLOBSTORE`、`REDIS_ENDPOINT`、
  `ZOEKT_HOST`/`INDEXED_SEARCH_SERVERS`、`SRC_ENABLE_EMBEDDINGS`、`PGRESTORE`、
  `IGNORE_PROCESS_DEATH`、`SOURCEGRAPH_5_1_DB_MIGRATION`、
  `ALLOW_SINGLE_DOCKER_CODE_INSIGHTS`、`MIGRATOR_PATH`（迁移器路径）。

---

## 3. `nginx`（对外入口）

- 路径：`/usr/sbin/nginx`
- 命令行：`nginx -p . -g 'daemon off;' -c /etc/sourcegraph/nginx.conf`
- 监听：`7080`（生成的 `nginx.conf` 中固定 `listen 7080;`；`sg-entry` 虽然会
  设置 `SRC_NGINX_HTTP_ADDR=:7080`，但本移植版不对端口做模板替换）
- 上游：`server 127.0.0.1:8080;`（生成的 `nginx/sourcegraph_backend.conf`）
- 行为：`location /` 反向代理到 frontend，设置 `Host`、`X-Forwarded-For`、
  `X-Forwarded-Proto`；大 SAML 头使用更大的 proxy buffer。
- 环境：无特殊变量；上游地址必须与 frontend 的 `SRC_HTTP_ADDR` 一致
  （两者都由 `sg-entry` 的默认值驱动）。

---

## 4. `frontend`（Rust `bins/frontend`）

- 路径：`/usr/local/bin/frontend`
- 命令行：`env CONFIGURATION_MODE=server frontend`
- 监听：
  - 外部：`SRC_HTTP_ADDR`（镜像内默认 `:8080`）
  - 内部：`SRC_HTTP_ADDR_INTERNAL`（默认 `0.0.0.0:3090`）
  - 调试：`SRC_PROF_HTTP`（为空关闭；默认常量 `:6060`）
- 关键环境变量：

  | 变量 | 默认 | 说明 |
  | --- | --- | --- |
  | `SRC_HTTP_ADDR` | `:8080`（`sg-entry` 注入；单独运行 `frontend` 时默认 `:3080`） | UI/API 监听 |
  | `SRC_HTTP_ADDR_INTERNAL` | `0.0.0.0:3090` | 内部 API；`/graphql` 无前缀 |
  | `SRC_PROF_HTTP` | 空 | pprof |
  | `INSECURE_DEV` | 空 | 为 true 时默认只绑回环 |
  | `PG*` / `CODEINTEL_PG*` / `CODEINSIGHTS_PG*` | 见 0.3 | 业务库 |
  | `REDIS_STORE_ENDPOINT` / `REDIS_CACHE_ENDPOINT` | 见 0.3 | 会话/缓存 |
  | `SRC_GIT_SERVERS` | `127.0.0.1:3178` | git 后端 |
  | `SEARCHER_URL` | `http://127.0.0.1:3181` | 未索引搜索 |
  | `SYMBOLS_URL` | `http://127.0.0.1:3184` | 符号服务 |
  | `REPO_UPDATER_URL` | `http://127.0.0.1:3182` | 仓库同步 |
  | `SRC_SYNTECT_SERVER` | `http://127.0.0.1:9238` | 语法高亮 |
  | `INDEXED_SEARCH_SERVERS` / `ZOEKT_HOST` | `127.0.0.1:3070` | zoekt |
  | `SRC_SESSION_COOKIE_KEY` | 空 | 会话 cookie 签名 |

- 依赖：postgres（PostgreSQL SQL）、redis store/cache（RESP）、gitserver/searcher/
  symbols/zoekt-webserver/repo-updater（gRPC）、syntect_server（HTTP）。
- 排障：`curl -s localhost:3090/graphql -d '{"query":"{ __typename }"}'` 可验证内部
  GraphQL；外部匿名访问受站点「私有模式」限制。

---

## 5. `gitserver`（Rust `bins/gitserver`）

- 路径：`/usr/local/bin/gitserver`
- 命令行：`env HOSTNAME="127.0.0.1:3178" gitserver`
- 监听：`GITSERVER_ADDR`（空时 `:3178`；`INSECURE_DEV=true` 时 `127.0.0.1:3178`）
- 关键环境变量：

  | 变量 | 默认 | 说明 |
  | --- | --- | --- |
  | `GITSERVER_ADDR` | `:3178` | 监听地址 |
  | `HOSTNAME` | 由 Procfile 设为 `SRC_GIT_SERVERS` | 对外声明的地址 |
  | `GITSERVER_EXTERNAL_ADDR` | 空 | 外部可达地址 |
  | `SRC_REPOS_DIR` | `$DATA_DIR/repos` | 裸仓库根目录 |
  | `SRC_REPOS_JANITOR_INTERVAL` | 基线默认 | 清理周期 |
  | `SRC_REPOS_JANITOR_DISABLE_DELETE_REPOS_ON_WRONG_SHARD` | 空 | 禁止删除错误分片仓库 |
  | `SRC_REPOS_SYNC_STATE_INTERVAL` / `_BATCH_SIZE` / `_UPSERT_PER_SEC` | 基线默认 | 仓库状态同步节流 |
  | `ENABLE_CUSTOM_GIT_FETCH` | 空 | 仅字面 `true` 生效 |
  | `SRC_FRONTEND_INTERNAL` | `127.0.0.1:3090` | gRPC `ConfigService`：启动时等待初始配置，Postgres DSN 也来自该配置 |

- 依赖：frontend 配置服务（gRPC）、git、Postgres（元数据）、磁盘（`SRC_REPOS_DIR`）。
  对 gitserver 的所有调用方（frontend/searcher/symbols/repo-updater）都使用 gRPC。
- 备注：gitserver 不依赖其他应用进程，是读路径的基础。

---

## 6. `searcher`（Rust `bins/searcher`）

- 路径：`/usr/local/bin/searcher`
- 命令行：`searcher`
- 监听：`SEARCHER_ADDR`（空时 `:3181`；`INSECURE_DEV=true` 时 `127.0.0.1:3181`）
- 关键环境变量：

  | 变量 | 默认 | 说明 |
  | --- | --- | --- |
  | `SEARCHER_ADDR` | `:3181` | 监听地址 |
  | `SEARCHER_CACHE_DIR` | `$DATA_DIR/cache` | 缓存目录 |
  | `SEARCHER_CACHE_SIZE_MB` | `50000` | 缓存上限 |
  | `SRC_GIT_SERVERS` | `127.0.0.1:3178` | 拉取文件内容 |
  | `INDEXED_SEARCH_SERVERS` / `ZOEKT_HOST` | `127.0.0.1:3070` | 混合搜索 |
  | `DISABLE_HYBRID_SEARCH` | 镜像内 `true` | 关闭 zoekt 混合分支 |
  | `MAX_TOTAL_PATHS_LENGTH` | 基线默认 | 路径总长上限 |
  | `PROCESSING_TIMEOUT` | 基线默认 | 单请求处理超时 |
  | `SRC_SEARCHER_EXHAUSTIVE_LOGGING_ENABLED` | 空 | exhaustive 日志 |

- 依赖：gitserver（gRPC 取内容）、可选 zoekt-webserver（gRPC；镜像内
  `DISABLE_HYBRID_SEARCH=true` 时不启用）。

---

## 7. `symbols`（Rust `bins/symbols`）

- 路径：`/usr/local/bin/symbols`
- 命令行：`symbols`
- 监听：`:3184`（常量 `DEFAULT_ADDR`；HTTP 与 gRPC 多路复用）
- 关键环境变量：

  | 变量 | 默认 | 说明 |
  | --- | --- | --- |
  | `SRC_GIT_SERVERS` | `127.0.0.1:3178` | 读取仓库 |
  | `CTAGS_COMMAND` | `universal-ctags` | Sourcegraph 补丁版 universal-ctags（支持 `--_interactive`） |
  | `SCIP_CTAGS_COMMAND` | `scip-ctags` | scip-ctags 支持语言的解析器 |
  | `SYMBOLS_CACHE_DIR` | `$DATA_DIR/cache` | 缓存 |
  | `SYMBOLS_CACHE_SIZE_MB` | `50000` | 缓存上限 |
  | `SYMBOLS_MAX_TOTAL_PATHS_LENGTH` | 基线默认 | 路径上限 |
  | `CTAGS_MAX_FILE_SIZE` | `524288` | 超过则跳过 |
  | `CTAGS_MAX_SYMBOLS` | `2000` | 单文件符号上限 |
  | `CTAGS_PATTERN_LENGTH_LIMIT` | `250` | pattern 长度上限 |
  | `CTAGS_PROCESSES` | 基线默认 | 并发解析进程数 |
  | `MAX_CONCURRENTLY_INDEXING` | `10` | 并发索引仓库数 |
  | `PROCESSING_TIMEOUT` | `2h` | 单仓库处理超时 |
  | `REQUEST_BUFFER_SIZE` | `8192` | 解析请求缓冲 |
  | `MAX_FILE_SIZE_KB` | 基线默认 | 索引时文件上限 |
  | `LOG_CTAGS_ERRORS` | `dev` 部署默认开启 | ctags 错误日志 |
  | `USE_ROCKSKIP` / `ROCKSKIP_REPOS` / `ROCKSKIP_MIN_REPO_SIZE_MB` | 基线默认 | Rockskip 索引 |

- 依赖：gitserver（gRPC 读取仓库）、`universal-ctags`、`scip-ctags`（stdio 子进程）。
- 备注：C/C++、Markdown 等语言走 universal-ctags；缺失时 `type:symbol` 搜索报
  `spawn universal-ctags: No such file or directory`，并连带导致源码页 hover 的
  “Go to definition / Find references” 不出现。

---

## 8. `worker`（Rust `bins/worker`）

- 路径：`/usr/local/bin/worker`
- 命令行：`worker`
- 监听：`:3189`（健康检查；`SRC_PROF_HTTP` 控制 pprof）
- 关键环境变量：

  | 变量 | 默认 | 说明 |
  | --- | --- | --- |
  | `WORKER_JOB_ALLOWLIST` | `all` | 逗号分隔的允许 job 列表 |
  | `WORKER_JOB_BLOCKLIST` | 空 | 逗号分隔的禁止 job 列表（优先于 allowlist） |
  | `SRC_FRONTEND_INTERNAL` | `127.0.0.1:3090` | gRPC `ConfigService`（初始配置/热更新）；部分作业使用 HTTP `/.internal/{graphql,search/stream,compute/stream}`（带/不带前缀均可）；业务数据主要直连 Postgres |
  | `SRC_GIT_SERVERS` / `SEARCHER_URL` / `SYMBOLS_URL` | 见 0.3 | 各后端 |
  | `INDEXED_SEARCH_SERVERS` | `127.0.0.1:3070` | zoekt |
  | `PG*`、`CODEINTEL_PG*`、`CODEINSIGHTS_PG*` | 见 0.3 | 数据库 |
  | `REDIS_STORE_ENDPOINT` / `REDIS_CACHE_ENDPOINT` | 见 0.3 | Redis |
  | 对象存储配置 | 由作业装配注入 | 字段同第 10 节；打包镜像里 `sg-entry` 已把 blobstore 端点写入 `PRECISE_CODE_INTEL_UPLOAD_*` |

- 依赖：frontend 内部地址（gRPC `ConfigService` + HTTP `/.internal/*`）、
  gitserver/searcher/symbols/zoekt（gRPC）、blobstore（S3）、Postgres、
  Redis（store+cache）。

---

## 9. `repo-updater`（Rust `bins/repo-updater`）

- 路径：`/usr/local/bin/repo-updater`
- 命令行：`repo-updater`
- 监听：`:3182`（常量 `DEFAULT_PORT`）
- 关键环境变量：

  | 变量 | 默认 | 说明 |
  | --- | --- | --- |
  | `INSECURE_DEV` | 空 | 回环监听 |
  | `SRC_PROF_HTTP` | 空 | pprof |
  | `DISABLE_BATCH_CHANGES` | 空 | 为 true 时关闭批量变更相关同步 |
  | `DISABLE_REPO_PURGE` | 空 | 为 true 时关闭仓库清理 |
  | `PG*` | 0.3 | `external_services`、`repo` 等表 |
  | `SRC_GIT_SERVERS` | 0.3 | 触发 clone/fetch |
  | `SRC_FRONTEND_INTERNAL` | `127.0.0.1:3090` | gRPC `ConfigService`：初始配置（含 DSN）与热更新 |
  | `HTTP_PROXY` / `HTTPS_PROXY` | 空 | 访问 code host |

- 依赖：frontend 配置服务（gRPC）、Postgres、gitserver（gRPC clone/fetch）、
  外网/code host（HTTPS）。

---

## 10. `precise-code-intel-worker`（Rust `bins/precise-code-intel-worker`）

- 路径：`/usr/local/bin/precise-code-intel-worker`
- 命令行：`precise-code-intel-worker`
- 监听：`:3188`（常量 `DEFAULT_PORT`）
- 关键环境变量：

  | 变量 | 默认 | 说明 |
  | --- | --- | --- |
  | `PRECISE_CODE_INTEL_UPLOAD_BACKEND` | `blobstore`（`sg-entry` 注入） | `blobstore`/`s3`/`gcs` |
  | `PRECISE_CODE_INTEL_UPLOAD_BUCKET` | 基线默认 | bucket 名 |
  | `PRECISE_CODE_INTEL_UPLOAD_MANAGE_BUCKET` | 基线默认 | 是否创建 bucket |
  | `PRECISE_CODE_INTEL_UPLOAD_AWS_ENDPOINT` | `http://127.0.0.1:9000` | blobstore 端点 |
  | `PRECISE_CODE_INTEL_UPLOAD_AWS_REGION` / `_USE_PATH_STYLE` / `_USE_EC2_ROLE_CREDENTIALS` | 基线默认 | S3 选项 |
  | `PRECISE_CODE_INTEL_UPLOAD_AWS_ACCESS_KEY_ID` / `_SECRET_ACCESS_KEY` / `_SESSION_TOKEN` | 空 | S3 凭据 |
  | `PRECISE_CODE_INTEL_UPLOAD_GCP_PROJECT_ID` / `_GOOGLE_APPLICATION_CREDENTIALS_FILE(_CONTENT)` | 空 | GCS 凭据 |
  | `PRECISE_CODE_INTEL_UPLOAD_TTL` | 基线默认 | 上传保留期 |
  | `PRECISE_CODE_INTEL_WORKER_CONCURRENCY` / `_POLL_INTERVAL` / `_BUDGET` / `_MAXIMUM_RUNTIME_PER_JOB` | 基线默认 | 处理节流 |
  | `PRECISE_CODE_INTEL_AUTO_INDEX_MAXIMUM_INDEX_JOBS_PER_INFERRED_CONFIGURATION` | 基线默认 | 推断配置上限 |
  | `PRECISE_CODE_INTEL_DEFINITION_DUMPS_LIMIT` | 基线默认 | 定义导出上限 |
  | `PG*`、`CODEINTEL_PG*` | 0.3 | 上传/索引元数据 |
  | `SRC_FRONTEND_INTERNAL` | `127.0.0.1:3090` | gRPC `ConfigService`：初始配置与热更新 |

- 依赖：frontend 配置服务（gRPC）、Postgres（主库 + codeintel 库）、blobstore
  或云对象存储。

---

## 11. `embeddings`（Rust `bins/embeddings`，可选）

- 启用条件：`SRC_ENABLE_EMBEDDINGS=true`（`/server` 追加 Procfile 与 pprof 条目）
- 路径：`/usr/local/bin/embeddings`
- 命令行：`embeddings`
- 监听：`:9991`（`POST /search`）
- 关键环境变量：

  | 变量 | 默认 | 说明 |
  | --- | --- | --- |
  | `EMBEDDINGS_CACHE_SIZE` | 基线默认 | 索引缓存大小 |
  | `EMBEDDINGS_UPLOAD_BACKEND` | 必填（`S3`/`GCS`/`Blobstore`） | 索引存储 |
  | `EMBEDDINGS_UPLOAD_BUCKET` | `embeddings` | bucket |
  | `EMBEDDINGS_UPLOAD_MANAGE_BUCKET` | 基线默认 | 创建 bucket |
  | `EMBEDDINGS_UPLOAD_AWS_*` / `EMBEDDINGS_UPLOAD_GCP_*` | 同第 10 节语义 | 云存储凭据 |
  | `PG*` | 0.3 | `repo_embedding_jobs` 等 |
  | `SITE_CONFIG` / `SITE_CONFIG_FILE` | 空 | 离线站点配置（测试/特殊部署） |
  | `FORCE_ALLOW_EMBEDDINGS` | 空 | 绕过站点开关 |

- 依赖：Postgres、对象存储、站点配置中的 embeddings 服务地址。

---

## 12. `migrator`

- 路径：`/usr/local/bin/migrator`（crate `sg-bin-migrator`，引擎 `sg-migrator`）
- 调用方式：`migrator up -db <frontend|codeintel|codeinsights>`；无参数时默认
  `up`（与 Go `urfave/cli` 一致）。
- 本地子命令：`up`、`upto`、`downto`、`validate`、`describe`、`drift`、
  `add-log`、`upgrade`、`downgrade`、`run-out-of-band-migrations`。
- 迁移语料：`MIGRATIONS_DIR`（默认 `/migrations`），其中包含各 schema 的
  `<version>_<slug>/{up,down}.sql`、`metadata.yaml`、`schema*.json` 与
  `frozen/4.5.0.json`。stitched migration graph 编译时内嵌。
- 多数据库路由：`run-out-of-band-migrations` 同时连接 frontend、codeintel，
  必要时连接 codeinsights；每个 migrator 按其 Go 基线选择对应连接池。
- 环境：`PG*`/`CODEINTEL_PG*`/`CODEINSIGHTS_PG*`、`MIGRATIONS_DIR`；可选的
  `MIGRATOR_DELEGATE` 会改为把所有命令转发给指定程序（默认不再委托）。
- 时机：`/server` 在启动应用进程前依次执行；`PGRESTORE=true` 时跳过。

---

## 13. `postgres`

- 路径：`/usr/lib/postgresql/16/bin/postgres`
- 命令行（Procfile）：
  `su-exec postgres sh -c 'postgres -c listen_addresses=127.0.0.1 -D /var/opt/sourcegraph/postgresql'`
- 监听：`127.0.0.1:5432`
- 数据目录：`$DATA_DIR/postgresql`
- 初始化（`/server` 首次启动）：
  - `mkdir -p /run/postgresql`、`chown -R postgres /run/postgresql`
  - `su-exec postgres initdb -D <dir> --nosync`
  - `su-exec postgres pg_ctl -D <dir> -o "-c listen_addresses=127.0.0.1" -l /tmp/pgsql.log -w start`
  - `createdb sourcegraph`、`createdb sourcegraph-codeintel`、可选
    `createdb sourcegraph-codeinsights`
  - 写入 `$DATA_DIR/postgresql-markers/<db>` 标记
- 连接参数：见 0.3；外部数据库可通过 `PGHOST`/`PGDATASOURCE` 等覆盖（此时本地初始化跳过）。

---

## 14. `redis-server`（store / cache）

- 路径：`/usr/bin/redis-server`
- 命令行：
  - `redis-server /etc/sourcegraph/redis-store.conf | tail -n 1`
  - `redis-server /etc/sourcegraph/redis-cache.conf | tail -n 1`
- 监听：`6379`（store）、`6380`（cache），配置 `protected-mode no`
- 数据目录：`$DATA_DIR/redis`、`$DATA_DIR/redis-cache`
- 配置差异：
  - store `:6379`：`maxmemory 1gb`、`maxmemory-policy noeviction`、AOF 开启、
    快照 `save 300 1`
  - cache `:6380`：`maxmemory 1gb`、`maxmemory-policy allkeys-lru`、AOF 关闭、
    快照 `save 60 1`
- 覆盖：设置 `REDIS_ENDPOINT` 或对应的 `REDIS_STORE_ENDPOINT`/`REDIS_CACHE_ENDPOINT`
  后，本地实例不再启动。

---

## 15. `blobstore`（Rust，S3 兼容）

- 路径：`/usr/local/bin/blobstore`（crate `sg-bin-blobstore` / `sg-service-blobstore`），
  包装脚本 `/opt/s3proxy/run-docker-container.sh`
- 命令行：`/opt/s3proxy/run-docker-container.sh`（设置数据目录后 `exec blobstore`）
- 监听：`DEPLOY_TYPE=docker-container` 或 `INSECURE_DEV` 时为
  `127.0.0.1:9000`，否则 `0.0.0.0:9000`（与 Go `deploy.BlobstoreHostPort` 一致）
- 存储：`BLOBSTORE_DATA_DIR`（包装脚本取 `JCLOUDS_FILESYSTEM_BASEDIR`，
  默认 `$DATA_DIR/blobstore`）；对象落盘为
  `buckets/<bucket>/<url.QueryEscape(key)>`，与 Go 布局相同
- 支持的 S3 子集：`CreateBucket`、`Put/Get/Head/DeleteObject`、
  `ListObjectsV2`、`CreateMultipartUpload`/`UploadPart`/`UploadPartCopy`/
  `CompleteMultipartUpload`/`AbortMultipartUpload`、`DeleteObjects`；
  `GetObject` 额外支持 `Range`
- 环境变量：`BLOBSTORE_DATA_DIR`；`DEPLOY_TYPE`/`INSECURE_DEV` 只影响监听地址。
  不再依赖 `S3PROXY_*`/`JCLOUDS_*`，也不会连接 frontend 配置服务
- 关闭：`DISABLE_BLOBSTORE=1`（此时需自行提供外部对象存储）

---

## 16. 语法与符号工具

### `syntect_server`
- 路径：`/usr/local/bin/syntect_server`
- 命令行：`env QUIET=true ROCKET_ENV=production ROCKET_PORT=9238 ROCKET_LIMITS={json=10485760} ROCKET_KEEP_ALIVE=0 ROCKET_ADDRESS=127.0.0.1 syntect_server`
- 监听：`127.0.0.1:9238`
- 用途：语法高亮（`GitBlob.highlight`）

### `scip-ctags`
- 路径：`/usr/local/bin/scip-ctags`
- 用途：scip-ctags 支持语言（Go/Java/Python/Rust/Scala/TypeScript 等）的符号解析；
  symbols 服务通过 `SCIP_CTAGS_COMMAND` 调用。

### `universal-ctags`
- 路径：`/usr/local/bin/universal-ctags`
- 来源：Sourcegraph 补丁版（`dev/tool_deps.bzl` 固定
  `6.0.0.2783f009`，打包时 sha256 校验）
- 用途：C/C++、Markdown 等 scip 清单外语言的 `type:symbol` 解析；
  依赖 `--_interactive` 扩展协议。
- 注意：Ubuntu 仓库里的 `universal-ctags` 不带该协议，必须使用补丁版。

---

## 17. zoekt 索引与搜索

### `zoekt-sourcegraph-indexserver`
- 路径：`/usr/local/bin/zoekt-sourcegraph-indexserver`
- 命令行：`env GOGC=25 HOSTNAME=127.0.0.1:3070 zoekt-sourcegraph-indexserver -sourcegraph_url http://127.0.0.1:3090 -index $DATA_DIR/zoekt/index -interval 1m -listen 127.0.0.1:6072 -cpu_fraction 0.25`
- 依赖：frontend 内部地址上的 **gRPC `ZoektConfigurationService`**
  （`List`/`SearchConfiguration`/`UpdateIndexStatus`）、git、索引目录；
  `-sourcegraph_url` 的 host 用作 gRPC 拨号地址，不是 GraphQL。
- 实现现状：frontend 已在内部监听器上提供该 gRPC 服务：`List` 返回已克隆
  仓库，`SearchConfiguration` 复用 `sg-zoekt` 的索引选项并返回站点配置
  fingerprint，`UpdateIndexStatus` 写 `zoekt_repos`；`DocumentRanks` 暂返回
  空 ranks（不使用离线排序）。fingerprint 仅用于返回给客户端，未实现
  `MinLastChanged` 过滤（每次返回全部请求仓库，语义安全、效率略低）。
- 监听：`127.0.0.1:6072`（索引器自身的 HTTP debug/pprof 端口；与
  webserver 的 `/indexserver/` 代理无关，后者走索引目录下的 unix socket）

### `zoekt-webserver`
- 路径：`/usr/local/bin/zoekt-webserver`
- 命令行：`env GOGC=25 zoekt-webserver -rpc -pprof -indexserver_proxy -listen 127.0.0.1:3070 -index $DATA_DIR/zoekt/index`
- 监听：`127.0.0.1:3070`（gRPC/RPC 搜索端口）
- 代理：`-indexserver_proxy` 把 `/indexserver/` 前缀的 HTTP 请求经
  `<index>/indexserver.sock`（unix socket）转发给索引器
- 依赖：索引目录、zoekt-indexserver（unix socket 代理）

### `zoekt-git-index` / `zoekt-archive-index`
- 由 indexserver 按需调用，不常驻。

关闭本地 zoekt：设置 `ZOEKT_HOST` 或 `INDEXED_SEARCH_SERVERS` 指向外部实例，
Procfile 不再添加这两个进程。

---

## 18. `postgres_exporter`

- 路径：`/usr/bin/prometheus-postgres-exporter`
- 命令行：
  `env DATA_SOURCE_NAME="<dsn>" postgres_exporter --config.file="/postgres_exporter.yaml" --log.level=<debug|info|warn|fatal>`
- `DATA_SOURCE_NAME` 由 `sg-entry` 用主库 DSN 生成；`--log.level` 由
  `SRC_LOG_LEVEL` 转换（`dbug→debug`、`crit→fatal`）
- 默认监听 `:9187`
- 仅在启用 `observability` 打包特性时安装并启动；默认镜像没有该二进制，
  `/server` 会跳过对应 Procfile 行

---

## 19. 可选观测组件（默认关闭）

`DISABLE_OBSERVABILITY=1`（镜像 ENV）时以下进程不启动：

| 名 | 命令 | 默认地址 |
| --- | --- | --- |
| `prometheus` | `env STORAGE_PATH=/var/opt/sourcegraph/prometheus /bin/prom-wrapper` | `:9090` |
| `grafana` | `/usr/share/grafana/bin/grafana-server -config /sg_config_grafana/grafana-single-container.ini -homepath /usr/share/grafana` | `:3370` |

未设置 `DISABLE_OBSERVABILITY` 时会追加这两行，并使用
`$CONFIG_DIR/grafana` 等目录。

`prom-wrapper` 为 Rust 实现（`sg-bin-prom-wrapper`）：

- 路径：`/bin/prom-wrapper`（Procfile 契约；Go 版曾构建自
  `docker-images/prometheus/cmd/prom-wrapper`）
- 启动 `/prometheus.sh` 与 `/alertmanager.sh`，等待 Alertmanager `:9093`
  就绪后开始订阅站点配置
- 监听 `EXPORT_PORT`（默认 `:9090`）：`/` 反代 Prometheus `:9092`，
  `/alertmanager/*` 反代 Alertmanager `:9093`，并提供
  `/prom-wrapper/config-subscriber` 与 `/prom-wrapper/alerts-status`
- 环境：`EXPORT_PORT`、`PROMETHEUS_INTERNAL_PORT`、
  `ALERTMANAGER_INTERNAL_PORT`、`ALERTMANAGER_CONFIG_PATH`、
  `ALERTMANAGER_ENABLE_CLUSTER`、`DISABLE_ALERTMANAGER`、
  `DISABLE_SOURCEGRAPH_CONFIG`、`OPSGENIE_API_KEY`、
  `SRC_FRONTEND_INTERNAL`
- 观测栈是可选打包特性：默认镜像不包含 Prometheus/Alertmanager/
  prom-wrapper/配置/规则/target；使用
  `./docker/package/package.sh --features observability` 打包后才会安装。
  启用特性的镜像仍默认 `DISABLE_OBSERVABILITY=1`，设置空值即可启动观测。
  Grafana 未打包（`/server` 检测到二进制缺失时跳过 grafana 进程）。

---

## 20. 常用运维命令

```bash
# 查看进程树与监听
docker exec sg-rust ps -eo pid,ppid,args --sort=pid
docker exec sg-rust ss -ltnp

# 内部 GraphQL（内部 actor，不受私有模式登录限制）
docker exec sg-rust sh -c 'curl -s localhost:3090/graphql \
  -H "content-type: application/json" \
  -d "{\"query\":\"{ __typename }\"}"'

# 外部页面
curl -i http://127.0.0.1:7080/healthz

# Postgres / Redis
docker exec sg-rust su-exec postgres psql -h 127.0.0.1 -U postgres -d sourcegraph -c '\dt' | head
docker exec sg-rust redis-cli -p 6379 ping
docker exec sg-rust redis-cli -p 6380 ping

# 符号工具
docker exec sg-rust universal-ctags --version
docker exec sg-rust scip-ctags --version

# 环境与生成配置
docker exec sg-rust sh -c 'cat /etc/sourcegraph/env 2>/dev/null; ls /etc/sourcegraph'
docker exec sg-rust env | sort
```

## 21. 环境变量覆盖示例

```bash
docker run -d --name sg-rust -p 7080:7080 \
  -v sg-rust-data:/var/opt/sourcegraph \
  -e SRC_LOG_LEVEL=dbug \
  -e REDIS_CACHE_ENDPOINT=127.0.0.1:6381 \
  -e SRC_ENABLE_EMBEDDINGS=true \
  sourcegraph/server:rust
```

> 修改 `DATA_DIR`、`CONFIG_DIR`、`SRC_HTTP_ADDR`、`SRC_NGINX_HTTP_ADDR` 等启动级
> 变量时，需要同步调整卷/端口映射；`$CONFIG_DIR/env` 文件也可提供默认值（不覆盖
> 已存在的进程环境变量）。
