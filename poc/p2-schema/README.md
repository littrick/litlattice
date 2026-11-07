# P2 — SDL vs Go runtime introspection

验证 Rust 侧 GraphQL schema 模型与 Go baseline 的运行时 schema 是否一致，并固化差异。

## 输入

- SDL：`dev/origin/repo/cmd/frontend/graphqlbackend/*.graphql`（29 个文件，Go `//go:embed` 的同一集合）。
- 运行时 fixture：`testdata/graphql/introspection-go.json`，从运行中的 Go frontend（`POST /.api/graphql`，`SOURCEGRAPHDOTCOM_MODE=false`，企业 resolver 已编译）导出。
- 派生清单：`testdata/graphql/root-fields-runtime.json`（Query/Mutation 字段名）。

## 运行

```bash
. "$HOME/.cargo/env"
cargo run --release -- \
  /home/developer/ws/dev/origin/repo/cmd/frontend/graphqlbackend \
  /home/developer/ws/dev/rs/repo/testdata/graphql/introspection-go.json \
  dotcom
```

退出码 0 表示：SDL 与运行时根字段完全一致，除显式声明的受环境门控字段外无差异。

## 结果（2026-11-07）

```text
Query: sdl=130 runtime=129
  sdl-only (expected gated): ["dotcom"]
Mutation: sdl=206 runtime=205
  sdl-only (expected gated): ["dotcom"]
OK: root fields match modulo expected gating {"dotcom"}
```

运行时类型统计（fixture）：

| kind | count |
|---|---:|
| OBJECT | 485 |
| INPUT_OBJECT | 98 |
| ENUM | 94 |
| UNION | 28 |
| INTERFACE | 16 |
| SCALAR | 11 |
| **合计** | **732** |

阶段 1 记录的“488 type / 336 根字段”来自静态 SDL 的粗粒度统计，与运行时口径不同；
以本 fixture（732 types，129 Query + 205 Mutation）为契约基线。

## 关键结论

### 1. `dotcom` 是环境门控，不是遗漏

- SDL 中 `dotcom.graphql` 定义 `extend type Query { dotcom: DotcomQuery! }` 与
  `extend type Mutation { dotcom: DotcomMutation! }`；
- `graphqlbackend.NewSchema` 仅在 `OptionalResolver.DotcomRootResolver != nil` 时追加
  `dotcomSchema`；
- `cmd/frontend/internal/dotcom/init.go` 仅在 `SOURCEGRAPHDOTCOM_MODE=true` 时注册该 resolver。

因此运行时 schema 是 **env + 企业初始化结果的函数**，不是静态文件集合。Rust 必须复刻
同样的组装规则，并以目标实例的 introspection 作为契约测试基准。

### 2. schema 组装规则（`graphqlbackend.NewSchema`）

固定包含：`mainSchema`、`outboundWebhooksSchema`、`viewerSchema`。

按 resolver 是否存在追加（每个都对应一个企业 init，通常还有各自的 `IsEnabled()` 或许可校验）：

`batches`、`codeintel*`（多文件）、`insights`、`authz`、`codeMonitors`、`savedSearches`、
`gitHubApps`、`license`、`dotcom`、`searchContexts`、`notebooks`、`compute`、
`insightsAggregations`、`webhooks`、`embeddings`、`codyContext`、`rbac`、`own`、
`completions`、`guardrails`、`contentLibrary`、`searchJobs`、以及后续 telemetry 等。

已知门控示例：

| 扩展 | 门控 |
|---|---|
| dotcom | `SOURCEGRAPHDOTCOM_MODE=true` |
| batches | `batches.IsEnabled()`（license/feature flag） |
| codeMonitors | `codemonitors.IsEnabled()` |
| searchContexts | `searchcontexts.IsEnabled()` |
| notebooks | `notebooks.IsEnabled()` |
| insights | `insights.IsEnabled()`；单容器下注册 Disabled resolver（仍注册，schema 存在） |
| license | 总是注册（`licensing/init`） |
| compute / savedSearches / authz / codeintel | 初始化成功即注册 |

**设计含义**：`sg-graphql` 的 schema 构造器需要接收一个 `FeatureSet`（由 config/env 计算），
输出类型集合与根字段；golden 测试对每个 feature 组合断言 introspection diff 为空。

### 3. 解析器选择

`graphql-parser 0.4` 能正确解析全部 29 个 SDL 文件（含 `extend type`、多行描述、
指令定义），适合构建静态 schema 与生成 golden。P2 的对比程序即为 `sg-graphql` schema
装载器的雏形。

## 复现运行时 fixture

```bash
curl -s -b test_ws/run/cookies2.txt \
  -H 'Content-Type: application/json' \
  -H 'X-Requested-With: Sourcegraph' \
  --data '{"query":"query { __schema { types { kind name fields { name } } } }"}' \
  http://127.0.0.1:3080/.api/graphql > testdata/graphql/introspection-go.json
```

注意：私有实例需要登录 cookie；`X-Requested-With` 用于通过 CSRF 检查。
