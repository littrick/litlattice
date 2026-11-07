# P5 — sqlx 类型映射回环

对真实 Sourcegraph schema（PG16 基线）验证 sqlx 0.8 的类型解码/编码，并与 Go
`database/sql + pgx/v5` 的结果做语义对比。写入仅使用 `TEMP TABLE`，不改动既有表。

## 运行

```bash
export DATABASE_URL=postgres://sourcegraph:sourcegraph@127.0.0.1:5432/sourcegraph
. "$HOME/.cargo/env"
cargo run --quiet > ../../testdata/sqlx-types-rust.json
# Go 侧探针（不进入仓库）：/home/developer/ws/test_ws/p5-go
(cd /home/developer/ws/test_ws/p5-go && go run . > .../testdata/sqlx-types-go.json)
./compare.py ../../testdata/sqlx-types-go.json ../../testdata/sqlx-types-rust.json
```

`compare.py` 做语义比较（时间按瞬时、numeric 按十进制值、PG 数组按元素），
表示差异单独列出但不判失败。

## 结果（2026-11-07）

```text
NOTE  user.created_at format: ...Z vs ...+00:00
NOTE  user.updated_at format: ...Z vs ...+00:00
NOTE  roundtrip[0].num representation: 12345678901234567890.123456789 vs ...789000
NOTE  session timezone: Go=Etc/UTC Rust=UTC (sqlx forces TimeZone=UTC)
P5 SEMANTIC MATCH
```

覆盖类型：

| 类型 | Go（pgx） | Rust（sqlx） | 语义 |
|---|---|---|---|
| `citext`（`users.username`） | 直接 Scan 到 string | 直接 `String` 解码成功 | 一致 |
| `timestamptz` | `time.Time`（UTC） | `DateTime<Utc>` | 一致（微秒精度保留） |
| 带偏移输入 `+02` | 同一瞬时 | 同一瞬时 | 一致 |
| `jsonb` | `[]byte` → `json.Unmarshal` | `serde_json::Value` | 一致（PG 会规范化键序） |
| `text[]`（含 `"b,c"`、NULL） | 数组文本 `{a,"b,c",NULL}` | `Vec<Option<String>>` | 一致 |
| `bytea` | `[]byte` | `Vec<u8>` | 一致 |
| `bigint` | `int64` | `i64`（含 2^53+1） | 一致 |
| `numeric` | string（PG 文本） | `BigDecimal` | 数值一致，文本形式不同 |
| NULL | `sql.Null*` | `Option<T>` | 一致 |
| SQL commenter 前缀 | 可执行 | 可执行 | 一致 |

## 关键发现

### 1. sqlx 强制 `TimeZone=UTC`（必须处理）

`sqlx-postgres-0.8.6/src/connection/establish.rs:33` 在启动参数中硬编码
`("TimeZone", "UTC")`；Go pgx 不设置，沿用服务器默认（本基线为 `Etc/UTC`）。

影响：

- `timestamptz` 的瞬时读写不受影响；
- **`date_trunc`、`to_char`、`date` 转换、`timestamp without time zone` 的解释**依赖 session
  时区，若生产服务器默认不是 UTC，Go/Rust 结果会不同；
- 任何显式 `now()::date` 类 SQL 都可能偏移。

处置建议：`sg-db` 的连接初始化 `after_connect` 执行 `SET TIME ZONE DEFAULT`（恢复服务器
默认），并加入契约测试：两侧 `show timezone`、`date_trunc('day', '...')` 结果一致。
不要依赖 sqlx 默认值。

### 2. `numeric` 文本形式会补零

PG numeric 用 base-10000 存储，sqlx 解码时按 4 位一组计算 scale：
`12345678901234567890.123456789`（scale 9）读回为 `...123456789000`（scale 12）。
十进制值相等，但字符串化结果不同。

处置建议：

- 需要与 Go 文本输出逐字一致的字段用 `col::text` 查询（Go `database/sql` 扫 numeric 到
  string 时拿到的是 PG 文本）；
- 或在序列化层对 `BigDecimal` 去尾零（`normalized()`）；
- GraphQL 的 `BigInt`/数值标量输出需以 Go 的实际文本为准做 golden。

### 3. `citext` 可直接按 `String` 解码

`users.username` 为扩展类型 `citext`；sqlx 将其按文本协议解码为 `String` 成功，无需
`#[sqlx(type_name = "citext")]` 或显式 cast。但：

- `sqlx::query_as!` 宏 / 编译期校验需要 `citext` 类型信息，离线模式可能报 unknown type；
- 排序/比较语义仍由 PG 的 citext 决定，Rust 侧不要用 `String` 做大小写不敏感比较来替代；
- 若使用 `query_as` derive 结构体，建议统一 `username::text as username` 以避免类型元数据问题。

### 4. 其他

- `jsonb` 的键序由 PG 规范化（先按长度再按字节），Go/Rust 解码后的 `map` 语义一致；
  不要依赖原始键序。
- 微秒精度：`timestamptz` 输入输出一致（PG 精度为微秒，纳秒会被截断）。
- `text[]` 中的逗号、NULL 元素：sqlx `Vec<Option<String>>` 与 PG 数组语义一致。
- SQL commenter：sqlx 没有 sqlhooks/otelsql 等价钩子，`sg-db` 需要在构造 SQL 时前置
  `/*{...}*/` 注释；P5 已验证带注释的查询可正常执行。
- 连接池参数需显式设置以匹配 Go（`SRC_PGSQL_MAX_OPEN=30`、`MAX_IDLE=30`、idle 1m），
  sqlx 默认值不同（M5 `08_data_access.md`）。
