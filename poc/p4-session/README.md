# P4 — 会话 cookie 与 Redis gob 兼容性

验证 Rust 能否与 Go 基线**双向**互通 Sourcegraph 的会话格式：

- HTTP cookie `sgs`：`gorilla/securecookie` v1.1.1（HMAC-SHA256 + gob 序列化）；
- Redis `session_<id>`：`boj/redistore` `GobSerializer` 写出的
  `map[interface{}]interface{}`。

结论：**可以做到字节级兼容**。Rust 解出的 cookie/Redis 值与 Go 完全一致；Rust
重新编码的 cookie 与原 cookie 逐字节相同；单条目 gob map 与 Go 编码逐字节相同；
多条目 map 双方都能解码（字节序不保证，见 §7.1）。

## 运行

```bash
./verify.sh            # 全量检查（cargo test + Go 交叉验证 + Redis 端到端）
SKIP_GO=1 ./verify.sh  # 没有 Go 时只跑 Rust 侧
```

依赖：cargo、python3、Go 1.22（`/usr/local/go/bin`）、可选 Redis
（`REDIS_ADDR`，默认 `127.0.0.1:6379`；不可达时跳过 Redis 检查）。

```text
PASS  cookie-empty-key decode id / decode ts / re-encode byte-identical
PASS  cookie-empty-key wrong key rejected / expired rejected / within max age accepted
PASS  cookie-empty-key Go decodes
PASS  cookie-test-key  ...（同上，非空 HMAC key）
PASS  redis-empty / redis-single / redis-multi Rust decodes Go payload
PASS  empty map re-encode byte-identical
PASS  single-entry map re-encode byte-identical
PASS  multi-entry map Go decodes Rust payload
PASS  multi-entry map Rust round-trip
PASS  actor sessionInfo re-encode byte-identical
PASS  gen-id base32 52 chars
PASS  redis-get reads Rust payload
PASS  Go reads Rust cookie + payload
PASS  redis-get reads Go payload written by redis-cli
P4 ALL CHECKS PASSED
```

另外用基线运行中抓到的真实 cookie 验证过：Rust 解出 session ID
`OUYT6S23AHN2ZYTKWKMU7YFJT4DGO22FMINRI7TGSVV63QG2BWGA`，`session_<id>` 长度 193
字节，`actor` JSON 143 字节，与 Go 探针输出一致。

## 文件

```text
src/gob.rs          受限 gob 编解码器（解码任意会话值；编码 string / map）
src/securecookie.rs gorilla/securecookie v1.1.1 兼容实现
src/session.rs      sessionInfo/Actor 模型、Redis key/TTL、session ID 生成
src/main.rs         CLI（decode/encode cookie、gob、parse-actor、redis-get/set）
go-fixture/         Go 参考实现：生成 fixture、解码 Rust 产物
verify.sh           验收脚本
../../testdata/sessions/   已签入的 fixture
```

CLI：

```text
decode-cookie <cookie> [--key K] [--now N]
encode-cookie <id> [--key K] [--ts N]
decode-gob-string <hex|->          decode-map <hex|->
encode-map <json|->                parse-actor <json|->
gen-id
redis-get <cookie> [--redis A]     redis-set <id> <json|->   redis-del <id>
```

## 1. Cookie 契约

`cmd/frontend/internal/auth/session/session.go` + `gorilla/securecookie` v1.1.1：

| 项 | 值 |
|---|---|
| 名称 | `sgs` |
| HMAC 算法 | HMAC-SHA256 |
| 密钥 | `SRC_SESSION_COOKIE_KEY`；基线未设置 → 空密钥（HMAC 照常工作，安全风险） |
| 加密 | 无（`securecookie.New(key, nil)`，只认证不加密） |
| 序列化 | 默认 `GobEncoder`：session ID 字符串的 gob 编码 |
| `maxAge` | 30 天（`SecureCookie` 默认值，Sourcegraph 未覆盖）；decode 时过期即拒绝 |
| `minAge` | 0 |
| `maxLength` | 4096 |

wire 格式（全部为 padded `base64.URLEncoding`）：

```text
cookie      = b64url("<unix秒>|<b64url(gob(id))>|<MAC>")
MAC         = HMAC-SHA256(key, "sgs|<unix秒>|<b64url(gob(id))>")
```

gob(id) 例如 `370c00344f5559...`：`0x37` 消息长度，`0x0c` = int(6)=string，
`0x00` singleton 字段增量，`0x34` 长度 52，随后 52 字节 base32 ID。

Set-Cookie 属性（`redistore.Save` → `sessions.NewCookie`，`Options` 来自
`newSessionStore`）：

```text
Path=/; HttpOnly; Max-Age=2592000; Expires=<now+30d>
SameSite=Lax（HTTP 外部 URL）/ SameSite=None + Secure（HTTPS 外部 URL）
```

删除会话：`MaxAge=-1` → `DEL session_<id>` + `sgs=; Path=/; Max-Age=0; Expires=Thu, 01 Jan 1970...`。

会话 ID：`securecookie.GenerateRandomKey(32)`（32 字节随机）→
`base32.StdEncoding` 去掉 `=` → **52 字符** `[A-Z2-7]`。

## 2. Redis 契约

| 项 | 值 |
|---|---|
| key | `session_<id>`（`redistore.SetKeyPrefix("session_")`） |
| value | gob `map[interface{}]interface{}` |
| TTL | `SETEX`，秒数 = `session.Options.MaxAge` = 2592000（30 天） |
| 缺失 key | 视为新会话（`load` 返回 no data，不报错） |
| 序列化器 | `redistore.GobSerializer`（每次 `Save` 新建 `gob.Encoder`） |

value 的键/值类型：**键 `string`、值 `[]byte`**。值来自
`session.SetData(key, value)` 的 `json.Marshal(value)`；基线写入的键：

| key | 内容 |
|---|---|
| `actor` | `sessionInfo` JSON；`null` 表示登出 |
| `oauthState` | OAuth state 字符串（如 `"state-123"`） |
| `oidcState` | OIDC state 字符串 |
| `oidc@0` | `openidconnect.SessionKey`，OIDC 登录中间态 JSON |
| `soap@0` | `sourcegraphoperator.SessionKey` |

gob 形状（单条目 `{"actor": [...]}`）：

```text
0d7f040102ff8000011001100000                          # 类型定义：map[interface{}]interface{} = id 64
 7f        int(-64)
 04         wireType.MapT
 01 02 ff80  MapT.CommonType.Id = 64（Name 为空被省略）
 00         结束 CommonType
 01 10       key 字段，interface(id 8)
 01 10       elem 字段，interface(id 8)
 00 00      结束 MapT / wireType

25ff80000106737472696e670c0700056163746f72075b5d75696e74380a07000568656c6c6f
 25       消息长度
 ff80     int(64) 顶层类型 id
 00       singleton 字段增量
 01       map 条目数
 ...      interface 编码：uint(名称长度) 名称 int(具体类型 id) uint(长度) 值
          key:  "string"  id 6(string) → 00 05 "actor"
          elem: "[]uint8" id 5(bytes)  → 00 05 "hello"
```

`src/gob.rs` 的解码器覆盖 Go 会话可能写出的全部值类型（bool/int/uint/float/
complex/bytes/string/interface/slice/array/map/struct/GobEncoder）；编码器只实现
上表两种形状，并断言使用新编码器时类型 id 恒为 64（Go 基线每次 `Save` 新建
`gob.Encoder`，因此成立）。

## 3. 会话数据模型

`actor` 值的 JSON 由 Go 结构体字段顺序决定：

```json
{"actor":{"UID":1},"lastActive":"2026-11-07T15:19:15.703398646Z","expiryPeriod":7776000000000000,"userCreatedAt":"2026-11-07T13:56:25.553432Z"}
```

- `Actor`：`UID int32 omitempty`、`AnonymousUID string omitempty`、
  `Internal bool omitempty`、`SourcegraphOperator bool omitempty`；
  `FromSessionCookie` 是 `json:"-"`，不出现。
- `expiryPeriod`：`time.Duration` → **纳秒整数**（90 天 = 7776000000000000）。
- `time.Time` → RFC3339Nano：最多 9 位小数，**去掉尾零**，UTC 用 `Z`，否则保留
  原时区偏移。`src/session.rs::GoTime` 复刻该格式；`parse-actor` 子命令校验
  Rust 重编码与 Go 字节一致。
- `lastActive` 是 `time.Now()`（本地时区），`userCreatedAt` 来自数据库
  （UTC）。Rust 必须沿用同一时区来源，否则 JSON 文本不同（瞬时值仍相同）。

会话有效期并不依赖 Redis TTL，而是由 `authenticateByCookie` 检查
`lastActive + expiryPeriod < now`；超时后删除会话。

## 4. 兼容判定（写入 `interfaces/` 的结论）

| 面 | 兼容等级 | 依据 |
|---|---|---|
| cookie 解析（读既有会话） | 字节级 | Rust 解码 = Go 解码；坏 MAC/过期拒绝 |
| cookie 生成（登录/登出） | 字节级（给定时间戳） | 重编码逐字节相同 |
| Redis 读取 | 字节级 | 单/多条目 Go 载荷均正确解码 |
| Redis 写入 | 单条目字节级；多条目语义级 | 见 §7.1 |
| 会话 ID 生成 | 格式级 | 32 随机字节 → 52 字符 base32 |

## 5. 待实现行为（M7 输入）

1. `SetData`：`json.Marshal(value)` → `session.Values[key] = bytes` → 重新
   `SETEX` 全部值（不是增量更新，整张 map 重写）。
2. `GetData`：key 不存在时**不报错**、不改目标值；存在但 JSON 失败时报错。
3. cookie 解码失败（坏 MAC/过期/gob 错）→ `Get` 返回错误；上层
   `authenticateByCookie` 记录日志并删除 cookie（匿名继续），不是 500。
4. Redis 连接错误 → `Get` 的错误链包含 `*net.OpError` 时返回
   `500 failed to read session actor`，且**不删除** cookie。
5. 会话过期 → 删除会话（DEL + 清 cookie）并转为匿名。
6. 登出 → 写 `actor: null`（`json.Marshal(nil)` = `null`），而非删除 key。
7. 会话 ID 固定在 cookie 中；同一 ID 的 Redis key 会被反复覆盖。

## 6. 复现 fixture

```bash
cd go-fixture && go run . gen        # 写入 ../../testdata/sessions
```

fixture 时间戳是生成时刻（2026-11-07），`securecookie` 默认 30 天过期，因此
Go 侧解码器用 `MaxAge(0)` 关闭时间检查；Rust 侧用 `--now <ts>` 复现同一判定。
Rust 单测 `decode_fixture_if_present` 只在 fixture 存在时运行。

## 7. 关键发现与风险

### 7.1 gob map 顺序不确定 → 多条目写入只能保证语义兼容

Go 的 map 迭代顺序随机，同一个三条目 map 连续编码 5 次出现 2 种字节序列
（`test_ws/p4-order` 实测；单条目 100% 稳定）。因此：

- 不能对多条目 Redis value 做字节级 golden；
- 读取方必须按 map 语义比较（本 PoC 的 `entries_equal` 按 `(key, base64)` 集合比较）；
- Rust 编码顺序保持调用方给定顺序即可，不需要模拟 Go 的随机顺序。

### 7.2 空 `SRC_SESSION_COOKIE_KEY` 是有效配置

基线未设置该变量，`[]byte("")` 非 nil，`securecookie` 用**空 HMAC 密钥**工作。
Rust 必须允许空 key（不要 panic/拒绝），否则现有部署升级后所有用户被登出。
同时应在文档/发布说明中把它标为安全风险（会话 cookie 可被任意伪造）。
`poc/p4-session` 专门用空 key fixture 覆盖。

### 7.3 cookie 时间戳检查在解码侧

`securecookie` 的 30 天 `maxAge` 在 **decode** 时检查（不是 Redis TTL）。
Rust 若省略该检查，会把本应匿名的请求认证为有效会话。反向：Redis TTL 过期但
cookie 仍在 → 视为新会话（无错误）。

### 7.4 `time.Time` 文本依赖本地时区

`lastActive` 用 `time.Now()`，Go 按本地时区输出偏移；`GoTime` 保留解析到的
偏移并按 Go 规则裁剪小数。Rust 服务必须与 Go 使用相同 TZ（容器默认 UTC），
否则同一瞬时生成不同 JSON 文本。数据库时间统一 UTC。

### 7.5 值类型约束来自调用方，不来自 gob

Redis 里的值全部是 `[]byte`，因为 `SetData` 先 `json.Marshal`。若未来代码直接
写 `session.Values[k] = 其它类型`，gob 会出现其它具体类型；Rust 解码器已能读出
（`GobValue`），但 `into_session_map` 会拒绝非 `[]byte` 值——实现时应记录并跳过
该 key，而不是让整个会话读取失败（Go 的 `GetData` 会因类型断言 panic，属于
上游 bug；Rust 不应复制 panic）。

### 7.6 `gob` crate 不可用

crates.io `gob 0.1.0` 是 serde_schema 时代的实现，不支持 interface 值/Go 类型
定义，无法解码本格式。PoC 采用手写受限编解码器（约 700 行含测试），已覆盖
全部会话形状；M7 可将其提升为 `sg-session` 的内部模块。

### 7.7 其他

- `redistore` 的 `DefaultMaxAge`（20 分钟）只在 `Options.MaxAge == 0` 时使用，
  本路径不会触发；TTL 恒为 30 天。
- `securecookie.GenerateRandomKey(32)` 使用 `crypto/rand`；Rust 用 `OsRng`。
- 多个 codec（密钥轮换）按顺序尝试；基线只有一个 codec，Rust 可只实现单 key，
  但接口应允许 key 列表以兼容未来轮换。
