# P1 — gRPC + HTTP on one h2c port

验证 `internalgrpc.MultiplexHandlers` 的 Rust 等价实现：同一明文端口上按
`ProtoMajor == 2 && Content-Type 包含 "application/grpc"` 分流到 gRPC，其余走 HTTP。

## 组成

- `proto/echo.proto`：最小 unary gRPC 服务（`poc.echo.Echo/Say`）。
- `src/main.rs`：
  - `hyper-util` auto 连接（HTTP/1 或 HTTP/2 prior knowledge）；
  - 谓词与 Go 逐字对应（首值、大小写敏感的 `contains`）；
  - tonic `Routes` 直接作为 hyper service 使用，body 经 `tonic::body::boxed` 转换；
  - `tonic` 客户端模式验证 h2c unary 与 metadata 透传。
- `verify.sh`：一键验收（退出码 0）。

## 运行

```bash
. "$HOME/.cargo/env"
cargo run -- 127.0.0.1:18080          # server
cargo run -- client http://127.0.0.1:18080
./verify.sh
```

## 结果（2026-11-07，Rust 1.99.1，tonic 0.12.3，hyper 1.12）

| 场景 | 结果 |
|---|---|
| gRPC unary over h2c（tonic client） | PASS，自定义 metadata `x-test` 到达 resolver |
| HTTP/1.1 GET | PASS，HTTP handler，`HTTP/1.1` |
| HTTP/2 prior knowledge + `application/json` | PASS，HTTP handler，`HTTP/2.0` |
| HTTP/2 prior knowledge + `application/grpc`（未知服务） | PASS，进入 tonic，`grpc-status: 12` |
| HTTP/1.1 + `application/grpc` | PASS，仍走 HTTP（Go 谓词要求 HTTP/2） |
| HTTP/1.1 `Upgrade: h2c` | **DIVERGENCE**：作为普通 HTTP/1.1 处理 |

## 关键发现

### 1. 分流可行且语义可精确复刻

`hyper-util` 的 auto builder 支持 HTTP/2 prior knowledge（gRPC 客户端实际使用的方式），
`tonic::service::Routes` 可以直接嵌入 hyper 连接服务；不需要 axum 作为外层路由。

实现细节：

- tonic `Routes` 的请求/响应 body 是 `tonic::body::BoxBody`（`UnsyncBoxBody<Bytes, Status>`），
  需要 `tonic::body::boxed(hyper::body::Incoming)` 转换；
- hyper 的 service 错误类型需可转为 boxed error；PoC 用 `std::io::Error` 承接；
- `Route::call` 需要 `tower::Service` 在作用域内；
- tonic 0.12 的 `Routes` 内部由 axum 实现（`/ServiceName/*rest`）；这是 tonic 的实现细节，
  不影响外层 HTTP 路由一致性。

### 2. h2c Upgrade（RFC 7540 §3.2）hyper 不支持

Go 的 `h2c.NewHandler` 同时支持 prior knowledge 和 HTTP/1.1 Upgrade；baseline 验证：

```text
$ curl -s -o /dev/null -w '%{http_version} %{http_code}\n' --http2 http://127.0.0.1:3090/healthz
2 404        # Go 接受了 Upgrade，切换到 HTTP/2
```

hyper `auto` 只支持 prior knowledge。PoC 尝试自行处理：

1. 返回 `101 Switching Protocols` 成功，`hyper::upgrade::on` 拿到 `Upgraded`；
2. 读探针确认升级后的 IO 可读到客户端 preface（64+ 字节）；
3. 把 `Upgraded` 交给 `hyper::server::conn::http2` 后连接立即结束（无错误）。

原因在协议语义：RFC 7540 §3.2 规定升级前的 HTTP/1.1 请求占用 **stream 1**，
客户端升级后不会重发该请求，只等待 stream 1 的响应；Go 通过
`http2.ServeConnOpts.UpgradeRequest` 把原请求交给 handler。hyper（以及 `h2` crate）
没有对应 API，无法在接管连接后为 stream 1 生成响应。

**处置建议（M6/M7 决定）**：

| 选项 | 说明 | 取舍 |
|---|---|---|
| A. 接受差异（推荐默认） | 忽略 Upgrade，按 HTTP/1.1 正常响应 | gRPC 客户端用 prior knowledge；浏览器用 TLS h2；仅显式请求 h2c 升级的 HTTP 客户端多一次 1.1 往返。行为上限为协议版本差异，响应内容一致 |
| B. 前置代理终止 h2c | 由 nginx/envoy 处理升级 | 仅适用于对外端口；内部 3090 在 Go 中直接 h2c |
| C. 自研 HTTP/2 升级路径 | 用 `h2` 自行实现 stream 1 语义 | 成本高、收益低 |
| D. 保留 Go frontend | 该路径不替换 | 与重写目标冲突 |

若评审要求严格等价，选 C；否则选 A 并在 `interfaces/03_http_contracts.md` 记录为
**O 级（可观察行为差异）**，加入契约测试白名单。

## 其他结论

- `content-type` 比较必须保持大小写敏感（Go `strings.Contains`）；HTTP header 值通常小写，
  但契约测试应覆盖大写变体以确认差异。
- `h2c.NewHandler` 在 Go 中还校验 `HTTP2-Settings` 并应用客户端 settings；Rust 走 prior
  knowledge 时由 hyper 正常处理 SETTINGS 帧，无需额外逻辑。
- 连接级并发：每个 TCP 连接一个 tokio task；`Routes` 可 Clone 共享。
