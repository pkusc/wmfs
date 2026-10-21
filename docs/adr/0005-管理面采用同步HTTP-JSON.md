# ADR-0005 管理面采用同步 HTTP/JSON（替代 gRPC），事件总线 = changelog 长轮询

- 状态：accepted（人类于 2026-10-20 批准）
- 日期：2026-10-20
- 偏离条款：理念文档第 7 章 [默认]"管理 API：gRPC + 只读 REST"、"线上协议：…gRPC 仅用于控制面"；4.2"进程外（控制面）：gRPC 管理 API + 事件总线"
- 是否触及契约层：**是**（决定 4 的 changelog 流属于理念文档第 3 章"可观测性基座"；管理 API 是本体对外的稳定接口）
- 批准类别：**需批准**；确认批次 0（B 栏：技术契约，M1 就要实现带签名的管理 API 框架）
- 相关章节：design.md 3.11（changelog 流）、3.12、7.1–7.4

## 背景

Rust 的 gRPC 实现要么基于 tokio（tonic），要么需要编译 C++ gRPC 核心（grpcio），都违背"同步代码、零外部依赖"。使用者是新手，能用 curl 看接口更友好。管理 API 的服务端（core）与客户端（CLI、`wmfs mgr`、dashboard）由不同的人/agent 写，而 `wmfs-mgr` 按设计不能依赖 core——没有共享的类型定义，字段名就只能靠猜。

## 决定

1. 每个 `wmfs server` 用 `tiny_http` 提供 HTTP/JSON 管理 API（`/api/v1/...`）与 `/metrics`；客户端（CLI、`wmfs mgr`）用 `ureq 3`，必须 `proxy(None)`（本机设置了 http_proxy）。M1 就实现框架、签名认证、`/api/v1/health` 与 `/api/v1/failpoints`。
2. **每个端点的请求/响应结构定义在只依赖 serde 的小 crate `wmfs-api`**，core 与 `wmfs-mgr` 都用它，两侧都有金样测试。`POST /api/v1/attr {path, spec: "<原始字符串>"}`：属性字符串的切分、解析、规范化都在服务端做，回复规范化后的键值与警告。
3. 写操作 HMAC 签名（`Authorization: WMFS-HMAC key=,ts=,sig=`，绑定方法、路径、时间戳、body 哈希，±300 s）；读操作需要任一已认证 key；`/metrics` 与 `/api/v1/health` 免认证（health 返回本节点与 leader 的 rpc/管理地址，供 CLI 发现）。服务端不记录见过的签名：**5 分钟内截获的写请求可被重放**，首发只在可信网络中使用（ADR-0009）。
4. 依赖元数据的请求只由元数据 leader 回答，其他节点回 `409` + leader 地址；客户端接受多个地址，连不上换下一个、遇到 409 就跟随。
5. "事件总线" = changelog 长轮询 `GET /api/v1/changelog?after=&wait_ms=`，每个节点同时最多 2 个长轮询（第 3 个起立即返回空页，保证 CLI 与 dashboard 总有线程可用）；消费者游标存在本体（`CURSOR`），至少一次投递；消费者落后超出保留范围返回 `410 Gone`（缺口显式）。`wmfs mgr` 内部只有一个长轮询循环，按插件分发。

## 备选方案

- tonic gRPC（需要 tokio）；grpcio（需要 C++ 构建链）；在我们自己的帧协议上做管理协议（不便调试）。均否决。
- 不加 crate，只写一份 `docs/mgmt-api.md` 的 JSON 示例、两侧各做金样测试：少一个 crate，但两份手写的结构迟早会漂移。否决。
- 服务端缓存 5 分钟内见过的签名以拒绝重放：约 30 行，但多节点、leader 切换后仍有窗口；首发写明"仅可信网络"即可。推迟。

## 后果

- 好处：无 async；curl 可调；与 dashboard 天然契合；服务端与客户端对 JSON 结构不会各写各的。
- 代价：没有服务端推送流（用长轮询代替）；没有 IDL，结构写在 `wmfs-api` 里，路径带 `/v1` 版本号，变更遵守只增不改。
