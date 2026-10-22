# wmfs 首发设计规格

> 状态：**accepted（人类于 2026-10-20 一次性批准全部 ADR 与推荐方案）**。以后再改契约层（第 3 章）仍需新的 ADR 与人类批准，见 [roadmap.md](roadmap.md) 第 2 节。例外（2026-10-21 起，落实人类"只在需要拍板或 sudo 时停"的决定）：实现中发现的**纯技术缺口**（不改变 semantics.md 的任何承诺、不增删功能，例如补一张漏写的码表）由主管决定并写 ADR，在进度说明里告诉人类；改变对外承诺或要砍功能的，仍然先问人类。
> 依据：仓库根目录 `fs-design-philosophy.md`（下称"理念文档"，`{{PROJECT_NAME}} = wmfs`）。本文与理念文档冲突时以理念文档为准，并先指出冲突。
> 读者：① 实现 agent——字节布局、key 编码、trait 签名、消息枚举、错误码、线程归属以本文为准；② 人类——看懂每个模块做什么，按 roadmap 跑演示命令验收。
> **唯一真源**：M0 把本文 3.2.7–3.2.9、3.3、3.5.1、3.6.2、3.6.4、3.7.1、3.7.3、3.7.7、3.10、3.12 的全部类型、枚举、trait 原样落成可编译的 Rust 文件（只有签名）并生成 `tests/golden/` 金样。M0 之后以这些文件为准：本文对应的代码块**保留、仅供阅读**（新手读文档时不用来回翻源码），每个对应小节开头有一行"实现注记（M0）"写明以哪个源码文件为准，两者不一致时以源码为准；任何人改这些文件必须同时改 ADR 与本文。
> 实现注记（M0）：原计划是 M0 结束时把这些代码块改成"见 `core/src/…`"；主管于 2026-10-21 决定改为上一行的做法（保留代码块 + 每节注记），roadmap M0 同步修改。
> 用语：**必须 / 不得** 是硬性规则；"默认"是部署或属性的缺省值；"【ADR-xxxx】"指向 `docs/adr/` 里的决策记录。

---

## 0. 先读这一节

### 0.1 十八条关键决定

| # | 决定 | 一句话理由 |
|---|---|---|
| D1 | 只用同步代码：`std::thread` + `crossbeam-channel` + 阻塞 IO；不用 async、tokio、io_uring【ADR-0001、0003】；**socket 只由每连接的写线程写**，其他线程只往有界发送队列里 `try_send` | 对新手的承诺；可调试；小集群足够；一个不读 socket 的坏客户端卡不住执行器 |
| D2 | 元数据 = raft-rs（`raft` 0.7）+ **一个 redb 文件**（raft 日志、硬状态、状态机 KV 同库）【ADR-0002、0019】 | 应用与"已应用索引"同事务落盘，重启无需全量回放；快照直接从 redb 的 MVCC 读事务导出，无停顿 |
| D3 | leader 上**单线程串行执行器 + 单飞批次 + 写集复制**；批次写集按实际编码字节累计、超过 768 KiB 就结束本批；写集带 `expect_version`，各副本 apply 时比对，不等整批作废【ADR-0019】 | 命名空间逻辑是普通顺序代码；follower 无业务逻辑；过期批次一律变成"重试"，不会写坏状态；raft 条目有上界 |
| D4 | 线性一致读 = 批量 ReadIndex（`ReadOnlyOption::Safe`），**否决** `LeaseBased`【ADR-0019】 | 源码确认 raft-rs 0.7 的 LeaseBased 不检查时间，旧 leader 会读到旧数据 |
| D5 | **写操作只做点查**：执行器闭包里只允许按已知 key 读；需要扫描的工作放在只读阶段或后台任务里完成，再提交点删除 | 批内覆盖层就是一个 `BTreeMap`，不需要三路归并扫描 |
| D6 | 每个写请求的结果（成功或 POSIX 错误）都写进去重表，与修改同批提交 | 批次写集必然非空 ⇒ 错误回复也一定在"确认自己仍是 leader"之后发出；重试恰好一次 |
| D7 | chunk 文件 = **每个版本一个文件**：先写 `p/` 下的 pending 文件，提交 = 一次 `rename`；头部带块表（每个 64 KiB 块一条：校验、压缩、加密元数据）【ADR-0021】 | 崩溃原子性只靠 rename；不需要日志结构、压实、内存索引 |
| D8 | 数据复制 = CRAQ 变体：head 串行化每个 chunk、尾部先提交逆向确认、脏读重定向到最后一个 Serving 成员；**任何重启都以 Syncing 回链**；pending 一律前滚；chunk 版本永不重置（截断写新版本、只有整个 epoch 才删除）；链 `incarnation` 防止"唯一幸存者重启后重复分配版本"【ADR-0024】 | 线性一致、故障路径可证明 |
| D9 | **转发不占线程**：复制走独立 repl 线程池，发出转发后登记在 pending 表、立即返回；内存池为复制流量保留 25% 槽位（每档至少 1 个） | 消除"链上节点互相等线程/等缓冲"的分布式死锁 |
| D10 | layout `epoch` 是 fencing token：截断到 0、迁移、换类都换 epoch；数据在 `CommitWrite` 成功后才属于文件 | 迁移、截断与并发写之间没有静默丢数据的窗口 |
| D11 | 数据面授权 = 元数据签发的 HMAC 能力令牌（cap），带 layout 的链列表；数据节点只验签、不查元数据 | 服务端强制授权、客户端不可信，热路径 O(1) |
| D12 | 配额 = **quantum 额度模型**：客户端越过已授予的上限前先提交已写长度并申请下一段；超限上界 = quantum × 并发写入文件数（可配）；无任何预留表【ADR-0026】 | 与理念文档 8.2"短暂超限、上界可配置"逐字一致，零额外持久状态 |
| D13 | 租约一律"持有方先自停（0.8×）、授予方后回收"；客户端驱逐**两阶段**：先拉黑并等所有数据节点确认，再释放锁【ADR-0023】 | 只依赖时钟速率漂移有界；被驱逐客户端的在途写不可能在锁转交之后落地 |
| D14 | strong 一致性 = 客户端零缓存（direct_io + TTL 0）+ 每次读前线性一致 GetAttr + 每次写同步提交 + 集群锁 + 元数据分配 O_APPEND 偏移；strong 目录里新文件不 inline；**不支持跨节点共享可写 mmap（ENODEV）**【ADR-0013】。cto 下的 `O_DIRECT` 写走同一条同步路径 | 能证明正确的最简 strong |
| D15 | 插件：编译期静态注册，注册表在 `wmfs` 二进制 crate；动态库稳定 C ABI 首发只覆盖 `authz`（推荐方案，需你批准）【ADR-0012】；进程外控制面插件放在不依赖 core 的 `wmfs-mgr` crate【ADR-0017】 | 新增插件不改 core 一行；进程外边界由编译期保证 |
| D16 | 管理面 = tiny_http 的 HTTP/JSON（写操作 HMAC 签名），事件流 = changelog 长轮询【ADR-0005】；请求/响应结构、指标注册表、fail point 放在只依赖 serde 的小 crate `wmfs-api`，core 与 `wmfs-mgr` 共用 | 不引入 async/gRPC；新手能用 curl 看 `/metrics`；服务端与客户端不会对 JSON 字段各猜各的 |
| D17 | **插件集合全集群一致**：会影响策略的钩子（authn、authz、quota_admission、qos、placement、codec、transform、bg_task）启用了哪些插件（`钩子:名字`，不比版本，以便滚动升级）记在复制状态 GLOBAL `plugins` 里；节点的本地插件集合与它不一致就退出（等同拒绝启动），join 时不一致直接拒绝【ADR-0025】 | leader 切到一台少装了授权/配额插件的节点时，策略不会被静默跳过（原则 7） |
| D18 | 传输选择：元数据、集群管理、raft、快照永远走 `control_scheme`（默认 tcp）；只有 DataCall/PeerCall 在双方都公布了 `data_scheme`（例如 rdma）时才走它。节点地址是 `addrs: Vec<(scheme, 地址)>`，core 里没有任何 RDMA 专用字段【ADR-0018】 | 新增一个传输插件真正不改 core 一行（理念文档第 9 章） |

### 0.2 给实现 agent 的编码约定（"简单 Rust"）

1. 注释用中文，标识符用英文；每个文件 ≤ 400 行，超过就拆；每个模块文件顶部一段中文"这个模块做什么"。
2. 除 `#[derive(...)]` 外不写宏（不写 `macro_rules!`、不写过程宏）；可以调用标准库与依赖库已有的宏（`format!`、`vec!`、`include_str!`、`assert!`、`slog::o!`）。构造 raft 的 logger 用 `slog::Logger::root(SlogBridge, slog::o!())`（`slog::OwnedKV` 的字段是 `#[doc(hidden)]`，不是稳定 API）。
3. 不写复杂泛型：跨模块接口一律用 trait 对象（`Arc<dyn T>` / `Box<dyn T>`），trait 必须对象安全；生命周期参数只出现在短期借用视图上。
4. `unsafe` 只允许出现在：`core/src/buf/pool.rs`（mmap、切片）、`core/src/base/sys.rs`（libc 封装：`statvfs`、`copy_file_range`、`getrandom`、`fsync` 目录、`send_signal(pid, sig)`（实现注记（M0）：pid ≤ 0 一律 `EINVAL`——`kill(0, ..)`/`kill(-1, ..)` 会作用于整个进程组/全部进程）、`block_signals_and_wait(&[SIGTERM, SIGINT])`（启动时所有线程屏蔽这些信号，由 main 线程 `sigwait`）（实现注记（M0）：拆成 `block_signals(&[..])`（main 在创建任何线程之前调用，之后的线程继承屏蔽）与 `wait_signals(&[..])`（main 最后阻塞等待）两个函数——只在创建线程之后屏蔽的话，信号可能投递到别的线程并按默认动作杀掉进程）、`set_keepalive(&TcpStream)`、`memlock_limit()`；实现注记（M3）：另有 `euid_egid()`（`geteuid/getegid`，init 把根目录的属主设为运行它的用户，3.4.3））、`core/src/hooks/cabi.rs`（libloading 与 FFI 调用）、`plugins/rdma/`（sideway 的 `reg_mr` 等 unsafe 接口）。每处写 `// SAFETY:` 中文说明。其他 crate（cli、plugins/tcp、testkit）只调用 `sys.rs` 的安全封装；**不引入任何信号处理 crate**（ctrlc、signal-hook 等）。
5. 错误统一用 `ErrCode`（3.3.7）；网络与磁盘结果不得 `unwrap()`；`panic!` 只用于"不可能发生"的不变量破坏，并在消息里写清楚是哪条不变量。**例外（fail-stop）**：raft 线程与执行器线程上的 redb 错误（`begin_write/commit/读表` 失败）一律"写一条错误日志 + `std::process::abort()`"——状态机写一半时继续运行比崩溃更危险；重启后由 raft 日志恢复。伪代码里的 `or_abort(...)` 就是这个意思。（实现注记（M0）：解析和校验**用户输入**的函数——尺寸与时长、node.toml、key 文件、fail point 配置，M7 起的属性值——返回 `Result<T, String>`，内容是给人看的中文原因，因为 `ErrCode` 说不清"哪个键写错了"，而且 wmfs-api 用不了 core 的 `ErrCode`；系统、磁盘、网络与协议错误一律用 `ErrCode`；经 RPC 或管理 API 返回时由调用方映射成 `EINVAL` + message。）（实现注记（M1）：只被 CLI 调用、错误直接打印给人看的启动装配与 init 函数——`server::start`、`init_node`、`read_master`、`NodeFile::read`、`MgmtServer::bind`——也返回 `Result<T, String>`，其中的系统、磁盘、网络错误一律在消息里带上文件路径或地址；它们的调用方只是把消息打印出来。）
6. 所有时间来自 `core::base::clock`：`mono_ms()`（单调，租约与超时）与 `wall_ms()`（墙钟，mtime 与 cap 过期）；fail point 可以注入偏移与速率（3.11）。（实现注记（M1）：例外是 wmfs-mgr——它不依赖 core，`ApiClient` 的签名时间戳用标准库的 `SystemTime`，所以 `clock.wall.skew_ms` 对 CLI 与 mgr 的签名时间不起作用；其他代码一律用 `core::base::clock`。）
7. 所有随机数来自 `core::base::rand::fill(&mut [u8])`（`libc::getrandom`）；不引入 rand crate。（实现注记（M0）：wmfs-api 不依赖 core，所以 fail point 的 `pct()` 用标准库 `RandomState` 播种的每线程 xorshift，只决定"这次触发不触发"，不用于任何安全用途；其他代码一律用 `core::base::rand`。）
8. 所有对 `127.0.0.1` 的 HTTP 访问必须用 `ureq::Agent::config_builder().proxy(None)...build().into()` 构造的 agent（本机设置了 `http_proxy`，不这样会走代理失败）。
9. 新增任何可配置行为先回答理念文档 10.1 的六个问题；能是目录属性就必须是目录属性。
10. git 提交信息用中文简述，**不得包含任何 AI 署名或 co-author 行**（人类的明确要求）。
11. **postcard 编码 enum 用的是从 0 开始的变体序号，不是判别值**：凡是文档里写了固定数值的字段（角色、种类、状态……），线上与 KV 里一律存 `u8`，代码里立刻转成对应的 Rust enum（例如 `Role::from_u8`）。

---

## 1. 总览

### 1.1 进程与角色

只有一个二进制 `wmfs`，按子命令扮演不同进程：

| 进程 | 每台机器几个 | 内含角色 |
|---|---|---|
| `wmfs server`（首次由 `wmfs init` / `wmfs join` 创建并前台运行；之后用 `wmfs server --data-dir D` 重启） | 1（本机测试时可多个，端口和目录不同） | ① 元数据副本（voter / learner / 不参与）② 数据服务（0..N 个 target，一个 target = 一个目录/一块盘）③ 管理 API（HTTP）④ 只在元数据 leader 上活跃：串行执行器、cluster-mgr（租约、链管理、成员变更、驱逐）、后台任务调度器 |
| `wmfs mount` | 每个挂载点 1 个 | FUSE 客户端（fuser 多线程会话）+ 客户端库 |
| `wmfs mgr`（可选，手动启动） | 每个集群 0..1 | 进程外控制面插件宿主：dashboard、prometheus-exporter、alert、audit-log、webhook；只通过管理 API、`/metrics`、changelog 流接触系统；代码在不依赖 core 的 `wmfs-mgr` crate。server 不会拉起它：生产用 systemd（`docs/ops.md` 有 unit 示例），开发用 `wmfs dev mgr` |
| `wmfs set/get/unset/status/...` | 临时 | 管理 API 的 HTTP 客户端（与 `wmfs mgr` 共用 `wmfs-mgr` crate 里的 `ApiClient`，请求/响应类型来自 `wmfs-api`） |
| `wmfs dev ...` / `wmfs doctor` / `wmfs ping` / `wmfs fs ...` / `wmfs debug ...` | 临时 | 本机多进程集群工具、环境体检、传输自检、不经 FUSE 的调试客户端 |

```
              ┌──────────── 客户端机器 ────────────┐
 应用 ─POSIX─▶ 内核 FUSE ─▶ wmfs mount（fuser×16 线程）
              │              │ 客户端库：MetaClient / DataClient / 写回缓冲 / cache_store
              └──────────────┼───────────────────┘
                             │ 同一种帧：TCP 插件 或 RDMA 插件
   ┌─────────────────────────┼───────────── wmfs server（节点 n1..nN）───────────────────┐
   │  rpc 监听 ◀─────────────┘                                                          │
   │   ├ 元数据：meta-read×8 ──读闸门──▶ raft 线程 ◀── raft 消息 ──▶ 其他元数据副本         │
   │   │         meta-exec×1 ──写集───▶ raft 线程 ──▶ meta.redb（日志+硬状态+状态机）       │
   │   ├ 数据：QoS 队列 ─▶ io×16 ─▶ 变换 ─▶ chunk 文件（local-fs）                         │
   │   │       repl×4（转发/确认/同步）◀──▶ 链上前驱/后继（其他节点的数据服务）              │
   │   ├ hb（数据→元数据 leader：心跳、租约、拉集群图）  cluster-mgr（只在 leader 干活）      │
   │   ├ bg-sched（后台任务：协调器、scrub、gc、tiering、rebalance、trash-expire、ttl）      │
   │   └ 管理 API（tiny_http×4）+ /metrics                                                │
   └────────────────────────────────────────────────────────────────────────────────────┘
          ▲ HTTP/JSON（写操作 HMAC）                      ▲ HTTP/JSON + /metrics + changelog
   wmfs set/get/status（CLI）                 wmfs mgr（dashboard / exporter / alert / audit-log / webhook）
```

形态：单节点（profile `minimal`）= 一个 `wmfs server` 同时是唯一 voter 和唯一 target。三节点（`hpc-rdma` / `nas-safe`）= 三个 voter；第 4 个及以后的节点默认只做数据节点（元数据 learner 可选）。本机测试时"多节点" = 同机多进程（`wmfs dev up --nodes 3`），心跳上报 `machine_id`（`/etc/machine-id`）与 `host_id`（默认等于 machine_id；`wmfs dev` 用 `node.toml [debug] host_id` 给每个节点模拟不同主机，供 locality 演示），发现多个节点的 machine_id 相同时 `wmfs status` 与 dashboard 警告"故障域重叠：这不是真正的容错"。

### 1.2 线程模型

新手能读懂的四条规则：

1. **一个可变状态只有一个"主人"线程**：`RawNode` 和 redb 写事务只属于 raft 线程；批次构建只属于 meta-exec 线程；一个 chunk 的写入顺序只由该 chunk 在 head 上的状态表决定；**一个连接的 socket 只由它的写线程写、读线程读**。
2. 线程之间只用 `crossbeam_channel` 通信（请求 + 回复用 `bounded(1)`），或用 `Mutex` 保护的小结构；**任何线程都不得一边持锁一边等网络**。发帧一律 `ConnHandle::send` = `try_send` 进该连接的有界发送队列（1024 帧，满了就断开这条连接），真正写 socket 的是该连接的写线程（TCP 设 5 s 写超时，超时即断开并计数 `wmfs_conn_write_timeout_total`）。所以执行器、io、repl 线程永远不会被一个不读 socket 的客户端卡住。
3. **线程池里的线程不等待网络响应**：需要"等对方回复"的地方（链式转发、同步传播）用 `RpcClient::call_cb` 登记回调（3.12）；回调在连接读线程（或超时时在 `rpc-timer` 线程）上执行，**只做一件事：把结果 `try_send` 进 repl 队列**，由 repl 池处理。阻塞式的 `RpcClient::call` 只允许出现在 FUSE 工作线程、CLI、后台任务、hb、sync 线程里——这些线程不被别的节点依赖，而且每次最多等一个 RPC 超时（10 s）后重试；**F_SETLKW 的等待不在 FUSE 线程上**（8.6）。
4. 所有 IO 都是阻塞调用；每个线程有名字（`std::thread::Builder::name`）；连接读/写线程栈 256 KiB，其余默认。

`wmfs server` 线程表：

| 线程 | 数量 | 职责 | 阻塞点 |
|---|---|---|---|
| `main` | 1 | 装配；`sys::block_signals_and_wait(&[SIGTERM, SIGINT])`（SIGTERM → 若是 leader 先 `transfer_leader`（最多等 3 s）→ 停止接收新请求 → 退出）（实现注记（M0）：拆成两步，见 0.2 第 4 条——**创建任何线程之前** `sys::block_signals(&[SIGTERM, SIGINT])`，装配完成后 `sys::wait_signals(&[SIGTERM, SIGINT])`） | `sigwait` |
| `accept-<scheme>` | 每个传输插件 1 | 接受连接，为每个连接起读线程与写线程 | accept |
| `conn-<id>` | 每连接 1 | 读帧 → 握手/认证 → 按 `msg_type` 分发；**自己从不执行请求**：元数据只读请求交给 `meta-read`；元数据写请求连同回复回调交给 `meta-exec`；数据请求进 QoS 队列；Peer 请求与确认进 repl 队列；Raft 消息交给 raft 线程；`call_cb` 的回调也在这里执行（只 `try_send`） | 读 socket；取复制 lane 缓冲最多等 100 ms（3.6.1） |
| `conn-w-<id>` | 每连接 1 | 从该连接的有界发送队列取帧、写 socket（TCP 写超时 5 s）；fail point `plugin.<name>.transport` 包在这里（3.7.4） | 写 socket |
| `rpc-timer` | 1 | 每 100 ms 扫描 `RpcClient` 的在途表，超时的调用以 `ETIMEDOUT` 结束（3.12） | sleep |
| `meta-read-<i>` | 8 | 只读元数据请求：等读闸门 → 开 redb 读事务 → 读 → 回复；会话心跳、target 心跳也在这里 | 读闸门 |
| `raft` | 1 | 独占 `RawNode<RedbStorage>`：tick、step、propose、read_index、处理 Ready、写 redb、apply（3.4.1.3） | `recv_timeout(tick)`、fsync |
| `raft-send-<peer>` | 每个元数据对端 1 | 把 raft 消息的 protobuf 写进池 Buf 作为 bulk，经到该对端的连接发出（`ConnHandle::send`）；连接断了就重连（自己的输入队列有界 1024，满则丢弃并 `report_unreachable`） | 建连、取缓冲 |
| `snap-recv` / `snap-serve-<i>` | 按需 | follower 下载快照 / leader 提供快照流（3.4.1.7） | 网络、磁盘 |
| `meta-exec` | 1（只在 leader 活跃） | 单飞批次串行执行器（3.4.1.4）；没有锁等待队列（F_SETLKW 由客户端轮询，3.5.3） | 等请求、等批次结果 |
| `cluster-mgr` | 1（只在 leader 活跃） | 每 200 ms：会话租约、target 租约、驱逐两阶段、链管理、成员变更、修复；所有决定都是提交给执行器的内部操作 | sleep |
| `bg-sched` | 1 | 后台任务轮转（leader 上跑集群级任务，每个节点跑本地任务，3.10） | 限速器 |
| `io-<i>` | 16（每节点） | 从 QoS 调度器取请求（core 持有 `Mutex<()> + Condvar`，4.5）：chunk 读、head 写（读改写、变换、写 pending、发出转发后立即返回）、同步源服务 | 磁盘；队列空时在 Condvar 上等 |
| `repl-<i>` | 4（每节点） | 处理 ForwardWrite 到达、转发确认、同步/删除传播、NEED_FULL 补发；**从不等网络** | 磁盘 |
| `chain-timer` | 1 | 每 500 ms 扫描 pending 表：超过 3 s 未确认的版本按当前链视图重新驱动（3.4.2.6） | sleep |
| `hb` | 1 | 每 1 s 向元数据 leader 发 target 心跳；拉集群图；维护自我 fencing（3.4.4） | 网络 |
| `sync-<chain>` | 每节点最多 2 个并发 | 本节点作为 Syncing 成员时从 last serving 拉数据（3.4.2.8） | 网络、磁盘 |
| `mgmt-http-<i>` | 4 | tiny_http 请求处理；**同时最多 2 个 changelog 长轮询**（每个最多挂 30 s），第 3 个起立即返回空结果，所以至少 2 个线程永远留给 CLI 与 dashboard | 网络 |
| `rdma-cq-<dev>` | 每个 RDMA 设备 1 | 轮询完成队列（忙轮询 50 µs 后 sleep 50 µs）（实现注记（M2）：传输被丢弃、这块设备上的连接全部拆除之后退出） | CQ |
| `rdma-setup` | 每条 RDMA 引导连接 1 个（只在建连期间，≤ 5 s；连同建好的连接最多 `max_qps` 个）（实现注记（M2）：4.1） | accept 侧：在引导 TCP 上读对端的 RdmaSetup、建 QP、post 接收槽、回自己的 RdmaSetup，再把连接交给 core（起 `conn-<id>`/`conn-w-<id>`） | 引导 TCP 读写（超时 5 s） |

`wmfs mount` 线程表：

| 线程 | 数量 | 职责 |
|---|---|---|
| fuser 工作线程 | `n_threads = 16`，`clone_fd = true` | 实现 `Filesystem` 方法（方法取 `&self`，内部状态用 `Mutex`）；同步发元数据/数据 RPC（每次最多等一个 RPC 超时，然后按 3.3.8 重试） |
| `conn-<srv>` / `conn-w-<srv>` | 每个服务端连接各 1（每个 server 1 条连接，请求复用） | 读响应帧，按 `req_id` 唤醒等待者 / 写请求帧 |
| `rpc-timer` | 1 | 同服务端（3.12） |
| `session-hb` | 1 | 每 3 s 会话心跳，上报打开文件集合增量，处理驱逐 |
| `flusher` | 1 + 4 个写线程 | 写回缓冲超阈值/超龄（5 s）时刷出；4 个写线程并发调用 `DataClient::write` |
| `lock-wait` | 1 | F_SETLKW：持有被挂起的 `ReplyEmpty`，按退避轮询 `SetLock`，授予或失败时回复内核（8.6） |
| `atime` | 1 | 每 5 s 批量发送 relatime 更新（尽力而为） |

### 1.3 三类请求如何经过钩子管线（理念文档 4.1）

管线顺序由本体写死：`transport → decode(core) → authn → authz → quota_admission → qos → core exec → placement → transform(checksum→compress→encrypt) → codec → chunk_store → response → event_sink(异步)`。具体请求只经过相关钩子，不相关的钩子是空操作。`[x]` = 插件钩子，`(x)` = 本体代码。

**(a) create：`open("/home/a/f", O_CREAT|O_WRONLY)`**

```
应用 open(O_CREAT) ─▶ 内核 FUSE CREATE ─▶ [fuser 线程] Filesystem::create
  └▶ 客户端库：MetaCall{head{session, seq, uid, gid, groups, pid, job_id}, op: Create{parent, name, mode, umask, excl}}
        │                                                            trace_id 由客户端生成，写进帧头
        ▼ [transport: control_scheme，默认 tcp（元数据永远不走 RDMA，3.3.3）]   元数据 leader 的 conn 线程
   (decode：48 字节帧头 CRC → postcard 解 body)
   [authn] 连接建立时已由 shared-key 完成；这里取连接身份 ConnIdentity + 请求声明的 uid/gid
           （uid 必须在 key 的 uid 范围内，root_squash 生效）
   ─▶ 投递到 meta-exec 队列（元数据请求首发不做 QoS【ADR-0010】）
        ▼                                                            [meta-exec 线程，批次覆盖层]
   (会话有效？被驱逐 → SESSION_EVICTED)  (去重表命中 → 直接取原结果)
   [authz] posix-mode(210) → posix-acl(220) → path-caps(250)：对父目录要 W+X；任一 Deny 即拒绝
   [quota_admission] quota(310)：沿祖先检查 files + 1
   (core exec：解析父目录生效属性 → 分配 ino → 继承默认 ACL)
   [placement] label-match(510)：按 class/replicas/stripe/locality 选链 → 写进 inode.layout
   (写集：INODE、DENTRY、父目录、祖先 RSTAT、CHANGELOG、DEDUP)
   ─▶ WriteBatch{expect_version, ops} ─▶ raft 提交（多数派 fsync）─▶ 各副本 apply
   ◀── response：attr + layout + cap（能力令牌）
   [event_sink]（异步、进程外）：wmfs mgr 的 audit-log / webhook 长轮询 changelog 拿到 Create 事件
```

**(b) write：cto 模式，`close()` 触发写回一个 chunk**

```
应用 write()×N ─▶ 内核 FUSE WRITE（未开 writeback_cache，每次 write 都进守护进程）
  └▶ [fuser 线程] 越过已授予的配额上限？→ 先刷已有脏数据 + CommitWrite{want_quota}（quota_admission 在 leader 上执行）
                  额度为 0 → write() 直接返回 EDQUOT
     拷贝进写回缓冲（内存池 Buf；这是 FUSE 决定的唯一一次用户态拷贝）→ 返回
应用 close() ─▶ FUSE FLUSH ─▶ [fuser 线程] 刷该 inode：
  DataCall{cap, op: WriteChunk{chunk, chain_ver, extents}} + bulk ─▶ 该 chunk 所在链的 head
        ▼ [transport]（TCP：bulk 直接读进 head 的池 Buf；RDMA：head 发起 RDMA READ 拉进池 Buf）
   (decode)  [authn] 连接身份
   [authz] (core 校验 cap：HMAC、未过期、ino/epoch、W 权限、chunk 所在链 ∈ cap.chains、会话未被拉黑)
   [qos] mclock(410)：按 (tenant, qos_id, class) 入队 ─▶ [io 线程] 出队后再校验一次 cap
   (core exec：chunk 状态表加锁 → V = max(committed+1, incarnation<<40) → 对涉及的 64 KiB 块读改写)
   [transform] 每块：checksum(crc32c 701) → compress(lz4|zstd 731/732) → encrypt(aes-gcm 761)
   [codec] replica(610)：一个分片 = 整块原样
   [chunk_store] local-fs：写 pending 文件（变化的块 + 从已提交版本复制未改动块）
   (复制：ForwardWrite 给后继 → 登记 pending 表 → io 线程立即返回；
          … 尾节点写入并 rename 提交 → Ack 逆向传回 → 各成员在 repl 线程上 rename 提交)
   ◀── head 收到 Ack → 提交 → response{version}
  全部 chunk 完成后：MetaCall{CommitWrite{ino, epoch, new_size, mtime}} ─▶ leader：校验 epoch、更新 size/rstat/change_id → raft
   [event_sink]（异步）：changelog 的 SetSize 事件
```

**(c) read：cto 模式，内核页缓存未命中**

```
应用 read() ─▶ 页缓存命中？是 → 直接返回（open 时已按 change_id 决定是否保留页缓存）
            否 ─▶ FUSE READ ─▶ [fuser 线程]
  ├ 本 inode 的写回缓冲（读自己的写）→ inline 内容（open 时已带回）→ [cache_store] memory / local-nvme（键含 change_id）
  └ 未命中 → DataCall{cap, op: ReadChunk{chunk, chain_ver, off, len}}（RDMA：请求里附客户端接收缓冲描述）
        ─▶ 链上一个 Serving 成员（locality=prefer-client 时优先同节点）
   [transport] → (decode) → [authn] → [authz: cap 的 R 权限] → [qos] → [io 线程]
   (core exec：本成员该 chunk 有 pending 且自己不是 last serving → CHUNK_DIRTY{last_serving}，客户端改问它)
   [chunk_store] 读已提交版本的块 → [codec] replica：无操作
   [transform 逆序] decrypt → decompress → 校验 checksum（不符 → CHECKSUM_MISMATCH，客户端换副本，本成员标记待修复）
   ◀── response + bulk（TCP 跟在帧后；RDMA 由服务端 WRITE 进客户端缓冲）
  CHUNK_NOT_FOUND → 客户端先 GetAttr 确认 epoch 未变，再把该区间当空洞读 0
```

strong 模式差异：open 回 `FOPEN_DIRECT_IO`、TTL 0、不进任何缓存；每次 read 前一次线性一致 `GetAttr`；每次 write 同步写链并同步 `CommitWrite`；`O_APPEND` 先 `AppendReserve` 取偏移；strong 目录里新文件不 inline（8.5）。cto 下用 `O_DIRECT` 打开的文件，写走同一条同步路径（8.3）。

---

## 2. 仓库与 crate 布局

### 2.1 目录树（遵守理念文档 10.3）

```
wmfs/
├── Cargo.toml                  # [workspace] members = ["api","core","plugins","cli","dashboard","tests","examples/plugin-mem-transport"]
│                               # [workspace.package] edition = "2024"；[workspace.dependencies] 统一版本
├── api/                        # crate wmfs-api（lib，只依赖 serde、serde_json）：本体对外的稳定接口——管理 API 的请求/响应结构与
│                               #   changelog 事件的 JSON 字段（types.rs）、指标注册表（metrics.rs）、fail point（failpoint.rs）
├── core/                       # crate wmfs-core（lib）：本体
│   ├── include/wmfs_plugin.h   # 动态库 C ABI v1 头文件
│   └── src/{base,obs,proto,buf,net,hooks,meta,data,client,bg,mgmt}/ + server.rs init.rs lib.rs
├── plugins/                    # crate wmfs-plugins（lib）：全部进程内首发插件，每个插件一个目录
│   ├── Cargo.toml              # [lib] path = "lib.rs"；每个插件一个 cargo feature（默认全开）
│   ├── lib.rs                  # 只有声明：#[cfg(feature = "tcp")] #[path = "tcp/mod.rs"] pub mod tcp; …
│   ├── <name>/{mod.rs, manifest.toml, docs.md, tests.rs}
│   └── embedded-kv/{manifest.toml, docs.md}   # meta_engine 的代码在 core/src/meta/（元数据 Raft 是契约），这里只放 manifest 与文档
├── cli/                        # crate wmfs（bin）：唯一的二进制；子命令、插件注册表 registry.rs、FUSE 适配层
├── dashboard/                  # crate wmfs-mgr（lib）：进程外控制面插件 + 管理 API 客户端 ApiClient；**不依赖 wmfs-core**
│   └── src/{api.rs, host.rs, dashboard/, exporter/, alert/, audit_log/, webhook/}，每个插件目录含 manifest.toml、docs.md
├── gateways/README.md          # 二期：nfs / s3 / ublk（首发只放说明）
├── examples/
│   ├── plugin-mem-transport/   # 独立 crate：内存传输插件——"新增传输插件不改本体一行"的可执行证明，也给组件测试用
│   └── dylib-deny-uid/         # C 写的 authz 动态库示例（gcc 编译）+ 一个 abi_version=2 的坏插件
├── tests/                      # crate wmfs-testkit（dev-only）：多进程集群框架、Jepsen-lite 检查器、profile 测试、模拟掉电的存储后端（9.2）
│   ├── harness/  profiles/{minimal,hpc_rdma,nas_safe}.rs  golden/  docker/Dockerfile.tools
├── scripts/                    # accept.sh、loc.sh、build-tools-image.sh、profiles.sh、ci.sh
└── docs/                       # design.md（本文）、roadmap.md、semantics.md、adr/NNNN-标题.md、plugin-abi.md、ops.md
```

> 实现注记（M0）：各里程碑的验收检查放在 `scripts/accept.d/mN.sh`（定义函数 `accept_mN`），由 `scripts/accept.sh` 加载；根目录另有 `rustfmt.toml`（行宽 120、`use_small_heuristics = "Max"`），`cargo fmt --check` 是 CI 的第一步。`scripts/ci.sh rules` 机械检查 0.2 的硬规则（每个文件 ≤ 400 行、`unsafe` 白名单、不写 `macro_rules!`、不引入信号处理 crate 与 rand、不写过程宏），`// SAFETY:` 注释由 clippy 的 `undocumented_unsafe_blocks` 检查（根 `Cargo.toml` 的 `[workspace.lints]`）；两者都在 `scripts/accept.sh M0` 里。`scripts/accept.sh` 与 `scripts/ci.sh` 开头 `unset WMFS_GOLDEN_UPDATE WMFS_FAILPOINTS`（它们会让测试假通过或假失败）。

依赖方向（单向）：`cli → plugins → core → wmfs-api`、`cli → core`、`cli → wmfs-mgr → wmfs-api`。`wmfs-mgr` 只依赖 `wmfs-api`、`ureq`、`tiny_http`、`serde`、`serde_json`、`toml`、`hmac`、`sha2`，**不依赖 core**——"进程外插件只能通过管理 API、指标流、事件流接触系统"由编译期保证；它的插件照样有 fail point 与指标（都来自 `wmfs-api`）。`examples/plugin-mem-transport → core`。`wmfs-testkit → core + plugins + plugin-mem-transport`（组件测试放在 testkit，避免 core 的 dev-dependency 循环）。

为什么不是每个插件一个 crate：首发约 27 个插件，每个一个 Cargo.toml 太碎。`wmfs-plugins` 内每个插件一个目录、一个 feature，插件只能 `use wmfs_core::...` 的公开 API，不得 `use` 其他插件模块（`tests/` 里一个测试扫描源码强制这一点）。第三方插件可以是独立 crate（`examples/plugin-mem-transport` 就是范例）。

> 实现注记（M1）：这条源码扫描放在 `scripts/ci.sh rules`（与 0.2 的其他机械检查放在一起，`scripts/accept.sh M0` 会跑），而不是 `tests/` 里的测试：`plugins/<名字>/` 下不得出现 `wmfs_api`、不得引用别的插件模块（`crate::`/`super::`/`wmfs_plugins::` 加别的插件名），`plugins/Cargo.toml` 不得直接依赖 wmfs-api。插件的指标与 fail point 经 `wmfs_core::api`，部署参数表是 `wmfs_core::base::config::PluginTable`（= `toml::Table` 的别名），所以插件 crate 只依赖 wmfs-core（不必直接依赖 toml）。下一段 M2 验收里的"`core/src` 除注释外 grep 不到 rdma"也在 `rules` 里检查（命令与 roadmap 完全相同）。
> 实现注记（M2）：`scripts/ci.sh rules` 里这条检查改成比 roadmap 演示命令更严的写法——先去掉每行 `//` 之后的注释再 grep（与 `unsafe` 白名单同一写法）：roadmap 的 `grep -v '//'` 去掉的是"含 `//` 的整行"，`"rdma://…"` 这样的字符串与带行尾注释的代码都查不出来；roadmap 的命令照旧留给人看，`scripts/accept.sh M2` 两种都跑（roadmap 的原样命令一条，更严的写法经 `scripts/ci.sh rules` 一条，不在两个脚本里各写一遍）。"`core/` 零改动"另用 `git ls-files --others` 查未跟踪的新文件（`git diff` 看不到它们，而 cargo 会自动编译 `core/tests/*.rs` 这类新文件）。

**新增一个传输插件 = 新建插件目录（或独立 crate）+ `cli/src/registry.rs` 加一行 + 一个 feature，`core/` 一行不改**（理念文档第 9 章成功标准）。M2 的验收：开工时打 tag `m2-base`，完成后 `git diff --stat m2-base..m2 -- core/` 为空，并且 `core/src` 里除注释外 grep 不到 `rdma`。

### 2.2 模块清单与行数（不含测试）

| crate / 模块 | 内容 | 约行数 |
|---|---|---|
| wmfs-api | `types.rs`（管理 API 请求/响应、changelog 事件 JSON、`KeyFile`）、`codes.rs`（错误码名字 ↔ 数值）、`metrics.rs`（注册表 + Prometheus 文本）、`failpoint.rs` | 550 |
| core/base | `types.rs`（Ino、ChunkId、Ts、Role、Principal、ReqCtx）、`errcode.rs`、`config.rs`（node.toml）、`units.rs`（4M/30s 解析）、`clock.rs`、`rand.rs`、`log.rs`（分级日志 + 给 raft 的 `SlogBridge`）、`sys.rs`（libc 与信号封装）、`devinfo.rs`（st_dev → 物理盘/namespace，6.3） | 950 |
| core/obs | `trace.rs`（span 环形缓冲、慢请求日志） | 250 |
| core/proto | `frame.rs`、`handshake.rs`、`features.rs`、`meta.rs`、`data.rs`、`peer.rs`（PeerOp/NodeOp/ClusterMap）、`cap.rs` | 1,300 |
| core/buf | `pool.rs`：MemPool、Buf、FrozenBuf、BufSlice、lane、内存区域注册 | 420 |
| core/net | `server.rs`（accept、连接读/写线程、握手服务端、角色权限表、分发）、`client.rs`（RpcClient：req_id 复用、call/start/call_cb/stream、timer、重连、scheme 选择）、`retry.rs` | 800 |
| core/hooks | `traits.rs`（全部钩子 trait）、`manifest.rs`、`registry.rs`（PluginSet 校验 + 插件清单）、`pipeline.rs`（调用包装、预算、fail point）、`cabi.rs`（authz 动态库）、`policy.rs` | 1,060 |
| core/meta/raft | `storage.rs`、`driver.rs`、`peers.rs`、`snapshot.rs`、`membership.rs` | 1,570 |
| core/meta/engine | `executor.rs`、`view.rs`（批次/操作覆盖层、写集字节计数）、`read_gate.rs`、`engine.rs`（MetaEngine 实现、角色发布）、`dedup.rs` | 720 |
| core/meta/schema | `keys.rs`、`records.rs` | 620 |
| core/meta/ns | `common.rs`（authz/placement/new_layout 辅助）、`read_ops.rs`、`create.rs`、`remove.rs`（unlink/rmdir/回收站/孤儿）、`rename.rs`、`setattr.rs`、`xattr.rs`、`acl.rs`、`file_ops.rs`（open/commit_write/append/inline） | 2,000 |
| core/meta | `attrs.rs` + `attr_registry.rs`（680）、`rstat.rs` + `quota.rs`（400）、`sessions.rs` + `locks.rs`（470）、`changelog.rs`（190）、`service.rs`（330）、`gc_meta.rs`（150） | 2,220 |
| core/meta/cluster | `map.rs`（含集群警告）、`chains.rs`、`targets.rs`、`join.rs`、`plugins.rs`（插件清单比对）、`cluster_mgr.rs` | 1,260 |
| core/data | `format.rs`、`build.rs`、`state.rs`（chunk 状态表、pending、等待队列）、`write.rs`、`replica.rs`、`read.rs`、`redrive.rs`、`sync.rs`、`truncate.rs`、`service.rs`、`node_hb.rs` | 3,000 |
| core/client | `meta_client.rs`、`data_client.rs`、`files.rs`（inode/句柄表）、`writeback.rs`、`strong.rs`（strong 与 cto `O_DIRECT` 的同步写）、`locks.rs`（F_SETLKW 轮询） | 1,800 |
| core/bg | `scheduler.rs`、`limiter.rs`、`walker.rs`、`migrator.rs`、`status.rs`（"声明 vs 实际"，3.10） | 850 |
| core/mgmt | `http.rs`、`auth.rs`、`api.rs` | 790 |
| core | `server.rs`（节点运行时装配）、`init.rs`（init/join 本地侧） | 540 |
| **core 小计** | | **≈ 20,150** |
| plugins | tcp 300、rdma 920、shared-key 150、posix-mode 180、posix-acl 260、path-caps 90、quota 170、mclock 330、fifo 40、label-match 200、replica 50、crc32c/xxh3/blake3 各 35、lz4 60、zstd 60、aes-gcm 120、local-fs 420、cache-memory 140、cache-local-nvme 220、reconciler 280、scrub 200、tiering 140、rebalance 160、gc 220、trash-expire 70、ttl 90 | ≈ 4,980 |
| cli | main + 子命令 760、registry 110、fuse/ 1,150、dev 360（含 `dev wait`、`dev mgr`、清理）、doctor 180、fs 调试客户端 180、挂载路径映射 70 | ≈ 2,810 |
| dashboard（wmfs-mgr） | api 客户端 160、host 150（manifest 校验、单一长轮询循环、fail point 包装）、dashboard 130（另有 HTML/JS 约 250 行，不计）、exporter 90、alert 190、audit-log 90、webhook 110 | ≈ 920 |
| examples/plugin-mem-transport | | 150 |
| **合计** | | **≈ 29,600**（原定上限 30k，人类于 2026-10-21 放宽到约 36k、功能不砍；测试另计约 12k） |

> 实现注记（M0）：M0 结束时非测试代码约 4,050 行，超出 M0 累计预算（2,900）约 40%：3.2.7–3.12 的契约类型骨架按 `rustfmt`（行宽 120）展开后比估算大（proto ≈ 590、schema ≈ 540、hooks ≈ 440 行），另有审查后补的单调时钟注入、`raft_state` 编解码等。按 roadmap §0 第 7 条重估了剩余工作，三个模块的估算改为：**wmfs-api 550 → 900**（7.1 约 30 个管理端点的请求/响应结构都要放进 `types.rs`，一个还没写）；**cli 2,810 → 2,950**（doctor 已约 300 行，原估 180）；**core/proto 1,300 → 900**（M0 已写完全部消息类型，只剩握手证明与密钥派生、cap 签名与校验，约 200 行）。重估后 Σ max(当前, 估算) ≈ 29,650（上限 30,000）；若不下调 core/proto 则约 30,050。主管于 2026-10-21 确认接受这三项重估（core/proto 的消息类型确实已在 M0 写完）。`scripts/loc.sh` 用重估后的数字。
> 实现注记（M1）：M1 结束时非测试代码约 7,000 行（M1 新增约 2,950 行，预算 2,540，超出约 16%：主要是 core/net——服务端、客户端、连接包装、握手、重试一次写完——与 cli 的 `dev`/`ping`/`failpoint`）。按 roadmap §0 第 7 条重估了四个模块：**core/net 800 → 1,000**（已写完约 900，余下 leader_hint、OneWay 服务、call_cb 后台建连）；**examples/plugin-mem-transport 150 → 180**（已写完）；**plugins 4,980 → 4,810**（tcp 300 → 200、shared-key 150 → 80，都已写完）；**dashboard 920 → 880**（ApiClient 160 → 120，已写完）。重估后 Σ max(当前, 估算) ≈ 29,670（上限 30,000）。`scripts/loc.sh` 用重估后的数字。
> 实现注记（M1，审查修复后）：修 M1 审查发现（超限帧只让一个请求失败、握手前不发保活、流被丢弃回 EIO、回复缺 bulk 按 BUSY、本地失败交给 rpc-timer、热路径指标句柄、计数器预注册等）后非测试代码约 7,310 行，又重估两个模块：**core/net 1,000 → 1,120**（已写约 1,060，余下 leader_hint、OneWay 服务、call_cb 后台建连约 60）；**examples/plugin-mem-transport 180 → 195**（已写完：加了帧上限与收帧计时）。重估后 Σ max ≈ 29,800（上限 30,000）。
> 实现注记（M2）：rdma 插件实际约 1,520 行非测试代码（估算 920，超出约 600）：审查要求补上的推迟打开设备与注册、按本端 IPv4 自动选设备与 GID（gid.rs）、按帧计的信用控制、建连线程与 5 s 超时、设备能力核对、"QP 销毁之后缓冲才回池"的拆除顺序、两侧分开的访问位都在里面；M2 审查修复又把 QP 状态迁移的属性拆成可单测的纯函数（qp_attr.rs）、把设备部分从 verbs.rs 拆到 device.rs、日志带上 errno、链路 DOWN 与"没有设备"分开报（约 +30 行）。另有 cli 约 90 行（doctor 的 RDMA 一项、声明式的 `dev up --rdma`）。`scripts/loc.sh` 里 plugins 的估算 4,810 → 5,380，首发总量约 30,380。人类于 2026-10-21 把上限从 30,000 放宽到约 36,000、功能不砍，所以不动用"简化储备"。
> 实现注记（M3）：M3 新增约 2,150 行非测试代码（含审查修复，预算 2,300）：core/meta/raft 约 850（storage.rs、driver.rs 与它的子模块 driver/apply.rs、types.rs——driver.rs 超过 400 行，把 raft 线程的输入输出类型与 raft 参数拆进 types.rs、把"应用与发布"拆进 driver/apply.rs；peers.rs、snapshot.rs、membership.rs 在 M8）、core/meta/engine 约 600（另有 M0 的 txn.rs）、core/meta 的 service.rs 与 sessions.rs 约 135、core 的 init（含 `init/initial_kv.rs`）与 server 约 190、core/net 的单飞建连约 65（含审查后补的"拿着登记表的锁再查一次连接表"与建连线程 panic 时的收尾）、cli 约 310（`debug kv`、`dev wait`）、hooks 与 mgmt 约 20。按 roadmap §0 第 7 条重估两个模块：**core/net 1,120 → 1,180**（余下 leader_hint、OneWay 服务、call_cb 后台建连约 60）、**core/meta/engine 720 → 760**（余下 M4 的 changelog 接入、M8 的 leader 提示）。重估后 Σ max(当前, 估算) ≈ 30,475（上限 36,000）。`scripts/loc.sh` 把 `core/src/init/` 记在"core"名下（以前这类子目录会被漏数）。
> 实现注记（M4）：M4 新增约 2,790 行非测试代码（预算 2,650；其中审查修复约 110 行：按 seq 计的在途窗口、会话绑定 key、按名字找目录项之前先查执行权限、inline 内容只给有读权限的人、`wmfs fs` 发请求之前的失败也输出 JSON、`dev up` 等不到就绪的 leader 就失败并给出修法、target 不得重复或嵌套；`cli/src/dev.rs` 把 `--rdma` 的改写拆到 `dev/rdma.rs`）：core/meta/ns 约 1,170（common、read_ops、create、remove、rename、setattr、xattr、acl；file_ops 与回收站在 M5、M12）、core/meta 约 310（sessions.rs 的 open/close/心跳内存状态、changelog.rs、service.rs 的全部分派与 NodeCall、attrs.rs 的内置默认值）、core/meta/cluster 约 210（map.rs 含集群警告、chains.rs 的 gen_chains、cluster_mgr.rs）、core/client 约 190（meta_client.rs）、core/meta/schema 约 40（新文件 kv.rs：按记录类型读写、GLOBAL 计数器取号；records.rs 不动）、core/hooks 约 45（authz、placement 的管线与注册）、core/net 约 20（leader_hint、`Responder::drop_reply`）、core 约 60（init 的 `--target`、server 的装配）、plugins 约 190（posix-mode、label-match）、cli 约 425（`wmfs fs`、`leader.rs`、`init --target`、`dev up` 的默认 target 与集群警告）。没有模块超过它的估算，Σ max(当前, 估算) 仍是 30,475（上限 36,000），`scripts/loc.sh` 的估算不变。

体量控制：每个里程碑结束时 `scripts/loc.sh` 与 roadmap 的预算表对账，并**按模块重新估算剩余工作**（core/data、cli/fuse、core/client 最容易超）。上限原为 30k，人类于 2026-10-21 放宽到约 36k（功能不砍，30k 只当提醒线）；预计总量超过 36k 时停下来与人类商量（可从 roadmap"简化储备"里选），不悄悄扩大范围。

### 2.3 外部依赖（全部已在本机编译核对；新增的三个各附理由）

| crate | 用途 | 位置 |
|---|---|---|
| `raft = { version = "0.7", default-features = false, features = ["protobuf-codec"] }` | 元数据共识（同步核心，TiKV 生产验证） | core |
| `protobuf = "2.28"`（新增） | raft-rs 的 `eraftpb` 类型是 rust-protobuf 2 生成的，`write_to_bytes`/`merge_from_bytes` 需要 `protobuf::Message` trait 在作用域内；raft 不 re-export；版本与 raft-proto 0.7 一致 | core |
| `slog = "2"`（新增） | `RawNode::new` 要一个 `slog::Logger`；raft 不 re-export slog；我们用约 40 行的 `SlogBridge` 实现 `slog::Drain` 转到自己的日志，不引入 slog-term/envlogger | core |
| `redb = "2"` | raft 日志 + 硬状态 + 状态机；测试里用它的 `StorageBackend` + `Builder::create_with_backend` 模拟掉电（9.2） | core / tests |
| `fuser = { version = "0.18", default-features = false }` | FUSE（经 fusermount3 非 root 挂载，已实测） | cli |
| `postcard = { version = "1", features = ["use-std"] }` + `serde`（derive）+ `serde_json` | 消息体、KV value、管理 API | api / core / cli / dashboard |
| `crossbeam-channel = "0.5"` | 线程间通信（只用 `bounded/unbounded/recv/recv_timeout/try_recv/try_send`，不用 `select!`） | core |
| `crc32c 0.6`、`xxhash-rust 0.8`（feature `xxh3`）、`blake3 1` | 帧校验、块校验 | core / plugins |
| `lz4_flex 0.14`、`zstd 0.14`、`aes-gcm 0.11` | 变换插件 | plugins |
| `hmac 0.13` + `sha2 0.11` | shared-key、cap、管理 API 签名、密钥派生 | core / dashboard |
| `tiny_http 0.12` / `ureq 3` | 同步 HTTP 服务端 / 客户端 | core（服务端）/ dashboard（两者） |
| `clap 4`（derive） | CLI | cli |
| `libloading 0.9` | 动态库插件 | core |
| `toml 1`、`libc 0.2` | 配置与 manifest；系统调用 | core / dashboard（toml） |
| `sideway 0.4`（feature `rdma`） | RDMA（verbs；可注册外部内存、支持 RDMA READ/WRITE）；经 rdma-mummy-sys 在**运行时** dlopen libibverbs，所以不带 RDMA 的机器与容器不需要装 rdma-core | plugins/rdma |

**不用**：`ibverbs 0.9`（只有 SEND/RECV、只能注册自己分配的内存、类型带生命周期）、`tokio`、`openraft`、`io-uring`、`rand`、`log`、任何信号处理 crate。

> 实现注记（M1）：`ureq` 在根 `Cargo.toml` 里关掉默认 feature（不编 rustls/TLS 与 gzip）：首发只访问本机与内网的 http 管理 API；M14 的 webhook 若要 https 再打开 `rustls`。`wmfs-plugins` 在工作区依赖里也关掉默认 feature，由使用者（`cli/Cargo.toml` 的 `tcp`、`shared-key`、`mem` feature）决定编哪些插件。
> 实现注记（M2）：① sideway 0.4.3 的依赖链 tabled → tabled_derive → proc-macro-error2 2.0.1 有一处"将来的 Rust 会拒绝"的写法（第三方代码），cargo 每次构建都会在 stderr 打印一行 `warning: the following packages contain code that will be rejected by a future version of Rust`，而验收把 stderr 上以 warning 开头的行一律当成编译警告。仓库根的 `.cargo/config.toml` 用 `[future-incompat-report] frequency = "never"` 只关掉这一行提示（不影响任何编译器警告），`scripts/ci.sh` 在 clippy 之后打印 `cargo report future-incompatibilities`，让它保持可见。② rdma-mummy-sys 的 build.rs 用 cmake + bindgen（libclang）编译它的 dlopen 桩，所以 rdma 进了默认构建之后，**cmake 与 libclang 是构建前提**（本机有 cmake 3.28、clang/libclang 18）；运行时仍然不需要 rdma-core（没有 libibverbs 时 rdma-mummy-sys 的函数返回 `EOPNOTSUPP`、列不出设备；rdma 插件只在真要用设备时才去列、去打开，所以只用 tcp 不受影响）。③ rdma 插件私有的 RdmaSetup 帧沿用 core 的帧格式（postcard body + crc32c），所以 `wmfs-plugins` 的 `rdma` feature 另外带上可选依赖 `serde`、`crc32c`。


---

## 3. 契约层（理念文档第 3 章逐行落地）

本章每一节对应理念文档第 3 章表格的一行。契约层的任何修改都需要 ADR 与人类批准；首次实现前也要批准对应 ADR（roadmap 的确认批次）。

### 3.1 命名空间语义与一致性

本节（3.1.1–3.1.4）连同 3.5.3 锁语义、3.5.6 孤儿 inode 是"命名空间语义与一致性模型"这条契约的首次定义【ADR-0028】；strong 的实现与限制见【ADR-0013】，新增放松项见【ADR-0014】。

#### 3.1.1 POSIX 子集（`docs/semantics.md` 的权威来源）

| 类别 | 首发行为 |
|---|---|
| 支持 | 普通文件、目录、符号链接（目标 ≤ 4096 字节）、硬链接（仅文件，每个文件 ≤ 1000 个链接）、`mknod`（fifo/socket/字符/块设备节点，只存元数据）；名字 1–255 字节、不含 `/` 与 NUL；`rename`（含 `RENAME_NOREPLACE`、`RENAME_EXCHANGE`）；`chmod/chown/utimensat/truncate/ftruncate`；`fallocate` mode 0（扩展长度，受配额约束）；xattr（`user.*`、`security.*`、`trusted.*` 仅 uid 0、`system.posix_acl_access/default`；单 inode 合计 ≤ 64 KiB）；POSIX ACL；`fcntl` 字节范围锁与 `flock`（集群范围）；`O_APPEND/O_EXCL/O_TRUNC/O_DIRECT/O_SYNC/O_DSYNC`；`fsync/fdatasync`；稀疏文件；跨客户端"删除后仍打开"（3.5.6）；`statfs`；`readdir/readdirplus`（同一个目录句柄内 `seekdir` 有效） |
| 放松（理念文档 8.2 原文） | `atime` 按 relatime；跨节点共享可写 `mmap` 首发**不支持**（strong 下任何 `MAP_SHARED` 返回 `ENODEV`，cto 下只有节点内一致）【ADR-0013，偏离 8.2，需批准】；不支持强制锁；跨节点 `O_APPEND` 顺序仅 strong 保证；cto 下另一节点的写入在本节点下一次 open 后可见；配额在客户端写回提交前可能短暂超限，上界可查询、可配置（3.9） |
| 放松（本设计新增，【ADR-0014】需批准） | ① cto 下属性/目录项缓存最长 `attr_ttl` 陈旧（`consistency` 属性的子键，默认 1 s，再受挂载参数 `--max-attr-ttl` 限制，3.1.4 C1）；② `flock` 与 `fcntl` 锁共用一个锁空间（fuser 0.18 不把 `FUSE_LK_FLOCK` 传给文件系统），同一进程混用二者会互相冲突；③ `F_SETLKW` 不可被有处理函数的普通信号打断（fuser 不处理 FUSE INTERRUPT）；致命信号（`kill -9`、没有处理函数的 SIGINT/SIGTERM）会在下一次轮询时（≤ 1 s）让等待以 `EINTR` 结束；④ 不做死锁检测（不返回 `EDEADLK`）；⑤ 通过 `mmap` 写入的数据，配额错误在写回阶段发生，于 `msync/fsync/close` 报告；⑥ strong 下的 `O_APPEND`：读者可能短暂看到"已分配、未写完"的区域为 0，追加后 `lseek(fd,0,SEEK_CUR)` 的位置以内核为准、可能不精确；⑦ 缩短截断不是崩溃原子的（先截数据、后改长度），两步之间崩溃 → 长度不变、被截区域读为 0；⑧ 不同节点并发地 truncate 与 write 同一区域，可能得到"长度来自写、数据被截为 0"的结果，需要确定结果请加锁；⑨ 所有副本同时掉电时，未 `fsync` 的数据可能丢失（即使曾被读到过；文件大小可能已更新，丢失区间读为 0）；⑩ 客户端在提交长度前崩溃：它写入链但未提交长度的数据在 size 之外不可见，若之后别人把文件扩展到覆盖该区域，可能看到这段数据（等同本地文件系统崩溃后的部分写）；⑪ 目录 `st_size` = 条目数；inode 号永不复用、`generation` 恒为 0；⑫ mtime/ctime 用元数据 leader 的时钟；能力令牌要求节点间墙钟误差 < 5 分钟（NTP）；⑬ relatime 的 atime 更新是批量、尽力而为的，客户端崩溃或 leader 切换时可能丢一次；⑭ `F_SETLKW` 由客户端轮询实现（50 ms 起指数退避、上限 1 s，3.5.3），**不保证先来先得**：竞争激烈时某个等待者可能等很久；⑮ inotify/dnotify 只看得到本节点的变化，文件租约 `F_SETLEASE` 只在本节点有效；⑯ 回收站里的文件仍计入该子树的配额，删除目录树时目录结构不保留（文件平铺在 `.wmfs-trash/`） |
| 不支持 | 目录硬链接（`EPERM`）；`RENAME_WHITEOUT`（`EINVAL`）；`copy_file_range`/reflink（`EOPNOTSUPP`，内核自动回退为普通拷贝）；`fallocate` 的其他模式（`EOPNOTSUPP`）；`chattr` 标志（不实现 FUSE ioctl，返回 `ENOTTY`）；强制锁 |

#### 3.1.2 rename 原子性

整个命名空间在**一个** raft 组里（首发不分片，基于[待定]假设"≤ 64 节点、数千客户端"），rename 是执行器里的**一个闭包**，写集进入**一个 raft 条目**：对所有观察者要么旧名存在、要么新名存在。闭包内顺序：源 dentry 存在 → 目标父是目录 → `authz`（源父、目标父要写+执行；粘滞位约束）→ 不得把目录移进自己的子树（沿目标父的 `parent` 指针点查到根，遇到源 ino 即 `EINVAL`，O(深度)）→ `RENAME_NOREPLACE`/`RENAME_EXCHANGE` 语义 → 覆盖目标时类型匹配、目标目录必须为空（`size == 0`，否则 `ENOTEMPTY`）→ 跨配额域时用被移动对象的用量检查目标侧配额（`EDQUOT`，与 XFS 项目配额一致）→ 改 dentry、被移动 inode 的 `parent/name`、两个父目录的 `size/nlink/mtime/ctime/change_id` → 递归计数在旧/新祖先链之间搬移（到最近公共祖先为止）→ 被覆盖的目标：若 `trash` 生效则移入回收站，否则 nlink−1，归零即孤儿（3.5.6）→ 移动的是目录：`attr_epoch += 1`（继承关系变了）并写 `RECONCILE_Q(dir)` → changelog。

#### 3.1.3 可见性规则（一致性模型的精确定义）

1. **元数据**在 leader 上线性一致：所有修改在串行执行器里排序，在多数派 fsync 且 leader 应用之后才回复；所有读要么经过 ReadIndex 确认（3.4.1.6），要么命中客户端缓存（cto：≤ 生效的 `attr_ttl`；strong：不缓存）。
2. **数据**：一次 chunk 写在链上所有成员（含 Syncing）提交之后才回复写者；CRAQ 读规则（3.4.2.5）保证此后的任何读都读到它，且读永远不会"先看到新版本、后看到旧版本"。但数据只有在 `CommitWrite` 成功后才"属于文件"——`CommitWrite` 校验 layout `epoch`、更新 size/mtime/change_id。
3. **cto（默认）**：写者 `close()` 返回 = 该文件此前写入的数据已在链上提交、新长度已在元数据提交；之后任何节点**发起的** `open()` 一定看到这些数据（open 总是向 leader 线性一致地取最新属性，`change_id` 变了就失效内核属性与页缓存，3.1.4 C2）。文件打开期间不保证看到其他节点的新写入，但每次读到的每个 chunk 都是某个已提交版本。
4. **strong**（目录属性，对之后新打开的文件生效）：客户端不缓存数据与属性；每次 read 先做一次线性一致的 `GetAttr`（拿 size 与 epoch）再按 CRAQ 读；每次 write 同步写链并同步 `CommitWrite` 后才返回。于是任意两个节点之间 read-after-write 一致；单个 chunk 内的读写线性一致，跨 chunk 的一次大写不是原子的（与本地文件系统相同）。
5. **锁即同步点**（两种等级都成立）：拿到锁时客户端使该 inode 的内核属性与页缓存失效；释放锁前先 flush 并 `CommitWrite`。所以用锁协调的程序在 cto 下也正确（与 NFS 相同）。
6. **持久性**：元数据操作回复 = 已在多数派 fsync。cto 的 `write()` 返回 = 数据在本客户端写回缓冲；`close()` 返回 = 已到达链上全部副本的页缓存并提交长度（可见，但不保证落盘）；`fsync()` 返回 = 链上全部副本 `fdatasync` 完成且长度已提交。strong 的 `write()`、以及 cto 下 `O_DIRECT` 文件的 `write()` 返回 = 已到达全部副本页缓存并提交长度（8.3、8.5）。

#### 3.1.4 缓存失效协议

首发**没有服务端到客户端的回调**（没有 delegation / 读租约，二期）。失效全部由下面 6 条"客户端主动、服务端可验证"的机制完成：

| # | 机制 | 触发 | 作用对象 |
|---|---|---|---|
| C1 | FUSE 回复里的 entry/attr TTL = `min(AttrOut.ttl_ms, 挂载参数 --max-attr-ttl)`。`ttl_ms` 由服务端按该 inode 所在目录的生效属性给出：cto = 子键 `attr_ttl`（`consistency=cto,attr_ttl=1s`，可继承，默认 1 s，范围 0–10 s），strong = 0；`--max-attr-ttl` 默认 10 s，只能把它调小 | 每次 lookup/getattr/readdirplus 回复 | 内核 dentry/attr 缓存 |
| C2 | open 时向 leader 取最新属性（`Open` 是线性一致读）；与本挂载上次见到的 `change_id` 不同 → **在回复 open 之前**调用 `Notifier::inval_inode(ino, 0, 0)`（失效属性与全部页），回复不带 `FOPEN_KEEP_CACHE`；相同 → 回 `FOPEN_KEEP_CACHE`；`cache_store` 的键带 `change_id`，旧条目自然作废 | 每次 open | 内核属性、页缓存、客户端块缓存 |
| C3 | 加锁成功 → `inval_inode(ino, 0, 0)` + 丢弃属性缓存；解锁前先 flush | setlk | 页缓存、写回缓冲 |
| C4 | `CommitWrite` 返回 `STALE_LAYOUT` → 重新 `Open` 取 layout，把仍在写回缓冲里的数据按新 layout 重写（strong 与 cto `O_DIRECT`：用本次 `write` 回调里仍持有的数据重写）。另外，**任何改了 size 的 `SetAttr` 都回 `Opened(OpenOut)`**（新 attr、layout、cap），客户端用它更新 InodeState 与该 inode 的所有打开句柄，所以截断到 0（含 `O_TRUNC`）之后的写直接进新 epoch | 迁移/截断到 0 之后 | 写回缓冲、句柄的 layout |
| C5 | 读到 `CHUNK_NOT_FOUND` → 先 `GetAttr` 确认 epoch 未变，才把该区间当空洞读 0；epoch 变了 → 刷新 layout 重读 | 读空洞 | 防止"被 GC 的旧 epoch 被当成零" |
| C6 | 会话心跳回复带 `map_epoch`；变大则拉新集群图；数据请求返回 `STALE_CHAIN`/`NOT_SERVING` 也立即拉 | 心跳 / 错误 | 链表、target 地址、被拉黑会话 |

C2 在 open 处理函数里调用通知接口：open 期间内核不持有该 inode 的页锁，不会死锁；M6 用"挂载 A 写+close，挂载 B 在 TTL 窗口内 open+read"的测试确认。若该内核版本出现死锁，改为"由 `inval` 线程执行、open 最多等待 1 s"，并在 semantics.md 注明窗口。

#### 3.1.5 语义可查询（原则 7）

`wmfs get <path>` 在属性表之后固定输出一行 `semantics:`，例如：

```
semantics: consistency=cto attr_ttl=1s (来源 /); quota=quantum:64MiB overshoot<=64MiB*writers (来源 /home/alice); durability=replicas:声明3/实际1 fsync:all-replicas; isolation=shared-device(份额,不承诺尾延迟)
```

`durability` 里的"实际"= 这个目录下的新文件此刻能得到的副本数 `min(声明, 该存储类当前可用的故障域数)`；两者不同时整行之前再打印一行警告。`wmfs status <path>` 的第一行是集群警告（`ClusterMap.warnings`，例如"3 条链少于声明副本数""故障域重叠"），之后是"声明 vs 实际"的逐属性表（3.10）。同样的集群警告也出现在 server 启动日志（leader 就绪时、以及警告变化时）与 `wmfs mount` 的输出里——单节点上 init 写入的 `/home replicas=3` 不经过 `wmfs set`，靠这三处保证不会"实际只有 1 份却没有任何提示"。`GET /api/v1/semantics` 返回 semantics.md 的要点与当前集群参数（cap 有效期、租约时长）。

### 3.2 持久化格式

#### 3.2.1 通用规则

- 所有多字节整数一律**小端**；**唯一例外**：KV 的 key 里的整数用**大端**（字典序 = 数值序，范围扫描就是前缀扫描）。
- 校验默认 crc32c（aarch64 有硬件指令）。
- 所有持久化记录 = 1 字节记录版本号 + `postcard` 编码的结构体（例外：`INLINE` 是原始字节）。
- 唯一的原子替换工具 `atomic_write(path, bytes)`：写 `<dir>/.<name>.tmp-<随机>` → `fdatasync` → `rename` 覆盖 → `fsync(目录)`。`NODE`、`TARGET`、本地任务游标用它；chunk 的同步/修复安装用同样的步骤，只是临时文件放在 target 的 `t/` 目录（R3）。
  实现注记（M1）：`atomic_write` 在 `core/src/base/sys.rs`（不需要 unsafe），文件权限一律 0600（NODE、密钥、超级块、任务游标都只给本用户读）。

#### 3.2.2 节点数据目录

> 实现注记（M0）：node.toml 的结构（顶层 `debug_ops`、`failpoints_api`，以及 `[net]`、`[mgmt] listen`、`[mem]`、`[plugins] disable/dylib`、`[plugin.<name>]`、`[[target]] path/labels/capacity`、`[debug] host_id`）以 `core/src/base/config.rs` 为准；不认识的键 = 错误。

```
<data_dir>/
  NODE                      TOML：format = 1, cluster_id = "<32 位 hex>", node_id, created_ms（atomic_write）
  node.toml                 部署参数（各传输插件的监听地址、control/data scheme、target 列表、内存池大小、插件启停）——只放部署参数；启用的插件集合必须与集群插件清单一致（3.7.2）
  cluster.key               32 字节集群主密钥（权限 0600）；所有认证密钥、cap 密钥、数据密钥包裹密钥都由它派生（3.3.2）
  keys/admin.key            init 生成的管理员密钥文件（0600）；init 同时复制到 ~/.config/wmfs/
  keys/client.key           init 生成的默认客户端密钥文件（0600）
  meta/meta.redb            raft 日志 + 硬状态 + 状态机（只有元数据成员有）
  meta/snap-recv-<index>-<term>.snap   接收中的快照（安装后删除；启动时删除残留）
  targets/<tid>/TARGET      target 超级块（4096 字节）
  targets/<tid>/c/<chain:08x>/<ino%256:02x>/<ino:016x>-<epoch:08x>/<idx:08x>    已提交的 chunk 版本
  targets/<tid>/p/<chain:08x>-<ino:016x>-<epoch:08x>-<idx:08x>.p                 pending 版本（启动时整个目录清空）
  targets/<tid>/t/<同上>.t                                                         同步/修复安装用临时文件（启动时清空）
  logs/server.log           run/server.pid
```

pending 与已提交文件在同一个文件系统上，所以提交是同一文件系统内的 `rename`。按链分目录，是为了重同步时"列出我在这条链上的全部 chunk"只需一次目录遍历，不需要任何本地索引数据库。

#### 3.2.3 target 超级块 `TARGET`（4096 字节）

> 实现注记（M0）：本节的字节布局自 M0 起以 `core/src/data/superblock.rs` 为唯一真源（金样 `tests/golden/target_superblock.golden`）；下表仅供阅读，若不一致以源码为准。

| 偏移 | 长度 | 字段 |
|---|---|---|
| 0 | 8 | magic `b"WMFSTGT1"` |
| 8 | 2 | format_version = 1 |
| 10 | 2 | 保留 0 |
| 12 | 4 | features_compat |
| 16 | 4 | features_ro_compat |
| 20 | 4 | features_incompat |
| 24 | 16 | cluster_id |
| 40 | 4 | target_id |
| 44 | 4 | node_id |
| 48 | 8 | created_ms |
| 56 | 4036 | 保留 0 |
| 4092 | 4 | crc32c(0..4092) |

启动时：cluster_id 不符 → 拒绝启动（防止插错盘）；不认识的 incompat 位 → 拒绝；不认识的 ro_compat 位 → 只读服务（target 不进入任何链的写路径，上报 `read_only = true`）。

#### 3.2.4 chunk 文件格式 v1（契约）【ADR-0021】

> 实现注记（M0）：本节的固定头、块表条目、AAD 自 M0 起以 `core/src/data/format.rs` 为唯一真源（`ChunkId` 在 `core/src/base/types.rs`；金样 `tests/golden/chunk.golden`）；下面的代码块与表格仅供阅读，若不一致以源码为准。

一个 chunk = 文件中 `[idx × chunk_size, (idx+1) × chunk_size)` 这一段在某个 layout `epoch` 下的数据。数据面的 chunk 身份：

```rust
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChunkId { pub chain: u32, pub ino: u64, pub epoch: u32, pub idx: u32 }
// chain 由 layout 决定：chain = layout.chains[idx % layout.chains.len()]；在 (ino, epoch) 的生命周期内固定
```

**一个 chunk 版本 = 一个文件 = 固定头（256 字节）+ 块表 + 数据区。** 块（block）是读改写、校验、压缩、加密的单位：`block_size = min(65536, chunk_size)`。`chunk_size` 取值 64 KiB..=16 MiB 的 2 的幂（默认 4 MiB，`chunk` 属性）。

固定头（256 字节，小端）：

| 偏移 | 长度 | 字段 | 说明 |
|---|---|---|---|
| 0 | 8 | magic | `b"WMFSCHK1"` |
| 8 | 2 | format_version | = 1 |
| 10 | 2 | header_len_4k | 头 + 块表占多少个 4 KiB；数据区从 `header_len_4k × 4096` 开始（保持 4 KiB 对齐，为将来 O_DIRECT/RDMA 直读预留） |
| 12 | 4 | features_compat | |
| 16 | 4 | features_ro_compat | |
| 20 | 4 | features_incompat | bit0 `BLOCK_COMPRESS`（有块被压缩）、bit1 `BLOCK_ENCRYPT`（有块被加密）、bit2 保留给二期的动态库压缩算法（`DYN_TRANSFORM`，首发不实现、不写）；不认识的位 → 拒绝读（`FEATURE_UNSUPPORTED`） |
| 24 | 8 | ino | **所属 inode** |
| 32 | 4 | epoch | layout epoch |
| 36 | 4 | chunk_idx | |
| 40 | 4 | chain_id | |
| 44 | 4 | block_size | |
| 48 | 8 | chunk_version | **chunk 版本**（3.4.2.2） |
| 56 | 4 | chunk_size | 逻辑最大长度 |
| 60 | 4 | logical_len | 本 chunk 当前数据长度（≤ chunk_size；截断会缩小它） |
| 64 | 4 | nblocks | = chunk_size / block_size |
| 68 | 1 | checksum_alg | 1 crc32c、2 xxh3-64、3 blake3-256 |
| 69 | 1 | compress_alg | 0 none、1 lz4、2 zstd；128..=255 保留给二期动态库算法 |
| 70 | 1 | compress_level | i8（zstd 级别） |
| 71 | 1 | encrypt_alg | 0 none、1 aes-256-gcm |
| 72 | 4 | encrypt_key_id | 0 = 无 |
| 76 | 4 | snap_epoch | 二期快照预留，首发写 0 |
| 80 | 16 | cluster_id | |
| 96 | 8 | write_time_ms | 由 head 填写并随转发传给后继，保证各副本文件逐字节相同 |
| 104 | 148 | 保留 | 0 |
| 252 | 4 | header_crc | crc32c(固定头 0..252 ‖ 整个块表) |

块表：紧跟固定头，`nblocks` 个 72 字节条目：

| 偏移 | 长度 | 字段 | 说明 |
|---|---|---|---|
| 0 | 4 | stored_len | 数据区里实际存的字节数；0 = 洞（从未写过或被截断，读为 0） |
| 4 | 4 | plain_len | 该块明文有效长度（≤ block_size） |
| 8 | 1 | flags | bit0 `COMPRESSED`（本块真的压缩了）、bit1 `ENCRYPTED` |
| 9 | 3 | 保留 | 0 |
| 12 | 12 | nonce | AES-GCM nonce：**每次加密一个块时用 `rand::fill` 生成 12 个随机字节**，绝不从版本号推导 |
| 24 | 16 | gcm_tag | 未加密为 0 |
| 40 | 32 | checksum | **明文**的校验和：crc32c 用前 4 字节（小端），xxh3 用前 8 字节，blake3 用全部 32 字节 |

规则：

- **数据区**：第 i 块固定放在 `header_len_4k × 4096 + i × block_size`，只写 `stored_len` 字节；压缩省下的空间与洞靠稀疏文件真正省下来。
- **变换参数属于 layout**：头部的 `checksum_alg/compress_alg/compress_level/encrypt_alg/encrypt_key_id` 来自文件 layout（cap 携带）；同一 (ino, epoch) 的所有 chunk 用同一套参数。所以属性改动的生效方式是（与理念文档 5.2 "新数据"的偏离，【ADR-0021，需确认】）：改 `compress/encrypt` → **新文件与截断到 0 的文件**（截断到 0 时在同一个闭包里按当前生效属性重建整个 layout，3.4.2.9）立即生效；已有文件里新写入的数据仍按它当前的 layout，直到协调器把它迁移到新 layout（新 epoch）——**一直写打开的文件会被协调器跳过**，`wmfs set` 时警告、`wmfs status` 单独计数、跳过超过 24 h 发 `PolicyViolation`（3.10）；改 `checksum` → 只对新文件与截断到 0 的文件生效，不迁移。
- **变换顺序**（理念文档 4.1）：写 = checksum(明文) → compress（结果 > 明文 7/8 时存原文、不置 `COMPRESSED`）→ encrypt（原地加密）。读 = decrypt → decompress → 校验 checksum。
- **AAD**（AES-GCM 附加数据）= `b"WMFS-BLK1" ‖ cluster_id ‖ ino ‖ epoch ‖ chunk_idx ‖ block_idx`（大端整数）。**不含版本号**：未改动的块会被原样复制进新版本文件，其密文、nonce、tag 不变，仍然可解。防止块被挪到别的文件、位置或集群。
- **packed 形式**（网络传输整 chunk 时用：完整转发、同步、修复）= 固定头 ‖ 块表 ‖ 依块号顺序拼接所有 `stored_len > 0` 的块的存储字节。接收方把头和块表写在偏移 0，各块写回各自固定偏移，得到与源逐字节相同的稀疏文件。
- **快照预留**（理念文档 8.1"chunk 版本化从第一天设计"）：每个版本是独立文件，二期快照只需在快照点把当前已提交文件硬链接为 `<idx:08x>@<version:016x>` 并启用 incompat 位 `chunk.snapshots`；头部 `snap_epoch` 已预留。

#### 3.2.5 数据侧崩溃一致性规则

| 规则 | 内容 |
|---|---|
| R1 | 永不原地修改已提交文件。新版本 = `p/` 下的新 pending 文件（写头+块表、写改动块、用 `copy_file_range` 从已提交文件同偏移复制未改动块的存储字节，`EXDEV/ENOSYS` 时退回 `pread/pwrite`），提交 = `rename(p/…, c/…)` 覆盖旧版本 |
| R2 | 写路径**不 fsync**（持久性靠多副本）。客户端 `fsync` → `SyncChunks` 沿链传播，每个成员对涉及的已提交文件 `fdatasync`，并 `fsync` 其所在目录（rename 产生的目录项否则可能丢） |
| R3 | 同步/修复安装一律 `t/` 临时文件 → `fdatasync` → `rename` → `fsync(目录)`：同步完成后该成员可能成为唯一副本，fsync 过的数据必须在它上面真正落盘 |
| R4 | 删除 = `unlink` + 删空目录，不 fsync；崩溃后"复活"的文件由孤儿扫描清理（4.12 gc） |
| R5 | 启动：清空 `p/`、`t/`；chunk 头 CRC 在打开时校验；头 CRC 不符或块 checksum 不符 → 该 chunk 标为 **Corrupt**（计数、`ReportCorrupt` 上报，由 scrub/同步从其他副本修复；唯一副本则产生 `DataLossDetected` 事件——诚实上报）。Corrupt 标记**只在内存**（chunk 状态表里），重启后由下一次读或 scrub 重新校验发现。Corrupt 的 chunk：**读一律拒绝**（`CORRUPT`）；不保留任何旧块的写——整 chunk 覆盖写、`new_len = 0` 的截断——照常执行，提交新版本后清除标记；要保留旧块（部分写、`new_len > 0` 的截断）→ 链上还有其他 Serving 成员时先用与 scrub 相同的函数从它们修复该 chunk 再继续，没有则回 `EIO` |
| R6 | target **任何一次重启**（干净或崩溃）都以 Syncing 身份回到它的链里（唯一例外是"唯一幸存者"规则，3.4.2.7），所以"重启后丢了 pending 或丢了未 fsync 的提交"永远不会被读到 |

ext4（本机）与 XFS 都满足"rename 原子替换"。XFS 没有 ext4 的 `auto_da_alloc` 启发式，掉电后可能出现"rename 已持久、数据未持久"的文件——R5 的校验会发现它，R6 保证它不会被当成好数据服务。

#### 3.2.6 `meta.redb` 的表与写入规则

> 实现注记（M0）：`raft_state` 表里 `"format"`、`"snap_meta"`、`"applied"` 的编解码自 M0 起以 `core/src/meta/schema/raft_state.rs` 为唯一真源（金样在 `tests/golden/records.golden`）。本节没写 `MetaFormatV1.version` 的类型，取 `u16`（与 `ClusterInfoV1.format` 相同；postcard 对 1 的编码与 u32/u64 一样，都是一个字节 `0x01`）。

| 表 | key → value | 内容 |
|---|---|---|
| `raft_log` | `u64` → `&[u8]` | raft `Entry` 的 protobuf 字节 |
| `raft_state` | `&str` → `&[u8]` | `"hard_state"`：protobuf `HardState`；`"conf_state"`：protobuf `ConfState`；`"snap_meta"`：16 字节（压缩点 index u64、term u64，小端）；`"applied"`：24 字节（applied_index、applied_term、**state_version**，u64 小端）；`"format"`：`[1] ‖ postcard(MetaFormatV1{ version: 1, incompat: Vec<String> })` |
| `kv` | `&[u8]` → `&[u8]` | 状态机，布局见 3.2.7 |

写入规则（与 3.4.1.3 的 Ready 处理一一对应）：

| 规则 | 内容 |
|---|---|
| M1 | **任何一个写入 `applied = X` 的 redb 写事务，必须在同一事务里写入 commit ≥ X 的 `hard_state`**：第一个事务写 `rd.hs()`；若 `rd.hs()` 为 None 而本事务应用了条目，写 `node.raft.hard_state()`；第二个事务（应用 LightReady 条目）一律写 `node.raft.hard_state()`（advance 之后它已含新的 commit）。于是任何落盘状态都满足 `applied ≤ hs.commit`，重启不会触发 raft-rs 的 `applied_to` fatal |
| M2 | 第一个事务 `rd.must_sync()` 为真时 `Durability::Immediate`，否则 `Durability::None`；第二个事务一律 `Durability::None`（条目已在日志里持久，崩溃后可重放；None 提交对随后的读事务可见）。raft 线程每 1 s 检查：自上次 Immediate 之后有过 None 提交 → 补一次空的 Immediate 提交（限制"已应用未持久"的滞后，并让 redb 回收页面） |
| M3 | 日志压缩单独一个事务：压缩点 `C ≤ durable_applied − 10_000`（`durable_applied` = 最近一次 Immediate 提交时的 applied）；删除 `raft_log` 中 index ≤ C 的条目，`snap_meta = (C, term(C))`。因为 `durable hs.commit ≥ durable applied ≥ C`，raft-rs `load_state` 的 `hs.commit ≥ first_index − 1` 检查恒成立 |
| M4 | 重启：`raft::Config.applied = applied_index`；raft-rs 会把 `(applied, commit]` 的条目重新交给我们应用 |
| M5 | 快照安装（只在 follower，Ready 里 `snapshot()` 非空）：在第一个事务里 `delete_table(kv)` 后重建并写入全部键值、清空 `raft_log`、`snap_meta = (index, term)`、`conf_state` = 快照元数据里的、`applied = (index, term, 快照里的 state_version)`、`hard_state` = `rd.hs()` 且 `commit = max(commit, index)`；`Durability::Immediate` |

#### 3.2.7 元数据 KV 布局（契约）【ADR-0020】

> 实现注记（M0）：本节类型自 M0 起以 `core/src/meta/schema/keys.rs`（键空间标签、key 编码、GLOBAL 名字、保留 inode）为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

key 第 1 字节是键空间标签；整数大端。value = 1 字节记录版本号 + postcard（例外已注明）。

| 标签 | 名称 | key | value |
|---|---|---|---|
| `0x01` | INODE | `01 ino:u64` | `InodeV1` |
| `0x02` | DENTRY | `02 parent:u64 name` | `DentryV1 { ino: u64, kind: u8 }`；readdir = 前缀扫描 `02 parent`，按名字字节序 |
| `0x03` | XATTR | `03 ino:u64` | `XattrsV1 { entries: Vec<(Vec<u8>, Vec<u8>)> }`（含 `system.posix_acl_*`；合计 ≤ 64 KiB） |
| `0x04` | INLINE | `04 ino:u64` | 原始文件内容（≤ inline 阈值，硬上限 64 KiB）。**不变式：`len(INLINE) ≤ inode.size`，`[len, size)` 读为 0**；任何改 size 的操作都在同一批里维护它（3.5.1） |
| `0x05` | DIRATTR | `05 dir:u64` | `AttrMapV1 { entries: BTreeMap<String, String> }`（目录属性，值已规范化） |
| `0x06` | RSTAT | `06 dir:u64` | `RStatV1`（递归计数，3.9） |
| `0x07` | QUOTA_STATE | `07 dir:u64 class:u16` | `QuotaStateV1 { soft_since_ms: u64 }`（class = `0xFFFF` 表示总量配额） |
| `0x0A` | LOCKS | `0A ino:u64` | `LockTableV1 { locks: Vec<LockRecV1> }` |
| `0x0B` | SESSION | `0B session:u64` | `SessionRecV1` |
| `0x0C` | DEDUP | `0C session:u64 seq:u64` | `DedupV1`（编码后的回复，3.5.2） |
| `0x0D` | GC | `0D kind:u8 ino:u64 epoch:u32` | `GcRecV1`；kind 1 = 孤儿 inode（epoch 写 0），kind 2 = 废弃 epoch |
| `0x0E` | CHANGELOG | `0E seq:u64` | `ChangeRecV1` |
| `0x0F` | TRASH_INDEX | `0F expire_ms:u64 ino:u64` | 空（回收站到期索引） |
| `0x10` | NODE | `10 node:u32` | `NodeRecV1` |
| `0x11` | TARGET | `11 target:u32` | `TargetRecV1` |
| `0x12` | CHAIN | `12 chain:u32` | `ChainRecV1` |
| `0x13` | CLASS | `13 class:u16` | `StorageClassV1` |
| `0x14` | CLASS_NAME | `14 name` | `ClassIdV1 { id: u16 }` |
| `0x15` | TASK | `15 kind name` | `TaskRecV1`（后台任务状态与游标；kind/name 为 utf8，中间用 0x00 分隔） |
| `0x16` | KEYMETA | `16 key_id:u32` | `KeyMetaV1`（角色、租户、uid 范围、root_squash、**path_caps**、撤销标记；不含秘密） |
| `0x17` | DATAKEY | `17 datakey:u32` | `DataKeyV1`（用 `K_wrap` 包裹的数据加密密钥；datakey id 与认证 key id 是两套编号） |
| `0x18` | QOS | `18 dir:u64` | `QosSpecV1`（由 `qos` 属性派生，集群图里下发给数据节点） |
| `0x19` | RECONCILE_Q | `19 seq:u64` | `ReconcileItemV1 { dir: u64, reason: u8 }` |
| `0x1A` | CURSOR | `1A name` | `CursorV1 { seq: u64, updated_ms: u64 }`（changelog 消费者游标；`audit-log` 的游标由 core 在第一次启用 `audit` 时创建，3.8） |
| `0x1B` | EVICTED | `1B session:u64` | `EvictedV1 { evicted_ms: u64, map_epoch: u64 }`（保留 24 h，进集群图；`wmfs session list` 同时列出它们，状态显示 EVICTED） |
| `0x1C` | DATAKEY_NAME | `1C name` | `DataKeyIdV1 { id: u32 }`（与 CLASS_NAME 对称：`encrypt=tenant:<名字>` 在执行器闭包里靠它点查；`wmfs datakey create` 同批写 DATAKEY 与它） |
| `0x1F` | GLOBAL | `1F name` | 见下 |
| `0x20`–`0x2E` | 保留 | | 二期：快照 `0x20`、WASM 模块 `0x21`…… |
| `0x2F` | DEBUG | `2F key` | 原始字节；只有 `wmfs debug kv`（服务端开 `debug_ops`）读写，M3 演示用 |

`0x00`、`0x08`、`0x09`、`0x1D`–`0x1E` 与 `0xFF` 不使用。**插件不能拥有任何键空间**（理念文档 4.3.2）。

GLOBAL 的 name（utf8）：`next_ino`(u64)、`next_node`(u32)、`next_target`(u32)、`next_chain`(u32)、`next_class`(u16)、`next_key`(u32)、`next_datakey`(u32)、`next_changelog`(u64)、`next_reconcile`(u64)、`attr_epoch`(u64)、`map_epoch`(u64)、`cluster`(`ClusterInfoV1`)、`features`(`FeatureSetV1`)、`settings`(`ClusterSettingsV1`)、`plugins`(`PluginListV1`，集群插件清单，3.7.2)。计数器的 value 也是"版本字节 + postcard(u64/u32/u16)"。

**id 与计数器的初值**（init 与所有读者共同遵守）：node / target / chain / class / key / datakey 的 id 都从 1 开始，**0 保留表示"无"**（例如目录的 `class_id = 0`）；changelog 与 reconcile 的 seq 从 1 开始；`attr_epoch`、`map_epoch` 初值 1。init 之后：node 1、targets `1..=n`（n = `--target` 个数）、default 类 id 1、key 1 = admin、key 2 = 默认 client、根 = inode 1、`/home` = 16、`/scratch` = 17（init 不写 changelog），于是 `next_node = 2`、`next_target = n + 1`、`next_chain = 1`、`next_class = 2`、`next_key = 3`、`next_datakey = 1`、`next_changelog = 1`、`next_reconcile = 1`、`next_ino = 18`。key_id 0 不在 KEYMETA 里：它固定是服务端之间用的 Server 角色（3.3.2）。
实现注记（M4）：init 写的 TargetRecV1（`--target`，id 1..=n）的初值：`node = 1`、`state = 1`（Up）、`last_up_ms` = init 的时刻、`phys_devs` 为空、`boot_id = 0`（这两项由 M5 的 target 心跳填上）、`capacity` 取 `[[target]] capacity`（0 = 不限）、`read_only = false`；金样 `tests/golden/init_kv.golden` 的样本带一个 target。GLOBAL 里的整数计数器（u16/u32/u64）postcard 编码相同，代码里一律按 u64 读写（`core/src/meta/schema/kv.rs`），写出的字节不变。

保留 inode：1 = 根；2..=15 保留；普通 inode 从 16 开始分配、单调、永不复用。

#### 3.2.8 记录结构（`core/src/meta/schema/records.rs`）

> 实现注记（M0）：本节类型自 M0 起以 `core/src/meta/schema/records.rs`（`Role` 与 `Ts` 在 `core/src/base/types.rs`）为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。
> 实现注记（M0）：`ChangeRecV1.op`（u16，持久化）与 `AuditSpec.ops`（`Vec<u16>`）用的"操作码 ↔ 操作名"表见【ADR-0029】（2026-10-21 补齐）：表放在 `api/src/ops.rs`（M1 实现并加金样），数值显式、只追加、永不改号，名字不区分大小写，名字就是 `ChangeEvent.op`。1–14 是命名空间与会话操作（SessionOpen、SessionClose、Create、Mkdir、Mknod、Symlink、Link、Unlink、Rmdir、Rename、SetAttr、SetXattr、RemoveXattr、SetSize），100–107 是属性与系统事件（AttrSet、AttrUnset、SessionEvicted、PolicyViolation、SwitchLayout、ChecksumRepaired、DataLossDetected、TrashExpire），200–215 是管理操作（ClassCreate/Update/Delete、KeyCreate/Revoke、DataKeyCreate、NodeJoin/Drain/Remove/Promote、ChainForceServing、ClusterSet、PluginListChange、PluginListReset、FeatureEnable、TaskControl）。SessionHeartbeat、SetAtime、SetLock、DebugPut、游标提交不写 changelog。

全部 `#[derive(Serialize, Deserialize, Clone, Debug)]`，记录版本号均为 1：

```rust
// core/src/base/types.rs —— 全仓库唯一的角色编码：握手 Hello/Welcome、KeyMetaV1、ConnIdentity 都用它
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Role { Admin = 1, Client = 2, Server = 3 }
impl Role { pub fn from_u8(v: u8) -> Option<Role>; pub fn as_u8(self) -> u8; }   // 线上与 KV 里存 u8（0.2 第 11 条）

#[derive(Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Ts { pub secs: i64, pub nanos: u32 }

pub const KIND_FILE: u8 = 1; pub const KIND_DIR: u8 = 2; pub const KIND_SYMLINK: u8 = 3;
pub const KIND_FIFO: u8 = 4; pub const KIND_SOCK: u8 = 5; pub const KIND_CHR: u8 = 6; pub const KIND_BLK: u8 = 7;
pub const F_INLINE: u32 = 1; pub const F_ORPHAN: u32 = 2; pub const F_HAS_ACL: u32 = 4; pub const F_TRASH_DIR: u32 = 8;

pub struct InodeV1 {
    pub kind: u8,
    pub mode: u32,                          // 权限位 + suid/sgid/sticky（不含类型位）
    pub uid: u32, pub gid: u32,
    pub nlink: u32,
    pub size: u64,                          // 文件：逻辑大小；目录：直接子项数（rmdir 判空）；符号链接：目标长度
    pub atime: Ts, pub mtime: Ts, pub ctime: Ts, pub btime: Ts,
    pub rdev: u32,
    pub parent: u64,                        // 主链接所在目录（递归计数归属于它）
    pub name: Vec<u8>,                      // 主链接的名字（拼路径用）
    pub extra_links: Vec<(u64, Vec<u8>)>,   // 其余硬链接 (parent, name)
    pub flags: u32,
    pub change_id: u64,                     // 数据或属性每变一次 +1
    pub class_id: u16,                      // 文件数据实际所在的存储类；目录为 0
    pub layout: Option<LayoutV1>,           // 只有普通文件有
    pub pending_epoch: u32,                 // 迁移中的新 epoch（0 = 无）
    pub max_epoch: u32,                     // 曾分配过的最大 epoch；新 epoch = max_epoch + 1
    pub trash_expire_ms: u64,               // 在回收站里 = 到期时间，否则 0
    pub symlink: Vec<u8>,                   // 只有符号链接
}
pub struct LayoutV1 {
    pub epoch: u32,                         // fencing token
    pub chunk_size: u32, pub block_size: u32,
    pub chains: Vec<u32>,                   // 实际条带数 k = chains.len()（可用链不够时少于声明的 stripe，3.4.2.1）
    pub replicas: u8,                       // 创建时声明的副本数
    pub codec: u8,                          // 1 = replica
    pub checksum: u8, pub compress: u8, pub compress_level: i8, pub encrypt: u8, pub encrypt_key: u32,
    pub inline_max: u32,                    // 创建时的 inline 阈值
}
pub struct DentryV1 { pub ino: u64, pub kind: u8 }
pub struct XattrsV1 { pub entries: Vec<(Vec<u8>, Vec<u8>)> }
pub struct AttrMapV1 { pub entries: std::collections::BTreeMap<String, String> }
pub struct RStatV1 { pub bytes: u64, pub files: u64, pub dirs: u64, pub per_class: Vec<(u16, u64, u64)> } // (class, bytes, files)，按 class 升序
pub struct QuotaStateV1 { pub soft_since_ms: u64 }
pub const LOCK_EOF: u64 = i64::MAX as u64;            // "到文件尾"：与内核 OFFSET_MAX 一致（3.5.3）
pub struct LockRecV1 { pub session: u64, pub owner: u64, pub pid: u32, pub start: u64, pub end: u64, pub write: bool } // [start, end] 闭区间；end ≤ LOCK_EOF
pub struct LockTableV1 { pub locks: Vec<LockRecV1> }    // 按 (start, session, owner) 排序
pub struct SessionRecV1 { pub client_name: String, pub host: String, pub host_id: String, pub mount: String,
                          pub key_id: u32, pub tenant: u32, pub created_ms: u64,
                          pub locked_inos: Vec<u64>,     // 本会话持有锁的 inode（释放/驱逐时点操作，不扫描）
                          pub state: u8,                 // 1 Active，2 Evicting（第一阶段已完成，3.4.4）
                          pub evict_since_ms: u64, pub evict_map_epoch: u64 }
pub struct DedupV1 { pub code: u16, pub reply: Vec<u8> }  // code 0 = 成功（reply 为 postcard(MetaReply)），否则 POSIX errno
pub struct GcRecV1 { pub due_ms: u64, pub chains: Vec<u32>, pub created_ms: u64 }
pub struct ChangeRecV1 { pub time_ms: u64, pub op: u16, pub result: u16, pub trace_id: u64,
                         pub uid: u32, pub gid: u32, pub tenant: u32, pub session: u64, pub job_id: u64,
                         pub ino: u64, pub parent: u64, pub name: Vec<u8>, pub new_parent: u64, pub new_name: Vec<u8>,
                         pub size: u64, pub extra: Vec<(String, String)>, pub audited: bool }
pub struct NodeRecV1 { pub addrs: Vec<(String, String)>, // (scheme, 地址)，例如 ("tcp", "127.0.0.1:19100")、("rdma", "10.77.0.1:19101")；来自各传输插件 listen() 的返回值
                       pub mgmt_addr: String,
                       pub host_id: String, pub machine_id: String, pub hostname: String,
                       pub meta_role: u8,             // 0 不参与，1 learner，2 voter
                       pub features: Vec<String>, pub joined_ms: u64,
                       pub state: u8 }                // 1 Active，2 Draining，3 Removed
pub struct TargetRecV1 { pub node: u32, pub path: String, pub labels: Vec<String>,
                         pub phys_devs: Vec<String>,  // target 所在的物理整盘/namespace（例如 "nvme0n1"，6.3）；空 = unknown（loop、解析失败），一律按共享设备对待
                         pub capacity: u64, pub boot_id: u64,
                         pub state: u8,               // 1 Up，2 Offline，3 Draining，4 Removed
                         pub last_up_ms: u64, pub read_only: bool }
pub struct ChainRecV1 {
    pub ver: u64,                           // 任何成员/状态变化 +1；数据请求携带它作为 fencing
    pub incarnation: u32,                   // 唯一幸存者重启/强制 serving 时 +1（3.4.2.7）；从 1 开始
    pub class_id: u16,
    pub replicas: u8,                       // 期望长度
    pub members: Vec<(u32, u8)>,            // (target, 1 Serving | 2 Syncing)；顺序即链序；不变式 I0
    pub last_serving: Option<u32>,          // 最后一个 Serving 成员被移除时记下它
    pub removed: Vec<(u32, u64)>,           // 最近被移除的 (target, removed_ms)，rejoin_window 内回来可直接以 Syncing 追加
}
pub struct StorageClassV1 { pub id: u16, pub name: String, pub label: String, pub dedicated: bool, pub replica: u8,
                            pub iops_min: u32, pub iops_max: u32, pub bw_min: u64, pub bw_max: u64, pub burst_ms: u32,
                            pub weight: u32, pub latency_target_us: u32,
                            pub priority: u8,        // 1 normal，2 low（label 为 hdd 的类默认 low，4.5、6.3）
                            pub failure_domain: u8,  // 首发只有 1 = node
                            pub locality: u8,        // 0 any，1 prefer-client
                            pub tier_to: Option<String>, pub tier_when: Option<String> }
pub struct ClassIdV1 { pub id: u16 }
pub struct TaskRecV1 { pub state: u8,                // 1 Running 2 Paused 3 Done 4 Failed
                       pub cursor: Vec<u8>,          // 对 core 不透明；每步之后持久化 ⇒ 断点续跑
                       pub pass: u64, pub done: u64, pub total_hint: u64, pub backlog: u64,
                       pub bytes_per_sec: u64, pub items_per_sec: u64,
                       pub updated_ms: u64, pub last_error: String }
pub struct KeyMetaV1 { pub name: String, pub role: u8, // Role::as_u8()：1 admin，2 client（KEYMETA 里不会出现 3 server，server 只用 key 0）
                       pub tenant: u32, pub uid_ranges: Vec<(u32, u32)>, pub root_squash: bool,
                       pub path_caps: Vec<(u64, bool)>,  // (允许的子树根 ino, 只读?)；空 = 不限
                       pub revoked: bool, pub created_ms: u64 }
pub struct DataKeyV1 { pub name: String, pub tenant: u32, pub wrapped: Vec<u8> } // nonce(12) ‖ 密文(32) ‖ tag(16)
pub struct DataKeyIdV1 { pub id: u32 }
pub struct QosSpecV1 { pub r: u32, pub w: u32, pub l: u32, pub bw_max: u64, pub burst_ms: u32 } // 集群级数值；0 = 不设
pub struct ReconcileItemV1 { pub dir: u64, pub reason: u8 }   // 1 属性变化，2 目录 rename，3 存储类变化
pub struct CursorV1 { pub seq: u64, pub updated_ms: u64 }
pub struct PluginListV1 { pub plugins: Vec<String> }  // 排好序的 "钩子:名字"，例如 "authz:posix-mode"（只比名字，3.7.2）
pub struct EvictedV1 { pub evicted_ms: u64, pub map_epoch: u64 }
pub struct ClusterInfoV1 { pub cluster_id: [u8; 16], pub created_ms: u64, pub format: u16 }
pub struct FeatureSetV1 { pub compat: Vec<String>, pub ro_compat: Vec<String>, pub incompat: Vec<String> }
pub struct ClusterSettingsV1 {                // 集群运行参数：必须全集群一致（租约安全依赖它），所以存在复制状态里
    pub session_lease_ms: u64,                // 30_000
    pub target_lease_ms: u64,                 // 8_000
    pub cap_ttl_ms: u64,                      // 600_000
    pub repair_delay_ms: u64,                 // 1_800_000
    pub rejoin_window_ms: u64,                // 1_800_000
    pub epoch_gc_grace_ms: u64,               // 60_000
    pub bg_bytes_per_sec: u64,                // 200 MiB
}
```

`ClusterSettingsV1` 不是数据策略，而是协议时序参数：持有方自停与授予方回收必须用同一个租约时长，各节点配置不同会破坏安全性，所以它们放在复制状态里、经集群图下发，用 `wmfs cluster set <名>=<值>`（admin）修改并写 changelog（名字去掉 `_ms` 后缀、值写时长或字节数，例如 `wmfs cluster set epoch_gc_grace=0s`——演示里用它让旧 epoch 立即可删；`epoch_gc_grace` 为 0 也安全，持有旧 layout 的读写会按 C4/C5 刷新）。

#### 3.2.9 raft 条目负载 `WriteBatchV1`（契约）

> 实现注记（M0）：本节类型自 M0 起以 `core/src/meta/schema/records.rs` 为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

```rust
// EntryNormal：data 为空 = 新 leader 的空条目（no-op）；否则 data = [0x01] ‖ postcard(WriteBatchV1)；context 为空
#[derive(Serialize, Deserialize)]
pub struct WriteBatchV1 {
    pub expect_version: u64,    // 执行器构建批次时读到的 state_version（3.4.1.5）
    pub leader_term: u64,       // 构建时的 term（提议前校验；apply 时只作诊断）
    pub ops: Vec<KvOp>,         // 已按 key 排序、同 key 只留最后一次
    pub trace_ids: Vec<u64>,    // 诊断用
}
#[derive(Serialize, Deserialize)]
pub enum KvOp { Put { key: Vec<u8>, value: Vec<u8> }, Delete { key: Vec<u8> } }
// EntryConfChangeV2：raft-rs 自己的 protobuf；wmfs 只提议 V2，从不提议 v1 EntryConfChange
```

follower 只做"按顺序应用 ops"——没有业务逻辑，所以不同软件版本的节点可以混跑（滚动升级安全），只要 KV 格式不变。

#### 3.2.10 feature flags（参考 ZFS）

| 层级 | 位置 | 规则 |
|---|---|---|
| chunk | 头部三组位 | 不认识 incompat → 拒绝打开该 chunk；不认识 ro_compat → 只读；compat → 忽略 |
| target | `TARGET` 三组位 | 同上，作用于整个 target |
| 元数据 | `raft_state["format"]` + GLOBAL `features` | 不认识 incompat → 节点拒绝启动元数据角色 |
| 协议 | 握手 `features: u64` 与 `required: u64` | 对方不支持我方 `required` 中的位 → 拒绝连接（`PROTO_UNSUPPORTED`） |

启用新的 incompat 特性只能通过 `wmfs feature enable <name>`：leader 检查所有节点 `NodeRecV1.features` 都包含它后才写入 GLOBAL `features`，写 changelog，**不可逆**。首发没有默认启用的可选 incompat 特性；预留名（都是二期）：`chunk.snapshots`、`chunk.dyn_transform`（动态库压缩算法）、`meta.ec-layout`、`proto.pipelined-meta`。

#### 3.2.11 版本纪律（给实现 agent 的硬规则）

1. 任何持久化记录 = 版本字节 + postcard；**永不原地修改已发布的结构体**，改字段就新增 `XxxV2` 和新版本字节，解码器同时认识所有旧版本。
2. 帧头、chunk 头、块表条目、超级块的字节布局固定；扩展只能用保留字段 + feature 位。
3. postcard 不自描述：消息枚举的新变体只能加在末尾，并且只能在对端于握手中声明支持（feature 位）后发送；已有变体的字段永不增删改。
4. 每种消息与记录在 `tests/golden/` 里有一份固定字节的"金样"，测试比对，防止无意中改了线格式或磁盘格式。（实现注记（M0）：金样按类别存成文本文件 `tests/golden/<类别>.golden`，每行 `<样本名> <hex>`；比对测试在 `core/tests/golden_*.rs`，每个枚举的每个变体一份，并检查首字节就是变体序号；有意改格式（且已有 ADR）后用 `WMFS_GOLDEN_UPDATE=1 cargo test -p wmfs-core golden` 重新生成，再看 `git diff tests/golden/`。每个枚举还要确认没有更多的变体（在末尾加了变体却没加金样会失败）；`tests/golden/constants.golden` 钉住全部数值常量（msg_type、flags、feature 位、种类与标志、键空间标签、GLOBAL 名字、算法编号、错误码表……）；样本里相邻字段一定取不同的值，并有超过 u32 的整数。重写金样时测试会打印醒目提示；验收与 CI 会先 `unset WMFS_GOLDEN_UPDATE`。）


### 3.3 线上协议【ADR-0018】

> 实现注记（M0）：本节类型自 M0 起以 `core/src/proto/`（各小节开头写明具体文件）为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

TCP 与 RDMA 跑**同一种帧**（理念文档第 7 章 [默认]）。帧 = 48 字节帧头 + body（postcard 编码的消息，永远是小结构）+ bulk（原始字节：TCP 紧跟在 body 后；支持单边操作的传输用描述符，由"接收请求的一方"发起单边操作，3.6.4）。**大块字节一律走 bulk**——chunk 数据、packed chunk、raft 消息的 protobuf 都是。帧头编解码在 core（`proto/frame.rs`），传输插件只搬字节。

#### 3.3.1 帧头（48 字节，小端）

> 实现注记（M0）：本节类型自 M0 起以 `core/src/proto/frame.rs`（`FrameHeader` 的编解码、flags、长度上限、`ErrorBody`）为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

| 偏移 | 长度 | 字段 | 说明 |
|---|---|---|---|
| 0 | 4 | magic | `b"WMF1"` |
| 4 | 1 | proto_major | = 1；不等即断开 |
| 5 | 1 | kind | 0 Request，1 Response，2 OneWay，3 StreamChunk |
| 6 | 2 | msg_type | 3.3.3 |
| 8 | 8 | req_id | 连接内唯一；响应原样带回；OneWay 为 0 |
| 16 | 8 | trace_id | 客户端每个 FUSE 操作生成一个，全链路透传；0 = 接收方生成 |
| 24 | 2 | flags | bit0 `HAS_BULK`，bit1 `BULK_REMOTE`（bulk 是对端内存描述符表），bit2 `REPLY_REMOTE`（请求附带"响应 bulk 写到这里"的描述符），bit3 `ERROR`（body 是 `ErrorBody`），bit4 `LAST_CHUNK`（流结束）。bit1、bit2 只由支持单边操作的传输插件设置与解析，core 不看 |
| 26 | 2 | span_id | 每一跳 +1（慢请求时间线排序用） |
| 28 | 4 | body_len | ≤ 1 MiB（某个传输可以更小：RDMA 的帧头 + body ≤ 120 KiB，3.6.4） |
| 32 | 4 | bulk_len | ≤ 16 MiB + 128 KiB |
| 36 | 4 | body_crc32c | crc32c(body) |
| 40 | 4 | bulk_crc32c | crc32c(bulk)；无 bulk 为 0；单边传输由接收方在 READ 完成后校验 |
| 44 | 4 | header_crc32c | crc32c(帧头 0..44) |

`ErrorBody { code: u16, msg: String, leader_hint: Option<String>, retry_after_ms: u32, extra: u64 }`（`extra` 用于 `CHUNK_DIRTY` 携带 last serving 的 target id、`STALE_CHAIN` 携带当前 chain_ver）。超限或 CRC 错误 → 关闭连接（`PROTO_UNSUPPORTED` 计数）。

实现注记（M4）：规格没写 `leader_hint` 的格式，取 **leader 在 `control_scheme` 上的 rpc 地址**（例如 `"127.0.0.1:19100"`，与 M0 金样里 ErrorBody 样本的写法相同，客户端用自己的 control_scheme 去连它）；只在 `NOT_LEADER` 时由 core 自动填（`RpcServer::set_leader_hint` 登记的函数：leader 是自己就用本节点实际监听的地址，否则取已应用的 NodeRecV1，与 health 里的 leader 地址共用 `server.rs` 的 `leader_addrs`），不知道 leader 时为 None。

#### 3.3.2 握手、认证与能力协商

> 实现注记（M0）：本节类型自 M0 起以 `core/src/proto/handshake.rs`（feature 位在 `core/src/proto/features.rs`）为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。
> 实现注记（M1）：① 证明与派生的计算在 `core/src/proto/handshake.rs`（`derive_secret`、`auth_proof`、`server_proof` 等），客户端与 shared-key 插件共用。规格没写 `proof` 输入里整数的编码，取 **`role` 1 字节、`key_id` 大端 4 字节**（与 `secret(k)` 的 `k_be32` 一致），`cluster_id` 用 Hello 里的 16 字节（未知全 0）；金样 `tests/golden/auth.golden`。② 握手在每条连接的**读线程**上执行（发帧经写线程），见 `core/src/net/handshake.rs`；被拒时回的 ERROR 帧 `kind = Response`、`msg_type` 沿用请求的（Hello 或 Auth）、flags 带 ERROR，然后断开。③ M1 本端支持的协议 feature 位为 0（随功能实现逐个加入）。④ 密钥文件里没有角色，客户端在 Hello 里声明的角色：`RpcClient::new` 按 init 的约定推断（key 0 = server、key 1 = admin、其余 = client），别的 key 用 `RpcClient::new_with_role`；`wmfs ping --role` 可以故意声明别的角色（演示 client key 冒充 server 被拒）。⑤ M1 还没有元数据 KV，服务端认得的 key 是 init 写进 KEYMETA 的初始值（`core/src/init.rs` 的 `InitKeys`：key 1 = admin、key 2 = 默认 client），M3/M4 起改读复制状态。 ⑥ 握手成功后两端都调用 `ConnHandle::authenticated`（服务端同时记下 `ConnIdentity`），它再调用 `Conn::mark_authenticated`。"未完成认证的连接只允许握手帧"分两层执行：传输插件在 `mark_authenticated` 之前收到**声明了 bulk** 的帧就直接断开、不为它取池槽（握手帧从不带 bulk；tcp、mem 都这样做，rdma 对 `BULK_REMOTE/REPLY_REMOTE` 同理）；core 的握手代码收到别的帧也直接断开，并计入 `wmfs_conn_handshake_failed_total{code="PROTO_UNSUPPORTED"}`。
> 实现注记（M2）：协议 feature 位 bit0 `BULK_REMOTE` **首发不在握手里协商**（`OUR_FEATURES` 仍为 0，core 不改）：单边描述符只在 rdma 传输内部出现，双方都是 rdma 插件才会建出这条连接，由插件两端自己处理；tcp 收到带 `BULK_REMOTE`/`REPLY_REMOTE` 的帧就断开（mem 是进程内的示例传输，帧直接来自同一进程的 core，core 从不置这两位，所以 mem 不查）。

握手由 **core** 在传输插件交出的 `Conn` 上执行，与传输无关（TCP、RDMA、mem 完全相同；传输插件只负责把连接建起来）。

```
C → S  Hello     { proto_min: u8, proto_max: u8, features: u64, required: u64, role: u8 /*Role::as_u8()：1 admin 2 client 3 server*/,
                   cluster_id: [u8;16] /*未知全 0*/, key_id: u32, client_nonce: [u8;16], software: String }
S → C  Challenge { proto: u8, features: u64 /*双方交集*/, cluster_id: [u8;16], node_id: u32, server_nonce: [u8;16] }
C → S  Auth      { proof: [u8;32] }   // HMAC-SHA256(secret(key_id), "wmfs-auth-v1" ‖ client_nonce ‖ server_nonce ‖ role ‖ cluster_id ‖ key_id)
S → C  Welcome   { key_id: u32, role: u8 /*同 Hello*/, tenant: u32, server_proof: [u8;32] }  // HMAC(secret(key_id), "wmfs-srv-v1" ‖ server_nonce ‖ client_nonce)
       或 ERROR(AUTH_FAILED / PROTO_UNSUPPORTED)，然后断开
```

- 未完成认证的连接只允许握手帧，其他帧直接断开；认证完成后 core 调用 `Conn::mark_authenticated()`，传输插件在此之前不得为对端执行任何单边操作（3.6.4）。验证 `proof` 的是 `authn` 插件（shared-key，4.2）；客户端也校验 `server_proof`（双向认证，防止连到冒充的服务端）。
- **角色规则（core 强制，与 authn 插件无关）**：`Role` 只有一套编码（3.2.8）。`key_id == 0` ⇒ 角色必须是 Server；`key_id ≥ 1` ⇒ KEYMETA 里有它、未撤销，且 `KEYMETA.role == Hello.role`。authn 插件 Accept 之后 core 再核对 `ConnIdentity.role == Hello.role`；任何一条不满足 → `AUTH_FAILED`。于是 client key 不可能冒充 server 去调用 PeerCall/NodeCall。测试：金样测试 + "client key 申请 server 角色被拒"的单测。
- 密钥派生（只有 `cluster.key` 是秘密根，节点之间从不传输秘密）：`secret(k) = HMAC-SHA256(K_master, "wmfs-key-v1" ‖ k_be32)`；`K_cap = HMAC(K_master, "wmfs-cap-v1")`；`K_wrap = HMAC(K_master, "wmfs-wrap-v1")`；服务端之间用 key_id 0（Server）。密钥文件格式一行：`wmfs-key-v1:<key_id>:<64 位 hex secret>`。
- 版本：帧头 `proto_major` 必须相等；握手在 `[min, max]` 交集里选最高（首发只有 1）；收到不认识的 msg_type 或枚举变体 → `FEATURE_UNSUPPORTED`，不断开。
- 协议 feature 位：bit0 `BULK_REMOTE`（本端能处理单边 bulk 描述符），bit1 `STRONG`，bit2 `LOCKS`，bit3 `INLINE`，bit4 保留（二期 `DYN_TRANSFORM`）；bit 32 起保留（`PIPELINED_META`、`SNAPSHOT`、`EC`）。
- 首发**不加密线上流量**（无 TLS）：握手双向认证、不防窃听。部署前提"只在可信网络使用"写进 semantics.md 与 `docs/ops.md`【ADR-0009】。

#### 3.3.3 消息族、msg_type、角色权限与传输选择

> 实现注记（M0）：本节类型自 M0 起以 `core/src/proto/frame.rs`（msg_type 常量；`PingBody` 在 `core/src/proto/handshake.rs`）为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。
> 实现注记（M1）：服务端（`core/src/net/server.rs`）先查角色权限表（不符 → `EACCES`，计数 `wmfs_rpc_denied_total{role,family}`），再处理 bulk 被丢弃（→ `BUSY`），再分发：Ping 由 core 原样回显（bulk 用收到的池缓冲，零拷贝）；其他族交给注册的 `Service`（`RpcServer::add_service`，M3 起）；没有服务或不认识的 msg_type → `FEATURE_UNSUPPORTED`，不断开。**保活**：客户端连接**握手完成之后**，写线程空闲 5 s 发一个 `req_id = 0` 的 Ping（回复被忽略；握手完成之前不发——服务端会把它当成"未认证连接上的非握手帧"断开，于是 Challenge 慢于 5 s 的握手会永远失败）；服务端从不向客户端发请求；"15 s 收不到任何帧判死"由传输插件做（tcp：读超时 15 s）。角色权限表对 **OneWay 帧也查**：不符的 OneWay（例如 client 发 RaftMsg）没法回 `EACCES`，只计数 `wmfs_rpc_denied_total` 并丢弃（M8 加 OneWay 服务时不会绕过角色表）。指标 `wmfs_rpc_requests_total{family,op,code}` 的 `op` 在 M1 暂时等于 family，各服务解出具体操作后再细分；每个族的指标句柄在第一次用到时注册、之后从 `RpcServer` 里直接取（3.11"热路径无锁"，错误码的计数器第一次出现时注册）。

| msg_type | 名称 | kind | body | 族 |
|---|---|---|---|---|
| 0x0001–0x0004 | Hello / Challenge / Auth / Welcome | Request/Response | 3.3.2 | — |
| 0x0005 | Ping | Request → Response | `PingBody { payload: Vec<u8> }` + 可选 bulk，原样回显（传输自检、测延迟带宽） | 任何传输 |
| 0x0100 | MetaCall | Request → Response | `MetaCall { head: ReqHead, op: MetaOp }` → `MetaReply` | 控制 |
| 0x0200 | DataCall | Request → Response | `DataCall { cap: Vec<u8>, op: DataOp }` → `DataReply` | 数据 |
| 0x0300 | PeerCall | Request → Response | `PeerOp` → `PeerReply`（数据节点之间） | 数据 |
| 0x0400 | NodeCall | Request → Response | `NodeOp` → `NodeReply`（节点 ↔ 元数据 leader） | 控制 |
| 0x0500 | RaftMsg | OneWay | body 为空；**bulk** = protobuf 编码的 `eraftpb::Message`（一个 MsgAppend 可能超过 1 MiB，见 3.4.1.4） | 控制 |
| 0x0501 | SnapshotFetch | Request → StreamChunk… | 3.4.1.7 | 控制 |
| 0x0600–0x06FF | 传输插件私有 | 插件自定 | 不透明字节；只出现在插件自己的引导连接上，core 的分发器从不处理（例如 RDMA 插件用 0x0600 交换 QP 信息，4.1） | — |

一个 msg_type 对应一个固定的 body 结构；同族内的变化只能是"在枚举末尾加变体 + 用 feature 位门控"。指标里的 `op` 标签取变体名（一个 `fn name(&self) -> &'static str`）。

**角色权限**（core 在分发前检查，不符 → 回 `EACCES` 并计数 `wmfs_rpc_denied_total`）：

| 消息族 | Client | Admin | Server（key 0） |
|---|---|---|---|
| Ping、MetaCall、DataCall | ✓ | ✓（另可 `DebugPut/DebugGet`；`wmfs fs --as-uid` 声明任意 uid） | ✓ |
| NodeCall | 只有 `GetClusterMap`（过滤后：不含 keys、datakeys） | 同 Client | 全部 |
| PeerCall、RaftMsg、SnapshotFetch | ✗ | ✗ | ✓ |

**传输选择**（core 的 `RpcClient` 按族选 scheme，3.12；node.toml `[net] control_scheme = "tcp"`、`data_scheme = "tcp"|"rdma"|…`，挂载参数 `--transport` 设置客户端的 data_scheme）：

- 控制族（MetaCall、NodeCall、RaftMsg、SnapshotFetch）**永远**走 `control_scheme`。原因：这些 body 可能很大（集群图几十到上百 KB、ReadDir 回复、心跳全集、快照流），而 RDMA 的 SEND 槽位只有 120 KiB。
- 数据族（DataCall、PeerCall）走 `data_scheme`，前提是对端在 `NodeRecV1.addrs` 里公布了这个 scheme。客户端：对端没有公布 → **报错**（挂载时就检查并列出缺的节点；之后新加入的节点也一样报错），不静默降级；服务端之间：退回 `control_scheme`，并在 `wmfs status` 里警告"n→m 的复制未走 <scheme>"。
- core 里只有 scheme 字符串比较，没有任何传输名字的特殊分支。

#### 3.3.4 客户端 ↔ 元数据：`MetaOp`

> 实现注记（M0）：本节类型自 M0 起以 `core/src/proto/meta.rs` 为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

所有客户端→元数据请求以公共头开始。`seq` 由客户端为每个**修改型**请求递增（只读请求 `seq = 0`）；每个会话同时在途的修改型请求不超过 `DEDUP_WINDOW = 256` 个。

```rust
pub struct ReqHead { pub session: u64, pub seq: u64, pub uid: u32, pub gid: u32, pub groups: Vec<u32>, pub pid: u32, pub job_id: u64 }

pub enum MetaOp {
    // ---- 会话 ----
    SessionOpen { client_name: String, host: String, host_id: String, mount: String, version: String }, // W；session 由客户端随机生成，按 session 幂等
    SessionHeartbeat { opened: Vec<(u64, bool)>, closed: Vec<u64>, full: Option<Vec<(u64, bool)>> },    // R*；(ino, 是否写打开)
    SessionClose,                                                                                    // W
    // ---- 只读（R：ReadIndex 后在 meta-read 线程执行）----
    Lookup { parent: u64, name: Vec<u8> },
    GetAttr { ino: u64 },
    ReadDir { ino: u64, after: Vec<u8>, limit: u32, plus: bool },        // 按名字分页；after 为空 = 从头；limit ≤ 1024，服务端还保证回复编码 ≤ 512 KiB（先到先停，eof = false）
    ReadLink { ino: u64 },
    GetXattr { ino: u64, name: Vec<u8> },
    ListXattr { ino: u64 },
    StatFs,
    Open { ino: u64, flags: u32 },                                       // 不写 raft（flags 里不会有 O_TRUNC：内核随后另发 SetAttr{size: 0}，8.1）
    GetInline { ino: u64 },
    GetLock { ino: u64, owner: u64, start: u64, end: u64, write: bool },
    RenewCap { ino: u64, write: bool },
    ResolvePath { path: Vec<u8> },                                       // CLI / 调试客户端
    // ---- 修改（W：进执行器、走 raft）----
    Create { parent: u64, name: Vec<u8>, mode: u32, umask: u32, excl: bool },
    Mkdir { parent: u64, name: Vec<u8>, mode: u32, umask: u32 },
    Mknod { parent: u64, name: Vec<u8>, mode: u32, umask: u32, kind: u8, rdev: u32 },
    Symlink { parent: u64, name: Vec<u8>, target: Vec<u8> },
    Link { ino: u64, new_parent: u64, new_name: Vec<u8> },
    Unlink { parent: u64, name: Vec<u8> },
    Rmdir { parent: u64, name: Vec<u8> },
    Rename { parent: u64, name: Vec<u8>, new_parent: u64, new_name: Vec<u8>, flags: u32 },
    SetAttr { ino: u64, set: SetAttrIn },                                // 改了 size → 回 Opened(OpenOut)，否则回 Entry
    SetXattr { ino: u64, name: Vec<u8>, value: Vec<u8>, flags: u32 },
    RemoveXattr { ino: u64, name: Vec<u8> },
    SetAtime { items: Vec<(u64, Ts)> },                                  // relatime 批量更新（尽力而为）
    CommitWrite { ino: u64, epoch: u32, new_size: u64, mtime: Ts,
                  inline_patches: Vec<(u64, Vec<u8>)>,                   // inline 文件：按区间打补丁
                  uninline_expect_change_id: Option<u64>,                // Some = 内容已写进 chunk，清除 INLINE（CAS）
                  want_quota: bool, close: bool },
    AppendReserve { ino: u64, epoch: u32, len: u64 },                    // strong 的 O_APPEND：原子分配偏移
    Fallocate { ino: u64, offset: u64, len: u64, mode: u32 },           // 只支持 mode 0
    SetLock { ino: u64, owner: u64, pid: u32, start: u64, end: u64, kind: u8 /*1 读 2 写 3 解锁*/ },  // 永远非阻塞：冲突 → EAGAIN（F_SETLKW 由客户端轮询，3.5.3）
    // ---- 调试（仅 admin key，且服务端 node.toml 里 debug_ops = true）----
    DebugPut { key: Vec<u8>, value: Vec<u8> },                           // 写 0x2F 键空间
    DebugGet { key: Vec<u8> },
}
pub struct SetAttrIn { pub mode: Option<u32>, pub uid: Option<u32>, pub gid: Option<u32>, pub size: Option<u64>,
                       pub atime: Option<TimeSet>, pub mtime: Option<TimeSet>, pub epoch_seen: u32 }
pub enum TimeSet { Now, At(Ts) }

pub enum MetaReply {
    Session { lease_ms: u64, map_epoch: u64 },
    Heartbeat { lease_ms: u64, map_epoch: u64, need_full_open_set: bool },
    Entry { attr: AttrOut },
    Dir { entries: Vec<DirEntryOut>, eof: bool },
    Link { target: Vec<u8> },
    Xattr { value: Vec<u8> },
    XattrNames { names: Vec<Vec<u8>> },
    StatFs { bsize: u32, blocks: u64, bfree: u64, files: u64, ffree: u64, namelen: u32 },
    Opened(OpenOut),                                                     // Open、Create、改了 size 的 SetAttr 共用
    Inline { data: Vec<u8>, change_id: u64 },
    Lock { conflict: Option<LockOut> },
    Cap { cap: Vec<u8> },
    Resolved { ino: u64, kind: u8 },
    Committed { attr: AttrOut, quota_end: u64 },
    Offset { offset: u64, attr: AttrOut },
    Ok,
    DebugValue { value: Option<Vec<u8>> },
}
pub struct AttrOut { pub ino: u64, pub kind: u8, pub mode: u32, pub uid: u32, pub gid: u32, pub nlink: u32,
                     pub size: u64, pub atime: Ts, pub mtime: Ts, pub ctime: Ts, pub btime: Ts, pub rdev: u32,
                     pub change_id: u64, pub epoch: u32,
                     pub consistency: u8,       // 1 cto，2 strong（该 inode 所在目录的生效属性）
                     pub ttl_ms: u32,           // 内核 entry/attr 缓存时长：cto = 生效的 attr_ttl，strong = 0（3.1.4 C1）
                     pub flags: u32 }
pub struct DirEntryOut { pub name: Vec<u8>, pub ino: u64, pub kind: u8, pub attr: Option<AttrOut> }
pub struct OpenOut { pub attr: AttrOut, pub layout: Option<LayoutV1>, pub cap: Vec<u8>,
                     pub inline: Option<Vec<u8>>,  // inline 文件的完整内容（≤ 64 KiB），小文件 open+read 一次往返
                     pub quota_end: u64,           // 本次授予的写入上限（字节偏移）；u64::MAX = 子树无配额
                     pub atime_due: bool }         // relatime 条件成立，客户端应排队一次 SetAtime
pub struct LockOut { pub start: u64, pub end: u64, pub write: bool, pub pid: u32 }
```

"R*"：会话心跳走只读路径（必须先过 ReadIndex，确认自己仍是 leader 才续租），并更新 leader 内存里的 `last_seen` 与打开文件集合（3.5.6）。

实现注记（M3）：服务端在 `core/src/meta/service.rs`。`DebugPut`/`DebugGet`：不是 admin key、或 node.toml 没有 `debug_ops = true`（`wmfs dev` 打开，`wmfs init` 默认关闭）→ `EACCES`；其余 MetaOp 在实现它们的里程碑之前回 `FEATURE_UNSUPPORTED`（不断开）。修改型请求交给执行器（`MetaEngine::txn`，投递后立即返回，队列满回 `BUSY`），只读请求交给 8 个 `meta-read` 线程（队列 1024，满了回 `BUSY`）。指标 `wmfs_meta_ops_total{op,code}`。M3 还没有会话（M4），`wmfs debug kv` 用 seq = 0，不进去重表。

实现注记（M4）：命名空间操作在 `core/src/meta/ns/`，会话在 `core/src/meta/sessions.rs`，客户端在 `core/src/client/meta_client.rs`。

- ① 已实现：SessionOpen/SessionHeartbeat/SessionClose、Lookup、GetAttr、ReadDir、ReadLink、GetXattr、ListXattr、ResolvePath、Open（先只回属性、layout 与 inline 内容，`wmfs fs stat` 用它看 layout；cap 的签发在 M5、`quota_end` 在 M10）、Create、Mkdir、Mknod、Symlink、Link、Unlink、Rmdir、Rename、SetAttr、SetXattr、RemoveXattr；StatFs、GetInline、GetLock、RenewCap、SetAtime、CommitWrite、AppendReserve、Fallocate、SetLock 仍回 `FEATURE_UNSUPPORTED`。
- ② SessionOpen 与 SessionClose 一律按 seq 0 执行（不进去重表：SessionOpen 按 session 幂等；SessionClose 本身幂等——会话已不在照常回 Ok，而带 seq 的重试会因为会话已关闭拿到 `SESSION_EVICTED`）；其余修改型请求必须带会话与 seq（seq 0 → `EINVAL`：没有 seq 就没有去重，也绕过了会话检查）。
- ③ 请求头里的 uid：client 角色的 key 只在它的 uid 范围内被相信（不在范围内 → `EACCES`），`root_squash` 把 uid/gid 0 映射为 65534（附加组去掉 0）；admin 与 server 角色可以声明任意 uid（`wmfs fs --as-uid`，3.3.3）。会话操作（SessionOpen/SessionHeartbeat/SessionClose）不代表任何用户、也不碰 inode：**不按 uid 范围检查，身份一律记为 nobody**（65534，changelog 里也是）——否则一把 uid 范围不含挂载进程自己 uid 的 key 连会话都开不了。
- ④ **会话绑定打开它的 key**（`SessionRecV1.key_id`）：别的 key 拿着这个 session id（线上是明文）发修改、心跳、关闭或重新打开 → `EACCES`（`sessions::usable`，在查去重表之前），否则别人能抢先用它的 seq 污染去重表、或者替它关掉会话。
- ⑤ `Rename.flags` 与 `SetXattr.flags` 用 Linux 的数值（`RENAME_NOREPLACE = 1`、`RENAME_EXCHANGE = 2`、`RENAME_WHITEOUT = 4`；`XATTR_CREATE = 1`、`XATTR_REPLACE = 2`，`proto/meta.rs`，钉在 `constants.golden`）。
- ⑥ ResolvePath 从根逐段 lookup（每段要执行权限），`.` 跳过、`..` 回父目录，中间一段是符号链接 → `ENOTDIR`（不跟随），最后一段是符号链接时返回它本身。
- ⑦ `AttrOut.consistency`/`ttl_ms` 取内置默认值（cto、1 s），按目录属性给出在 M7。
- ⑧ `OpenOut.inline`（文件的完整内容）只给能读这个文件的人：Open 按这次是否读打开（O_RDONLY/O_RDWR），Create 打开已有的文件、改了 size 的 SetAttr 按调用者对它有没有读权限，新建的文件总带（空的）；不带时为 None，要内容用 GetInline（M5）。
- ⑨ 去重命中计入 `wmfs_meta_dedup_hits_total`（引擎启动时注册，新节点上是 0）并写一行 INFO 日志"去重命中"。
- ⑩ 客户端看不出一次重试是不是"去重命中"（协议里没有这个标志）：`MetaClient` 在超时重试成功、且回复里的 ctime 早于重试发出的时刻时，日志写"超时重试 seq=… → 去重命中"（`wmfs fs --verbose` 打印；判断用两边的墙钟，只影响这一行提示）。
- ⑪ "同时在途的修改型请求不超过 `DEDUP_WINDOW = 256` 个"**按 seq 计**：`MetaClient` 分配新 seq 时要求它小于"最小的在途 seq + 256"（不只是个数 ≤ 256），到了就等。服务端写入 seq 的结果时点删 seq − 256 的去重记录（3.4.1.4 规则 4），只有这样，一个回复丢了、正在等超时重试的旧请求的记录才不会被同一会话里别的线程的新请求挤掉（M6 的 FUSE 有 16 个线程共用一个会话）。
#### 3.3.5 客户端 ↔ 数据：`DataOp` 与能力令牌

> 实现注记（M0）：本节类型自 M0 起以 `core/src/proto/data.rs` 与 `core/src/proto/cap.rs` 为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

```rust
pub enum DataOp {
    ReadChunk { chunk: ChunkId, chain_ver: u64, off: u32, len: u32 },                    // 响应 bulk = 明文
    WriteChunk { chunk: ChunkId, chain_ver: u64, extents: Vec<(u32, u32)>, sync: bool },  // (chunk 内偏移, 长度)；请求 bulk = 各段依次拼接（发送方用 BufSlice 列表，不拷贝，3.6.2）
    TruncateChunk { chunk: ChunkId, chain_ver: u64, new_len: u32 },                       // 写一个新版本：new_len 之后全部清零（0 = 空版本）
    SyncChunks { chain: u32, chain_ver: u64, chunks: Vec<ChunkId> },                      // 链上每个成员 fdatasync
    DeleteEpoch { chain: u32, chain_ver: u64, ino: u64, epoch: u32 },                     // 需要 cap 的管理权限位（只签发给后台任务）
}
pub enum DataReply { Read { len: u32, version: u64 }, Written { version: u64 }, Ok, Deleted { files: u64 } }

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CapV1 {
    pub ino: u64, pub epoch: u32,
    pub rights: u8,                 // bit0 读，bit1 写，bit2 管理
    pub session: u64, pub tenant: u32, pub job_id: u64,
    pub chains: Vec<u32>,           // = layout.chains
    pub class_id: u16, pub qos_id: u64,   // qos_id = qos 属性来源目录的 ino（0 = 用存储类的 QoS 包络）
    pub chunk_size: u32, pub block_size: u32,
    pub checksum: u8, pub compress: u8, pub compress_level: i8, pub encrypt: u8, pub encrypt_key: u32,
    pub expires_ms: u64,            // 墙钟；签发时 now + cap_ttl（默认 10 min）
}
// 线上：postcard(CapV1) ‖ mac[16]，mac = HMAC-SHA256(K_cap, "WMFS-CAP-1" ‖ postcard 字节)[0..16]
```

数据节点校验（全部本地 O(1)，**在 io 线程出队后执行**，不是入队时）：MAC 正确；`expires_ms + 5 min ≥ wall_ms()`；`chunk.ino == cap.ino && chunk.epoch == cap.epoch`；权限位；`chunk.chain == cap.chains[chunk.idx % cap.chains.len()]`（客户端不能把数据塞进 layout 之外的链）；`cap.session` 不在集群图的被驱逐列表里。这就是数据请求的 `authz`：授权插件在元数据侧 `Open/Create` 时求值，cap 是被签名缓存的授权决定（理念文档 4.1"是（可缓存）"）。变换参数在 cap 里，客户端无法绕过 `encrypt` 策略。

#### 3.3.6 服务端 ↔ 服务端

> 实现注记（M0）：本节类型自 M0 起以 `core/src/proto/peer.rs`（3.4.1.7 的快照流消息 `SnapDesc`、`SnapshotFetch`、`SnapHeader`、`SnapTrailer` 也在这里）为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

```rust
pub enum PeerOp {
    ForwardWrite(ForwardWrite),
    ForwardSync { chain: u32, chain_ver: u64, from_target: u32, chunks: Vec<ChunkId> },
    ForwardDeleteEpoch { chain: u32, chain_ver: u64, from_target: u32, ino: u64, epoch: u32 },
    ListChunks { chain: u32, after: Option<ChunkId>, limit: u32 },   // 同步源列出已提交版本
    FetchChunk { chunk: ChunkId },                                   // 响应 bulk = packed chunk（3.2.4）
}
pub struct ForwardWrite {
    pub chunk: ChunkId, pub chain_ver: u64, pub from_target: u32,
    pub base_version: u64,          // 增量转发的基础版本；0 = chunk 原本不存在
    pub version: u64,
    pub full: bool,                 // true：bulk 是完整 packed chunk，忽略 base_version
    pub header: Vec<u8>,            // 新版本的 256 字节固定头（含 header_crc，覆盖新块表）
    pub blocks: Vec<(u32, Vec<u8>)>,// 改动块的 (块号, 72 字节块表条目)；bulk = 这些块的存储字节按此顺序拼接
    pub sync: bool,                 // O_SYNC：每个成员提交前先 fdatasync
}
pub enum PeerReply { Ack { version: u64 }, NeedFull, Chunks { items: Vec<(ChunkId, u64)>, more: bool },
                     Fetched { version: u64 }, Ok, Deleted { files: u64 } }

pub enum NodeOp {
    Heartbeat(NodeHeartbeat),
    GetClusterMap { since_epoch: u64 },
    Join(JoinReq),
    SyncDone { target: u32, chain: u32, chain_ver: u64 },
    CheckEpochs { items: Vec<(u64, u32)> },                          // 孤儿扫描：(ino, epoch) 是否已死
    ReportCorrupt { target: u32, chunk: ChunkId },
}
pub struct NodeHeartbeat {
    pub node: u32, pub boot_id: u64,          // boot_id：每次进程启动随机生成
    pub host_id: String, pub machine_id: String, pub map_epoch_seen: u64,
    pub targets: Vec<TargetReport>,
    pub forward_failures: Vec<(u32, u32, u64)>,   // (chain, 下游 target, 持续失败起始 ms)
    pub features: Vec<String>, pub wall_ms: u64,
    pub qos_deficit: Vec<(u64, u32)>,             // (qos_id, 最近 10 s 平均保底欠额，成本单位/秒)；只报非 0 的，给"声明 vs 实际"用（3.10）
}
pub struct TargetReport { pub target: u32, pub used: u64, pub capacity: u64, pub phys_devs: Vec<String>, pub read_only: bool }
pub struct JoinReq { pub hostname: String, pub host_id: String, pub machine_id: String,
                     pub addrs: Vec<(String, String)>, pub mgmt_addr: String,      // 同 NodeRecV1.addrs
                     pub meta_role: u8 /*0 none 1 auto*/,
                     pub targets: Vec<(String, Vec<String>, u64, Vec<String>)>,   // (path, labels, capacity, phys_devs)
                     pub features: Vec<String>,
                     pub plugins: Vec<String> }                                   // 本节点的插件清单；与 GLOBAL plugins 不同 → 拒绝加入并列出差异
pub enum NodeReply { HeartbeatOk { lease_ms: u64, map_epoch: u64, leader_wall_ms: u64 }, Map(Box<ClusterMap>),
                     Joined { node_id: u32, target_ids: Vec<u32>, map: Box<ClusterMap> },   // 带上当前集群图：新节点的 KV 是空的，靠它找到 leader 与快照源
                     Ok, Epochs { dead: Vec<bool> } }

pub struct ClusterMap {
    pub map_epoch: u64, pub cluster_id: [u8; 16], pub leader: Option<u32>,
    pub settings: ClusterSettingsV1, pub features: FeatureSetV1,
    pub nodes: Vec<(u32, NodeRecV1)>, pub targets: Vec<(u32, TargetRecV1)>, pub chains: Vec<(u32, ChainRecV1)>,
    pub classes: Vec<StorageClassV1>, pub keys: Vec<(u32, KeyMetaV1)>, pub datakeys: Vec<(u32, DataKeyV1)>,
    pub qos: Vec<(u64, QosSpecV1)>, pub evicted: Vec<(u64, EvictedV1)>,
    pub plugins: PluginListV1,                    // 集群插件清单（3.7.2）
    pub warnings: Vec<String>,                    // leader 由已提交状态算出的集群警告（中文）：短链、故障域不足、故障域重叠、2 voter 无高可用……
}
```

`GetClusterMap` 在 `since_epoch < map_epoch` 时返回完整集群图（小集群，几十 KB），否则 `Ok`。集群图在 leader 上由已提交状态构建并按 `map_epoch` 缓存；**任何**改变节点、target、链、存储类、key、数据密钥、QoS 表、被驱逐列表、集群参数或插件清单的批次都在同一批次里 `map_epoch += 1`。`warnings` 在构建集群图时顺带计算（不进 KV）。

实现注记（M4）：集群图在 `core/src/meta/cluster/map.rs`：缓存键是 (GLOBAL `map_epoch`, leader)，读者每次只点查一次 `map_epoch`，变了才重建，所以 raft 线程应用批次时不用另外通知谁"缓存失效"；执行器每个批次开始时、只读请求在 ReadIndex 之后，各自从自己的读事务里取快照（3.5.1 的 `OpCtx.map`，集群参数 `OpCtx.settings` 也取自它）。警告只有"短链"一类（ADR-0016）：存储类声明的副本数大于它可用的故障域数（"default 类声明 3 副本，只有 1 个故障域：新文件实际为 1 副本"；一个可用的 target 都没有时说"新文件无法创建"）、链的成员少于声明副本数（"有 n 条链少于声明副本数"）；故障域重叠、2 voter 无高可用在 M8/M9，目录属性上的 `replicas` 在 M7。NodeCall 服务与 MetaCall 是同一个服务（共用 8 个 meta-read 线程），M4 只有 `GetClusterMap`（经 ReadIndex；client 与 admin 拿到的不含 keys、datakeys），其余回 `FEATURE_UNSUPPORTED`。

#### 3.3.7 错误码

> 实现注记（M0）：本节类型自 M0 起以 `core/src/base/errcode.rs`（名字 ↔ 数值表在 `api/src/codes.rs`，单元测试逐个核对两边一致，1–255 的码另与 libc 的 errno 逐个核对；全部码钉在 `tests/golden/constants.golden`）为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

`pub struct ErrCode(pub u16)`（不用 enum，未知码也能透传）。1–255 直接用 Linux errno：`EPERM 1, ENOENT 2, EINTR 4, EIO 5, E2BIG 7, EBADF 9, EAGAIN 11, ENOMEM 12, EACCES 13, EBUSY 16, EEXIST 17, EXDEV 18, ENOTDIR 20, EISDIR 21, EINVAL 22, EFBIG 27, ENOSPC 28, EROFS 30, EMLINK 31, ERANGE 34, ENAMETOOLONG 36, ENOLCK 37, ENOSYS 38, ENOTEMPTY 39, ELOOP 40, ENODATA 61, ENODEV 19, ENOTTY 25, EOVERFLOW 75, EOPNOTSUPP 95, ECONNRESET 104, ETIMEDOUT 110, ESTALE 116, EDQUOT 122`（`ECONNRESET` 与 `ETIMEDOUT` 由 `RpcClient` 在本地产生：连接断开、调用超时）。集群错误从 1000 起：

| 码 | 名称 | 含义 | 客户端处理 |
|---|---|---|---|
| 1000 | NOT_LEADER | 不是元数据 leader（`leader_hint`） | 换到 hint 立即重试 |
| 1001 | RETRY | 结果未知或批次作废 | 用**同一个 seq** 退避重试（去重表保证恰好一次） |
| 1002 | NOT_READY | leader 尚未就绪 / 节点启动中 | 退避重试 |
| 1003 | STALE_CHAIN | 链版本不一致（`extra` = 当前 chain_ver） | 拉新集群图后重试 |
| 1004 | CHUNK_DIRTY | 本成员有未提交版本且不是 last serving（`extra` = last serving target） | 改向它读 |
| 1005 | NOT_SERVING | 本成员不是 Serving / 链不可用 | 拉新集群图后重试 |
| 1006 | FENCED | 数据节点 target 租约已失效、自我隔离中 | 换副本或拉新集群图 |
| 1007 | BUSY | 内存池/队列满、chunk 等待队列满 | 退避重试 |
| 1008 | NEED_FULL | 后继缺少增量转发的基础版本（只在服务端之间） | 发送方改发完整 chunk |
| 1009 | CHUNK_NOT_FOUND | 该 chunk 从未写过 | 先 GetAttr 校验 epoch（C5），再按空洞读 0 |
| 1010 | CHECKSUM_MISMATCH | 本副本该块校验失败 | 换副本读；全部失败 → `EIO` |
| 1011 | CORRUPT | chunk 已被标为损坏 | 同上 |
| 1012 | SESSION_EVICTED | 会话已被驱逐 | 已打开句柄全部 `EIO`；新建会话 |
| 1013 | BAD_CAP | cap 校验失败或过期 | `RenewCap`/重新 `Open` 一次，再失败 → `EACCES` |
| 1014 | STALE_LAYOUT | `CommitWrite` 等的 epoch 与当前 layout 不符 | 重新 `Open` 取 layout，重写写回缓冲（C4） |
| 1015 | （保留） | 首发不使用（F_SETLKW 改为客户端轮询后不再需要） | — |
| 1016 | AUTH_FAILED | 认证失败 | `EACCES`，挂载失败 |
| 1017 | PROTO_UNSUPPORTED | 协议版本不兼容 | `EOPNOTSUPP`，日志写明对端版本 |
| 1018 | FEATURE_UNSUPPORTED | 不认识的消息/变体/特性/ABI | `EOPNOTSUPP` |
| 1019 | SHUTTING_DOWN | 对端正在退出 | 拉新集群图后重试 |
| 1020 | INTERNAL | 内部不变量破坏 | `EIO`，并产生告警事件 |

#### 3.3.8 超时与重试（满足"单节点宕机，客户端除超时重试外无错误"）

- 单次 RPC 超时：元数据 10 s、数据 10 s、`SnapshotFetch` 10 min（流的每一帧之间）。超时由 `RpcClient` 的 `rpc-timer` 线程判定（3.12）。
- **传输层的任何错误都按"连接断开"处理**：`Conn::send/recv` 返回错误、写超时（5 s）、发送队列满、fail point `plugin.<name>.transport` 触发——一律关闭这条连接，它上面的在途调用以 `ECONNRESET` 结束，调用方重连后重试。唯一例外：帧超过该传输的上限（`FEATURE_UNSUPPORTED`）只让这一个请求失败，不断开。
- 可重试码（1000–1007；1013 重新取 cap 一次；1014 重写；1019；`ECONNRESET`、`ETIMEDOUT`）按指数退避重试：10 ms 起、×2、上限 1 s。1008 只在服务端之间出现；1009 按 C5 处理；1010/1011 换副本。修改型元数据请求重试用同一个 seq；数据请求天然幂等（同一 extents 写两次结果相同）。
- 总期限 = 挂载参数 `--op-timeout`（默认 120 s，类似 NFS hard 挂载）；超过才把 `EIO` 交给应用，并在慢请求日志留下 trace_id。
- 实现注记（M1）：退避重试在 `core/src/net/retry.rs`（`retry(what, 总期限, op)`，每次重试写一行日志，连接断开时写"连接断开 → 重连重试"）；`RpcClient` 本身不重试。建连失败、握手中连接出错一律按 `ECONNRESET`（原因写进日志）；地址本身写错（解析不出）→ `EINVAL`，不重试。CLI 一次性命令的总期限用 30 s（`wmfs ping`）。
- 实现注记（M1）："帧超过该传输的上限只让这一个请求失败"的做法：协议上限（body > 1 MiB、bulk > 16 MiB + 128 KiB）在 `ConnHandle::send` 里同步检查、直接返回 `FEATURE_UNSUPPORTED`（`Responder` 回复超限时改发一个 `ERROR(FEATURE_UNSUPPORTED)`）；传输自己的上限（例如 RDMA 的帧头 + body ≤ 120 KiB）由写线程处理——`Conn::send` 返回 `FEATURE_UNSUPPORTED` 时不断开：请求帧通知 `RpcClient`，这个调用以 `FEATURE_UNSUPPORTED` 结束；回复帧与流帧改发一个同 req_id、同 msg_type 的 ERROR 帧；OneWay 只丢掉这一帧。fail point `plugin.<name>.transport` 触发时**不论配置的是哪个码**（包括 `FEATURE_UNSUPPORTED`）都按连接断开处理（3.7.4）。`FEATURE_UNSUPPORTED` 不可重试。调用的期限 = 现在 + timeout + 1 ms（`mono_ms` 只精确到毫秒，不加 1 的话调用可能比 timeout 早不到 1 ms 结束）。
- 预期故障切换时间：元数据 leader 切换约 1–3 s（选举超时 1–2 s + 新 leader 就绪）；数据链重配 ≈ `target_lease`（8 s）+ 集群图传播（≤ 1 s）。都远小于 120 s。
- 实现注记（M4）：`MetaClient`（`core/src/client/meta_client.rs`）回 `NOT_LEADER` 且带 `leader_hint` 时换到 hint 立即重试（连续跟随最多 3 次，之后照常退避）；**没有 hint 时不是"按 health 找 leader"**，而是退避后换下一个认识的节点（`connect` 给的种子与集群图里的节点，轮流试；不是 leader 的节点回 `NOT_LEADER`，再换下一个）——core 里没有 HTTP 客户端（管理 API 的客户端在 wmfs-mgr），按 health 找 leader 由调用方（CLI 的 `leader.rs`）在 `connect` 之前做，给出种子。重试日志是 INFO 级（`wmfs fs --verbose` 才看得到），所以 MetaClient 不用 `net::retry::retry`（它写 WARN，也不会换节点）。


### 3.4 复制、成员、选举、fencing

#### 3.4.1 元数据 Raft（raft-rs 0.7）【ADR-0019】

##### 3.4.1.1 参数

```rust
let cfg = raft::Config {
    id: node_id as u64,
    election_tick: 10,            // tick = 100 ms → 选举超时 1 s（raft-rs 随机化到 [1 s, 2 s)）
    heartbeat_tick: 2,            // 200 ms
    applied: durable_applied,     // 规则 M4：从 raft_state["applied"] 读出
    check_quorum: true,           // leader 失去多数派后下台
    pre_vote: true,               // 被隔离后回来的节点不打断集群
    read_only_option: raft::ReadOnlyOption::Safe,   // 3.4.1.6：绝不用 LeaseBased
    max_size_per_msg: 512 << 10,  // 注意 raft::util::limit_size 总会保留第一条，所以单条超过它的条目照样整条发出（3.4.1.4 的批次上限 + RaftMsg 走 bulk 兜住）
    max_inflight_msgs: 256,
    max_uncommitted_size: 64 << 20,
    max_committed_size_per_ready: 16 << 20,
    ..Default::default()
};
cfg.validate()?;
let logger = slog::Logger::root(crate::base::log::SlogBridge, slog::o!());
let node = raft::RawNode::new(&cfg, storage, &logger)?;
```

##### 3.4.1.2 `raft::Storage` 的 redb 实现

> 实现注记（M3）：`RedbStorage` 在 `core/src/meta/raft/storage.rs`（init 的初始状态由同一文件的 `bootstrap` 写）。最近条目的缓存另有 64 MiB 的字节上限（单条最大约 1.2 MiB，只按 4096 条算可能占到 GB 级内存）；`snapshot()` 在 M8 之前回 `SnapshotTemporarilyUnavailable`（单节点没有需要快照的对端；`install_snapshot` 与 pins 也在 M8）；写方法里的 redb 错误先写一行带原因的日志再回 `EIO`，由 driver `or_abort`；读方法（缓存没命中时读盘）上的 redb 错误直接 fail-stop。合约测试在 `storage/tests.rs`（照 raft-rs `MemStorage` 的测试改写，跑在 redb 的内存后端上）。

```rust
pub struct RedbStorage {
    db: Arc<redb::Database>,
    hs: HardState, cs: ConfState,         // 内存副本，与 redb 同步更新（只在 raft 线程改）
    snap_index: u64, snap_term: u64,      // 最近一次压缩点（init 后为 (1, 1)）
    last_index: u64,                      // 无条目时 = snap_index
    recent: VecDeque<Entry>,              // 最近 4096 条的缓存，term()/entries() 热路径不读盘
    pins: Arc<Mutex<VecDeque<Arc<PinnedSnap>>>>,   // 快照用的 redb 读事务（3.4.1.7）；同一个 Arc 交给 snap-serve 线程；snapshot() 只拿 &self，所以用 Mutex
    node_id: u32,
}
impl raft::Storage for RedbStorage {
    fn initial_state(&self) -> raft::Result<RaftState> { Ok(RaftState::new(self.hs.clone(), self.cs.clone())) }
    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>, _ctx: GetEntriesContext) -> raft::Result<Vec<Entry>>;
        // low <= snap_index → Err(Error::Store(StorageError::Compacted))；high > last_index + 1 → panic（raft 合约）
        // 先查 recent，再查 raft_log 表；用 raft::util::limit_size 截断，但至少返回 1 条
    fn term(&self, idx: u64) -> raft::Result<u64>;
        // idx == snap_index → snap_term；idx < snap_index → Compacted；idx > last_index → Err(Error::Store(StorageError::Unavailable))
    fn first_index(&self) -> raft::Result<u64> { Ok(self.snap_index + 1) }
    fn last_index(&self) -> raft::Result<u64> { Ok(self.last_index) }
    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot>;   // 3.4.1.7，开销很小，可在 raft 线程上调用
}
// 写方法只由 raft 线程经 node.mut_store() 调用，都接收外部传入的 redb 写事务：
impl RedbStorage {
    pub fn append(&mut self, tx: &redb::WriteTransaction, ents: &[Entry]) -> Result<(), ErrCode>;     // 先删 index >= ents[0].index 的旧条目
    pub fn set_hard_state(&mut self, tx: &redb::WriteTransaction, hs: &HardState) -> Result<(), ErrCode>;
    pub fn set_conf_state(&mut self, tx: &redb::WriteTransaction, cs: &ConfState) -> Result<(), ErrCode>;
    pub fn install_snapshot(&mut self, tx: &redb::WriteTransaction, meta: &SnapshotMetadata, file: &Path) -> Result<u64 /*state_version*/, ErrCode>; // 规则 M5
    pub fn compact(&mut self, tx: &redb::WriteTransaction, upto: u64) -> Result<(), ErrCode>;          // 规则 M3
}
```

##### 3.4.1.3 驱动循环与 Ready 处理顺序（raft 线程）

```rust
pub enum RaftInput {
    Msg(Message),                                                         // 对端 raft 消息（MsgSnapshot 的数据已先下载，3.4.1.7）
    Propose { data: Vec<u8>, leader_term: u64, done: Sender<ProposeOutcome> },   // 执行器
    ConfChange { cc: ConfChangeV2, done: Sender<ProposeOutcome> },        // cluster-mgr 的成员管理
    ReadIndex { done: Sender<Result<(), ErrCode>> },                      // meta-read 线程（3.4.1.6）
    ReportSnapshot { to: u64, ok: bool },
    ReportUnreachable(u64),
    TransferLeader(u64),
    Shutdown,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ProposeOutcome { Applied { ok: bool }, Lost, Dropped }

fn raft_loop(mut node: RawNode<RedbStorage>, rx: Receiver<RaftInput>, mut st: Driver) {
    let tick = Duration::from_millis(100);
    let mut next_tick = Instant::now() + tick;
    loop {
        match rx.recv_timeout(next_tick.saturating_duration_since(Instant::now())) {
            Ok(input) => st.handle_input(&mut node, input),          // 只有这里调用 step/propose/read_index/campaign（坑 P1）
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
        while let Ok(input) = rx.try_recv() { st.handle_input(&mut node, input); }   // 一次吃光，减少 Ready 次数
        if Instant::now() >= next_tick { node.tick(); st.read_gate.on_tick(&mut node); next_tick += tick; }
        st.read_gate.pump(&mut node);                                 // 需要时发起一次 read_index（3.4.1.6）
        st.on_ready(&mut node);
        st.publish_role(&node);   // RoleInfo { role, term, leader_id, ready: state==Leader && node.raft.apply_to_current_term() && 插件清单一致（3.7.2） }
                                  // 以及 RaftStatus { commit, applied, conf_state, progress: Vec<(id, matched)> }：其他线程只读这份快照，绝不碰 RawNode
        st.maybe_durable_tick();  // 规则 M2：补空的 Immediate 提交
    }
}

// 提议：先确认自己仍是"同一任期、已就绪"的 leader，否则直接 Dropped。
// 原因：降级成 follower 的 raft-rs 会把 MsgPropose 原样转发给新 leader（raft.rs step_follower），过期写集绝不能进入新 leader 的日志。
fn propose(&mut self, node: &mut RawNode<RedbStorage>, data: Vec<u8>, leader_term: u64, done: Sender<ProposeOutcome>) {
    let r = &node.raft;
    if r.state != StateRole::Leader || r.term != leader_term || !r.apply_to_current_term() {
        let _ = done.send(ProposeOutcome::Dropped);
        return;
    }
    let before = node.raft.raft_log.last_index();
    let ok = node.propose(vec![], data).is_ok() && node.raft.raft_log.last_index() == before + 1;   // propose 不返回 index（坑 P10）
    if ok { self.pending.push_back(Pending { index: before + 1, term: node.raft.term, kind: PendKind::Batch, done }); }
    else { let _ = done.send(ProposeOutcome::Dropped); }       // 例如 max_uncommitted_size 超限、leader 转移进行中
}
// 条目 (index, term) 被应用时：队首 pending.index < index → Lost；== index 且 term 相同 → Applied{ok}；term 不同 → Lost。
// conf change 被 raft-rs 改写成空的 EntryNormal（已有未应用的 conf change 等原因）→ Applied{ok:false}。
// 角色从 Leader 变为其他 → 全部 pending 判 Lost（结果不确定，靠去重表兜底）。raft 线程保证每个 pending 最终得到三者之一。

fn on_ready(&mut self, node: &mut RawNode<RedbStorage>) {
    if !node.has_ready() { return; }
    let mut rd = node.ready();
    // ① 角色变化：不再是 leader → 在途提议判 Lost、读闸门全部 NOT_LEADER
    if let Some(ss) = rd.ss() { self.on_role_change(ss.raft_state, node.raft.term); }
    // ② leader 的消息可在落盘前发出（raft 论文 10.2.1；follower 时这里为空）（坑 P2）
    self.send_all(rd.take_messages());
    // ③ 第一个写事务：快照安装 → 应用已提交条目 → 追加新条目 → 硬状态 → applied（规则 M1、M5）
    // 下面的 or_abort(..) = 出错就写日志并 abort（0.2 第 5 条 fail-stop）
    let mut tx = or_abort(self.db.begin_write(), "redb begin_write");
    if !rd.snapshot().is_empty() {   // 规则 M5：同时把 applied_index/applied_term 设为快照的 index/term
        self.state_version = or_abort(node.mut_store().install_snapshot(&tx, rd.snapshot().get_metadata(), &self.snap_file(rd.snapshot())), "install");
    }
    let applied = self.apply_entries(node, &tx, rd.take_committed_entries());   // 这些条目本地已持久（坑 P3），可与新条目同事务
    or_abort(node.mut_store().append(&tx, rd.entries()), "append");
    if let Some(hs) = rd.hs() { or_abort(node.mut_store().set_hard_state(&tx, hs), "hs"); }
    else if !applied.is_empty() { let hs = node.raft.hard_state(); or_abort(node.mut_store().set_hard_state(&tx, &hs), "hs"); }   // 规则 M1
    tx.set_durability(if rd.must_sync() { Durability::Immediate } else { Durability::None });
    let _ = fail_point("raft.persist.before_commit");                  // 测试里配置成 abort
    or_abort(tx.commit(), "redb commit");                              // ← 需要时在这里 fsync
    self.after_apply(&applied);                                        // 解析 pending 提议、唤醒 changelog 等待者
    // ④ follower 的消息必须在落盘之后发出（坑 P2）
    self.send_all(rd.take_persisted_messages());
    for rs in rd.take_read_states() { self.read_gate.on_read_state(rs.index, &rs.request_ctx); }
    // ⑤ advance → LightReady：新的 commit index、消息、更多已提交条目
    let mut light = node.advance(rd);
    let ents = light.take_committed_entries();
    if light.commit_index().is_some() || !ents.is_empty() {
        // 第二个写事务：必须同时写入含新 commit 的硬状态（规则 M1，修复"单节点 applied > hs.commit 重启 fatal"）
        let mut tx = or_abort(self.db.begin_write(), "redb begin_write");
        let hs = node.raft.hard_state(); or_abort(node.mut_store().set_hard_state(&tx, &hs), "hs");
        let applied = self.apply_entries(node, &tx, ents);
        tx.set_durability(Durability::None);                           // 条目已在日志里持久（规则 M2）
        or_abort(tx.commit(), "redb commit");
        self.after_apply(&applied);
    }
    self.send_all(light.take_messages());
    node.advance_apply();
    self.read_gate.on_applied(self.applied_index);
    self.maybe_compact(node);                                          // 规则 M3，每应用 10 000 条检查一次
}

fn apply_entries(&mut self, node: &mut RawNode<RedbStorage>, tx: &redb::WriteTransaction, ents: Vec<Entry>) -> Vec<AppliedEntry> {
    let mut out = Vec::new();
    for e in ents {
        let mut ok = true;
        match e.get_entry_type() {
            EntryType::EntryNormal if e.data.is_empty() => { ok = false; }   // 新 leader 的空条目，或被拒绝而改写的 conf change（坑 P4 的反面）
            EntryType::EntryNormal => {
                let wb = WriteBatchV1::decode(&e.data).expect("不变量破坏：已提交的 raft 条目无法解码为 WriteBatchV1");
                if wb.expect_version == self.state_version { kv_apply(tx, &wb.ops); self.state_version = e.index; }
                else { ok = false; metrics::inc("wmfs_meta_batch_stale_total"); }   // 3.4.1.5：整批作废，确定性
            }
            EntryType::EntryConfChangeV2 => {
                let mut cc = ConfChangeV2::default();                       // 空 data = raft-rs 自动发起的 leave-joint，必须应用（坑 P4）
                if !e.data.is_empty() { cc.merge_from_bytes(&e.data).expect("conf change 损坏"); }
                let cs = node.apply_conf_change(&cc).expect("不变量破坏：已提交的 conf change 无法应用");
                or_abort(node.mut_store().set_conf_state(tx, &cs), "cs");   // 与条目同事务（坑 P5）
                self.peers.on_conf_state(&cs);                              // 增删 raft-send 线程（对端地址从最新集群图的 NodeRecV1.addrs 按 control_scheme 解析；查不到时用该对端发来消息的那条连接回发）
            }
            EntryType::EntryConfChange => panic!("wmfs 只提议 ConfChangeV2，日志里出现了 v1 EntryConfChange（index {}）", e.index),
        }
        self.applied_index = e.index; self.applied_term = e.term;
        out.push(AppliedEntry { index: e.index, term: e.term, ok, entry_type: e.get_entry_type() });
    }
    if !out.is_empty() { save_applied(tx, self.applied_index, self.applied_term, self.state_version); }
    out
}
```

`after_apply` 还负责：若本批写入了 `0x0E` 前缀（changelog），唤醒长轮询等待者（`Condvar`，携带最大 seq）；若写入了影响集群图的键，标记集群图缓存失效。

> 实现注记（M3）：`Driver` 在 `core/src/meta/raft/driver.rs`（应用与发布在子模块 `driver/apply.rs`；`RaftInput`、`ProposeOutcome`、`Published`、`DriverOpts`、`config` 在 `types.rs`，driver 原样转出；伪代码 raft_loop 里"输入之后的 pump + on_ready + publish_role"是 `Driver::process_ready`）。与上面伪代码的差别，都是等价或更保守的写法：① 第一个写事务只在有东西要写时才开（已提交条目、新条目、硬状态任一非空）——空事务等于没写，于是只有 read state 或消息的 Ready 不产生提交，也不会因此每秒多一次补 Immediate；② 时间一律按 tick 计（1 tick = 100 ms：读闸门的 500 ms / 2 s、规则 M2 的"每 1 s"），确定性模拟不依赖真实时间；③ 要发给对端的消息攒进 outbox，M3 没有对端，驱动循环把它们计入 `wmfs_raft_msg_dropped_total`（M8 的 raft-send 线程从 outbox 取）；④ Ready 里出现快照 = 不变量破坏（M8 之前不可能），panic；⑤ raft 线程与执行器线程上的任何 panic（不变量破坏）都写日志（含 panic 的原因）并 abort（`engine.rs` 的 `fail_stop_thread`）：不能让 raft 线程悄悄死掉、节点却还在应答；⑥ 插件清单与已提交的 GLOBAL `plugins` 不一致时只"不就绪"并写一条列出差异的错误日志，M8 起再按 3.7.2 退出（退出码 3）；⑦ 收到 Shutdown（SIGTERM）时补一次 Immediate 提交、在途提议判 Lost 再退出，transfer_leader 在 M8。fail point：`raft.persist.before_commit`、`raft.persist.after_commit`（第一个写事务提交前 / 后）、`raft.apply.before`（应用已提交条目前）。

##### 3.4.1.4 串行执行器：单飞批次 + 写集复制

一个批次在 raft 里提交的几毫秒内（本机实测 redb Immediate 提交约 2.6 ms），新到的请求排队，下一批一次带走——负载越高批越大，吞吐靠自然攒批。**同一时刻 raft 里最多只有一个批次在途**，所以构建下一批时上一批已经应用，执行器直接读"已提交状态"，覆盖层只活一个批次，失去 leader 时没有跨批状态要清理。

**批次上限**：≤ 256 个操作，且写集的**实际编码字节数**（覆盖层里全部 `KvOp` 的 key + value 长度之和）一旦 ≥ 768 KiB 就结束本批。闭包的写集只有执行之后才知道，所以执行器"边取边执行"：每执行完一个请求就看一次累计字节数，到了上限就不再从队列取，没取的请求留给下一批。最后一个操作可能让写集超过 768 KiB 一点（单个操作最大约 400 KiB：1000 个硬链接的 inode + 64 KiB xattr），所以一个条目最大约 1.2 MiB——raft 的 `limit_size` 总会整条发出第一条，于是 **RaftMsg 的 protobuf 放在 bulk 里**（上限 16 MiB，3.3.3），body 的 1 MiB 上限不会被撞到。回归测试：3 节点下提交一个 900 KiB 写集的批次。

```rust
pub type TxnOp = Box<dyn FnOnce(&mut OpView, &OpCtx) -> Result<MetaReply, ErrCode> + Send>;
pub struct TxnRequest {
    pub ctx: ReqCtx,                                                // principal、session、seq（内部操作为 0）、trace_id、now（leader 墙钟，写进写集）
    pub op: TxnOp,
    pub done: Box<dyn FnOnce(Result<MetaReply, ErrCode>) + Send>,   // RPC 层：编码后经 Responder 回复（只 try_send，不碰 socket）；内部调用者：发进 bounded(1)
    pub exclusive: bool,                                            // 集群拓扑类内部操作：必须是本批最后一个（规则 5）
}

fn executor_loop(rx: Receiver<TxnRequest>, eng: Arc<EmbeddedEngine>, env: Arc<OpEnv>) {
    loop {
        let first = match rx.recv() { Ok(r) => r, Err(_) => return };   // 没有请求就一直睡（没有定时工作要做）
        let role = eng.role();
        if !role.is_ready_leader() {                                // 非 leader、新 leader 尚未应用本 term 的空条目、或插件清单不一致（3.7.2）
            reject_all(first, &rx, role.not_leader_error());        // NOT_LEADER{hint} 或 NOT_READY
            continue;
        }
        let rtx = or_abort(eng.db().begin_read(), "redb begin_read");   // redb 出错 = 写日志并 abort（0.2 第 5 条）
        let base_version = read_applied(&rtx).state_version;
        let mut view = BatchView::new(&rtx);                        // 覆盖层：BTreeMap<Vec<u8>, Option<Vec<u8>>>，并累计写集编码字节数
        let mut results = Vec::new();
        let mut next = Some(first);
        while let Some(req) = next.take() {
            let end_here = req.exclusive;
            let r = run_one(&mut view, &env, &req.ctx, req.op);
            results.push((req.done, r));
            // 结束本批的条件：独占操作 / 刚生成了新链（拓扑变了）/ 256 个 / 写集 ≥ 768 KiB
            if end_here || view.topology_changed() || results.len() >= 256 || view.encoded_len() >= (768 << 10) { break; }
            next = rx.try_recv().ok();
        }
        if view.is_empty() { for (d, r) in results { d(r) } continue; }   // 只可能全是"已提交的事实"：去重命中、会话已驱逐、内部 CAS 前提不成立
        let wb = WriteBatchV1 { expect_version: base_version, leader_term: role.term, ops: view.into_ops(), trace_ids: vec![] };
        let outcome = eng.propose_and_wait(wb);                     // 阻塞到 raft 线程给出 Applied/Lost/Dropped（raft 线程保证每个提议最终得到其一）
        for (d, r) in results {
            d(match outcome {
                ProposeOutcome::Applied { ok: true } => r,
                ProposeOutcome::Applied { ok: false } | ProposeOutcome::Dropped => Err(ErrCode::RETRY),  // 客户端用同一 seq 重试
                ProposeOutcome::Lost => Err(ErrCode::NOT_LEADER),   // 结果不确定：重试时去重表兜底
            });
        }
    }
}

fn run_one(view: &mut BatchView, env: &OpEnv, ctx: &ReqCtx, op: TxnOp) -> Result<MetaReply, ErrCode> {
    if ctx.seq != 0 {
        if !sessions::is_active(view, ctx.session) { return Err(ErrCode::SESSION_EVICTED); }  // 驱逐是单调的事实
        if let Some(r) = dedup::get(view, ctx.session, ctx.seq) { return r; }               // 重试：返回原结果
    }
    view.begin_op();                                                // 操作级覆盖层：失败整体丢弃（原子性）
    let res = op(&mut view.op(), &env.op_ctx(ctx));                 // 内部调用 authz / quota / placement 钩子
    if res.is_ok() { view.commit_op() } else { view.rollback_op() }
    if ctx.seq != 0 && dedup::is_final(&res) {                     // 成功，或 1..=255 的 POSIX errno（含锁冲突的 EAGAIN）
        dedup::put(view, ctx.session, ctx.seq, &res);              // 错误也记录：重试得到同样的答案，且批次写集必然非空
        if ctx.seq > DEDUP_WINDOW { dedup::delete(view, ctx.session, ctx.seq - DEDUP_WINDOW); }   // 点删除，无需扫描
    }
    changelog::record(view, env, ctx, &res);                       // 成功的修改都写；被拒绝的只在 audited 时写
    res
}
```

规则（实现与评审逐条核对）：

1. **执行器闭包里不得做网络 IO、不得睡眠、不得扫描**：`OpView` 只提供 `get/put/delete`，没有 `scan`。需要扫描的工作（readdir、去重表清理、`get -r`、`status`、GC）在只读路径或后台任务里完成，再提交点删除。数据面配合（截断、删 chunk）由闭包写下 GC 记录，由客户端或后台任务在闭包之外执行。
2. **错误回复也在提交之后发出**：因为每个带 seq 的请求都写去重记录，批次写集必然非空，所以错误一定是在"确认自己仍是 leader 并提交成功"之后才回复——避免过期 leader 回出错误的 `ENOENT/EEXIST`。集群类错误码（≥ 1000）不写去重表：它们表示"没有执行，请重试"，其覆盖层已丢弃。
3. **leader 就绪**的定义：`state == Leader && node.raft.apply_to_current_term()`（本 term 的空条目已应用，于是前任提交的一切都已在状态机里）**且本节点插件清单 == 已提交的 GLOBAL `plugins`**（3.7.2）。就绪之前执行器不构建批次、读闸门不发 ReadIndex。
4. 客户端保证每个会话在途的修改型请求 ≤ 256 个、seq 逐个递增；所以写入 `seq` 的结果时删除 `seq − 256` 的结果是安全的。会话关闭或驱逐后，剩余去重记录由 gc 任务扫描删除。
5. 改变集群拓扑的内部操作（链、target、节点、存储类）`exclusive = true`：**它必须是本批的最后一个操作**，之后的请求进下一批（代码里就是"执行完它就结束本批"）。create 或截断到 0 时若 `gen_chains` 生成了新链（`view.topology_changed()`），也在执行完后结束本批。原因：placement 读的是批次开始时的集群图快照（`OpCtx.map`），这条规则保证同一批次里没有人在拓扑变了之后还按旧快照做决定。
6. 内部操作（cluster-mgr、后台任务）`seq = 0`，不进去重表；它们都写成带前提条件的 CAS（"链版本仍是 v 才改""change_id 与 epoch 都没变才切换 layout"），重复执行无害，失败就在下一轮重新评估。

> 实现注记（M3）：执行器在 `core/src/meta/engine/executor.rs`（边取边执行是 `fill`，单个请求是 `run_one`，批次结局到回复的映射是 `settle`），覆盖层在 `view.rs`，去重在 `dedup.rs`，`sessions::is_active` 在 `core/src/meta/sessions.rs`（M4 再加会话的其余部分）。`view.topology_changed()` = 本批写过 CHAIN 键（0x12）：gen_chains 生成新链一定写 CHAIN 记录，覆盖层据此判断，命名空间代码不用另外打标记。执行器的输入队列 1024 个请求，满了回 `BUSY`；fail point `meta.exec.before_propose` 配 `return(码)` 时本批每个请求都得到这个码（批次没有提议、一个 op 也没有应用）；`changelog::record` 在 M4 接入（M3 的 DebugPut 按 ADR-0029 本来就不写 changelog）。批次指标：`wmfs_meta_batch_ops`、`wmfs_meta_batch_bytes`、`wmfs_meta_commit_seconds`（提议到应用）。
> 实现注记（M4）：
> - ① 每个批次开始时从批次的读事务里取一份集群图快照（`OpEnv::op_ctx(req, map)`，按 map_epoch 缓存，3.3.6 的注记）。
> - ② `changelog::record`：成功的修改由各操作在自己的闭包末尾写（`core/src/meta/changelog.rs` 的 `append`）——只有闭包知道 inode 号、名字与长度，写在闭包里它就与修改原子地一起提交、一起回滚；run_one 里"被拒绝的修改在 audited 时也写"随 `audit` 属性在 M12 加入。
> - ③ 驱动循环 after_apply 不用为集群图做事（按 map_epoch 失效）；"写入了 changelog 就唤醒长轮询等待者"随 changelog 流（`GET /api/v1/changelog`）在 M7 加入。
> - ④ run_one 里查会话用 `sessions::usable(view, session, key_id)`（伪代码的 `is_active` 之外还核对会话是不是这把 key 打开的，不是 → `EACCES`，3.3.4 的注记 ④）；它在查去重表之前，所以别的 key 的请求既拿不到、也写不进这个会话的去重记录。
> - ⑤ 规则 4 的"在途的修改型请求 ≤ 256 个"指**所有在途请求的 seq 都 > 新 seq − 256**，不只是个数（3.3.4 的注记 ⑪）：只数个数时，一个在等超时重试的旧请求的去重记录会被同一会话里别的线程的第 256 个新请求点删掉，它的重试就会重新执行、回出假的 `EEXIST`。

##### 3.4.1.5 `state_version` 与 `expect_version`

`state_version` = 最近一个**改变了 KV 的**批次条目的 raft index（存在 `raft_state["applied"]`，与 KV 同事务更新；空条目与 conf change 不改变它；快照安装时取快照里的值）。执行器在构建批次的读事务里读到 `base_version`，写进 `WriteBatch.expect_version`；**每个副本** apply 时比较：相等才应用并令 `state_version = entry.index`；不等则整批跳过（所有副本看到同样的日志、做同样的判断，所以是确定性的）。

它挡住的情况：① 实现 bug 导致执行器在未就绪时读到旧状态；② 过期写集绕过提议前检查进入日志；③ 将来引入流水线后，前一批被丢弃导致后一批的前提失效。代价：一次 u64 比较。批次作废时客户端收到 `RETRY`，用同一个 `(session, seq)` 重试。正常运行中它永远不会触发（指标 `wmfs_meta_batch_stale_total` 应恒为 0，非 0 即告警）。

##### 3.4.1.6 线性一致读：批量 ReadIndex（否决 LeaseBased）

**为什么否决 `ReadOnlyOption::LeaseBased`**：raft-rs 0.7 源码里 LeaseBased 分支直接用 `raft_log.committed` 生成 ReadState，不检查任何时间租约；它只依赖 `check_quorum` 让失联 leader 下台，而 leader 每个 election_timeout 才发一次 `MsgCheckQuorum`、检查的是上一个周期留下的活跃标志，失联后最坏约 2 个选举超时才下台；follower 约 1 个选举超时就选出新 leader 并提交新写。这段重叠窗口里旧 leader 的读会返回旧数据（与时钟准不准无关），违反原则 7。将来要做 leader 租约读，必须在我们自己的代码里实现"收到多数派心跳响应的时刻 + 选举超时 × (1 − 漂移)"式租约（TiKV 做法）并写 ADR。

**做法**（`Safe`，每轮确认一次心跳往返，本机回环约 0.1–0.3 ms，不 fsync）：

```rust
pub struct ReadGate {
    waiting: Vec<Waiter>,                  // 尚未进入任何一轮的读者
    inflight: Option<Round>,               // 已调用 read_index、等待 ReadState
    ready: VecDeque<(u64, Vec<Waiter>)>,   // 已拿到 ReadState，等 applied ≥ index
}
struct Round { ctx: u64, readers: Vec<Waiter>, sent: Instant, first_sent: Instant }
struct Waiter { done: Sender<Result<(), ErrCode>>, arrived: Instant }
```

1. meta-read 线程向 raft 线程发 `RaftInput::ReadIndex{done}`，阻塞等待（3 s 超时）。
2. `pump`：没有在途轮次、有等待者、leader 已就绪 → 把**当前全部等待者**打包，`node.read_index(ctx.to_be_bytes().to_vec())`（ctx 递增）。在这次调用之后到达的读者等下一轮（否则可能漏掉本轮确认之后才提交的写）。不是 leader → 全部回 `NOT_LEADER{hint}`；是 leader 但未就绪 → 继续等。
3. Ready 里出现 `ReadState{index, request_ctx}` 且 ctx 匹配 → 这一包移入 `ready`；`applied ≥ index` 时逐个回 `Ok`；读者随后各自开 redb 读事务读取并回复（redb 的 None 提交对新读事务可见）。
4. `on_tick`：在途轮次 500 ms 没有 ReadState → 用新 ctx 为同一批读者**重发** read_index（leader 本任期尚未提交时 raft-rs 会静默丢弃 MsgReadIndex，坑 P6）；首次发出已超过 2 s → 全部回 `RETRY`；等待超过 2 s 的读者也回 `RETRY`。角色变化 → 全部 `NOT_LEADER`。
5. follower 不服务读，回 `NOT_LEADER{hint}`（首发不做 follower read）。
6. 单节点集群：raft-rs 对单成员组立即产生 ReadState，无额外开销（已在 spike 验证）。

> 实现注记（M3）：读闸门在 `core/src/meta/engine/read_gate.rs`，由 raft 线程的 Driver 持有；500 ms / 2 s 按 5 / 20 个 tick 计。`MetaEngine::read` 等 ReadIndex 最多 3 s（超时 `RETRY`），放行后开 redb 读事务回调（`view.rs` 的 `ReadView`：点查 + 有上限的范围扫描；meta-read 线程上的 redb 读错误回 `EIO`，不 abort）。指标 `wmfs_meta_read_gate_seconds`。

##### 3.4.1.7 快照：导出、带外传输、安装

- **导出**（leader，开销很小，可在 raft 线程上做）：`Storage::snapshot(request_index, to)`：若已有 pin 满足 `pin.index ≥ request_index` 且 `pin.index ≥ snap_index`（未被日志压缩越过），复用；否则开一个 redb 读事务（MVCC，不阻塞写），读出 `applied(index, term, state_version)` 与 `conf_state`（它们与 KV 同事务更新，所以与读事务里的 KV 一致），新建 `Arc<PinnedSnap { index, term, state_version, txn: ReadTransaction, created }>` 放进 `pins`（最多保留 2 个，10 min 过期；`ReadTransaction` 是 `Send + Sync`，已编译验证；`pins` 是 `Arc<Mutex<…>>`，snap-serve 线程持有同一个 Arc，按 index 取出对应的 pin 只读遍历）。返回 `Snapshot { metadata: {index, term, conf_state}, data: postcard(SnapDesc { index, term, source_node }) }`——**快照内容不进 raft 消息**。
- **传输**：follower 的连接线程收到 `MsgSnapshot` 时**不立即 step**，交给 `snap-recv` 线程（已有下载在进行则丢弃这条消息，raft 会重发）：向 `source_node` 发 `SnapshotFetch { index, term }`（地址从最新集群图解析——刚加入的节点用 `Joined` 回复里带的集群图；查不到时就用 MsgSnapshot 到来的那条连接）；leader 的 `snap-serve` 线程用对应 pin 顺序遍历 `kv` 表，以 StreamChunk 帧发送：首帧 body = `SnapHeader { index, term, state_version, conf_state: Vec<u8>, pairs_total: u64 }`，中间帧 body = `Vec<(Vec<u8>, Vec<u8>)>`（每帧 ≤ 1 MiB），末帧置 `LAST_CHUNK`、body = `SnapTrailer { pairs: u64, crc32c: u32 }`。follower 写入 `meta/snap-recv-<index>-<term>.snap`（格式：`b"WMFSSNP1"` ‖ u32 长度 + SnapHeader ‖ 重复 `{klen: u32, vlen: u32, key, value}` ‖ `b"WMFSEND!"` ‖ pairs u64 ‖ crc32c u32），校验后 fsync，**才**把原 `MsgSnapshot` 交给 raft 线程。
- **报告**（坑 P11）：`snap-serve` 流成功结束 → `RaftInput::ReportSnapshot{to, ok: true}`；出错 → `ok: false`。raft-send 线程记录"何时向谁发了 MsgSnapshot"，60 s 内对方没完成 `SnapshotFetch` → 报告 `ok: false`，避免 leader 卡在 Snapshot 状态。
- **安装**（follower，Ready 里 `snapshot()` 非空）：规则 M5；安装后删除快照文件。若 raft-rs 判定快照过旧而不安装，残留文件 10 min 后由 raft 线程清理。

##### 3.4.1.8 日志压缩

每应用 10 000 条检查一次：`C = durable_applied − 10 000`（给慢 follower 留余量，超出余量的 follower 走快照），执行规则 M3（`retain_in(..=C, |_, _| false)`）。状态机就在 redb 里，压缩不需要写快照文件。

##### 3.4.1.9 成员变更

- **init**：`wmfs init` 直接写一个"index = 1、term = 1、voters = [1]"的初始状态：`snap_meta = (1, 1)`、`hard_state = {term: 1, vote: 0, commit: 1}`、`conf_state = {voters: [1]}`、`applied = (1, 1, state_version = 1)`、初始 KV（3.4.3）——与 raft-rs `five_mem_node` 示例一致：之后加入的节点一律先通过快照追上，保证所有节点初始状态相同。启动时若 `voters == [自己]`，调用一次 `node.campaign()` 尽快成为 leader。
  实现注记（M3）：启动时 campaign 的条件就是 `voters == [自己]`（learner 不影响：它们不投票）；空 ConfState 或多个 voter 时不自己 campaign（坑 P14 的回归用例）。
- **join**：新节点以**空存储、空 ConfState** 启动 `RawNode`（不可提升、不会自己 campaign，坑 P14），用 `Joined` 回复里的集群图找到 leader 与其他成员的地址。leader 先在一个批次里写 `NodeRecV1`，再由 cluster-mgr 提议 `ConfChangeV2 { AddLearnerNode(id) }`。leader 发现 learner 缺少已压缩的日志 → 发快照。
- **提升**：cluster-mgr 每秒读 raft 线程发布的 `RaftStatus` 检查：`voters < 3` 且 `voters + 已追上的 learner ≥ 3`（追上 = 该 learner 的 `matched + 1000 ≥ commit`；raft 线程发布时取自 `node.raft.prs().get(id).matched`）→ 提议**一个包含多个 `AddNode` 的 ConfChangeV2**（raft-rs 对多变更自动进入 joint consensus），1 voter 一步变 3 voter，不经过"2 voter、任一宕机即不可用"的中间态。2 节点集群保持 1 voter + 1 learner，`wmfs status` 警告"元数据没有高可用"。
- **leave-joint**：raft-rs 在 leader 应用 joint 配置时自动追加空的 `ConfChangeV2` 退出 joint（`Raft::commit_apply`），**我们不在 Ready 处理中途再提议**。兜底（坑 P13，源码 TODO 承认"leader 中途下台可能永远不退出"）：cluster-mgr 发现发布的 `conf_state.voters_outgoing`（raft 线程取自 `node.raft.prs().conf().to_conf_state()`）非空超过 5 s 且没有在途 conf change → 经 `RaftInput::ConfChange` 提议空 `ConfChangeV2::default()`（在 raft 线程的输入阶段 propose）。
- **移除**：只能由管理员 `wmfs node remove <id>` 触发（自动移除 voter 太危险）；若有已追上的 learner，同一个 joint 变更里顺带提升它。
- 同一时间只允许一个在途 conf change（raft-rs `pending_conf_index`）；cluster-mgr 排队，应用后重新读 conf_state 判断是否生效（被 raft-rs 改写成空条目时下一轮重试）。
- **优雅退出**（SIGTERM）：若本节点是 leader，`transfer_leader` 给 matched 最大的 voter，最多等 3 s。

##### 3.4.1.10 leader 的内存状态（故障切换时会丢、如何重建）

| 内存状态 | 切换后 | 重建方式 |
|---|---|---|
| 在途批次 / 读闸门等待者 | 客户端收到 `NOT_LEADER`/超时 | 客户端带同一 seq 重试，去重表兜底 |
| 会话 last_seen、target last_hb | 全部重置为"新 leader 就绪时刻" | 驱逐/移除最多推迟一个租约周期（推迟是安全的，提前才危险）；就绪后 1 个 `target_lease` 内不做链的移除/降级决定，只记录心跳 |
| 打开文件集合（3.5.6） | 丢失 | 心跳回复 `need_full_open_set = true`，客户端重发全集；孤儿 GC 暂停到条件满足 |
| 属性解析缓存、集群图缓存 | 丢失 | 懒重建 |
| 后台任务进度 | 丢失 | 从 `TaskRecV1` 游标续跑 |
| 驱逐第二阶段的确认进度 | 丢失 | 从 `SessionRecV1.state = Evicting` 与 `evict_map_epoch` 重新判断（3.4.4） |

实现注记（M4）：会话的内存状态（`last_seen`、打开文件集合）在 `core/src/meta/sessions.rs` 的 `Live`（由元数据服务持有）：每条记着它是在哪个 term 里记下的，term 变了（换过 leader）就当作不知道——心跳回 `need_full_open_set = true`，客户端重发全集；SessionOpen 成功时记下（还没有打开的文件，算收到过全集），SessionClose 时删掉。会话租约到期的驱逐（cluster-mgr）与两阶段驱逐在 M10。

#### 3.4.2 数据链式复制（CRAQ 变体）【ADR-0024】

##### 3.4.2.1 链与名词

- 一条链 = `ChainRecV1`：有序成员 `members: [(target, Serving|Syncing)]` + `ver` + `incarnation`。**不变式 I0：所有 Syncing 成员都排在所有 Serving 成员之后**（新成员总是以 Syncing 追加到末尾；降级为 Syncing 必须同时移到末尾；Serving 成员被移除时其余保持顺序）。
- **head** = 第一个 Serving 成员（写入口）；**last serving（L）** = 最后一个 Serving 成员（脏读的仲裁者、同步源）；**tail** = 最后一个成员（可能是 Syncing，提交从这里开始）。没有 Serving 成员 = 链不可用（`NOT_SERVING`）。
- **生成**（`gen_chains(v, c, class, replicas) -> Result<Vec<ChainView>, ErrCode>`，在 create 或截断到 0 的执行器闭包里、`OpCtx.map` 里找不到 (class, replicas) 的链时调用；只写 CHAIN 记录与 `map_epoch += 1`，**返回新建链的 `ChainView` 列表**——它们不在批次开始时的集群图快照里，placement 用 "快照里同 (class, replicas) 的链 + 返回值" 作为候选；调用后本批结束（3.4.1.4 规则 5）；之后由链管理器维护）：候选 = 状态 Up、标签匹配存储类 `label` 的 target（`dedicated` 的类独占其 target，其他类不得使用）；按 target id 排序；链数 = 候选数（每个 target 当一次 head）；第 i 条链从候选 i 开始按环形顺序取，跳过同一节点（故障域 = 节点）的 target，直到 `replicas` 个或候选用完。故障域不够时链以较短长度创建（期望长度仍为 replicas，`wmfs status` 与 dashboard 显示差距，【ADR-0016】）。新链 `ver = 1, incarnation = 1`，成员全部 Serving（尚无数据）。
- 文件 layout 记录 k 条链，`k = min(stripe, 可用的互不相同的链数)`：可用链不够 `stripe` 条时**用 k 条创建**而不是报错（`k = 0` 才 `ENOSPC`），与短链同一原则【ADR-0016】——`wmfs set stripe=…` 时警告、`wmfs status` 统计"条带数不足"的文件；chunk idx 落在 `chains[idx % k]`。副本数是链的属性，文件不记录每个 chunk 的位置。
- 实现注记（M4）：`gen_chains` 在 `core/src/meta/cluster/chains.rs`（候选、环形取成员是纯函数 `candidates`/`plan`，有带种子的随机测试）；它写 CHAIN、`next_chain` 与 `map_epoch += 1`，一个候选 target 都没有时什么也不写（调用方回 `ENOSPC`）。`ChainView.used` 来自 target 心跳（M5），在那之前是 0；`capacity` 取成员里**有上限的** target 容量的最小值，成员全都不限才是 0（= 不限）——一个不限容量的成员不会让整条链显得不限。

##### 3.4.2.2 chunk 版本、每个 chunk 的状态与不变式

- **版本分配**（只在 head）：`V = max(committed + 1, (chain.incarnation as u64) << 40)`，chunk 不存在时 committed = 0。同一 incarnation 内每个 chunk 有 2^40 个版本可用。
- **chunk 版本永不重置**：截断写新版本（3.4.2.9），只有整个 `(ino, epoch)` 才会被删除，而 `(ino, epoch)` 永不复活（inode 号不复用，epoch 来自 `max_epoch + 1`）。
- 每个成员内存里对"有 pending 或有排队写"的 chunk 维护状态（64 路分片的 `Mutex<HashMap<ChunkId, ChunkState>>`，没有 pending 时删除条目；committed 版本首次访问时从文件头读出并缓存）：

```rust
pub struct ChunkState {
    pub committed: u64,                     // 已提交版本；0 = 不存在
    pub pending: Option<PendingWrite>,      // I1：最多一个
    pub waiters: VecDeque<QueuedWrite>,     // 只在 head：排队的客户端写（≤ 8 个，持有请求与 Buf）
}
pub struct PendingWrite {
    pub version: u64,
    pub upstream: Option<Responder>,        // 客户端（head）或前驱（后继成员）的回复句柄；重新驱动时可被替换；回复过 RETRY 后置 None
    pub successor: Option<(u32, u64)>,      // 已转发给哪个 target、在哪个 chain_ver 下；None = 自己是尾或尚未转发
    pub started: Instant, pub last_sent: Instant,
    pub sync: bool,                         // O_SYNC：提交前 fdatasync
}
```

| 不变式 | 内容 |
|---|---|
| I1 | 任一成员上一个 chunk 最多一个 pending 版本，且 `pending.version > committed` |
| I2 | 沿链方向，`latest`（pending 或 committed）不增，`committed` 不减（尾部先提交、逆向确认）；唯一的暂时例外见 3.4.2.6 规则 2 |
| I3 | 版本 V 只有在**所有成员（含 Syncing）都提交 V 之后**才回复写者 |
| I4 | 成员只接受**当前前驱**发来的、`chain_ver` 与自己视图一致的转发 |
| I5 | 成员只有在"自己是 Serving + target 租约有效 + 集群图里自己的 `boot_id` 与本进程一致 + 本地集群图的 `map_epoch` ≥ 最近一次心跳回复里的 `map_epoch`"时才服务读写（租约续上后若回复里的 map_epoch 更新，先拉到新集群图再恢复服务——这保证被驱逐列表在恢复服务前已生效） |
| I6 | 同一 incarnation 内，同一 chunk 的同一版本号在所有成员上内容逐字节相同 |

##### 3.4.2.3 写流程（head，io 线程）

```
1. 校验 cap（3.3.5）、chain_ver == 本地视图、自己是 head、I5        否则 BAD_CAP / STALE_CHAIN / NOT_SERVING / FENCED
2. 锁 chunk 状态（分片 Mutex：只保护内存状态，不跨网络、不跨大块磁盘 IO 持有）
     pending 存在 → 等待队列 < 8 则入队并返回；否则 BUSY
     V = max(committed + 1, incarnation << 40)；pending = Some(PendingWrite{ V, upstream: 客户端回复句柄, .. })
   解锁
3. 构建新版本（data/build.rs）：extents 涉及的每个块——整块覆盖 → 取 bulk 切片；部分覆盖 → 读已提交块并解码、打补丁（chunk 被标为 Corrupt 时按 R5：先修复或回 EIO）；
   每块 transform 编码（加密用新的随机 nonce）→ 新块表条目 + 存储字节；未改动块进入"从已提交版本复制"清单；
   新固定头：chunk_version = V、logical_len = max(旧值, 最大 extent 末尾)、write_time_ms = wall_ms()
4. chunk_store.write_pending(…)（R1）；失败 → 删 pending 文件、pending = None、回复 EIO
5. 有后继 → ForwardWrite{ full: false, base_version: committed, version: V, header, blocks, sync } + bulk（改动块存储字节）
             用 RpcClient::call_cb 发出，回调把 Ack / NeedFull / 错误投递到 repl 队列；登记 successor；io 线程立即返回
   无后继（链长 1）→ 提交：commit_pending（rename）→ committed = V、pending = None → 回复 Written{V} → 处理等待队列队首
6. [repl 线程] 收到 Ack{V}：锁 chunk；pending.version == V → commit_pending → committed = V → pending = None → 回复 upstream Written{V}
   → 等待队列非空则取队首，在本 repl 线程上从第 2 步重新执行
7. [repl 线程] 收到 NeedFull：读本地 pending 文件的 packed 形式 → 重发 ForwardWrite{ full: true, version: V }
8. 回调收到其他错误或 3 s 无确认：**不回滚、保留 pending**；回复 upstream RETRY 并置 upstream = None；由 chain-timer 前滚（3.4.2.6）
```

为什么是前滚而不是回滚：pending 版本可能已在下游提交并被读到，回滚会让已读到的数据消失；前滚永远安全（该写尚未确认给写者，"发生"或"不发生"都合法，而写者的重试会作为新版本写入同样内容，结果相同）。

##### 3.4.2.4 后继处理 `ForwardWrite`（repl 线程）

先比 `chain_ver`：请求的更新 → 通知 hb 线程立即拉集群图，回 `STALE_CHAIN`；请求的更旧 → `STALE_CHAIN`；`from_target` 不是本地视图里的前驱 → `STALE_CHAIN`（I4）；自己不在链上 → `NOT_SERVING`。然后锁 chunk：

| 本地状态 | `full = false`（增量） | `full = true`（完整） |
|---|---|---|
| `committed ≥ V` | 重复：回 `Ack{committed}`（I6：同版本即同内容） | 同左 |
| `pending.version == V` | 重新驱动的重复：用本请求替换 upstream；自己是尾 → 提交并 Ack；否则向当前后继重发（完整） | 同左 |
| 无 pending 且 `committed == base_version` | 正常：写 pending（基础块表 + 改动条目；改动块写 bulk，未改动块从已提交文件复制）；自己是尾 → 提交并 `Ack`；否则增量转发给后继并登记 | 写 pending（整份），其余同左 |
| 其他（缺基础版本：`committed ≠ base_version` 且 `< V`；或存在 `version < V` 的旧 pending） | 自己是 Syncing → 回 `NeedFull`；自己是 Serving → 回 `INTERNAL` 并在心跳里上报（不变量破坏，链管理器把它降为 Syncing） | 放弃旧 pending（若有），写 pending（整份替换本地），其余同"正常" |

写 pending、转发、提交、回复的细节与 head 的第 4–8 步相同（尾节点没有后继，写完 pending 立即提交并 Ack）。Syncing 成员处理任何转发后，把该 chunk 加入本轮同步的 `touched` 集合（3.4.2.8）。

##### 3.4.2.5 读流程（CRAQ 规则）

```
成员 X 收到 ReadChunk：校验 cap、chain_ver、I5
  该 chunk 不存在 → CHUNK_NOT_FOUND
  X 上该 chunk 有 pending 且 X 不是 last serving → CHUNK_DIRTY{extra = last serving 的 target}（客户端改问它）
  否则读已提交版本：每块读存储字节 → transform 逆序解码（校验 checksum）→ 复制所需区间；洞 = 0；超出 logical_len = 0
```

正确性：last serving 成员 L 是"对外可见"的线性化点。L 提交 V 之前，没有任何 Serving 成员返回过 V（L 之前的成员仍 pending，会把读重定向给 L；L 之后只有 Syncing 成员，不服务读）；L 提交 V 之后 L 返回 V，L 之前仍 pending 的成员也把读重定向给 L。所以读永远不会"先看到新版本、后看到旧版本"。

##### 3.4.2.6 确认、链变化与前滚（re-drive）

成员得知新的链版本时（hb 线程拉到新集群图），**先对该链完成下面的处理，再把本地视图切换到新版本**（读写请求都带 `chain_ver`，视图切换之前不会有人按新链读到它）：

1. 自己被移出该链，或被降为 Syncing → 放弃自己在该链上的全部 pending（删 pending 文件，回复 upstream RETRY）。Syncing 成员之后会重新同步。
2. 自己在新链中是 **last serving**，而旧链的 last serving 已不在新链的 Serving 成员里 → 把自己在该链上的全部 pending 版本**立即本地提交**。原因：被移除的旧 L 可能已把这些版本返回给读者；若新 L 继续返回旧的已提交版本，就会出现"先读到新、后读到旧"。本地先提交会暂时违反 I2，但它后面只可能是 Syncing 成员（不服务读），没有读者能观察到；随后的重新驱动会把这些版本推给后面的成员。
3. 自己成为 tail（没有后继了）→ 提交全部 pending 并向 upstream Ack。
4. 后继变了 → 把 pending 版本（完整形式）重新转发给新后继。

`chain-timer` 线程每 500 ms 扫描：pending 超过 3 s 未确认 → 按当前视图重新驱动（有后继 → 完整重发；没有后继 → 提交并 Ack）。

##### 3.4.2.7 唯一幸存者、last_serving、整链重启与 incarnation

链管理器（leader 上）对 target T 的处理规则：

| 情况 | 动作 |
|---|---|
| T 的心跳带来新的 `boot_id`（进程重启过），且 T 所在链里**还有别的存活 Serving 成员**（Up，且最近 2 s 内以未变的 boot_id 心跳过） | T 在该链降为 Syncing **并移到 members 末尾**（I0）；`ver += 1` |
| T 重启了，且它是该链**唯一存活的 Serving 成员**（"唯一幸存者"，例如单副本链 `[t1]`） | T 保持 Serving；`incarnation += 1`；其余成员（若有）全部降为 Syncing 并排在 T 之后；`ver += 1` |
| 链因最后一个 Serving 成员 X 离线而被记 `last_serving = X`（链不可用） | X 回来 → X 恢复为 Serving（它有最新的已提交数据），`incarnation += 1`，清除 last_serving；其他回来的成员以 Syncing 追加。只有其他成员回来时**不得**让它们顶替（链保持不可用，客户端重试到超时得到 `EIO`，dashboard 告警"链 X 等待最后的副本"） |
| 整链同时重启（例如整机断电，元数据也重启了）：新 leader 就绪后等待 1 个 `target_lease` 收集心跳，若某链所有 Serving 成员都换了 boot_id | 选**按链序最后一个、当前 Up 的原 Serving 成员**为幸存者（I2：它的已提交版本最全）；它保持 Serving，`incarnation += 1`，其余降为 Syncing |
| 管理员 `wmfs chain force-serving <chain> <target>`（明确承认可能丢失其他成员独有的写） | 指定 target 成为唯一 Serving，`incarnation += 1`，其余 Syncing；写 changelog |

**为什么需要 incarnation**：唯一幸存者重启后可能丢了 pending（启动时清空 `p/`），掉电时甚至可能丢了未 fsync 的提交；而下游成员可能还持有同一版本号、不同内容的数据。如果幸存者重新分配同一个版本号，下游会按"`committed ≥ V` ⇒ 重复"把新写吞掉，副本静默分叉（附录 C 的 C-8）。incarnation 加一后，新版本 ≥ `incarnation << 40`，大于此前任何 incarnation 里分配过的版本，旧副本永远不会被误认为新版本；版本仍然单调。正常情况（还有存活 Serving 成员时）不需要加一：存活的 head 知道它分配过的所有版本，重启的成员只是降级重同步。

未 `fsync` 的写在整链同时断电时可能丢失（即使曾被读到）——与本地文件系统同等的 POSIX 语义，写进 semantics.md；`fsync` 过的数据在所有成员上都已落盘（`SyncChunks` 经过整条链），不会丢。

##### 3.4.2.8 重同步协议（Syncing 成员主动拉取）

```
Syncing 成员 S，对每条自己是 Syncing 的链（sync 线程，每节点最多 2 条并发）：
  src = 该链的 last serving 成员；没有 → 等待
  touched = ∅（本轮开始后收到过转发的 chunk；转发处理时加入）
  1. 分页 ListChunks(chain, after, 1000) 向 src 取 [(chunk, committed_version)]（src 遍历 c/<chain>/ 读文件头）
  2. 对每项：chunk ∈ touched → 跳过；本地缺失或本地已提交版本 ≠ src 版本（无论高低）→ FetchChunk(chunk)：
       src 打开已提交文件（rename 之后旧 fd 仍指向原版本，天然是一致快照，不需要冻结源端），返回 packed 形式
       S 加 chunk 锁：若此时 chunk ∈ touched → 丢弃取回的数据；否则 t/ 临时文件 → fdatasync → rename → fsync 目录（R3），committed = 取回的版本
  3. 本地有、src 列表里没有、且不在 touched 里的 chunk → 删除
  4. SyncDone(target, chain, 当前 chain_ver) → leader：chain_ver 未变且 S 是第一个 Syncing 成员（紧跟最后一个 Serving）→ S 变 Serving（ver += 1）；
     否则回 RETRY，S 重跑本协议（只拷有差异的 chunk，代价很小）
```

要点：同步源是 **last serving**（不是直接前驱——前驱可能也是 Syncing）；S 处理转发与同步线程互不阻塞（同步线程只在安装那一刻持有 S 的 chunk 锁），所以不会出现"源端等 S 的确认、S 等源端的 FetchChunk"的死锁；本地版本**比源端高**也以源端为准（它只可能是从未在全链提交的旧 pending 残留，或属于更早的 incarnation）；FetchChunk 在源端走 QoS 的后台低优先级队列（4.5），受后台限速。

##### 3.4.2.9 截断、删除 epoch、同步传播

- **截断到 s > 0**：客户端对 `[floor(s / chunk_size), ceil(old_size / chunk_size))` 范围内的每个 chunk 发 `TruncateChunk{chunk, new_len}`（`new_len = clamp(s − idx × chunk_size, 0, chunk_size)`）到其链 head。head 把它当作一次带版本的写：`new_len` 之后的块变成洞（`stored_len = 0`），边界块重新编码（`new_len` 之后填 0），`logical_len = min(旧值, new_len)`；照常转发、提交。**从不删除 chunk 文件**（保持版本单调）。全部完成后客户端发 `SetAttr{size: s}`（先数据、后元数据：崩溃在两步之间 → 长度不变、被截区域读 0，semantics.md 放松 ⑦）。inline 文件（F_INLINE）没有 chunk：客户端直接发 `SetAttr{size: s}`，服务端在同一闭包里把 INLINE 截到 `min(len, s)`（3.2.7 的不变式），之后把文件扩回来也只会读到 0。
- **截断到 0**（含 `O_TRUNC`、覆盖式 `cp`）：只有元数据，一个闭包：`epoch = max_epoch + 1`，**按当前生效属性重建整个 layout**（与 create 共用 `ns::common::new_layout`：存储类、副本、chunk、stripe、inline、checksum/compress/encrypt 全部取当前值——所以截断到 0 的文件和新文件一样立即享有新属性），size = 0，`class_id` 改为新类（per_class 的文件数随之搬移），F_INLINE 按新 layout 重新判定、INLINE 清空；旧 epoch 写 GC 记录（kind 2，宽限 `epoch_gc_grace`（默认 60 s）后由 gc 任务删除；旧 inode 有 F_INLINE 时没有 chunk，不写）。回复 `Opened(OpenOut)`（新 layout 与 cap，3.1.4 C4）。旧 chunk 瞬间全部不可达，不需要逐个处理。
- **DeleteEpoch**（只由 gc 任务发出，cap 带管理权限位）：head 放弃该 (ino, epoch) 的 pending、删除本地目录，再 `ForwardDeleteEpoch` 沿链传播，尾回 `Ok` 后逆向确认。离线期间错过删除的成员由本地孤儿扫描（`CheckEpochs`）清理。
- **SyncChunks**（客户端 fsync）：head 对列出的 chunk 的已提交文件 `fdatasync`（新建过目录项的再 `fsync` 目录），`ForwardSync` 沿链传播（`call_cb`，不占线程），尾确认后逆向回复。已经确认给写者的版本在全链都已提交（I3），所以 fsync 覆盖了此前所有已完成的写。

##### 3.4.2.10 链管理器（cluster-mgr，每个决定都是一个 `exclusive` 内部操作：`chain.ver += 1`、`map_epoch += 1`）

| 事件 | 动作 |
|---|---|
| target T 超过 `target_lease`（8 s）无心跳 | T 状态 Offline；从所有链移除并记入 `chain.removed`；若 T 是某链最后一个 Serving 成员 → `last_serving = T` |
| T 心跳带来新的 `boot_id` | 3.4.2.7 |
| Offline 的 T 恢复心跳，且离线 < `rejoin_window`（30 min） | 对 T 曾所在、长度 < replicas 的链：以 Syncing 追加到末尾；`last_serving == T` 的链按 3.4.2.7 |
| `SyncDone` | 3.4.2.8 第 4 步 |
| 链长度 < replicas 超过 `repair_delay`（30 min），且存在合格的空闲 target（标签匹配、节点不同） | 以 Syncing 追加该 target |
| T 离线超过 `rejoin_window` | 不再放回旧链；从 `removed` 清除；T 回来后它在这些链上的 chunk 目录成为孤儿，由本地 gc 删除 |
| 管理员 `wmfs node drain <n>` | 先为相关链追加替代成员（Syncing），等它们 Serving 后再移除 n 的 target |
| 成员上报不变量破坏（INTERNAL） | 降为 Syncing 并移到末尾，告警 |
| 成员心跳上报"持续 10 s 无法把转发送达后继"（成员之间分区、但各自心跳正常） | 移除**下游**那一个（保住更靠前、数据更新的成员） |
| 新 target 加入 | 为已在用的每个 (class, replicas) 生成以它为 head 的新链；补齐长度不足的链（Syncing） |

#### 3.4.3 成员与加入（init / join）

```
wmfs init --data-dir D [--addr 127.0.0.1] [--rpc-port 9300] [--mgmt-port 9380] [--target PATH[:label,label]]...
          [--profile default|minimal|nas-safe|hpc-rdma] [--owner UID:GID] [--no-start]
  1. 确认 D 为空（否则拒绝：防止覆盖已有集群，坑 P14）
  2. 生成 cluster_id、cluster.key（rand::fill）；node_id = 1；写 NODE、node.toml（--addr/--rpc-port 写成 [plugin.tcp] listen，
     [net] control_scheme = data_scheme = "tcp"）、各 TARGET 超级块
  3. 写 meta.redb 初始状态（3.4.1.9）。初始 KV：根 inode 1（目录，mode 0755，owner = --owner，默认为运行 init 的用户）；
     /home 与 /scratch（mode 1777）；存储类 default（id 1，label "*"，replica 3，priority normal）；按 profile 写目录属性与 node.toml：
       default  ：/home replicas=3，/scratch replicas=2（理念文档第 7 章默认冗余）
       minimal  ：default 类 replica=1，不写 replicas 属性（单节点不出现降级警告）；node.toml 禁用 quota 与 mclock（qos 用 fifo）
       nas-safe ：/home replicas=3,trash=7d,audit=on（audit 第一次启用：同批创建 CURSOR(audit-log) = 0，3.8）
       hpc-rdma ：/scratch replicas=2，/home replicas=3；node.toml 启用 rdma（[plugin.rdma] listen、[net] data_scheme = "rdma"）
     NodeRecV1(1)（addrs 来自各传输插件 listen() 的返回值）、TargetRecV1、KEYMETA：key 1 = admin，key 2 = 默认 client
     （tenant 0，uid 范围 0..=u32::MAX，不 squash）；GLOBAL 计数器（初值见 3.2.7）、cluster、features、settings（默认值见 3.2.8）、
     plugins = 本节点按 node.toml 装出来的插件清单（3.7.2）
  4. 写 keys/admin.key、keys/client.key 并复制到 ~/.config/wmfs/（0600）；打印挂载与加入命令；声明在当前集群无法满足时
     打印警告（单节点 default profile："/home 声明 replicas=3、/scratch 声明 replicas=2，只有 1 个故障域：新文件实际为 1 副本；
     加入节点后自动补齐"）
  5. 前台运行 server（--no-start 则退出）
wmfs join <任一节点 rpc 地址> --cluster-key <cluster.key 文件> --data-dir D [--addr A] [--rpc-port..] [--meta auto|none] [--target PATH[:labels]]...
  1. 本地先装好插件、启动各传输的 listen() 拿到 addrs；用 key 0（Server）握手；发 NodeOp::Join（带 addrs、targets、features、plugins）
  2. leader：JoinReq.plugins ≠ GLOBAL plugins → 拒绝并列出差异（例如"本节点启用了 quota，集群清单里没有"）；
     否则一个批次：分配 node_id、target_id，写 NodeRecV1/TargetRecV1，map_epoch+1；回 Joined{node_id, target_ids, map}
  3. 新节点写 NODE/TARGET，以空 meta 存储启动（用 Joined 里的集群图找到 leader）；--meta auto 且现有元数据成员 < 3
     → cluster-mgr 提议 AddLearnerNode → 快照追上 → 提升（3.4.1.9）
  4. 它的 target 由链管理器补进链（Syncing）并生成以它为 head 的新链
```

`cluster.key` 需要管理员自己复制到新机器（scp）；**不通过网络传输秘密**（首发无 TLS）。`wmfs dev` 在单机上就是同一个文件。

> 实现注记（M1）：M1 的 `wmfs init` 只做第 1、2、4、5 步里"目录、密钥与 node.toml"的部分（`--data-dir`、`--addr`、`--rpc-port`、`--mgmt-port`、`--no-start`）：写 NODE、cluster.key、node.toml、keys/admin.key、keys/client.key，然后前台运行 server；元数据初始状态（第 3 步）M3 起、TARGET 超级块与 `--target` M5 起、`--profile`/`--owner` 随用到它们的里程碑加入。复制到 `~/.config/wmfs/` 时，那里已有**内容不同**的同名文件就不覆盖（可能属于另一个集群），并提示用 `--key-file`。写文件的顺序是 cluster.key → node.toml → keys/* → **NODE 最后**（都用 `atomic_write`）：NODE 是 init 的提交点，"NODE 存在 = init 已完成"（`wmfs dev` 靠它判断节点是否已初始化）；init 中途失败或被杀时目录里没有 NODE，删掉目录重来即可。
> 实现注记（M3）：init 在写 NODE 之前写 `meta/meta.redb` 的 index = 1 初始状态（第 3 步；初始 KV 由 `core/src/init/initial_kv.rs` 生成，金样 `tests/golden/init_kv.golden`，连同 `raft_state` 的五个值）。规格没写的初值按最小方案：根的 `parent = 0`（"无"，与"id 0 表示无"一致）、`name` 为空；目录的 `nlink = 2 + 子目录数`、`size = 直接子项数`、`change_id = 1`；时间戳都取 init 的时刻；default 类 `weight = 1`、其余 QoS 字段 0、`priority = 1`、`failure_domain = 1`、`locality = 0`；NodeRecV1(1) 的 `addrs` 取 node.toml 的监听地址（`--addr:--rpc-port`）、`features` 为空、`host_id` 没给时取 machine_id（`/etc/machine-id`）、`hostname` 取 `/proc/sys/kernel/hostname`；根目录与 `/home`、`/scratch` 的属主 = 运行 init 的用户（本进程的有效 uid/gid，`sys::euid_egid()`；数据目录可以是别的用户事先建好的空目录，所以不取它的属主）；读不到 `/etc/machine-id` 或 hostname 时写一行警告、该项留空。`meta.redb` 写好之后先 fsync `meta/` 目录（新建文件的目录项落盘），再写 NODE。GLOBAL `plugins` = 本节点按 node.toml 装出来的插件清单（CLI 用注册表算好传给 init）。M3 只写 default profile（`--profile`/`--owner` 随用到它们的里程碑加入），还没有 `--target`（n = 0，`next_target = 1`）。`wmfs dev` 的节点打开 `debug_ops`。`meta.redb` 的权限与 NODE、密钥文件一样是 0600；启动时没有它（M3 之前的 init 建的目录）就拒绝启动并提示 `wmfs dev down --purge`（server 绝不自己新建元数据库，坑 P14）。节点地址之后改变（例如 `wmfs dev up --rdma`）时 NodeRecV1 不会自动更新——集群图（M4）与 join（M8）用到它时再处理。
> 实现注记（M4）：`wmfs init --target PATH[:label,label]...`（可以给多次）写进 node.toml 的 `[[target]]` 与初始 KV 的 TargetRecV1（id 1..=n，`next_target = n + 1`，初值见 3.2.7 的注记）；路径换成绝对路径，标签只能是小写字母、数字与 `-`，路径里不能有 `:`；`--target` 没有容量的写法（不限，`capacity = "0"`）；同一个路径给两次、或一个 target 在另一个里面（按路径的各段比较）→ 拒绝初始化（两个 target 的超级块与 chunk 目录会互相踩到）。target 目录与它的 TARGET 超级块（3.2.3）随 M5 的 chunk_store 一起写（M4 还不存数据）。第 4 步的警告由同一套集群警告算出（`init::initial_warnings`）。`wmfs dev up` 给每个节点一个默认 target（`<节点目录>/targets/1`，容量上限 20 GiB，12 节第 13 条），起好节点后取一次集群图，把集群警告打印出来（ADR-0016）。没有 `--target` 的 `wmfs init` 没有地方放文件数据：新建文件得到 `ENOSPC`，集群警告说"没有可用的 target"。

#### 3.4.4 租约与客户端驱逐【ADR-0023】

| 租约 | 授予方 | 时长（`ClusterSettingsV1`） | 续租 | 持有方自停 | 授予方回收 |
|---|---|---|---|---|---|
| 客户端会话 | 元数据 leader | `session_lease` = 30 s | 每 3 s 会话心跳 | 距最近一次**成功心跳的发送时刻**超过 0.8 × 30 s → 停止一切数据写与元数据修改（排队等待），继续尝试续租 | 距最近一次**收到心跳**超过 30 s → 驱逐第一阶段 |
| target 成员资格 | 元数据 leader | `target_lease` = 8 s | 每 1 s target 心跳 | 0.8 × 8 s 后停止服务读写（I5，返回 `FENCED`） | 8 s 后从链移除 |
| 能力令牌 cap | 元数据 leader（签名） | `cap_ttl` = 10 min | 用到 80% 时 `RenewCap` | — | 数据节点按墙钟过期拒绝（容差 5 min） |

安全性：持有方的计时起点（发送时刻）早于授予方的计时起点（收到时刻），持有方的期限只有 0.8 倍；只要两台机器单调时钟的速率差 < 20%（现实中 < 0.01%），持有方一定先停——**不依赖时钟同步**。心跳回复（续租）必须经过 ReadIndex，被隔离的旧 leader 无法续租。leader 切换后所有计时重置（3.4.1.10）。租约时长存在复制状态里（3.2.8），全集群一致。

**两阶段驱逐**（cluster-mgr，只在 leader）：

1. **第一阶段**（一个批次）：`SessionRecV1.state = Evicting`、`evict_since_ms = now`；写 `EVICTED(session) = {evicted_ms, map_epoch: 新值}`；`map_epoch += 1`；changelog `SessionEvicted`。此后该会话的任何元数据请求 → `SESSION_EVICTED`；数据节点一拿到新集群图就拒绝该会话的 cap（在 io 线程**执行时**校验，不是入队时）。
2. 等待：每个 Up 状态的 target 都已在心跳里报告 `map_epoch_seen ≥ evict_map_epoch`，或已超过 `target_lease` 没有心跳（它已自我隔离，不可能再接受写）。
3. **第二阶段**（一个批次）：释放该会话的全部锁（遍历 `SessionRecV1.locked_inos`，点操作；别的客户端的轮询随后就能拿到）；删除会话记录（EVICTED 记录仍保留 24 h）；去重记录交给 gc 清理。**锁在这一步才释放**：此时任何数据节点都不会再接受该会话的写，而被驱逐客户端在第一阶段之前已被 head 接受的写，版本号必然小于新锁持有者之后的写（同一 chunk 在 head 上串行分配版本），所以不可能在锁转交之后覆盖新持有者的数据。
4. `EVICTED` 记录保留 24 h 后由 gc 删除。

被驱逐的客户端再来任何请求 → `SESSION_EVICTED`：所有已打开句柄此后返回 `EIO`（写回缓冲里未提交的数据丢失，并在下一次 `write/fsync/close` 报 `EIO`——**不静默**），对已知 inode 调 `inval_inode` 清内核缓存，新建会话后新打开的文件正常工作。

#### 3.4.5 fencing 与脑裂防护汇总

| 场景 | 防护 |
|---|---|
| 元数据 leader 被隔离但自认是 leader | 写无法提交（无多数派）；读要 ReadIndex（需多数派心跳）→ 卡住，客户端超时转向新 leader；`check_quorum` 使其下台 |
| 新旧两个 leader 短暂并存 | 只有新 term 能提交；提议前检查 term 与就绪；`expect_version` 让过期批次作废 |
| 旧 head 与元数据隔离、但客户端仍能连上 | 租约到期自停（I5）；即使不自停，后继持有更高 `chain_ver` 会拒绝其转发（I4） |
| 被移除的成员继续服务旧读 | 0.8 × lease 自停先于 1.0 × lease 移除 |
| target 快速重启、丢了 pending 或未 fsync 的提交 | 新 `boot_id`：集群图确认它之前不服务（I5）；有存活 Serving 成员 → 降级重同步；唯一幸存者 → incarnation + 1 |
| 被驱逐客户端继续写 | 客户端 0.8 × lease 自停；数据节点按 `evicted` 列表拒绝；两阶段驱逐保证锁转交之后不会有它的写落地 |
| 客户端用旧 layout（迁移、截断到 0 之后）写 | `CommitWrite` 校验 epoch → `STALE_LAYOUT` → 按新 layout 重写；旧 epoch 的数据只是垃圾，由 GC 删除 |
| 客户端把数据写进 layout 之外的链 | cap 带 `chains` 列表，数据节点校验 |
| 锁持有者失联 | 只有在驱逐第二阶段之后才释放其锁 |
| 元数据节点之间网络分区 | raft 多数派；少数派一侧不可读写（客户端超时后转向多数派） |
| 时钟跳变 | 租约只用单调时钟；cap 用墙钟 + 5 min 容差（要求 NTP，写进文档） |


### 3.5 事务与锁原语

#### 3.5.1 元数据事务模型

> 实现注记（M0）：本节类型自 M0 起以 `core/src/meta/engine/txn.rs`（含 3.4.1.4 的 `TxnOp`、`TxnRequest`；`OpView` 是 `dyn KvTxn` 的别名，M3 的覆盖层实现 `KvTxn`）为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

隔离级别 = **严格可串行化**（真正的串行执行，回复在提交之后发出）；原子性 = 一个闭包的全部写入进同一个 raft 条目；持久性 = raft 多数派 fsync。理念文档要求"授权、配额、快照必须在同一事务内生效"：authz、quota_admission、placement 三个钩子都在闭包**内部**、对着同一个视图调用，判定与修改之间没有 TOCTOU 窗口（二期快照同理）。

```rust
pub trait KvRead { fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ErrCode>; }
pub trait KvScan: KvRead {
    /// [start, end) 升序，最多 limit 条；只在只读视图上提供（执行器闭包拿不到它，3.4.1.4 规则 1）
    fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ErrCode>;
}
pub trait KvTxn: KvRead {
    fn put(&mut self, key: &[u8], value: Vec<u8>);
    fn delete(&mut self, key: &[u8]);
}
pub type ReadFn<'a> = &'a mut dyn FnMut(&dyn KvScan, &OpCtx) -> Result<MetaReply, ErrCode>;

/// meta_engine 钩子（唯一）。首发实现 embedded-kv = raft-rs + redb（3.4.1，代码在 core/src/meta/，因为元数据 Raft 是契约）。
/// 将来的 TiKV / FoundationDB 插件：read 映射为它们的线性一致快照读；txn 映射为带写集的乐观事务（执行器仍是单写者，冲突时重跑闭包）。
pub trait MetaEngine: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    /// 线性一致读：回调被调用时已确认 leader 身份且 applied ≥ read index（阻塞调用者，只在 meta-read 线程、后台任务、管理 API 线程调用）
    fn read(&self, ctx: &ReqCtx, f: ReadFn) -> Result<MetaReply, ErrCode>;
    /// 读写事务：投递后立即返回；req.done 恰好被调用一次（提交并应用之后，或明确失败：NOT_LEADER / RETRY / 业务错误）
    fn txn(&self, req: TxnRequest);
    fn role(&self) -> RoleInfo;                 // 角色、term、leader、就绪
    fn status(&self) -> EngineStatus;           // applied、commit、voters、learners（管理 API）
}
pub fn txn_blocking(eng: &dyn MetaEngine, ctx: ReqCtx, op: TxnOp) -> Result<MetaReply, ErrCode>;  // 内部调用者用：bounded(1) 包一层

/// 闭包能用到的一切。插件拿不到 redb 本身。
pub struct OpCtx<'a> {
    pub req: &'a ReqCtx,                // principal、session、seq、trace_id、now（leader 墙钟，写进写集，follower 不再取时间）
    pub pipeline: &'a Pipeline,         // authz / quota / placement / policy 钩子
    pub attrs: &'a AttrResolver,        // 目录属性解析（带 attr_epoch 缓存，3.8）
    pub map: &'a ClusterMap,            // 批次开始时的集群图快照（placement 用）
    pub settings: &'a ClusterSettingsV1,
}
```

命名空间操作就是普通的顺序代码（示例 `core/src/meta/ns/create.rs`）：

```rust
pub fn create(v: &mut dyn KvTxn, c: &OpCtx, r: &CreateIn) -> Result<MetaReply, ErrCode> {
    check_name(&r.name)?;                                             // 1..=255 字节，无 '/' 与 NUL
    let parent = get_inode(v, r.parent)?.ok_or(ErrCode::ENOENT)?;
    if parent.kind != KIND_DIR { return Err(ErrCode::ENOTDIR); }
    common::authz(v, c, AuthzOp::Create, None, Some(&parent))?;      // 钩子 authz（组装 AuthzInput，调 Pipeline::authz，3.12）
    if get_dentry(v, r.parent, &r.name)?.is_some() { return Err(ErrCode::EEXIST); }
    let eff = c.attrs.resolve(v, r.parent)?;                          // 生效属性（3.8）
    let chain = c.attrs.ancestors(v, r.parent)?;                      // parent → … → 根（点查 + 缓存）
    quota::admit(v, c, &chain, QuotaDelta { files: 1, bytes: 0, class_id: eff.class_id })?;   // 钩子 quota_admission
    let ino = alloc_ino(v)?;
    let layout = common::new_layout(v, c, &eff, ino, 0)?;             // 钩子 placement（必要时 gen_chains，3.4.2.1）；size_hint = 0。
                                                                      // encrypt 生效或 consistency=strong 时 layout.inline_max = 0（不 inline）
    let (mode, acl) = acl::inherit_on_create(v, &parent, &eff, r.mode, r.umask)?;   // 父目录有默认 ACL（或 acl.default 生效）→ 按 ACL 规则、不用 umask；
                                                                      // 否则 mode & !umask（内核协商了 FUSE_DONT_MASK，传来的 mode 未被剥过，8.1）
    let rec = InodeV1::new_file(ino, mode, c.req.who.uid, new_gid(&parent, c.req), c.req.now, r.parent, &r.name, eff.class_id, layout);   // inline_max > 0 时置 F_INLINE
    put_inode(v, ino, &rec);
    put_dentry(v, r.parent, &r.name, ino, KIND_FILE);
    if let Some(a) = acl { put_xattrs(v, ino, &a); }
    touch_dir(v, r.parent, c.req.now, 1)?;                            // mtime、ctime、size+1、change_id+1
    rstat::apply(v, &chain, RDelta { files: 1, class_id: eff.class_id, ..Default::default() })?;
    Ok(MetaReply::Opened(open_out(c, ino, &rec, Rights::RW, quota::grant(v, c, &chain, ino)?)))
}
```

每个修改型操作的 KV 影响（实现清单，全部是点操作）：

| 操作 | 读 | 写 |
|---|---|---|
| create / mkdir / mknod / symlink | 父 inode、dentry、生效属性、祖先 RSTAT 与 DIRATTR（配额） | inode、dentry、父 inode、祖先 RSTAT、GLOBAL `next_ino`、（ACL）XATTR；mkdir 另写新目录的空 RSTAT、父 nlink+1 |
| link | inode、目标 dentry | dentry、inode（nlink+1、ctime、`extra_links` 加一项） |
| unlink | 父、dentry、inode | 删 dentry；`trash` 生效 → 改为 rename 进回收站（第 5 章"回收站细节"）；否则 nlink−1，若删的是主链接且还有其他链接 → 主链接改为 `extra_links[0]` 并搬移计数；nlink 为 0 → 标 `F_ORPHAN` + GC 记录（kind 1），立即从祖先 RSTAT 扣除 |
| rmdir | 父、dentry、inode（`size == 0` 判空） | 删 dentry、inode、RSTAT、DIRATTR、QOS；父 nlink−1、size−1；祖先 RSTAT dirs−1 |
| rename | 见 3.1.2 | 见 3.1.2 |
| setattr（mode/uid/gid/times） | inode | inode（ctime、change_id+1）；有 ACL 时同步 `system.posix_acl_access` 的 mask |
| setattr（size = 0） | inode、生效属性、集群图 | inode（`epoch = max_epoch + 1`、用 `new_layout` 按当前生效属性重建整个 layout、`class_id`、size 0、change_id+1、F_INLINE 重新判定）、INLINE 清空、旧 epoch 的 GC 记录（kind 2；旧 inode 有 F_INLINE 时不写）、祖先 RSTAT（bytes − 旧 size；类变了则 per_class 的文件数搬移）；回复 `Opened(OpenOut)` |
| setattr（size = s > 0） | inode、配额 | inode（size、change_id+1）、祖先 RSTAT（数据面截断已由客户端先完成，3.4.2.9）；F_INLINE 文件把 INLINE 截到 `min(len, s)`；回复 `Opened(OpenOut)` |
| setxattr / removexattr | inode、XATTR | XATTR、inode.ctime；`system.posix_acl_access` 同步改 mode 与 `F_HAS_ACL` |
| CommitWrite | inode（epoch、INLINE 标志）、祖先配额 | inode（size = max(size, new_size)、mtime、ctime、change_id+1）、INLINE 补丁（补丁越过当前 INLINE 长度时中间补 0；补完仍保证 `len ≤ size`）或删除、祖先 RSTAT；`want_quota` 时计算新额度 |
| Fallocate（mode 0） | inode、祖先配额 | inode.size = max(size, offset + len)、祖先 RSTAT；F_INLINE 文件的 INLINE 不变（新增区域按不变式读 0） |
| AppendReserve | inode、祖先配额 | inode.size += len、祖先 RSTAT（只用于 strong，strong 文件不会是 F_INLINE：写打开前已转出，8.5） |
| SetLock | LOCKS、SESSION | LOCKS、SESSION.locked_inos |

实现注记（M4）：命名空间操作在 `core/src/meta/ns/`（common、read_ops、create、remove、rename、setattr、xattr、acl），顺序与上面的示例相同。与示例不同的几处：

- ① inode 用 `ns::common::Node`（号 + 记录 `rec`：`InodeView` 要 ino），`common::authz` 多一个 `victim` 参数（unlink/rename 的粘滞位）；`new_layout` 多一个 `epoch` 参数（create 为 1，截断到 0 为 `max_epoch + 1`）。
- ② 递归计数 RSTAT 的增量在 M7（roadmap M7 的 `meta/rstat`）、配额在 M10、回收站在 M12、目录属性的继承在 M7——这里的操作还不碰它们（rmdir 会点删目录自己的 RSTAT/DIRATTR/QOS；删了 QOS 就在同一批里 `map_epoch += 1`，因为 QoS 表在集群图里，3.3.6）。
- ③ POSIX 的细节照 Linux：新文件带 setgid + 组执行位而调用者不在那个组里 → 去掉 setgid；父目录有 setgid → gid 继承、新目录也带 setgid；字符/块设备节点只有 root 能建（`EPERM`）；chmod 时不在文件所属组里的非 root 设不了 setgid（静默去掉）；chown 之后非目录去掉 setuid（与带组执行位时的 setgid），非属主改长度也去掉；rename 两个名字是同一个文件的两个链接 → 什么也不做。
- ④ **按名字找目录项之前先查"能不能在父目录里查名字"**（执行权限，`AuthzOp::Lookup`，`common::searchable_dir`）：lookup、unlink、rmdir、rename（两个父目录）、不带 O_EXCL 的 create 都这样——与 Linux 的路径遍历相同，没有权限的人从 `ENOENT`/`EEXIST`/`EISDIR` 与 `EACCES` 的区别里看不出目录里有哪些名字，也不能按名字打开里面的文件。mkdir、mknod、symlink、link 对父目录要写 + 执行，本来就先查。
- ⑤ xattr 名字空间的规则（`trusted.*` 读写只给 root、`security.*` 写只给 root、`user.*` 只能挂在文件与目录上且粘滞目录上只有属主能写、ACL 只有属主或 root 能设、默认 ACL 只能挂在目录上——与 Linux 的 xattr_permission / cap_inode_setxattr 相同）在 core 检查，因为 `AuthzInput` 里没有 xattr 的名字；对 inode 的读/写权限仍交给 authz 钩子。
- ⑥ ACL 的格式与 Linux 的 xattr 相同（`ns/acl.rs`，钉在 `constants.golden`）；与权限位等价的访问 ACL 不存、只改权限位；扩展 ACL 的访问判定（posix-acl 插件）在 M10——在那之前，带扩展 ACL 的 inode 上 posix-mode 对非 root 弃权（root 先放行，与 Linux 的 CAP_DAC_OVERRIDE 相同），所以只有 root 能访问。
- ⑦ 测试在 `tests/it/meta_ns*.rs`（内存 KV 视图上逐条检查语义，另有带种子的随机不变式测试）与 `meta_ns_engine.rs`（经 raft：掉电重启、rename 原子可见、会话与去重、会话绑定 key、uid 范围）。

#### 3.5.2 请求去重（会话 + seq）

- 每个客户端会话为**修改型**请求分配单调递增的 `seq`，重试**必须**用同一个 seq；同时在途的修改型请求 ≤ `DEDUP_WINDOW = 256`。`SessionOpen` 用 seq 0（按客户端随机生成的 session id 幂等）。
- 执行器对每个带 seq 的请求先查 `DEDUP(session, seq)`：命中 → 返回原结果（成功或 POSIX 错误）；未命中 → 执行，并把结果与修改**同批**写入（3.4.1.4 `run_one`）。
- 这是"单节点宕机除超时重试外无错误"的关键：leader 换届时在途请求得到 `NOT_LEADER/RETRY`，客户端用同一 seq 重试；若原请求其实已提交，去重表直接返回原结果，不会出现假的 `EEXIST/ENOENT`。

#### 3.5.3 字节范围锁与 flock

- **存储**：每个 inode 一条 `LOCKS` 记录（`Vec<LockRecV1>`，按 start 排序）；会话记录里维护 `locked_inos`。**锁是复制状态**（进 raft），leader 切换不丢锁，不会出现两个客户端同时持有冲突锁。
- **所有者** = `(session, owner)`，owner 取 fuser 传来的 `LockOwner`（内核里 POSIX 锁是 files_struct，flock 是 struct file）。
- **区间**：`[start, end]` 闭区间；"到文件尾"= `LOCK_EOF = i64::MAX as u64`，与内核的 `OFFSET_MAX` 一致（内核在 getlk 回复里看到 `end > OFFSET_MAX` 会直接回 `EIO`）。所有入口（FUSE 适配层、`wmfs fs`、测试代码）把 ≥ `i64::MAX` 的 end 一律归一为 `LOCK_EOF`；区间运算一律 `checked_add/saturating_add`（合并相邻区间时 `end + 1` 不得溢出）；`GetLock` 回复前断言 `start ≤ end ≤ LOCK_EOF`。M12 用一个小 spike 实测 F_GETLK 对 `l_len = 0` 的冲突锁返回正确。
- **冲突**：区间相交 ∧ 至少一方是写锁 ∧ 所有者不同。
- **加锁 `SetLock`（永远非阻塞）**：无冲突 → 删除/切分同一所有者在该区间内的旧锁、插入新锁、合并相邻同类区间（约 120 行纯函数，单测与暴力模型对比）；有冲突 → `EAGAIN`（与其他 POSIX 错误一样写去重记录）。
- **F_SETLKW = 客户端轮询**：服务端**没有**锁等待队列。客户端的 `lock-wait` 线程用 `SetLock` 按指数退避重试（50 ms 起、×2、上限 1 s），每次重试前检查发起请求的进程是否已退出或正在被致命信号杀死（fuser 不转发 FUSE_INTERRUPT；被杀的进程在内核里等我们回复才能退出，所以必须主动回 `EINTR`，8.6），拿到锁或遇到非 `EAGAIN` 错误时回复内核。代价：**不保证先来先得**（放松 ⑭）。
- **解锁/释放**：显式解锁；FUSE `flush(lock_owner)`（释放该 owner 在该文件上的 POSIX 锁）；`release` 带 `lock_owner`（`FUSE_RELEASE_FLOCK_UNLOCK`，释放 flock）；会话关闭；驱逐第二阶段（3.4.4）。
- `GetLock`（F_GETLK）走只读路径，返回第一个冲突锁（类型、区间、pid）。
- 不支持强制锁；不做死锁检测；`flock` 与 `fcntl` 共用锁空间（fuser 0.18 的 `setlk` 不暴露 `FUSE_LK_FLOCK`，已读源码确认）——semantics.md 放松 ②③④⑭。
- **锁即同步点**（3.1.3 规则 5）：客户端拿到锁 → 回复内核之前 `inval_inode(ino, 0, 0)` 并丢属性缓存；解锁前 → 先 flush 该 inode 的写回缓冲并 `CommitWrite`。

#### 3.5.4 租约

首发的租约只有三种，全部在 3.4.4 定义：客户端会话租约（30 s）、target 成员租约（8 s）、能力令牌有效期（10 min，可续）。**没有**读写委托（delegation），所以 strong 通过"不缓存"而不是"租约回调"实现（3.1.3）；带回调的缓存租约是二期（KV 已预留键空间）。

#### 3.5.5 跨节点原子操作

| 操作 | 原子性来源 |
|---|---|
| rename / link / `O_EXCL` create / 加锁 | 执行器里一个闭包 = 一个 raft 条目 |
| `O_APPEND`（strong） | `AppendReserve(ino, epoch, len)`：闭包里原子地返回 `offset = size` 并 `size += len`（配额精确检查）；客户端随后写 `[offset, offset+len)` 并 `CommitWrite` 更新 mtime。写数据前崩溃 → 该区间读 0（放松 ⑥） |
| `CommitWrite` | `size = max(size, new_size)` + **epoch CAS**：`epoch != layout.epoch` → `STALE_LAYOUT` |
| inline → chunk | 客户端先把完整内容写进 chunk，再 `CommitWrite{uninline_expect_change_id}`：change_id 未变才清除 INLINE（CAS），否则 `STALE_LAYOUT`，客户端重取内容重做 |
| 迁移换布局 `SwitchLayout`（协调器内部操作） | CAS：`change_id` 与 `epoch` 都没变、且 leader 内存打开集合里没有写打开才换；同批写旧 epoch 的 GC 记录、搬移 per_class 计数、清 `pending_epoch`、写 changelog |
| 截断到 0（含 `O_TRUNC`） | epoch + 1 并按当前属性重建 layout（元数据单批）；旧 epoch 由 GC 删除 |
| 截断到 s > 0 | 先数据（各 chunk 的 `TruncateChunk` 新版本）、后元数据（`SetAttr`）；崩溃在两步之间 → 放松 ⑦ |
| unlink / 覆盖式 rename | 一个闭包；数据在无人打开后由 GC 删除 |
| 配额额度 | 在 `CommitWrite{want_quota}` 闭包里计算，与 size 更新原子 |
| 会话驱逐 | 两个批次（3.4.4）；每个批次都是原子的 |

#### 3.5.6 删除后仍打开（孤儿 inode）

- 最后一个链接被删除 → inode 标 `F_ORPHAN`、nlink = 0、写 GC 记录 kind 1（`due_ms = now + 6 s`），立即从祖先 RSTAT 扣除（配额立即释放，物理空间稍后回收）。
- 客户端会话心跳上报"打开文件集合"的增量（新打开 / 已关闭，带是否写打开），leader 只在内存里维护所有存活会话的并集。**open 本身不写 raft**。
- gc 任务删除孤儿的条件（全部满足）：① 不在任何存活会话的打开集合里；② 距 unlink 已超过 2 个心跳周期（6 s，确保"刚 open 的客户端"已上报）；③ 本 leader 就绪已超过 2 个心跳周期，且所有存活会话都已在就绪后发送过一次全集（3.4.1.10）。然后在执行器之外对 layout（及 `pending_epoch`）的每条链 `DeleteEpoch`，最后一个批次删除 inode、XATTR、INLINE、LOCKS、GC 记录。
- 对孤儿的 `CommitWrite` 允许（size 照常更新），但不再计入任何目录的 RSTAT。

### 3.6 缓冲区与内存模型【ADR-0022】

#### 3.6.1 预注册内存池

> 实现注记（M0）：`MemPool`、`Lane`、`PoolConfig`（含 `server()/client()/dev_server()/small()` 的取值）自 M0 起以 `core/src/buf/pool.rs` 为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。
> 实现注记（M0）：`SizeClass` 的两张空闲表放进**同一把锁** `free: Mutex<FreeSlots { normal, repl }>`，`cv` 只配这一把锁（不是下面代码块里的两个 `Mutex<Vec<u32>>`）：`Lane::Repl` 要"先取 Repl 槽、再取 Normal 槽"，两把锁时等待者只能拿其中一把，要么丢失另一张表上的唤醒，要么嵌套持锁（1.2 规则 2）；std 的 Condvar 配多把锁还可能 panic。`PoolInner`/`SizeClass` 是私有结构，不属于契约。

```rust
// core/src/buf/pool.rs —— 启动时一次 mmap(MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE) 一整块内存，切成三档；永不归还给操作系统
pub struct MemPool { inner: Arc<PoolInner> }        // Clone 只增引用计数
struct PoolInner {
    base: usize, len: usize,                         // SAFETY：只在本模块内转成指针
    classes: [SizeClass; 3],                         // S = 128 KiB；M = 4 MiB + 128 KiB；L = 16 MiB + 128 KiB
}
struct SizeClass { slot_size: usize, offset: usize, free_normal: Mutex<Vec<u32>>, free_repl: Mutex<Vec<u32>>, cv: Condvar }
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Lane { Normal, Repl }                       // 复制流量专用 lane：每档 25% 的槽位（向上取整，至少 1 个）只给 Repl
pub struct PoolConfig { pub s_mb: u32, pub m_slots: u32, pub l_slots: u32, pub repl_lane: bool }
impl PoolConfig { pub fn server() -> Self; pub fn client() -> Self; pub fn dev_server() -> Self; pub fn small() -> Self; }
```

- 默认大小（`node.toml [mem]`，部署参数）：服务端 `server()` ≈ 1 GiB（S 256 MiB、M 124 槽 ≈ 512 MiB、L 15 槽 ≈ 242 MiB）；客户端 `client()` ≈ 768 MiB（S 64 MiB、M 155 槽、L 4 槽，没有 Repl lane）；`wmfs dev` 下每个服务端进程 `dev_server()` ≈ 384 MiB（S 128 MiB、M 31 槽、L 8 槽）；CLI 的一次性命令（`wmfs fs`、`wmfs ping`、`wmfs debug`）用 `small()` ≈ 70 MiB（S 16 MiB、M 9 槽、L 1 槽），免得每敲一次命令就 `MAP_POPULATE` 几百 MiB。Repl lane 每档 25%、**向上取整、至少 1 个槽**（L 档只有几个槽时也保证 16 MiB chunk 的复制有保底）。本机 memlock 上限约 15 GiB，够 RDMA 注册整个池；`wmfs doctor` 会检查。
- **RDMA 接收槽从 S 档取**（4.1）：每个 QP 固定占 16 个 S 槽（2 MiB）。启动时校验 `[plugin.rdma] max_qps × 16 × 128 KiB ≤ S 档的 50%`，不满足就拒绝启动并写明"需要把 S 档调到至少 X MiB"（默认服务端 `max_qps = 64`，dev 为 32）。
  实现注记（M2）：① 校验写成 `2 × max_qps × 16 × 128 KiB ≤ S 档字节数`（S 档字节数 = `MemPool::stats()` 里 S 项的槽数 × 槽大小），**只在监听的一侧**（`listen`）做：客户端用的是 `small()`/`client()` 池，照搬会让 `wmfs ping rdma://…` 永远失败；connect 侧只要求能为这条连接当场取到槽位，取不到回 `BUSY`。② 不满足时 `listen` 先 `log::error` 写明"max_qps = N 需要 M MiB 接收槽……需要把 S 档调到至少 X MiB（node.toml [mem] s_mb）"，再返回 `EINVAL`（core 的报错里有"详情见日志"）。③ 每条连接另占 1 个 S 槽做发送暂存（帧头 + body + 描述符拷进去再 SEND），所以实际是 17 个；校验仍按 16 个算，暂存槽来自另一半。④ 接收槽与暂存槽一律用 `Lane::Normal` 取（用 Repl 会吃掉复制 lane 的保底）。
- **客户端写回缓冲按槽位计**：一个挂载的写回缓冲最多占 M 档与 L 档各自槽位的 60%，单个 inode 最多 16 个槽位，剩下的留给读路径的 `recv_into`；`chunk ≤ 128 KiB` 的文件用 S 档槽位。
- `get(min_len, lane, wait)` 选能放下 `min_len` 的最小一档：`Lane::Repl` 先取 Repl 槽、再取 Normal 槽；`Lane::Normal` 只能取 Normal 槽。没有空槽就在 Condvar 上等，最多 `wait`，超时回 `BUSY`（天然背压）。
- **谁用 Repl lane**：接收 `ForwardWrite` 的 bulk、`FetchChunk` 的响应缓冲、NeedFull/重新驱动时打包 pending 文件。客户端请求、读请求一律 Normal。于是客户端流量再大也不会让链上后继收不进前驱的转发——这是避免"链上节点互相等缓冲"的第一道保证；第二道是"连接读线程取 Repl 缓冲最多等 100 ms，取不到就把这条转发的 bulk 读进一个固定的 64 KiB 暂存区丢弃并回 `BUSY`"，发送方保留 pending，由 chain-timer 稍后重发。没有任何线程会无限期等缓冲或等网络，所以不存在分布式死锁。
- **注册**：池在传输插件启动之前创建；每个传输插件启动时 `register_region(&RegionInfo { base, len })` 一次：RDMA 对整个池 `reg_mr(base, len, LocalWrite | RemoteRead | RemoteWrite)` 得到一个 MR（`ProtectionDomain::reg_mr` 是 `unsafe fn`，在 rdma 插件里调用），TCP 什么也不做。
- 实现注记（M1）：`MemPool::get` 返回的 `Buf` 的 `len()` = `min_len`、`capacity()` = 槽位大小；槽位里是上一个主人留下的旧字节，调用方只能读自己写过的部分。归还槽位用 `notify_all`（等待者里可能同时有 Normal 与 Repl）。另加 `MemPool::stats() -> Vec<ClassStats>`（每档的名字、槽位大小、槽位数与两张空闲表的长度，给 `/api/v1/node` 与测试用）与指标 `wmfs_bufpool_free{class,lane}`、`wmfs_bufpool_wait_seconds`。整个池（最后一个 `Buf` 也归还之后）被丢弃时才 `munmap`（只在进程退出与测试里发生）。

#### 3.6.2 用 Rust 所有权表达"零拷贝所有权显式转移"

> 实现注记（M0）：本节类型自 M0 起以 `core/src/buf/pool.rs` 为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

```rust
/// 独占的可写缓冲：不实现 Clone。按值传递 = 所有权转移；Drop = 归还池。
pub struct Buf { pool: Arc<PoolInner>, class: u8, slot: u32, lane: Lane, len: u32 }
/// 冻结后的只读缓冲：Clone 只是引用计数 +1；最后一个持有者 Drop 时归还池。
#[derive(Clone)]
pub struct FrozenBuf(Arc<Buf>);

impl MemPool {
    pub fn new(cfg: &PoolConfig) -> Result<MemPool, ErrCode>;
    pub fn get(&self, min_len: usize, lane: Lane, wait: Duration) -> Result<Buf, ErrCode>;
    pub fn region(&self) -> RegionInfo;                        // 给传输插件注册
}
impl Buf {
    pub fn as_slice(&self) -> &[u8];
    pub fn as_mut_slice(&mut self) -> &mut [u8];
    pub fn len(&self) -> usize;
    pub fn set_len(&mut self, n: usize);                       // n <= capacity
    pub fn capacity(&self) -> usize;
    pub fn addr(&self) -> u64;                                 // RDMA 用：池内绝对地址
    pub fn freeze(self) -> FrozenBuf;                          // 显式转移：从此只读、可共享
}
impl FrozenBuf {
    pub fn as_slice(&self) -> &[u8];
    pub fn addr(&self) -> u64;
    pub fn try_unfreeze(self) -> Result<Buf, FrozenBuf>;       // 引用计数为 1 时取回可写所有权
    pub fn slice(&self, off: u32, len: u32) -> BufSlice;       // 子区间视图（引用计数 +1，不拷贝）；越界 = 不变量破坏
    pub fn whole(&self) -> BufSlice;
}
/// 一个 FrozenBuf 的子区间：客户端一个 chunk 的脏数据在一个 Buf 里，只发其中几段 extent 时就用几个 BufSlice
#[derive(Clone)]
pub struct BufSlice { pub buf: FrozenBuf, pub off: u32, pub len: u32 }
impl BufSlice { pub fn as_slice(&self) -> &[u8]; pub fn addr(&self) -> u64; }   // addr = buf.addr() + off
impl Drop for Buf { fn drop(&mut self) { /* 槽位放回对应 lane 的空闲表，notify_one */ } }
```

`Buf` 的字段里不存裸指针（只存池引用与槽位号），所以它自动是 `Send + Sync`，不需要 `unsafe impl`；`as_slice` 内部的 `slice::from_raw_parts` 附 SAFETY 说明：一个 `Buf` 独占其槽位，`FrozenBuf` 只给出 `&[u8]`。

#### 3.6.3 请求生命周期（以 head 上的一次写为例）

```
transport 收帧：bulk 直接落进池内存 → Buf（TCP：read_exact 进 Buf；RDMA：本端发起 RDMA READ 进 Buf）
  → InFrame { bulk: Option<Buf> } 按值交给分发 → 按值进 QoS 队列 → 按值交给 io 线程（出队后校验 cap）
  → 读改写：部分块先把旧块解码进一个 S 档 Buf，再打补丁
  → transform 链：fn encode(&self, ctx, Buf, &mut BlockMeta, &MemPool) -> Result<Buf, ErrCode>
        checksum：算完原样返回同一个 Buf（零拷贝）；encrypt：原地加密返回同一个 Buf
        compress：从池取新 Buf 写入结果，输入 Buf 在函数返回时 Drop 归还
  → 改动块的存储字节组成一个 Buf → freeze() → FrozenBuf
        clone 1 → chunk_store.write_pending（pwrite）
        clone 2 → ConnHandle::send(OutFrame{ bulk: vec![clone 2.whole()] })（ForwardWrite；进发送队列，写线程交给传输：TCP 写完即 Drop；RDMA 下后继来 READ，后继的响应到达后传输层才 Drop）
  → 两个 clone 都 Drop → 内存回池；pending 表里只记版本与回复句柄，不持有缓冲（重新驱动时从 pending 文件重新读取）
```

规则：服务端数据路径上不出现 bulk 数据的 `Vec<u8>` 拷贝（只有帧头和 body 是 `Vec<u8>`）；客户端从 fuser 的 `&[u8]` 拷进池是唯一一次拷贝（FUSE API 决定）；任何持有 `FrozenBuf` 的一方都不得修改它（类型系统保证）；一个请求最多同时持有"输入 + 输出"两组缓冲，内存占用有上界；请求结束（回复、超时、出错、连接断开）时它持有的 `Buf` 随之 Drop 回池，不泄漏、不会"还了还在用"。`Responder`（3.12）被 Drop 而没有回复时自动回 `EIO` 并计数 `wmfs_req_dropped_total`；回复只是 `try_send` 进连接的发送队列，队列满就断开这条连接（对端会重试），所以回复永远不会阻塞执行器或 io 线程。

#### 3.6.4 TCP 与 RDMA 如何共用

> 实现注记（M0）：本节类型自 M0 起以 `core/src/proto/frame.rs`（`OutFrame` / `InFrame`）为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

两个传输插件实现同一个 `Transport/Conn` trait（3.7.3），帧格式完全相同，区别只在 bulk 怎么搬。只有数据族（DataCall、PeerCall）与 Ping 会走 RDMA（3.3.3），它们的 body 都很小：

| | TCP | RDMA（sideway，RC QP） |
|---|---|---|
| 控制帧（帧头 + body） | 写进 socket（body ≤ 1 MiB） | SEND 到对端预先 post 的接收槽：**每 QP 16 个 S 档槽位**（建 QP 时从池里取、连接关闭时归还，3.6.1 的启动校验保证它们最多占 S 档一半）；插件内部做信用控制——在途请求达到 16 个时 `send` 阻塞**写线程**（不影响其他线程），所以永不触发 RNR，`rnr_retry = 7` 兜底；帧头 + body 必须 ≤ 120 KiB，超过 → `send` 返回 `FEATURE_UNSUPPORTED`（不截断，只让这一个请求失败） |
| 请求携带 bulk（客户端写、服务端转发） | bulk 紧跟 body | 请求帧带 `BULK_REMOTE`，帧后附描述符表：`n: u8`（1..=16）+ n 个 `{addr: u64, rkey: u32, len: u32}`（每个对应一个 `BufSlice`）；**接收请求的一方**按顺序发起 RDMA READ，读进自己的**一个**池 Buf；它的响应到达即表示读完，发送方此时才 Drop 那些 `BufSlice` |
| 响应携带 bulk（读 chunk、FetchChunk） | bulk 紧跟响应 body | 请求方在请求里带 `REPLY_REMOTE` 描述符（1 个：自己预先分配的 `recv_into` Buf 的 `{addr, rkey, len}`）；服务端 RDMA WRITE 进去，再 SEND 响应帧（RC 保序：响应到达时数据已可见） |
| 内存注册 | 无 | 启动时对整个池一次 `reg_mr`；**rkey 只随每个请求的描述符给出**——请求方给出自己缓冲的 rkey，响应方只操作请求方提供的缓冲；没有任何"建连时交换 rkey"的步骤 |
| 建连 | TCP 连接 | RDMA 插件**自己的** TCP 引导端口（`[plugin.rdma] listen` 的地址）：只用插件私有消息 0x0600 `RdmaSetup{qpn, psn, gid, mtu}` 交换 QP 信息，QP 状态迁移照 sideway 示例 `rc_pingpong` 但**改三处**（4.1）；然后把 QP 包成 `Conn` 交给 core，**core 在它上面跑 3.3.2 握手**（与 TCP 完全相同）；握手完成（`mark_authenticated`）之前插件不为对端执行任何 RDMA READ/WRITE；引导 TCP 连接保持打开，关闭即代表 RDMA 连接作废 |
| 完成处理 | 连接读线程 | 每设备一个 CQ、一个轮询线程 |

**安全约束**：只有"接收请求的一方"发起 RDMA 操作——客户端→服务端是服务端去读客户端内存，服务端→客户端是服务端去写客户端提供的缓冲。服务端从不向客户端发请求，所以**客户端永远拿不到服务端的 rkey**，不可信客户端无法读写服务端内存；服务端之间的 PeerCall 里，发起方给出自己的 rkey（服务端彼此可信，ADR-0009）。core 只看到：

```rust
pub struct OutFrame { pub hdr: FrameHeader, pub body: Vec<u8>, pub bulk: Vec<BufSlice>, pub recv_into: Option<Buf> }
pub struct InFrame  { pub hdr: FrameHeader, pub body: Vec<u8>, pub bulk: Option<Buf>, pub bulk_dropped: bool } // bulk_dropped：取不到缓冲，bulk 已读出丢弃；请求 → core 回 BUSY，OneWay（RaftMsg）→ 丢弃（raft 会重发）
```

`bulk` 的各段在线上按顺序拼成一段（TCP 依次 `write_all`；RDMA 依次 READ 进一个 Buf），接收方看到的永远是一个 `Buf`。`recv_into` 是请求方为"期望的响应 bulk"预先分配的缓冲：TCP 把响应 bulk `read_exact` 进去，RDMA 让对端 WRITE 进去——两种传输对 core 的语义完全一致。所以加 RDMA 不改数据路径（原则 5），新增传输插件不改 core 一行。

> 实现注记（M1）：① 收帧时给 bulk 找缓冲的规则（回复帧优先用 `recv_into`；否则 `PeerCall` 用 Repl lane、其余用 Normal lane，最多等 100 ms）写成 core 的一个函数 `proto::frame::bulk_buf`，tcp、mem（以及 M2 的 rdma）都调用它，不各抄一遍。② `bulk_dropped` 也可能出现在**回复**上（请求方没给 `recv_into` 或给得不够大、本端池又满了）：`RpcClient` 把这样的回复当成 `BUSY`（可重试）交给调用方，流也一样（之后流结束），缺数据的回复不会被当成成功。③ 示例的 mem 传输可以用 `MemTransport::with_frame_limit(n)` 给帧头 + body 设上限（模拟 RDMA 的 120 KiB），测试"超过传输上限只让这一个请求失败"（3.3.8）。

> 实现注记（M2）：rdma 插件两端私有的细节（core 不看；源码在 `plugins/rdma/`，金样在 `plugins/rdma/setup.golden`）：① **描述符表**接在帧头 + body 之后、小端（与 3.3.1 一致）：有 `BULK_REMOTE` 时先写 `n: u8`（1..=16）再写 n × 16 字节 `{addr: u64, rkey: u32, len: u32}`（一项对应一个**长度不为 0** 的 `BufSlice`：长度为 0 的段在 core 里合法、tcp 遇到它什么也不写，rdma 不为它生成描述符，全是空段时不带 `BULK_REMOTE`；16 段的上限也只数非空段）；再有 `REPLY_REMOTE` 时接着写**恰好一个** 16 字节描述符、不带 n（len = `recv_into` 的容量）；最多 1 + 256 + 16 = 273 字节，加上 ≤ 120 KiB 的帧头 + body 一定放得进 128 KiB 的接收槽。② **RdmaSetup** 用标准 48 字节帧头（msg_type 0x0600，connect 侧发 Request、accept 侧回 Response，req_id = 1）+ postcard body `{qpn, psn, gid, mtu}`，所以 `FrameHeader::decode` 照样校验它；psn 取 `core::base::rand::fill` 的低 24 位。③ 描述符来自对端、不可信：解码时就拒绝 `addr + len` 溢出 u64 的（之后算 READ 的范围、WRITE 的偏移都不会溢出，网络输入触发不了 panic）；接收方发 READ 之前再校验描述符：n 在 1..=16、每段 len > 0、Σlen == 帧头的 bulk_len，不对就断开——否则一个已认证的对端可以声明很小的 bulk_len（只分到一个小槽）却给出很长的描述符，MR 覆盖整个池，READ 会写穿相邻的池槽。④ 回复的 bulk 已被对端 WRITE 进在途表里的 `recv_into`，收的一侧只 `set_len`，**不**再调 `frame::bulk_buf`（它在 recv_into 不够大时会悄悄换一个空缓冲）。⑤ 回复带 bulk 但请求没给 `REPLY_REMOTE`、或 bulk 大于描述符的 len，`send` 返回 `FEATURE_UNSUPPORTED`，由 core 改发一个 ERROR，绝不越界 WRITE；`OutFrame.bulk` 超过 16 段、帧头 + body 超过 120 KiB 同样返回 `FEATURE_UNSUPPORTED`（不截断）。⑥ 池里 100 ms 取不到 bulk 缓冲时不发 READ，交出 `bulk_dropped = true`（core 回 BUSY），请求方收到这个回复时才放下它的 BufSlice。⑦ 信用控制按**帧**计：每发一个 Request 帧减一、每收到一个 Response 帧（含 ERROR）加一（不超过 16）；按 req_id 记账的话两个 `req_id = 0` 的保活会互相覆盖。等信用**不设期限**（与上表"`send` 阻塞写线程"一致）：一直等到有回复还回信用，或连接关闭（`close` 唤醒等待者，`ECONNRESET`）——"16 个请求都还没回"说明对端回得慢，不是 3.3.8 的"对端不读"，不能按写超时断开（tcp 在同样的情况下也不断开）；对端真死了由读线程发现（15 s 收不到帧，或引导 TCP 读到 EOF），它关连接时唤醒这里。等 SEND（连同它前面的 WRITE）完成仍最多等 `[net] write_timeout_ms`。剩下的差别：16 个请求都超过 15 s 未回时，保活 Ping 也拿不到信用，两端 15 s 判死、断开重连（这些调用早已在 10 s 时超时；tcp 下保活照常）；要彻底消除得给保活单独留接收槽与信用，会改 3.6.1 的"每 QP 16 个 S 槽"，首发不做（已报主管）。⑧ 流帧与 OneWay 不走 rdma：发 → `FEATURE_UNSUPPORTED`，收到 → 断开。⑨ **拆除顺序**（Rust 的所有权管不到网卡 DMA）：交给网卡的缓冲（接收槽、发送暂存、在途的 BufSlice 与 recv_into、READ 的目标、WRITE 的来源）只在它的工作请求完成之后、或 QP 销毁之后才回池；`close()` 只置标志、唤醒等待者、把 QP 改到 ERR、关掉引导 TCP；连接对象的第一个字段是 QP，字段按声明顺序 Drop，所以 QP 先销毁、缓冲后回池。core 的调用超时**不**放在途表里的缓冲。⑩ "15 s 收不到帧判死"由插件做（等 RECV 完成最多 15 s）；引导 TCP 每秒看一眼（非阻塞 peek），读到 EOF 或不该有的字节就判这条连接作废。

### 3.7 钩子管线与插件 ABI【ADR-0025】

#### 3.7.1 manifest

> 实现注记（M0）：本节类型自 M0 起以 `core/src/hooks/manifest.rs`（另有 `C_ABI_VERSIONS = [1]`、`Hook::name()` 与 `Hook::ALL`）为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

```rust
// core/src/hooks/manifest.rs
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Hook { Transport, Authn, Authz, QuotaAdmission, Qos, Placement, Codec, Transform,
                ChunkStore, MetaEngine, CacheStore, BgTask, EventSink, Gateway, MgmtExt }
pub struct Manifest {
    pub name: &'static str,
    pub version: &'static str,
    pub hooks: &'static [Hook],
    pub altitude: u16,                   // 唯一型钩子填 0
    pub hot_path: bool,
    pub config_schema: &'static str,     // TOML 文本：每个配置项 { type, default, doc }
    pub abi_version: u32,                // 静态插件 = CORE_ABI_VERSION；动态库 = 其 C ABI 版本
    pub attrs: &'static [&'static str],  // 本插件消费的目录属性键（属性注册表据此校验，3.8）
    pub budget_ns: u64,                  // 热路径单次调用预算；超出计入 wmfs_hook_over_budget_total
    pub fail_points: &'static [&'static str],
}
pub const CORE_ABI_VERSION: u32 = 1;     // 任何钩子 trait 或其参数结构变化（哪怕只加一个字段）都 +1
```

每个插件目录里另有给人看的 `manifest.toml`（理念文档 4.3.1 的 7 个必填字段 + `attrs`、`budget_ns`、`fail_points`），内置插件用 `include_str!` 编进二进制，单元测试断言它与代码里的常量逐字段相等，防止漂移：

```toml
name = "quota"
version = "0.1.0"
hooks = ["quota_admission"]
altitude = 310
hot_path = true
abi_version = 1
attrs = ["quota", "quota.*"]
budget_ns = 5000
fail_points = ["plugin.quota.quota_admission"]
[config_schema]
# quota 没有部署参数：宽限期的缺省值（7d）是属性注册表内置的默认值，只能用 quota=…,grace= 逐目录覆盖（3.8）——
# 行为策略一律不进插件配置。有部署参数的插件在这里列出，例如 tcp：listen = { type = "addr", default = "0.0.0.0:9300", doc = "监听地址" }
```

#### 3.7.2 altitude 分配与组合规则

| 钩子 | 区间 | 首发插件（altitude） | 组合规则 |
|---|---|---|---|
| transport | 每个 scheme 唯一 | tcp、rdma（另有示例 mem） | 可同时启用；按族选 scheme（3.3.3 的 control/data 规则）；**不静默降级**：客户端要求的 data_scheme 对端没有公布、或建连失败，就报错 |
| authn | 100–199 | shared-key（110） | 升序；第一个 Accept 生效；任一 Reject 即拒绝；全部 Abstain → 拒绝 |
| authz | 200–299 | posix-mode（210）、posix-acl（220）、path-caps（250） | 升序串行；任一 Deny 即拒绝；否则有 Allow 即允许；全部 Abstain → `EACCES`（失败即关闭）。posix-mode 在 inode 有扩展 ACL 时 Abstain，posix-acl 在无 ACL 时 Abstain |
| quota_admission | 300–399 | quota（310） | 串行；任一 Deny 即拒绝；SoftWarn 由 core 记录事件与宽限起点；额度取所有 Allow 里的最小 `max_growth` |
| qos | 400–499，**只允许一个活跃** | mclock（410）、fifo（401） | — |
| placement | 500–599 | label-match（510） | 升序依次过滤/排序同一个候选链列表；core 取前 `k = min(stripe, 互不相同的链数)` 条（k = 0 → `ENOSPC`，3.4.2.1） |
| codec | 600–699 | replica（610） | 按 layout 的 codec id 选择（首发只有 1） |
| transform | 700–799，子序固定：checksum 700–729 → compress 730–759 → encrypt 760–789 | crc32c 701、xxh3 702、blake3 703、lz4 731、zstd 732、aes-gcm 761（733–759 留给二期的动态库压缩） | 每个 layout 每类最多一个（由属性选定）；写按 checksum→compress→encrypt，读逆序 |
| chunk_store | 唯一 | local-fs | — |
| meta_engine | 唯一 | embedded-kv | — |
| cache_store | 客户端唯一 | memory、local-nvme（挂载参数二选一，或 none） | — |
| bg_task | 不排序 | reconciler、scrub、tiering、rebalance、gc、trash-expire、ttl | — |
| event_sink / mgmt_ext | 进程外，不排序 | audit-log、webhook / prometheus-exporter、dashboard、alert | — |

启动时 `PluginSet::validate()`（core）检查：altitude 在区间内且同钩子内唯一；transform 落在其类别子区间；唯一型钩子恰好一个；`abi_version` 匹配（不匹配拒绝启动，不尝试适配，理念文档 4.3.6）；热路径插件（含 transport、chunk_store）`budget_ns > 0`；`fail_points` 非空；配置键都在 `config_schema` 里（不认识的键 = 错误）。`node.toml` 只能启停插件：`[plugins] disable = ["path-caps"]` 与 `[plugin.<name>]` 配置表，**不能改顺序**。插件配置只允许部署参数；行为策略必须走目录属性。

> 实现注记（M1）：`PluginSet` 在 `core/src/hooks/registry.rs`；`validate(&cfg.plugin)` 返回给人看的中文错误（`Result<(), String>`），同时检查 `[plugin.<name>]` 只能给已注册的插件、只能写它 `config_schema` 里有的键，以及 `fail_points` 里必须声明 `plugin.<名字>.<钩子>`。M1 的 `PluginSet` 里只有 transport 与 authn 两种钩子（只有它们的 add 方法），`validate` 也只校验这两种用得到的规则：每个插件的 manifest（abi_version、钩子声明、altitude 区间、传输一定是 hot_path、热路径插件给出 budget_ns、fail point 声明、config_schema），同一钩子内不重名、altitude 不相同，不同的插件不同名（同一个插件可以挂在多个钩子上：manifest 内容相同即视为同一个插件，`Manifest` 因此加了 `PartialEq`），传输的 scheme 唯一。其余钩子特有的规则随各自的 add 方法一起加入：唯一型钩子（chunk_store、meta_engine）"恰好一个"（M3、M5），transform 的类别子区间与 qos"只允许一个活跃"（M5、M13）。`[plugins] disable` 里不认识的名字不报错（例如 minimal profile 的 `quota`、`mclock` 在实现之前）。`manifest.toml` 与代码常量的逐字段比对是 `core::hooks::manifest::check_manifest_toml`，各插件的单元测试调用它。
> 实现注记（M3）：meta_engine 的实现（embedded-kv）在 core（元数据 Raft 是契约），注册表登记它的 manifest：`PluginSet::meta_engine(&wmfs_core::meta::engine::engine::MANIFEST)`（`plugins/embedded-kv/` 只放 manifest.toml 与 docs.md）。`validate` 查"最多一个"（只做传输的进程——`wmfs ping`、组件测试——不需要元数据引擎），`server::start` 查"恰好一个且是 embedded-kv"（被 `[plugins] disable` 禁掉就拒绝启动）。
> 实现注记（M4）：`PluginSet` 加了 `authz`、`placement` 两种钩子（各自的 add 方法与访问器）；它们的规则（altitude 区间、同一钩子内不重名、altitude 不相同）已由通用的 `validate` 覆盖。`Pipeline::authz` 与 `Pipeline::place` 按 3.7.2 的组合规则实现，每次调用经 3.7.4 的包装（fail point `plugin.<名字>.authz` / `.placement` 触发 = 以注入的码失败）。默认装上 posix-mode 与 label-match，所以 init 写的集群插件清单是 `authn:shared-key`、`authz:posix-mode`、`placement:label-match`（M3 建的开发集群要 `wmfs dev down --purge` 之后重建，否则插件清单不一致、不就绪）。

**插件集合全集群一致（集群插件清单）**：authz、quota、placement 在 leader 上执行，qos、transform 在数据节点上执行——如果各节点启用的插件不同，leader 一换、或者请求落到另一个数据节点，已设置的授权、配额、QoS 策略就会被静默跳过（违反原则 7）。所以：

1. `PluginSet::plugin_list()` 给出本节点会影响策略的插件清单：authn、authz、quota_admission、qos、placement、codec、transform、bg_task 这 8 个钩子上启用的每个插件（含动态库）记一条 `"钩子:名字"`，排序。**只比名字、不比版本**（版本不同属于滚动升级，由 ABI 版本规则管；版本差异只在 `wmfs node list` 里显示）。transport（各节点可以不同）、chunk_store、meta_engine（唯一型，由 ABI 与格式版本保证）、cache_store（客户端）不在内。
2. `wmfs init` 把本节点的清单写进 GLOBAL `plugins`；之后只能用 `wmfs cluster plugins add|remove <钩子:名字>`（admin）修改，写 changelog，`map_epoch += 1`。
3. **比对**：每个节点在拿到集群图时（启动后的第一张、以及之后每一张）比对本地清单与 `ClusterMap.plugins`；leader 在就绪时比对本地清单与已提交的 GLOBAL `plugins`（3.4.1.4 规则 3）。不一致 → 写一条错误日志列出差异（"缺少 authz:deny-uid""多出 qos:fifo"）并给出修复命令，**进程退出**（退出码 3）——等同拒绝启动；leader 先把已排队的回复发出去，1 s 后再退出。`wmfs join` 时不一致直接拒绝加入（3.4.3）。
4. 所以修改插件集合的流程是："`wmfs cluster plugins add …` → 所有节点因不一致退出 → 用新配置重启全部节点"（开发环境：`wmfs dev restart-all --dylib …`）。只给一个节点改配置然后重启，它会被拒绝。
5. **逃生口**：如果清单里的某个插件再也装不上（例如动态库文件丢了），集群会起不来。此时管理员在一个元数据节点上用 `wmfs server --data-dir D --adopt-local-plugins` 启动：它成为就绪 leader 时不退出，而是提交一个批次把 GLOBAL `plugins` 改成自己的本地清单（写 changelog `PluginListReset`，日志大声警告），其他节点随后按新清单比对。


#### 3.7.3 各钩子的 Rust trait

> 实现注记（M0）：本节类型自 M0 起以 `core/src/hooks/traits.rs`（`ConnHandle` 在 `core/src/net/conn.rs`；`EventSink`、`MgmtExt`、`MGR_ABI_VERSION` 在 `dashboard/src/host.rs`；`ChangeEvent` 在 `api/src/types.rs`，字段与 `ChangeRecV1` 一一对应、另加 `seq`，`op` 用操作名）为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

```rust
// ---- transport（3.6.4）----
pub trait Transport: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn scheme(&self) -> &'static str;                                         // "tcp" | "rdma" | "mem"……core 只把它当字符串比较
    fn register_region(&self, region: &RegionInfo) -> Result<(), ErrCode>;   // 启动时一次
    /// 按插件自己的部署配置（[plugin.<name>] listen = …，构造时传入）开始监听；
    /// 返回要写进 NodeRecV1.addrs 的地址（不带 scheme）；None = 本节点不监听这种传输（例如客户端）
    fn listen(&self, pool: MemPool, on_conn: Arc<dyn Fn(Box<dyn Conn>) + Send + Sync>) -> Result<Option<String>, ErrCode>;
    /// 只建立"能收发帧"的通道；3.3.2 握手由 core 在返回的 Conn 上进行（与传输无关）
    fn connect(&self, addr: &str, pool: MemPool, timeout: Duration) -> Result<Box<dyn Conn>, ErrCode>;
}
pub trait Conn: Send + Sync {
    fn send(&self, f: OutFrame) -> Result<(), ErrCode>;      // 只由 core 为该连接起的写线程调用（阻塞；TCP 写超时 5 s）；bulk / recv_into 的所有权交给传输层
    fn recv(&self) -> Result<InFrame, ErrCode>;              // 只由该连接的读线程调用（阻塞）
    fn mark_authenticated(&self) {}                          // core 完成 3.3.2 握手后调用；单边传输在此之前拒绝执行任何 READ/WRITE
    fn peer(&self) -> String;
    fn close(&self);                                         // 任意线程可调用；之后 send/recv 立即返回错误
}
// core 对 Conn 的包装（core/net）：读线程 + 写线程 + 有界发送队列；其他代码只用它
pub struct ConnHandle { /* id、Arc<dyn Conn>、Sender<OutFrame>（bounded 1024）、认证后的 ConnIdentity */ }
impl ConnHandle { pub fn send(&self, f: OutFrame) -> Result<(), ErrCode>; pub fn close(&self); pub fn identity(&self) -> &ConnIdentity; }
// send = try_send：队列满 → 关闭连接并返回 ECONNRESET（3.3.8）

// ---- authn：连接建立时 ----
pub struct AuthnInput<'a> { pub key_id: u32, pub role: Role /*Hello.role 已由 core 转成 Role；非法值直接 AUTH_FAILED*/, pub cluster_id: [u8; 16],
                            pub client_nonce: [u8; 16], pub server_nonce: [u8; 16], pub proof: &'a [u8; 32], pub peer: &'a str }
pub struct ConnIdentity { pub key_id: u32, pub role: Role, pub tenant: u32, pub uid_ranges: Vec<(u32, u32)>,
                          pub root_squash: bool, pub path_caps: Vec<(u64, bool)> }
pub enum AuthnResult { Accept(ConnIdentity), Reject, Abstain }
pub trait KeyLookup { fn secret(&self, key_id: u32) -> Option<[u8; 32]>; fn meta(&self, key_id: u32) -> Option<KeyMetaV1>; }
pub trait AuthnPlugin: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn authenticate(&self, input: &AuthnInput, keys: &dyn KeyLookup) -> AuthnResult;
    fn server_proof(&self, input: &AuthnInput, keys: &dyn KeyLookup) -> Option<[u8; 32]>;
}

// ---- authz：每个元数据请求（数据请求凭 cap，3.3.5）----
pub enum AuthzOp { Lookup, GetAttr, ReadDir, ReadLink, Open { read: bool, write: bool }, Create, Mkdir, Mknod, Symlink, Link,
                   Unlink, Rmdir, RenameFrom, RenameTo, SetAttr { mode: bool, owner: bool, size: bool, times: bool, times_now: bool },
                   GetXattr, SetXattr, ListXattr, RemoveXattr, Lock, Admin }
pub struct InodeView<'a> { pub ino: u64, pub kind: u8, pub mode: u32, pub uid: u32, pub gid: u32, pub acl_access: Option<&'a [u8]> }
pub struct AuthzInput<'a> {
    pub who: &'a Principal,                    // uid（squash 后）、gid、groups、tenant、job_id、ConnIdentity
    pub op: AuthzOp,
    pub target: Option<&'a InodeView<'a>>,     // 被访问对象
    pub parent: Option<&'a InodeView<'a>>,     // 目录项操作的父目录
    pub victim: Option<&'a InodeView<'a>>,     // unlink/rename 被删对象（粘滞位）
    pub ancestors: &'a [u64],                  // 根 → 父（core 缓存；path-caps 用）
}
pub enum Decision { Allow, Deny(ErrCode), Abstain }
pub trait AuthzPlugin: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn check(&self, input: &AuthzInput) -> Decision;
}

// ---- quota_admission：分配空间或创建对象前（执行器闭包内）----
pub struct QuotaScope { pub dir: u64, pub class: Option<u16>,       // None = 总量配额
                        pub hard_bytes: u64, pub soft_bytes: u64, pub grace_ms: u64, pub hard_files: u64, // 0 = 不限
                        pub quantum: u64, pub used: RStatV1, pub soft_since_ms: u64 }
pub struct QuotaInput<'a> { pub who: &'a Principal, pub scopes: &'a [QuotaScope],
                            pub delta_bytes: i64, pub delta_files: i64, pub class_id: u16, pub now_ms: u64 }
pub enum QuotaDecision { Allow { max_growth: u64 }, SoftWarn { dir: u64, max_growth: u64 }, Deny { dir: u64, code: ErrCode } }
pub trait QuotaPlugin: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn admit(&self, input: &QuotaInput) -> QuotaDecision;   // 纯函数：状态由 core 提供，soft_since 由 core 写回
}

// ---- qos：请求进入执行队列（数据节点）----
#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub struct QueueKey { pub tenant: u32, pub qos_id: u64, pub class_id: u16 }
pub struct QosItem { pub key: QueueKey, pub job_id: u64, pub cost: u32, pub bytes: u32, pub background: bool, pub token: u64 }
pub struct QosSpec { pub r: f64, pub w: f64, pub l: f64, pub bw_max: f64, pub burst_ms: u32,   // 本节点份额；0 = 不设
                     pub low_priority: bool }   // 来自存储类 priority = low（4.5）
pub trait QosPlugin: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn enqueue(&self, item: QosItem, spec: QosSpec);     // 不得阻塞；token → 请求的映射表由 core 维护
    /// 非阻塞。Ok(token) = 立即执行；Err(Some(t)) = 最早 t 时刻再问；Err(None) = 队列全空。睡眠与唤醒由 core 负责（4.5）
    fn dequeue(&self, now: Instant) -> Result<u64, Option<Instant>>;
    fn stats(&self) -> Vec<QueueStat>;                   // 含每队列的保底欠额（"声明 vs 实际"与心跳 qos_deficit 用）
}

// ---- placement：create 时与迁移时 ----
pub struct ChainView { pub id: u32, pub class_id: u16, pub replicas: u8, pub serving: u8,
                       pub members: Vec<(u32, u32)>,            // (target, node)
                       pub used: u64, pub capacity: u64 }
pub struct PlacementInput<'a> { pub eff: &'a EffectiveAttrs, pub class: &'a StorageClassV1, pub replicas: u8,
                                pub stripe: u16, pub client_node: Option<u32>, pub seed: u64, pub exclude: &'a [u32],
                                pub size_hint: u64 }   // 理念文档 4.1 的"大小提示"：create 与截断到 0 时为 0，迁移时为文件大小
pub trait PlacementPlugin: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn rank(&self, input: &PlacementInput, candidates: &mut Vec<ChainView>) -> Result<(), ErrCode>;
}   // core 取排序后前 k = min(stripe, 互不相同的链数) 条；k = 0 → ENOSPC（3.4.2.1）

// ---- codec：写入分片 / 读取重组 ----
pub enum ShardSpec { Whole }                                   // 二期 ec：Data { index: u8, k: u8, m: u8 }
pub trait CodecPlugin: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn codec_id(&self) -> u8;                                  // 1 = replica
    fn shards(&self, chain_len: usize) -> Vec<ShardSpec>;      // 链上每个成员存什么
    fn read_order(&self, serving: &[(u32, u32)], prefer_node: Option<u32>, chunk: &ChunkId) -> Vec<u32>;  // 读副本的尝试顺序
}

// ---- transform：字节 ↔ 字节（每个 64 KiB 块）----
pub enum TransformKind { Checksum, Compress, Encrypt }
pub struct BlockMeta { pub checksum: [u8; 32], pub flags: u8, pub nonce: [u8; 12], pub tag: [u8; 16], pub plain_len: u32 }
pub struct TransformCtx<'a> { pub chunk: &'a ChunkId, pub block_idx: u32, pub level: i8,
                              pub key_id: u32, pub key: Option<&'a [u8; 32]>, pub aad: &'a [u8] }
pub trait TransformPlugin: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn kind(&self) -> TransformKind;
    fn alg_id(&self) -> u8;                                    // 写进 chunk 头
    fn encode(&self, ctx: &TransformCtx, input: Buf, meta: &mut BlockMeta, pool: &MemPool) -> Result<Buf, ErrCode>;
    fn decode(&self, ctx: &TransformCtx, input: Buf, meta: &BlockMeta, pool: &MemPool) -> Result<Buf, ErrCode>;
}

// ---- chunk_store：最内层，只存字节；chunk 格式由 core 定义并编解码（3.2.4），插件不解析头部 ----
pub trait ChunkStore: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn open_target(&self, root: &Path, target_id: u32) -> Result<Arc<dyn TargetStore>, ErrCode>;
}
pub trait TargetStore: Send + Sync {
    fn read_at(&self, id: &ChunkId, off: u64, buf: &mut [u8]) -> Result<Option<usize>, ErrCode>;   // 已提交版本；不存在 → None
    fn open_committed(&self, id: &ChunkId) -> Result<Option<Box<dyn ChunkReader>>, ErrCode>;        // 句柄在之后的 rename 中仍指向原版本
    fn write_pending(&self, id: &ChunkId, writes: &[(u64, &[u8])], copies: &[(u64, u64)]) -> Result<(), ErrCode>; // R1
    fn read_pending_at(&self, id: &ChunkId, off: u64, buf: &mut [u8]) -> Result<usize, ErrCode>;     // 打包 pending 版本
    fn commit_pending(&self, id: &ChunkId, sync: bool) -> Result<(), ErrCode>;                        // rename；新目录时 fsync 父目录
    fn abort_pending(&self, id: &ChunkId) -> Result<(), ErrCode>;
    fn install(&self, id: &ChunkId, writes: &[(u64, &[u8])]) -> Result<(), ErrCode>;                  // R3
    fn sync(&self, ids: &[ChunkId]) -> Result<(), ErrCode>;                                           // R2
    fn delete_epoch(&self, chain: u32, ino: u64, epoch: u32) -> Result<u64, ErrCode>;
    fn delete_chunk(&self, id: &ChunkId) -> Result<(), ErrCode>;                                      // 只给同步与 scrub 用
    fn list_chain(&self, chain: u32, after: Option<&ChunkId>, limit: usize) -> Result<Vec<ChunkId>, ErrCode>;  // 升序
    fn list_epochs(&self, after: Option<(u32, u64, u32)>, limit: usize) -> Result<Vec<(u32, u64, u32, u64)>, ErrCode>; // (chain, ino, epoch, 目录 mtime ms)
    fn delete_chain_dir(&self, chain: u32) -> Result<(), ErrCode>;
    fn clear_temp(&self) -> Result<(), ErrCode>;                                                      // 启动：清空 p/、t/（R5）
    fn statfs(&self) -> Result<StoreStat, ErrCode>;                                                   // 容量、已用（物理盘由 core 的 devinfo 从目录的 st_dev 解析，6.3）
}
pub trait ChunkReader: Send { fn read_at(&mut self, off: u64, buf: &mut [u8]) -> Result<usize, ErrCode>; fn len(&self) -> u64; }

// ---- meta_engine：见 3.5.1 的 MetaEngine ----

// ---- cache_store：客户端 ----
pub struct CacheKey { pub ino: u64, pub change_id: u64, pub block: u64 }   // 文件内 1 MiB 块号
pub trait CacheStore: Send + Sync {
    fn manifest(&self) -> &'static Manifest;
    fn get(&self, key: &CacheKey, into: &mut [u8]) -> Option<usize>;
    fn put(&self, key: &CacheKey, data: &[u8]);
    fn invalidate_ino(&self, ino: u64);
    /// 二期 passthrough 预留：完整缓存了该文件某个 change_id 的明文时，返回本地文件路径
    fn local_file(&self, _ino: u64, _change_id: u64) -> Option<std::path::PathBuf> { None }
}

// ---- bg_task：见 3.10 的 BgTask ----

// ---- event_sink / mgmt_ext：进程外，trait 定义在 wmfs-mgr crate（dashboard/src/host.rs），只能用管理 API 客户端 ----
pub const MGR_ABI_VERSION: u32 = 1;                      // 这两个 trait 或其参数变化就 +1
pub trait EventSink: Send {
    fn manifest_toml(&self) -> &'static str;             // include_str!("manifest.toml")：mgr 启动时校验 7 个必填字段与 abi_version
    fn deliver(&mut self, batch: &[ChangeEvent]) -> Result<(), String>;   // ChangeEvent 来自 wmfs-api（changelog 记录的 JSON 形态）
}
pub trait MgmtExt: Send {
    fn manifest_toml(&self) -> &'static str;
    fn start(&mut self, api: &ApiClient, router: &mut Router) -> Result<(), String>;
    fn tick(&mut self, api: &ApiClient);                 // 每 5 s 一次
}
// mgr 宿主（host.rs）对每次 deliver/tick 先过 fail_point("plugin.<name>.<event_sink|mgmt_ext>")，再计时、计数（指标来自 wmfs-api 注册表）
```

ACL 的二进制格式编解码与"新文件继承默认 ACL"放在 core（`meta/ns/acl.rs`），因为继承是 create 语义的一部分；posix-acl 插件只负责访问判定。

#### 3.7.4 调用包装与时间预算

core 的 `Pipeline` 对每次钩子调用统一包一层：`fail_point(&fp_name)?`（`fp_name = "plugin.<name>.<hook>"` 在注册时拼好存起来，于是"fail points 覆盖每个钩子"是自动的，热路径不分配）→ 计时 → 调用 → 超预算计入 `wmfs_hook_over_budget_total{hook,plugin}` → 错误计数 `wmfs_hook_errors_total`。`Instant::now()` 在 aarch64 上读 `cntvct`，约 20–50 ns。

预算（`Manifest.budget_ns`，全部是明确的数字，**没有豁免**）：authz 5 µs、quota 5 µs、qos 2 µs、placement 50 µs、codec 2 µs、transform（每 64 KiB 块）checksum 20 µs / lz4 150 µs / zstd 600 µs / aes-gcm 100 µs、cache_store memory 20 µs / local-nvme 50 ms、chunk_store local-fs 每次调用 100 ms、transport tcp/rdma 100 ms。transport 计时的是"写线程里一次 `send`"与"收到帧头之后读完 body + bulk"（不含空闲等待帧头的时间）；chunk_store 与 transport 的本职就是 IO（理念文档 4.3.3"不得阻塞"对它们的含义是：除本职 IO 外不阻塞），超预算照常计数，dashboard 上就能看到慢盘与慢网络。

transport 的 fail point `plugin.<name>.transport` 包在 core 写线程对 `Conn::send` 的调用外层：触发 = 这条连接作废（关闭、计为连接错误），于是走"连接断开 → 重连重试"的路径（3.3.8）。

> 实现注记（M1）：包装是 `core/src/hooks/pipeline.rs` 的 `HookCall`（`enter()` 过 fail point 并开始计时，`exit()` 计数、计时、超预算与错误计数），authn 与 transport 已经用上；`Pipeline::new(PluginSet)`、`Pipeline::authn(..)`（返回连接身份与接受它的插件算出的 `server_proof`）、`Pipeline::transports()` 是 M1 加的。transport 的计时分两半：发的一半由 core 的写线程包在 `Conn::send` 外层（带 fail point；fail point 触发时不论配置的是哪个码都让连接作废）；收的一半由传输插件自己计——读到帧头之后开始、读完 body + bulk 时调用 `HookCall::exit`（不过 fail point），插件用 `HookCall::new(&MANIFEST, Hook::Transport)` 拿到与 core 同一组指标句柄（tcp、mem 都这样做，M2 的 rdma 照做，core 不用改）。

#### 3.7.5 静态注册（首发全部内置插件）

注册表在二进制 crate（不在 core），core 只认 trait：

```rust
// cli/src/registry.rs —— 全仓库唯一"知道有哪些插件"的文件
// 实现注记（M1）：实际代码以 cli/src/registry.rs 为准——tcp 的构造多一个参数 TcpTransport::new(&cfg.plugin_cfg("tcp"), cfg.net.write_timeout_ms)
// （写超时来自 [net]），校验写成 s.validate(&cfg.plugin)，错误是给人看的中文字符串；dylib 在 M14 之前直接报"不支持"。
pub fn build_plugins(cfg: &NodeConfig) -> Result<PluginSet, ErrCode> {
    let mut s = PluginSet::new(&cfg.plugins.disable);
    #[cfg(feature = "tcp")]  s.transport(Arc::new(wmfs_plugins::tcp::TcpTransport::new(cfg.plugin_cfg("tcp"))?));
    #[cfg(feature = "rdma")] s.transport(Arc::new(wmfs_plugins::rdma::RdmaTransport::new(cfg.plugin_cfg("rdma"))?));
    #[cfg(feature = "mem")]  s.transport(Arc::new(plugin_mem_transport::MemTransport::new()));   // 独立 crate 示例
    s.authn(Arc::new(wmfs_plugins::shared_key::SharedKey::new()));
    s.authz(Arc::new(wmfs_plugins::posix_mode::PosixMode::new()));
    // …… 其余插件同理，每个一行 ……
    for path in &cfg.plugins.dylib { s.add_dylib(core::hooks::cabi::load(path, cfg)?); }   // 3.7.6；加载失败 = 拒绝启动
    s.validate()?;          // core：altitude、唯一性、abi_version、预算、fail point 声明
    Ok(s)                   // 之后 server 用 s.plugin_list() 与集群插件清单比对（3.7.2）
}
```

#### 3.7.6 动态库稳定 C ABI（首发只覆盖 authz）【ADR-0012，需批准】

理由：先把"冻结的 C ABI + 版本列表 + 不兼容拒绝加载 + panic 不越过 FFI 边界"这套规则真实做出来、测出来——理念文档要求热路径能走稳定 C ABI，做好一个钩子就足以证明规则成立。选最窄的 authz（扁平结构进、判定出，不碰 `Buf` 所有权）。压缩类 transform 的 C ABI 会连带 chunk 头 feature 位、集群级 feature 启用、各节点加载一致性与属性校验规则，是"不要过度工程化"下最该推迟的部分：chunk 头 incompat bit2、`compress_alg` 128..=255、feature 名 `chunk.dyn_transform`、协议 feature bit4 都**只保留、不实现**（二期每个新钩子 = 新的 `_v1` 结构与符号，不影响已有）。头文件 `core/include/wmfs_plugin.h`：

```c
#include <stdint.h>
#include <stddef.h>
#define WMFS_PLUGIN_ABI_V1   1u
#define WMFS_HOOK_AUTHZ      (1u << 2)
typedef struct {
    uint32_t    abi_version;   /* 必须 == WMFS_PLUGIN_ABI_V1，否则 wmfs 拒绝加载 */
    const char *name;          /* NUL 结尾，集群内唯一；进集群插件清单（3.7.2） */
    const char *version;
    uint32_t    hooks;         /* 首发只能是 WMFS_HOOK_AUTHZ */
    uint16_t    altitude;      /* authz：200..=299 */
    uint8_t     hot_path;      /* 必须为 1 */
    uint8_t     reserved0;     /* 填 0 */
    uint64_t    budget_ns;
    const char *config_schema; /* TOML 文本 */
} wmfs_manifest_v1;

const wmfs_manifest_v1 *wmfs_plugin_manifest_v1(void);
void   *wmfs_plugin_init_v1(const char *config_toml);   /* NULL = 初始化失败，wmfs 拒绝启动 */
void    wmfs_plugin_fini_v1(void *state);

typedef struct {
    uint32_t uid, gid, tenant; uint64_t job_id;
    const uint32_t *groups; uint32_t ngroups;
    uint32_t op;                                         /* 编号表见 docs/plugin-abi.md，与 Rust AuthzOp 一一对应 */
    uint64_t ino;        uint32_t kind, mode, owner_uid, owner_gid;      /* 无目标时 ino = 0 */
    uint64_t parent_ino; uint32_t parent_mode, parent_uid, parent_gid;
    const uint64_t *ancestors; uint32_t nancestors;      /* 根 → 父 */
} wmfs_authz_input_v1;
/* 返回 0 = abstain，1 = allow，2 = deny（*err 写 errno）。必须线程安全、不阻塞、不做大块分配。 */
int32_t wmfs_authz_check_v1(void *state, const wmfs_authz_input_v1 *in, int32_t *err);
```

ABI 规则（写进 `docs/plugin-abi.md`）：① v1 结构与符号**冻结**，永不修改；新能力 = 新版本号 + 新结构 + 带 `_v2` 后缀的新符号；② core 明确列出它支持的 C ABI 版本（首发 `[1]`），不在列表里一律拒绝加载并说明原因；③ Rust 写的动态插件通过 C 壳导出：`#[unsafe(no_mangle)] pub extern "C" fn …`（edition 2024 下裸 `#[no_mangle]` 是硬错误，已在本机 rustc 1.99 验证），函数体内 `std::panic::catch_unwind`，panic 视为 deny 并计数；④ `Library` 句柄在进程生命周期内保持加载，不支持热卸载；⑤ 动态库插件与静态插件一样进集群插件清单，所有节点必须加载同名插件（3.7.2）。加载：`unsafe { libloading::Library::new(path) }` → `lib.get::<unsafe extern "C" fn() -> *const wmfs_manifest_v1>(b"wmfs_plugin_manifest_v1\0")`（签名已编译验证）。示例与验收：`examples/dylib-deny-uid/`（C，gcc 编译，拒绝某个 uid 的写操作；附一个 `abi_version = 2` 的坏插件，验证它被拒绝加载、server **拒绝启动**并写明原因——配置了却加载不了的授权插件若被跳过，就等于静默少了一条授权规则，违反原则 7）。

#### 3.7.7 策略求值点（为二期 WASM 预留）

> 实现注记（M0）：本节类型自 M0 起以 `core/src/hooks/policy.rs` 为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

```rust
pub enum PolicyPoint { CreatePlacement, TierCondition, AuditFilter, AttrValidate }
pub struct PolicyInput<'a> { pub point: PolicyPoint, pub attrs: &'a EffectiveAttrs, pub file: Option<&'a FileFacts>,
                             pub event: Option<&'a ChangeRecV1>, pub kv: Option<(&'a str, &'a str)> }
pub enum PolicyOutput { Default, Class(u16), Bool(bool), Reject(String) }
pub trait PolicyEvaluator: Send + Sync { fn eval(&self, input: &PolicyInput) -> PolicyOutput; }
```

首发只有 `BuiltinPolicy`（第 5 章的简单表达式）。四个求值点都**不在每次读写路径上**：CreatePlacement 在 create 时求值、结果写进 layout；TierCondition 在 tiering 任务里；AuditFilter 在写 changelog 时（结果按 `(目录, attr_epoch)` 缓存）；AttrValidate 在 `wmfs set` 时。属性值以 `wasm:` 开头 → 首发返回 `FEATURE_UNSUPPORTED`（"二期功能"），不静默忽略。二期把 `BuiltinPolicy` 换成"WASM 优先、内置兜底"的实现，调用点不变。


### 3.8 目录属性：存储、继承、来源、变更通知、校验【ADR-0026】

- **存储**：每个设置过属性的目录一条 `DIRATTR` 记录（`BTreeMap<String, String>`，值已规范化，例如 `4M` 存为 `4194304`）。属性只能挂在目录上（对文件设置 → `EINVAL`；文件级的差异是协调器的决策，记在 inode 的 layout 与 class_id 上）。
- **注册表**（core，`meta/attr_registry.rs`）：每个键一条

```rust
pub struct AttrSpec {
    pub key: &'static str,                     // "quota"、"replicas"……；"quota.<class>" 用前缀 "quota." 匹配
    pub kind: AttrKind,                        // Inherit（取最近的祖先值）| Stacked（所有祖先同时生效：quota、quota.<class>）
    pub subkeys: &'static [&'static str],      // 允许跟在值后面的子键：quota 的 soft/grace/files/quantum，qos 的 r/w/l/bw/burst，
                                               // consistency 的 attr_ttl，audit 的 ops/uids
    pub takes_rest: bool,                      // acl.default：值里本身有逗号
    pub parse: fn(&str, &[(String, String)]) -> Result<AttrValue, String>,   // 解析 + 规范化
    pub effect: Effect,                        // NewFiles | NewDataAndReconcile | Immediate | NewOpens | Reconciler
    pub consumers: &'static [&'static str],    // 消费它的进程内插件名；"core" 表示本体自己
}
```

  启动时用插件 manifest 的 `attrs` 与注册表对账：**没有任何已启用插件消费的键不可设置**——例如 `minimal` profile 禁用了 quota 插件，`wmfs set /x quota=1G` 直接报错"quota 插件未启用"，而不是静默接受（原则 7）。对账**只统计进程内插件和 core**，进程外消费者（`wmfs mgr` 里的 audit-log 等）不参与——它们不在 PluginSet 里，而且可能根本没运行。所以 `audit` 的 consumers = `["core"]`：core 在写 changelog 时计算 `audited` 标记，这就是 audit 属性在本体里的全部效果；真正把审计记录写进文件的是进程外的 audit-log（4.13）。各节点的插件集合由集群插件清单保证一致（3.7.2），所以"有没有消费者"在任何一个节点上判断都一样。
- **`wmfs set` 的切分规则**（**在服务端做**：CLI 只把原始字符串发给 `POST /api/v1/attr {path, spec}`，服务端用注册表切分、解析、规范化，回复规范化后的键值与警告；所以 `wmfs-mgr` 不需要知道注册表）：按逗号切成片段；片段形如 `k=v` 且 `k` 是顶层属性名 → 开始一个新属性；`k` 是当前属性的子键 → 归到当前属性；片段不含 `=` 且当前属性 `takes_rest` → 并入当前属性的值（保留逗号）；其他情况 → 报错"未知属性 k"。例：`wmfs set /p quota=10G,soft=8G,grace=7d,replicas=2,qos=r=100,w=10,l=1000`；`wmfs set /p consistency=cto,attr_ttl=3s`；`wmfs set /p acl.default=u::rwx,g::r-x,o::---`。值里有 `>`、`&`、空格时整体加单引号（`'tier=vssd->vhdd:idle>30d'`）。
- **校验**（`wmfs set` 时，执行器闭包内，全部是点查）：键已注册且有消费者 → 值可解析 → 交叉检查（存储类存在（`CLASS_NAME` 点查）、`replicas ∈ 1..=8`、`chunk` 为 2 的幂且在 64K..=16M、`inline ≤ 64K`、`stripe ∈ 1..=64`、`attr_ttl ∈ 0..=10s`、`encrypt=tenant:<名字>` 的数据密钥存在（`DATAKEY_NAME` 点查，属性里存规范化后的数字 id）且属于设置者的租户（admin 例外）、`tier` 两端的类存在、`qos` 的 `r ≤ l`、`quota` 的 `soft ≤ hard` 且 `quantum ∈ [1M, 1G]`、`consistency=strong` 与 `compress`/`encrypt` 只有在对应功能已实现的版本里才接受）→ 消费插件的语义校验（`PolicyPoint::AttrValidate`）。失败返回 `EINVAL` 和中文原因。
- **成功但要警告的情况**（回复里的 `warnings`，CLI 原样打印；不静默）：
  1. 声明在当前集群无法满足：`replicas` 大于该类可用的故障域数，或 `stripe` 大于可用的互不相同的链数——"当前只有 1 个故障域，新文件实际为 1 副本；`wmfs status` 会显示差距；加入节点后自动补齐"【ADR-0016，需人类决定】。
  2. `compress`、`encrypt`、`class`、`replicas` 这类"新数据 + 协调器"的键："已有文件在协调器转换完成前，新写入仍按旧设置；一直写打开的文件不会被转换（`wmfs status` 单独计数）"（3.2.4、3.10）。
  3. `audit` 设为非 off，而 `audit-log` 游标 10 分钟没有前进："审计记录要靠 `wmfs mgr` 的 audit-log 写进文件，它现在没有在消费（落后 N 条）"。
- **继承解析** `resolve(dir)`：沿 `parent` 指针从 dir 点查到根，逐级收集；`Inherit` 键取最近的定义；遇到 `inherit=false` 的目录，**它自己的值仍生效，但不再向上取 `Inherit` 键**；`Stacked` 键（配额）**不受 `inherit=false` 影响**——否则子树可以借此逃出上级配额。最后用内置默认值补齐（来源显示为 `(默认值)`）：`class=default`、`replicas=存储类默认`、`chunk=4M`、`stripe=1`、`inline=64K`、`compress=none`、`checksum=crc32c`、`encrypt=none`、`consistency=cto`、`attr_ttl=1s`、`trash=off`、`ttl=off`、`audit=off`、`locality=any`、`qos=存储类的 QoS 包络`；`quota` 没写 `grace=` 时宽限期为 7d、没写 `quantum=` 时为 64M（这些缺省值只在注册表里，不在任何插件配置里）。
- **缓存**：leader 上 `AttrResolver` 缓存 `dir → (attr_epoch, Arc<Resolved>)`。**任何**属性修改和**任何**目录 rename 都令 `attr_epoch += 1`，缓存整体失效——简单且正确（属性修改很少）。祖先链（authz 的 `ancestors`、rstat 的更新路径、配额 scope）用同一套缓存。
- **来源查询**：解析结果的每个值都带来源目录 ino；按路径输出时沿来源目录的 `parent/name` 拼出路径。`wmfs get <path>` 输出生效值 + 来源 + 生效方式 + 一行 `semantics:`（3.1.5）；`wmfs get -r <path>` 另外列出子树内所有显式设置了属性的目录（只读路径扫描 `DIRATTR` 前缀，按祖先关系过滤，结果数有上限并提示截断）。
- **变更通知**：`set/unset` 在同一个批次里：① 写 changelog `AttrSet/AttrUnset{dir, key, old, new, principal}`（理念文档 5.1"所有属性变更写入 changelog"）；② `attr_epoch += 1`；③ `NewDataAndReconcile` 与 `Reconciler` 类键 → 写 `RECONCILE_Q(dir)`；④ `qos` → 更新 `QOS(dir)` 记录并 `map_epoch += 1`（数据节点 ≤ 1 s 拿到）；⑤ `quota` → 若已超软限立即记 `soft_since`；⑥ `audit` 第一次被设为非 off（`CURSOR(audit-log)` 还不存在）→ 同批创建 `CURSOR(audit-log) = 当前最大 changelog seq`，从此 changelog 的保留策略与 `410 Gone` 机制对审计消费者生效：mgr 一直没运行时，记录按游标保留（最多 7 天 / 1000 万条），超出部分被修剪后，audit-log 启动时会得到显式的 `410`（缺口可见，不是静默丢失）；`wmfs status`、`/api/v1/health` 与 dashboard 一直显示"审计消费者落后 N 条"。
- **权限**：首发所有策略属性的写入都需要 admin 角色（经管理 API）；读取对所有已认证的 key 开放。
- **规则复述**（理念文档 5.2 末句）：新增任何可配置行为先问"能不能是目录属性"，能就必须是；`node.toml` 里只允许部署参数；集群运行参数（租约时长等，3.2.8）是协议时序参数，不是数据策略。

### 3.9 递归用量计数与配额【ADR-0026】

- **结构**：`RStatV1 { bytes, files, dirs, per_class: [(class, bytes, files)] }`，每个目录一条，覆盖其整个子树（不含自身）：`bytes` = 子树内**普通文件**逻辑大小之和；`files` = 非目录 inode 数（符号链接、设备节点计 1 个文件、0 字节）。**读取 = 一次 KV get，常量时间**（`wmfs usage`、dashboard、配额检查都直接读它）。
- **更新路径**：在执行器闭包里，对受影响 inode 的**主父目录**到根的每个祖先做读-改-写（批次覆盖层让同一批内对根目录的多次更新只写一次），O(深度)。串行执行器保证没有并发更新丢失。

| 操作 | 增量（作用于主父目录 → 根） |
|---|---|
| 创建文件 / 符号链接 / 设备节点 | files + 1（并计入 per_class） |
| mkdir / rmdir | dirs ± 1（新目录自身的 RSTAT 初始化为 0） |
| `CommitWrite` / `AppendReserve` / `Fallocate` / 截断 / `SetAttr(size)` 使 size 变化 Δ | bytes + Δ，per_class[文件类].bytes + Δ |
| unlink 最后一个链接（含进入孤儿状态） | bytes − size、files − 1（per_class 同步） |
| rename（文件或目录） | 被移动对象的计数（目录 = 它的 RSTAT + 1 个 dir；文件 = 它的 size 与 1 个 file）从旧祖先链减、新祖先链加，到最近公共祖先为止；新祖先链一侧先过 quota_admission |
| link | 不变（硬链接只在主父目录下计一次，与 CephFS rstat 相同） |
| 删除主链接但仍有其他链接 | 主父目录改为 `extra_links[0]`，计数在两条祖先链之间搬移 |
| 协调器换存储类 | per_class[旧] − size、per_class[新] + size（总量不变） |
| 移入/移出回收站 | 等同 rename（回收站在设置 `trash` 的目录之下，所以仍计入该子树） |

- **配额 = quantum 额度模型**（与理念文档 8.2"配额在客户端缓存写回前可能短暂超限，超限幅度有上界并可配置"逐字一致）：
  1. 一个文件的配额 scope = 它所有祖先目录上的 `quota` 与 `quota.<class>`（Stacked）。没有任何 scope → `quota_end = u64::MAX`，整条路径零额外开销。
  2. `Open`（写）与 `Create` 回复 `quota_end = size + grant`，其中 `grant = min(各 scope 的 quantum, quota 插件给出的 max_growth)`；`max_growth` = 各 scope 在当前有效上限下的剩余量的最小值（宽限期内按硬限，宽限过后按软限）。
  3. 客户端只在一次写会让文件越过 `quota_end` 时才去要额度：先把该 inode 的脏数据刷完，发 `CommitWrite{new_size, want_quota: true}`——服务端在同一个闭包里提交长度、更新 RSTAT、重新计算 `quota_end` 返回；若这次写仍越过新的 `quota_end` → `write()` **立即返回 `EDQUOT`**（不是等到 close 才报）。在已有长度之内的覆盖写永远不需要额度。
  4. 服务端护栏（防不守协议的客户端）：`CommitWrite` 使某个 scope 的用量超过 `hard + 16 × quantum`，或单次增长超过 quantum 且结果超过硬限 → 拒绝增长（`EDQUOT`，长度不更新，客户端在下一次 `write/fsync/close` 报告）。
  5. `AppendReserve`、`Fallocate`、`SetAttr` 扩展长度、跨 scope 的 rename、create 的文件数 → 在闭包里**精确**检查。
- **配额精度（可查询，写进 semantics.md）**：守协议的客户端——任一配额目录的用量最多超出硬限 `quantum × 同时向该子树写入的文件数`（quantum 默认 64 MiB，可用 `quota=…,quantum=1M` 逐目录调小）；单个写者精确不超；任何客户端（含不守协议的）最多超出 `16 × quantum`；文件数配额精确；软限 + 宽限期在操作时刻精确判定；计量对象是逻辑大小（稀疏文件按表观大小）；孤儿不计；回收站里的文件仍计入；不计量"写进 chunk 但从未提交长度"的数据（只占物理空间，`wmfs status` 同时显示逻辑用量与各 target 物理用量，差异可见）。leader 切换不破坏上界（额度不是持久状态，每次都按已提交用量重新计算）。
- **自检**：协调器每次全量遍历时顺带累加子树计数，与 `RSTAT` 比较；不一致 → 发 `RStatDrift` 事件并用一个批次修正（纵深防御，正常情况下永远为 0）。离线工具 `wmfs debug rstat-check <data_dir>`。

### 3.10 后台任务框架（协调器是第一个实例）【ADR-0027】

> 实现注记（M0）：本节类型自 M0 起以 `core/src/bg/task.rs`（`Walker`、`Migrator`、`RateLimiter` 暂为空壳，实现时移到各自文件）为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。

```rust
pub enum RunOn { MetaLeader, EveryDataNode }
pub struct StepResult { pub cursor: Vec<u8>, pub items: u64, pub bytes: u64, pub backlog: u64, pub pass_done: bool }
pub trait BgTask: Send {
    fn manifest(&self) -> &'static Manifest;
    fn kind(&self) -> &'static str;
    fn runs_on(&self) -> RunOn;
    fn interval(&self) -> Duration;           // 一轮结束后隔多久开始下一轮
    /// 做一小步（≤ 100 个对象 / ≤ 64 MiB / ≤ 1 s）。必须幂等：从同一游标重放，效果相同。
    fn step(&mut self, ctx: &mut TaskCtx, cursor: &[u8]) -> Result<StepResult, ErrCode>;
}
pub struct TaskCtx<'a> {
    pub meta: &'a dyn MetaEngine,             // 内部身份（admin）：只读 read() 与内部 txn
    pub data: &'a DataClient,                 // 数据面客户端：管理 cap，请求标记 background（走 QoS 低优先级队列）
    pub walker: &'a Walker,                   // 可续跑的命名空间遍历
    pub migrate: &'a Migrator,                // 迁移原语
    pub limiter: &'a RateLimiter,             // 任务级 + 节点级令牌桶
    pub targets: &'a [Arc<dyn TargetStore>],  // 只有 EveryDataNode 任务有
    pub policy: &'a dyn PolicyEvaluator,
    pub now_ms: u64,
}
```

- **调度**：每个节点一个 `bg-sched` 线程，轮转本节点该跑的任务：`MetaLeader` 任务只在元数据 leader 就绪时运行，失去 leader 立即停（新 leader 从 `TaskRecV1` 游标续跑——断点续跑）；`EveryDataNode` 任务（scrub、本地孤儿扫描）在每个数据节点运行，游标由 core 框架以 `atomic_write` 存在 `<data_dir>/run/task-<kind>.cursor`（本地数据的本地进度，插件自己不持有状态）。每步：取令牌（任务级 `bytes/items per sec` + 节点级 `ClusterSettingsV1.bg_bytes_per_sec`）→ `step` → 每步后或至少每 5 s 持久化游标与进度。
- **为什么要求幂等**：`step` 的副作用（例如迁移了一个文件）与游标持久化不在同一个事务里；崩溃后从旧游标重放，幂等保证不出错（迁移原语本身用 CAS，重复执行只会"已经是目标状态 → 跳过"）。
- **控制**：`wmfs task list|show|pause|resume|run <kind>`；限速 `wmfs task set <kind> bw=100M,items=1000`（运维参数，存在 `TaskRecV1`，不是数据策略）。
- **可观测**：`wmfs_task_items_total{task}`、`wmfs_task_bytes_total{task}`、`wmfs_task_backlog{task}`（理念文档 5.3："进度与积压量是一等指标"）、`wmfs_task_progress_ratio{task}`、`wmfs_task_step_seconds{task}`、`wmfs_task_errors_total{task}`；dashboard 显示进度条。
- **遍历器 `Walker`**：两种模式。① 子树 DFS：游标 = `Vec<(dir_ino, 最后处理的名字)>` 的 postcard 编码，每次读 256 个 dentry，输出 `(ino, InodeV1, parent, 生效属性)`（属性按目录缓存）；② 全量扁平扫描：按 ino 顺序扫 `INODE` 前缀，游标 = 上次的 ino（最简单、天然可续跑）。
- **迁移原语 `Migrator::migrate(ino, declared)`**（协调器、tiering、rebalance 共用）：
  1. 读 inode（layout L、change_id c、size s）；leader 内存打开集合里有写打开 → `Skip`（稍后重试）。Migrator 在内存里记下每个"因写打开而跳过"的文件第一次被跳过的时刻（leader 切换后重新计时）：`wmfs status` 单独列出这类文件的个数，某个文件被跳过超过 24 h → 发一次 `PolicyViolation{kind: "migration_blocked_by_open"}` 事件。
  2. 内部批次：`max_epoch += 1`，`pending_epoch = max_epoch`（CAS：change_id 仍是 c）——孤儿扫描把 `pending_epoch` 视为存活。
  3. placement 按声明生成 L′（新 epoch、新链、新变换参数；`size_hint = s`）；inline 文件：声明仍允许 inline → 只换元数据，直接到第 5 步；声明不允许 inline（`encrypt` 生效或 `consistency=strong`）→ 第 4 步把 INLINE 内容按 L′ 写进 chunk，第 5 步同时清除 F_INLINE 与 INLINE。
  4. 逐 chunk：按 L 读（数据面）→ 按 L′ 写（新 head 按新变换编码）→ 对 L′ 的全部 chunk `SyncChunks`（**换布局前必须持久**）。
  5. `SwitchLayout` CAS（c 与 L.epoch 都没变、仍没有写打开）：layout = L′、class_id、清 `pending_epoch`、旧 epoch 写 GC 记录、搬移 per_class、写 changelog。
  6. CAS 失败（期间有写入）→ 新 epoch 记为垃圾（GC 记录）、清 `pending_epoch`，稍后重试。正在写的客户端随后的 `CommitWrite` 会得到 `STALE_LAYOUT` 并把缓冲重写到新布局——数据不会丢。进程重启或 leader 切换后发现 `pending_epoch ≠ 0` 而本进程没有在迁移它 → 按第 6 步放弃，下一轮重做。
- **协调器**（第一个实例，`plugins/reconciler/`）：输入 = `RECONCILE_Q`（属性变化、目录 rename 用子树 DFS 处理；存储类的 `replica` 或 `lifecycle` 被 `wmfs class update` 改了时写一条 `{dir: 1, reason: 3}`，从根做一次整树协调）+ 每 24 h 一次全量扁平扫描；`encrypt` 或 `consistency=strong` 生效的目录里的 inline 文件也算"不一致"，迁移时转成 chunk；对每个文件计算 `declared(file) = resolve(主父目录) 再套用 tier 规则`（与 `wmfs status`、tiering 用同一个函数，所以分层后不会被迁回去），与实际（`class_id`、`layout.replicas`、`layout.compress/encrypt/encrypt_key`）比较，不同即 `migrate`；`checksum` 不迁移；链的副本补齐**不归协调器管**，由链管理器做（3.4.2.10）。处理完的 `RECONCILE_Q` 项在同一个内部批次里删除。
- **"声明 vs 实际"（`wmfs status <path>`）**：在 leader 上对子树做一次只读扫描（受后台令牌桶限速，上限 100 万个文件，超过则 `truncated = true` 并改为显示协调器上一轮全量扫描的统计）。理念文档第 9 章要求**所有**策略旋钮都能看到声明与实际，所以每个属性的"实际"都有明确的来源（`core/bg/status.rs`；协调器、tiering 与它共用 `declared(file)`）：

| 属性 | "实际"从哪来 | 不一致的含义 |
|---|---|---|
| `class` | 每个文件的 `inode.class_id` | 协调器还没迁移（或因写打开被跳过） |
| `replicas` | `layout.replicas` 与该文件各条链当前 Serving 成员数的较小值 | 等协调器迁移，或链降级等链管理器补齐 |
| `compress`、`encrypt` | `layout.compress/compress_level`、`layout.encrypt_key` | 等协调器迁移（写打开的文件单独计数） |
| `chunk`、`stripe`、`inline`、`checksum` | 逐文件比较 `layout.chunk_size`、`layout.chains.len()`、`layout.inline_max`、`layout.checksum` | **标注"不迁移"**：只对新文件与截断到 0 的文件生效；stripe 不足（可用链不够）单独计数 |
| `quota`、`quota.<class>` | RSTAT 的用量（总量 / per_class）、软硬限、`QUOTA_STATE.soft_since`、剩余宽限 | 超软限、宽限已过、超硬限（护栏范围内） |
| `qos` | 下发到每个节点的份额（按节点均分后的 `QosSpec`）与各节点心跳上报的 `qos_deficit` | 保底欠额 > 0 |
| `consistency`、`acl.default`、`locality`、`inherit` | 实际 = 声明（它们在每次 open/create 时由 core 直接按生效属性执行，不存在存量漂移） | — |
| `trash` | 该回收站里的文件数与字节数（来源目录下 `.wmfs-trash` 的 RSTAT） | — |
| `tier`、`ttl` | 条件已满足、但还没被 tiering / ttl 任务处理的文件数 | 任务积压 |
| `audit` | `audit-log` 游标落后最新 changelog 的条数（`CURSOR(audit-log)`） | 审计消费者没在运行或跟不上 |

  `GET /api/v1/status` 对每个键返回 `{key, declared, source, effect, actual, mismatched}`（`actual` 是上表对应字段组成的 JSON 对象，结构定义在 `wmfs-api`），外加 `warnings`（集群警告，3.1.5）、协调器进度与积压、位于降级链上的文件数与字节数、qos 的**隔离承诺**（6.3）。CLI 与 dashboard 用同一个 API；M14 的验收逐个键检查。

### 3.11 可观测性基座【ADR-0027】

> 实现注记（M0）：指标注册表与 fail point 自 M0 起以 `api/src/metrics.rs`、`api/src/failpoint.rs` 为唯一真源。另加了一个函数 `fail_point_value(name) -> Option<i64>`，给 `clock.mono.skew_ms`、`clock.mono.rate_ppm`、`clock.wall.skew_ms` 注入偏移与速率用；为此 **`return()` 的参数除错误码名外，也接受十进制整数**（可以为负、可以超过 u16）：`fail_point_value` 原样返回它；`fail_point` 把它当错误码用，不在 1..=65535 里的（含 0——0 在去重表与 changelog 里表示成功）一律按 `INTERNAL`。环境变量 `WMFS_FAILPOINTS` 写错时打印原因并以退出码 2 结束整个进程（不用 panic：panic 只会杀死第一个用到 fail point 的线程）。

- **trace id 贯穿**：客户端为每个 FUSE 操作生成 `trace_id`（每线程 xorshift，种子来自 `rand::fill`），放进帧头；元数据把它写进 changelog 记录与 `WriteBatch.trace_ids`；数据节点转发时原样携带、`span_id + 1`。每个进程保留最近 65 536 个 span `{trace_id, span_id, node, component, op, start_ms, dur_us, code}` 的环形缓冲；超过阈值（元数据 50 ms、数据 100 ms，可调）的请求写一行慢日志（含各阶段耗时：decode、authz、quota、执行、raft 提交、转发、磁盘）。`GET /api/v1/trace/<id>` 返回本节点的 span；`wmfs trace <id>` 并发查询所有节点并按 span 排出时间线（从第一个服务端开始：元数据 leader 或链头）。FUSE 客户端进程没有 HTTP 端点，它的 span 只进客户端自己的慢请求日志（`--log` 文件），日志行里带 trace_id，可以和服务端时间线对上。
- **计数器注册表**（`wmfs-api` crate 的 `metrics.rs`，core 与 `wmfs-mgr` 共用，所以进程外插件也能注册指标）：

```rust
pub fn counter(name: &'static str, help: &'static str, labels: &[(&'static str, &str)]) -> Arc<Counter>;
pub fn gauge(name: &'static str, help: &'static str, labels: &[(&'static str, &str)]) -> Arc<Gauge>;
pub fn histogram(name: &'static str, help: &'static str, labels: &[(&'static str, &str)], buckets: &'static [f64]) -> Arc<Histogram>;
pub fn render_prometheus(out: &mut String);       // GET /metrics，Prometheus 文本格式（自己写，约 150 行）
```

  注册时加锁（启动阶段或首次使用），取值是 `AtomicU64`（热路径无锁）。命名 `wmfs_<子系统>_<量>_<单位>`；标签基数受控：路径标签只用于设置了配额/qos 的策略根目录。每个插件至少注册一个自己的指标（理念文档 4.3.4）。主要指标见 7.3。（实现注记（M1）：不带标签的连接层计数器——`wmfs_conn_write_timeout_total`、`wmfs_conn_send_queue_full_total`、`wmfs_conn_proto_errors_total`、`wmfs_req_dropped_total`——在 `RpcServer::new` / `RpcClient::new` 时就注册，新进程的 `/metrics` 里也能看到 0，分得清"没发生过"与"没有这个指标"；带标签的（`wmfs_rpc_denied_total{role,family}`、`wmfs_conn_handshake_failed_total{code}` 等）照常在第一次发生时出现。热路径上的句柄（每个消息族的请求计数与耗时、传输退回计数）注册一次后存在结构体里，不再每次查注册表。）
- **策略违约可见**：配额软超限 / 硬拒绝、链降级、实际副本 < 声明、QoS 保底未满足、类的 p99 延迟超过 `latency_target`、校验失败、属性无法满足、隔离降级等，既有指标也写 changelog 事件（`op = PolicyViolation`，`extra` 写明种类），dashboard 与 alert 消费它们。
- **changelog 流**：`GET /api/v1/changelog?after=<seq>&limit=1000&wait_ms=30000`（长轮询：raft 线程应用新 changelog 记录时唤醒等待者；每个节点同时最多 2 个长轮询，第 3 个起立即返回空页，1.2）；记录的 JSON 字段定义在 `wmfs-api`（`ChangeEvent`）；消费者 `POST /api/v1/changelog/cursor {name, seq}` 提交游标（存 `CURSOR`，插件不持有本体不知道的状态）；`wmfs mgr` 内部只有**一个**长轮询循环，从各插件游标的最小值开始拉，按插件分发，每个 sink 各自提交自己的游标；保留策略 = 所有游标的最小值之后的全部记录，但最多 7 天 / 1000 万条，由 gc 任务修剪（扫描后点删除）；消费者落后超出保留范围时返回 `410 Gone` 和最早可用 seq——**缺口是显式的，不是静默的**。
- **fail points**（`wmfs-api` crate 的 `failpoint.rs`，函数而非宏；core 与 `wmfs-mgr` 共用）：

```rust
pub fn fail_point(name: &str) -> Result<(), u16>;   // Err(码) = 配置了 return(<码名>)；panic/abort/sleep 在函数内部执行；
                                                    // 未启用任何 fail point 时只有一次 Relaxed 原子读；名字可以是运行时拼出来的
pub fn fail_point_hit(name: &str) -> bool;          // 只问"这次是否触发"：drop、corrupt 这类动作由调用方自己做
// 错误码的名字 ↔ 数值表（3.3.7）也放在 wmfs-api 的 codes.rs，core 的 ErrCode(u16) 直接用它，所以 return(EIO)、return(BUSY) 两边都认识
// 带后缀的名字（net.drop.to.2、plugin.quota.quota_admission）按完整字符串匹配；热路径上的名字在注册时拼好存起来，不在每次调用时 format!
// 动作：off | return(<错误码名>) | panic | abort | sleep(<ms>) | corrupt（只对 chunk.read.corrupt：翻转一个比特）
//      | pct(<p>,<动作>) | times(<n>,<动作>)
// 配置：环境变量 WMFS_FAILPOINTS="chunk.pending.torn=times(1,abort);net.drop.to.2=return(BUSY)"
//      或 wmfs failpoint set <node> <name> <action>（经 POST /api/v1/failpoints，M1 起就有；仅当 node.toml 里 failpoints_api = true，wmfs dev 默认打开）
```

  首批：`raft.persist.before_commit`、`raft.persist.after_commit`、`raft.apply.before`、`meta.exec.before_propose`、`meta.reply.drop`（吞掉一次元数据回复，演示重试 + 去重）、`snapshot.fetch.midway`、`chunk.pending.eio`、`chunk.pending.torn`（只写一半 pending 文件然后 abort）、`chunk.commit.before_rename`、`chunk.read.eio`、`chunk.read.corrupt`、`chunk.sync.eio`、`chain.forward.drop`、`chain.forward.delay`、`chain.head.crash_after_pending`、`chain.ack.drop`、`sync.fetch.midway`、`net.drop.to.<node>` / `net.drop.from.<node>`（不用 iptables 就能模拟网络分区）、`net.delay`、`clock.mono.skew_ms`、`clock.mono.rate_ppm`、`clock.wall.skew_ms`、`client.flush.before_commit`、`client.heartbeat.stop`、`bg.step.crash_after_effect`，以及每个插件自动拥有的 `plugin.<name>.<hook>`。
  实现注记（M3）：已埋好并各有测试：`raft.persist.before_commit`、`raft.persist.after_commit`、`raft.apply.before`（abort 用子进程测，`tests/it/failpoint_raft_abort.rs`；panic 用来验证坑 P2 的先后，`failpoint_raft_order.rs`）、`meta.exec.before_propose`、`plugin.embedded-kv.meta_engine`（`failpoint_meta.rs`）。`meta.reply.drop` 随 M4 的去重演示加入。
  实现注记（M4）：`meta.reply.drop` 在 `core/src/meta/service.rs`：带 seq 的修改型请求执行完、批次提交之后吞掉这一次回复（`Responder::drop_reply`：什么也不回，Drop 时也不自动回 EIO）；只读请求与会话的打开/关闭不受它影响（它们的重试本来就幂等，演示的是去重）。测试 `tests/it/failpoint_reply_drop.rs`（客户端等一个 RPC 超时后用同一个 seq 重试、去重命中、结果成功；fail point 一直开着时只读请求与会话的关闭、打开照常；一个请求卡在超时重试里、同一会话里别的线程做完 300 个修改，它的重试仍命中去重表——在途窗口按 seq 计，3.3.4 注记 ⑪）。另有每个新插件的 `plugin.posix-mode.authz`、`plugin.label-match.placement`（`failpoint_ns_plugins.rs`）。新指标：`wmfs_meta_dedup_hits_total`、`wmfs_posix_mode_decisions_total{result}`、`wmfs_label_match_ranked_total`。
- **时钟可注入**：`clock::mono_ms()` / `clock::wall_ms()` 是所有租约、超时与时间戳的唯一来源，fail point 可注入偏移与速率，用于租约安全性测试。（实现注记（M0）：运行中 set/clear 也安全——`rate_ppm` 只作用于设置之后的时间（以设置时刻的读数为基点），清除后多走或少走的部分保留；`skew_ms` 变大 = 立刻前跳，变小（含清除）= 停走直到真实时间追上；`mono_ms()` **永不倒退**，第一次读数是 1000 而不是 0。见 `core/src/base/clock.rs`。）数据节点用心跳回复里的 leader 墙钟估算偏差，导出 `wmfs_clock_skew_seconds`，超过 1 s 告警（cap 过期判断容忍 5 min）。


### 3.12 跨模块接口（M0 落成可编译的签名，各模块的 agent 照此对接）

> 实现注记（M0）：本节类型自 M0 起以下面注释里写的各文件（`core/src/base/types.rs`、`core/src/net/{client,server,conn}.rs`、`core/src/client/{meta_client,data_client}.rs`、`core/src/hooks/pipeline.rs`、`core/src/meta/attrs.rs`；小类型：`RegionInfo` → `core/src/buf/pool.rs`，`KeyFile` → `api/src/types.rs`，`NetConfig` → `core/src/base/config.rs`，`RoleInfo`/`EngineStatus` → `core/src/meta/engine/txn.rs`，`QueueStat`/`StoreStat` → `core/src/hooks/traits.rs`，`FileFacts` → `core/src/hooks/policy.rs`，`TierRule`/`AuditSpec`/`QuotaDecl` → `core/src/meta/attrs.rs`；`ApiClient`/`ApiError` → `dashboard/src/api.rs`，`HttpIn`/`HttpOut`/`Router` → `dashboard/src/host.rs`）为唯一真源；下面的代码块仅供阅读，若不一致以源码为准。
> 实现注记（M1）：在骨架之外新增（只加不改）：`RpcClient::new_with_role(pool, transports, key, role, net)` 与 `client::default_role(key_id)`（见 3.3.2 注记）、`client::check_reply(InFrame)`（ERROR 帧 → 错误码；`call` 本身把服务端的 ERROR 帧原样返回，需要 `leader_hint`/`extra` 的调用方用 `InFrame::error()`）、`Responder::into_stream() -> StreamTx`（`StreamTx::send(body, bulk, last)`，SnapshotFetch 用）、`RpcServer`/`ServerId`/`Service`（服务端装配与按 msg_type 注册的服务）、`ErrorBody::new`、`frame::msg_name`。`call_cb` 在还没有连接时会在调用线程上建连（M9 的 repl 线程用它之前改成后台建连）。core 把 wmfs-api 原样转出为 `wmfs_core::api`，独立 crate 的插件只依赖 wmfs-core。另：① `call`/`start`/`call_cb`/`stream` 在本地的任何失败（选路失败、建连或握手失败、入队失败、请求帧超过传输上限）都先记进在途表，由 `rpc-timer` 在 100 ms 内以该错误码结束——于是 `call_cb` 的回调恰好一次、而且只在连接读线程或 `rpc-timer` 线程上执行，从不在调用方线程上（M9 的 repl 线程可以放心用）；`RpcClient` 被丢弃时还没结束的调用以 `ECONNRESET` 结束。② `StreamTx::send(&mut self, body, bulk, last)`、`StreamTx::error(code)`（以错误码结束流）；`StreamTx` 没发最后一帧就被丢弃时与 `Responder` 一样自动回 `EIO` 并计数 `wmfs_req_dropped_total`（客户端的流立即结束，不用等 10 min 的流超时）。请求在流结束时才计入 `wmfs_rpc_requests_total`。③ `Role::name()/from_name()`（"admin"/"client"/"server"，指标标签与 `wmfs ping --role` 用）、`init::SERVER_KEY = 0`、`MemPool::stats() -> Vec<ClassStats>`、`base::config::PluginTable`、`wmfs_api::hex::{hex, unhex}`（全仓库唯一的 hex 编解码）、`wmfs_api::auth::AuthHeader`（`parse_auth_header` 的结果）。
> 实现注记（M3）：只增不改（ADR-0030 第 9 条）：`meta::raft::driver::{Driver, DriverOpts, RaftInput, ProposeOutcome, Published, config}`（Driver 是"输入一步、处理一次 Ready"的结构体：`handle_input`/`tick`/`process_ready`/`take_outbox`/`run`，`applied()` 返回 `Applied`）、`meta::raft::{or_abort, redb_eio}`、`meta::raft::storage::{RedbStorage, bootstrap, read_applied, read_global, put_state, KV, RAFT_LOG, RAFT_STATE}`、`meta::engine::engine::{EmbeddedEngine, EngineOpts, OpEnv, KvKeys, MANIFEST}`（`EmbeddedEngine::{open, start, local_get, node_addrs, keys, shutdown}`；`meta::engine::{EmbeddedEngine, EngineOpts, KvKeys, MANIFEST}` 是同一组的短路径）、`meta::engine::{view::{BatchView, ReadView}, read_gate::ReadGate, dedup}`、`meta::service::MetaService::start(Arc<dyn MetaEngine>, debug_ops)`、`meta::sessions::is_active`、`PluginSet::{meta_engine, meta_engine_manifest}`、`mgmt::{MgmtCtx.role, RoleFn}`、`init::{InitOpts.debug_ops, InitOpts.plugins, InitialKv, initial_kv, HOME_INO, SCRATCH_INO}`、`base::sys::euid_egid`；key 号常量 `SERVER_KEY`/`ADMIN_KEY`/`CLIENT_KEY` 移到 `proto::handshake`（协议常量，3.3.2），`init::` 下原样转出；`txn_blocking` 实现了。`RpcClient` 的建连改成按 `(scheme, 地址)` 单飞（ADR-0031 第 11 条）：多个线程同时对同一对端发第一次调用时只有一个线程建连，其余等它的结果（成功就共享这条连接，失败就都拿到同一个错误，再各自按 3.3.8 重试）。登记"正在建连"时（拿着建连登记表的锁）再查一次连接表，第一次查完之后别人恰好建好的连接不会被重复建；建连的线程无论怎样离开（包括传输插件 panic）都交出结果、摘掉登记，等待者不会白等到期限。
> 实现注记（M4）：只增不改，另有两处小的签名调整：① `OpEnv::new(pipeline)`、`OpEnv::op_ctx(req, map)`——集群图不再是 OpEnv 里一份固定的空图，而是每个批次/每个只读请求各自的快照（3.3.6 的注记）；② `ns::common::authz(v, c, op, target, parent, victim)` 与 `new_layout(v, c, eff, ino, epoch, size_hint)`（3.5.1 的注记）。新增（按模块）：
> - core/base：`base::lock(&Mutex)`（锁中毒了也照样拿，全仓库共用）、`clock::ts_ms(Ts)`（时间戳 → Unix 毫秒）。
> - core/proto：`ClusterMap`、`ReqHead` 加了 `Default`（只是 derive，线格式不变）。
> - core/net：`RpcClient::net()`、`RpcServer::set_leader_hint`、`net::server::HintFn`、`Responder::drop_reply`。
> - core/client：`MetaClient::{connect_with, session}`、`MetaClientOpts`、`client::meta_client::fetch_map`（不开会话、只取一次集群图）。
> - core/hooks：`PluginSet::{authz, placement, authz_plugins, placement_plugins}`。
> - core/meta：`meta::{ns, changelog, cluster}`、`meta::schema::kv`（`get/put/next_id/bump_map_epoch/scan_tag/key_id`）、`meta::service::NOBODY`、`MetaService::live`、`meta::sessions::{usable, open, close, Live}`、`EmbeddedEngine::{cluster_map, stopped}`、`EffectiveAttrs::defaults`、`attrs::{CONSISTENCY_CTO, CONSISTENCY_STRONG, DEFAULT_ATTR_TTL_MS}`、`AttrResolver` 实现了 `Default`。
> - core/init：`init::{parse_target, initial_warnings, default_class}`、`InitOpts.targets`、`InitialKv.targets`。

下面这些类型被 core/net、core/data、core/client、core/meta、cli/fuse、wmfs-mgr 同时使用，签名以这里为准（M0 的接口骨架原样落成文件）。

```rust
// ===== core/base/types.rs =====
pub struct Principal { pub uid: u32, pub gid: u32, pub groups: Vec<u32>,   // 已按 key 的 uid 范围校验、按 root_squash 映射
                       pub tenant: u32, pub job_id: u64, pub conn: Arc<ConnIdentity> }
pub struct ReqCtx { pub who: Principal, pub session: u64, pub seq: u64, pub pid: u32,
                    pub trace_id: u64, pub now: Ts, pub now_ms: u64,      // now：leader 墙钟，写进写集
                    pub internal: bool }                                  // 内部操作（cluster-mgr、后台任务）：seq = 0、who = 内部 admin

// ===== core/net/client.rs —— 到多个对端的连接管理；每个进程一个实例 =====
pub enum Family { Control, Data }                    // 按族选 scheme（3.3.3）
pub struct Endpoint { pub node: u32, pub addrs: Vec<(String, String)> }   // 来自集群图 NodeRecV1.addrs；CLI 也可以手写一个
pub struct RpcClient { /* 连接表（每个对端每个 scheme 最多 1 条）、在途表、rpc-timer 线程 */ }
impl RpcClient {
    pub fn new(pool: MemPool, transports: Vec<Arc<dyn Transport>>, key: KeyFile, net: NetConfig) -> Result<Arc<RpcClient>, ErrCode>;
    /// 阻塞：发出请求、等回复。只允许在可以阻塞的线程里用（1.2 规则 3）。不重试（重试在 MetaClient/DataClient/retry.rs）
    pub fn call(&self, to: &Endpoint, fam: Family, f: OutFrame, timeout: Duration) -> Result<InFrame, ErrCode>;
    /// 发出请求、立即返回；之后用 PendingCall::wait 等（kv bench 64 路并发等）
    pub fn start(&self, to: &Endpoint, fam: Family, f: OutFrame, timeout: Duration) -> Result<PendingCall, ErrCode>;
    /// 回调式：回复、错误或超时时调用 cb，恰好一次。cb 在连接读线程或 rpc-timer 线程上执行，只允许 try_send 到某个队列，不得阻塞
    pub fn call_cb(&self, to: &Endpoint, fam: Family, f: OutFrame, timeout: Duration,
                   cb: Box<dyn FnOnce(Result<InFrame, ErrCode>) + Send>);
    /// 流式（SnapshotFetch）：timeout 是相邻两帧之间的上限
    pub fn stream(&self, to: &Endpoint, f: OutFrame, timeout: Duration) -> Result<StreamRx, ErrCode>;
}
pub struct PendingCall { /* bounded(1) 的接收端 */ }
impl PendingCall { pub fn wait(self) -> Result<InFrame, ErrCode>; }
pub struct StreamRx { /* … */ }
impl StreamRx { pub fn next(&mut self) -> Result<Option<InFrame>, ErrCode>; }   // None = 已收到 LAST_CHUNK
// 超时：rpc-timer 线程每 100 ms 扫描在途表，过期的以 Err(ETIMEDOUT) 结束（唤醒 call/wait，或调用 cb）
// 连接断开：该连接上的在途调用全部以 Err(ECONNRESET) 结束

// ===== core/net/server.rs =====
/// 服务端对一个请求的回复句柄：按值传递，恰好用一次
pub struct Responder { conn: ConnHandle, req_id: u64, msg_type: u16, trace_id: u64, span_id: u16, done: bool }
impl Responder {
    pub fn reply(self, body: Vec<u8>, bulk: Vec<BufSlice>);   // 编成 Response 帧，ConnHandle::send（try_send，不阻塞）
    pub fn error(self, code: ErrCode, extra: u64);            // ERROR 帧；leader_hint 由 core 自动填
    pub fn identity(&self) -> &ConnIdentity;
}
impl Drop for Responder { fn drop(&mut self) { /* 没回复过 → 回 EIO，计数 wmfs_req_dropped_total */ } }

// ===== core/client/meta_client.rs —— FUSE 层与 CLI 用；跟随 leader、seq、按 3.3.8 重试 =====
pub struct Caller { pub uid: u32, pub gid: u32, pub groups: Vec<u32>, pub pid: u32, pub job_id: u64 }
pub struct SessionInfo { pub client_name: String, pub host: String, pub host_id: String, pub mount: String }
pub struct MetaClient { /* RpcClient、leader 提示、会话 id、seq 计数器、在途窗口 ≤ 256、集群图 */ }
impl MetaClient {
    pub fn connect(rpc: Arc<RpcClient>, seeds: &[Endpoint], info: SessionInfo) -> Result<MetaClient, ErrCode>;  // GetClusterMap + SessionOpen
    pub fn read(&self, who: &Caller, op: MetaOp) -> Result<MetaReply, ErrCode>;      // 只读请求（seq = 0）
    pub fn write(&self, who: &Caller, op: MetaOp) -> Result<MetaReply, ErrCode>;     // 修改型：分配 seq，重试用同一 seq
    pub fn heartbeat(&self, opened: Vec<(u64, bool)>, closed: Vec<u64>, full: Option<Vec<(u64, bool)>>) -> Result<MetaReply, ErrCode>;
    pub fn map(&self) -> Arc<ClusterMap>;                                            // 当前集群图
    pub fn refresh_map(&self) -> Result<Arc<ClusterMap>, ErrCode>;                   // 心跳 map_epoch 变大、或 STALE_CHAIN/NOT_SERVING 时
    pub fn close(&self);                                                             // SessionClose
}

// ===== core/client/data_client.rs —— FUSE 层、CLI、后台任务用；选副本、CHUNK_DIRTY 重定向、换副本、按 3.3.8 重试 =====
pub struct DataClient { /* RpcClient、取集群图的回调、是否 background */ }
impl DataClient {
    pub fn new(rpc: Arc<RpcClient>, map: Arc<dyn Fn() -> Arc<ClusterMap> + Send + Sync>, background: bool) -> DataClient;
    pub fn read(&self, cap: &[u8], layout: &LayoutV1, chunk: ChunkId, off: u32, len: u32,
                into: Buf, prefer_node: Option<u32>) -> Result<(Buf, u32 /*实际长度*/), ErrCode>;
    pub fn write(&self, cap: &[u8], chunk: ChunkId, extents: Vec<(u32, u32)>, data: Vec<BufSlice>, sync: bool) -> Result<u64 /*version*/, ErrCode>;
    pub fn truncate(&self, cap: &[u8], chunk: ChunkId, new_len: u32) -> Result<u64, ErrCode>;
    pub fn sync(&self, cap: &[u8], chain: u32, chunks: Vec<ChunkId>) -> Result<(), ErrCode>;
    pub fn delete_epoch(&self, cap: &[u8], chain: u32, ino: u64, epoch: u32) -> Result<u64, ErrCode>;   // 只给 gc（管理权限 cap）
}

// ===== core/hooks/pipeline.rs —— 按 altitude 排好的插件；每次调用都经过 3.7.4 的包装 =====
pub struct Pipeline { /* PluginSet */ }
impl Pipeline {
    pub fn authz(&self, input: &AuthzInput) -> Result<(), ErrCode>;                  // 串行；任一 Deny → Err；全部 Abstain → EACCES
    pub fn quota(&self, input: &QuotaInput) -> QuotaDecision;                         // 按 3.7.2 合并多个插件的结果
    pub fn place(&self, input: &PlacementInput, candidates: Vec<ChainView>) -> Result<Vec<ChainView>, ErrCode>;   // 依次 rank
    pub fn qos(&self) -> &Arc<dyn QosPlugin>;
    pub fn codec(&self, id: u8) -> Option<&Arc<dyn CodecPlugin>>;
    pub fn transform(&self, kind: TransformKind, alg: u8) -> Option<&Arc<dyn TransformPlugin>>;
    pub fn policy(&self) -> &dyn PolicyEvaluator;
    pub fn plugin_list(&self) -> PluginListV1;                                        // 3.7.2
}
// meta/ns/common.rs 里的便捷函数（命名空间代码只用它们）：
//   authz(v, c, op, target: Option<&InodeV1>, parent: Option<&InodeV1>) -> Result<(), ErrCode>
//   new_layout(v, c, eff, ino, size_hint) -> Result<LayoutV1, ErrCode>   // 候选链 + gen_chains + Pipeline::place + 取 k 条（3.4.2.1）

// ===== core/meta/attrs.rs =====
pub struct EffectiveAttrs {
    pub class_id: u16, pub replicas: u8, pub chunk: u32, pub stripe: u16, pub inline_max: u32,
    pub checksum: u8, pub compress: u8, pub compress_level: i8, pub encrypt_key: u32 /*0 = 不加密*/,
    pub consistency: u8 /*1 cto 2 strong*/, pub attr_ttl_ms: u32,
    pub trash_ms: u64 /*0 = off*/, pub trash_root: u64, pub ttl_ms: u64, pub tier: Option<TierRule>,
    pub audit: AuditSpec, pub locality: u8, pub acl_default: Option<Vec<u8>>, pub qos_id: u64,
    pub quotas: Vec<(u64 /*目录*/, QuotaDecl)>,      // Stacked：所有祖先上的 quota 与 quota.<class>
    pub sources: Vec<(&'static str, u64)>,           // 每个键的来源目录（0 = 内置默认值）
}
pub struct AttrResolver { /* dir → (attr_epoch, Arc<EffectiveAttrs>) 缓存；祖先链缓存 */ }
impl AttrResolver {
    pub fn resolve(&self, v: &dyn KvRead, dir: u64) -> Result<Arc<EffectiveAttrs>, ErrCode>;
    pub fn ancestors(&self, v: &dyn KvRead, dir: u64) -> Result<Arc<Vec<u64>>, ErrCode>;   // dir → … → 根
}

// ===== 其余被多个模块引用的小类型 =====
pub struct RegionInfo { pub base: u64, pub len: u64 }                                    // core/buf：给传输插件注册内存
pub struct KeyFile { pub key_id: u32, pub secret: [u8; 32] }                             // 放在 wmfs-api（core 与 wmfs-mgr 都要读密钥文件）：一行 wmfs-key-v1 的解析结果
pub struct NetConfig { pub control_scheme: String, pub data_scheme: String,              // node.toml [net] 或挂载参数
                       pub write_timeout_ms: u32 /*5000*/, pub send_queue: u32 /*1024*/ }
pub struct RoleInfo { pub role: raft::StateRole, pub term: u64, pub leader_id: u32, pub ready: bool }   // ready 见 3.4.1.4 规则 3
pub struct EngineStatus { pub applied: u64, pub commit: u64, pub voters: Vec<u32>, pub learners: Vec<u32>, pub progress: Vec<(u32, u64)> }
pub struct QueueStat { pub key: QueueKey, pub depth: u32, pub served_per_s: f64, pub deficit_per_s: f64, pub limited_total: u64 }
pub struct StoreStat { pub capacity: u64, pub used: u64 }
pub struct FileFacts { pub ino: u64, pub size: u64, pub atime: Ts, pub mtime: Ts, pub btime: Ts, pub class_id: u16, pub uid: u32 }  // 策略求值用
pub struct TierRule { pub from: u16, pub to: u16, pub cond: String }                     // cond 已在 set 时校验过
pub struct AuditSpec { pub on: bool, pub ops: Vec<u16> /*空 = 全部修改类操作*/, pub uids: Vec<u32> /*空 = 所有人*/ }
pub struct QuotaDecl { pub class: Option<u16>, pub hard: u64, pub soft: u64, pub grace_ms: u64, pub files: u64, pub quantum: u64 }
```

`wmfs-mgr` 里的两个接口（不依赖 core，类型来自 `wmfs-api`）：

```rust
// dashboard/src/api.rs —— 带 HMAC 签名的管理 API 客户端，CLI 与 mgr 共用
pub struct ApiClient { /* ureq::Agent（proxy(None)）、地址列表、key */ }
impl ApiClient {
    pub fn new(addrs: Vec<String>, key: KeyFile) -> ApiClient;                         // addrs：逗号分隔的 WMFS_CLUSTER 拆开
    pub fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, ApiError>;   // 依次尝试地址；409 跟随 leader
    pub fn post<B: serde::Serialize, T: serde::de::DeserializeOwned>(&self, path: &str, body: &B) -> Result<T, ApiError>;
    pub fn delete<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, ApiError>;
}
pub struct ApiError { pub status: u16, pub code: String, pub message: String }      // = 服务端的 {"error","message"}
// dashboard/src/host.rs —— mgr 自己的 HTTP 路由（tiny_http）；MgmtExt 插件在 start() 里注册页面
pub struct HttpIn { pub method: String, pub path: String, pub query: String, pub body: Vec<u8> }
pub struct HttpOut { pub status: u16, pub content_type: &'static str, pub body: Vec<u8> }
pub struct Router { /* 路径前缀 → 处理函数 */ }
impl Router { pub fn add(&mut self, prefix: &str, h: Box<dyn Fn(&HttpIn) -> HttpOut + Send + Sync>); }
```

---

## 4. 首发插件逐个设计

格式：**职责 / 最简实现 / 运行位置 / 热路径预算**。所有插件都满足理念文档 4.3：manifest 齐全、不持有本体不知道的持久状态、带单元测试 + ≥ 1 个 fail point 用例（至少 `plugin.<name>.<hook>`）+ 指标 + `docs.md`。

### 4.1 transport

- **tcp**（`plugins/tcp/`，≈ 300 行）。职责：帧的收发。实现：`std::net`；`TCP_NODELAY`、`SO_KEEPALIVE`（调用 core 的 `sys::set_keepalive`，插件里没有 unsafe）、`set_write_timeout(Some(5 s))`；`listen` 读自己的 `[plugin.tcp] listen` 配置，返回实际绑定的地址；`send`（只在 core 的写线程上调用，所以不需要锁）依次 `write_all` 帧头、body、各段 bulk（`BufSlice` 逐个写，不拼接）；`recv`：读 48 字节帧头 → 校验 CRC 与长度上限 → 读 body 进 `Vec` → 有 bulk 时：响应帧读进请求方给的 `recv_into`，请求帧按 msg_type 选 lane（`PeerCall` 用 `Lane::Repl`，其余 `Lane::Normal`）从池取 Buf 后 `read_exact` → 校验 bulk CRC；Repl lane 100 ms 取不到（Normal lane 同样最多等 100 ms）→ 把 bulk 读进固定的 64 KiB 暂存区丢弃，交出 `InFrame { bulk: None, bulk_dropped: true }`，由 core 回 `BUSY`（OneWay 的 RaftMsg 直接丢弃）。空闲 5 s 发 Ping，15 s 无任何帧判死。`register_region` 空操作。位置：server 与 client 的连接读/写线程。预算 100 ms（3.7.4），`wmfs_tcp_bytes_total{dir}`。
  实现注记（M1）：① body 与 bulk 的 CRC 由 core 在读线程统一校验（`net/conn.rs` 的 `recv_checked`，对所有传输都一样），tcp 只用 `FrameHeader::decode` 校验帧头；② 写超时按**整帧**算：阻塞的 socket 在 SO_SNDTIMEO 到期但已写出一部分时会返回"部分写"，`write_all` 会再等一个 5 s，所以 tcp 自己循环 `write`，**每次 write 之前把 SO_SNDTIMEO 设成这一帧剩下的时间**（至少 1 ms；已过期限就直接 `ETIMEDOUT`），一帧最多等满一个写超时；③ "空闲 5 s 发 Ping"由 core 的客户端写线程做（对所有传输都一样，握手完成之后才发），tcp 只负责读超时 15 s；④ 丢弃 bulk 用 `io::copy(take(n), sink())`（8 KiB 栈缓冲），不另开 64 KiB 暂存区；⑤ 写超时来自 `[net] write_timeout_ms`（构造参数）；⑥ 收帧的计时（3.7.4）从读到帧头之后开始、读完 body + bulk 结束，由 tcp 自己调 `HookCall::exit`；⑦ `mark_authenticated` 之前收到声明了 bulk 的帧直接断开、不取池槽（3.3.2 实现注记 ⑥）；给 bulk 找缓冲用 core 的 `frame::bulk_buf`（3.6.4 实现注记）。
- **rdma**（`plugins/rdma/`，≈ 920 行，cargo feature `rdma`）。职责：同一帧格式跑在 RC QP 上（只有数据族与 Ping 会用它，3.3.3）。实现：sideway（verbs 部分）；设备、`gid_index`、`listen`（引导 TCP 地址）、`max_qps` 来自 `node.toml [plugin.rdma]`（RoCEv2 要选 IPv4 映射的 GID，`wmfs doctor` 列出候选；实现注记（M0）：M0 的 doctor 只报告 RDMA 设备与端口状态，列出 GID 候选推迟到 M2，与"doctor 识别 rxe 设备"一起做）；每设备一个 PD、一个 CQ、一个轮询线程；对整个内存池一次 `reg_mr(LocalWrite | RemoteRead | RemoteWrite)`；每连接一个 RC QP、16 个接收槽（S 档，3.6.1 的启动校验）。建连：在引导 TCP 连接上用插件私有消息 0x0600 `RdmaSetup{qpn, psn, gid, mtu}`（**不含 rkey**）交换 QP 信息 → QP 状态迁移 → 包成 `Conn` 交给 core 做 3.3.2 握手；`mark_authenticated` 之前收到带 `BULK_REMOTE/REPLY_REMOTE` 的帧 → 断开。QP 状态迁移照 sideway `examples/rc_pingpong.rs`，**但不能照抄三处**：INIT 的访问位要 `LocalWrite | RemoteWrite | RemoteRead`（示例没有 RemoteRead）；RTR 用 `setup_max_dest_read_atomic(16)`（示例是 0）；RTS 用 `setup_max_read_atomic(16)`（示例是 0）——否则 RDMA READ 会失败。RTS 其余：`setup_timeout(14)`、`setup_retry_cnt(7)`、`setup_rnr_retry(7)`；地址向量用 `setup_grh_src_gid_index(gid_index)`。bulk 规则见 3.6.4（描述符表 n ≤ 16，接收方依次 READ 进一个 Buf）；发送方把 `BufSlice` 留在"在途表"（按 req_id）里，响应到达或连接关闭才 Drop；在途请求 16 个时 `send` 阻塞写线程（信用控制）。位置：连接读/写线程 + `rdma-cq-<dev>` 线程。预算 100 ms（每次 post 本身 ≤ 2 µs）。本机无可用 RDMA 链路，测试需要人类启用 Soft-RoCE（roadmap M2）。
  实现注记（M2）：① **访问位按两侧分开**（与上文"INIT 的访问位要 LocalWrite | RemoteWrite | RemoteRead"不同）：单边操作永远由接收请求的一方（accept 侧）发起，读写的是 connect 侧的内存，所以 accept 侧（服务端）的 QP 从不开放远程读写；connect 侧 INIT 时也不开放，握手完成后在 `mark_authenticated` 里用 RTS → RTS 的 modify_qp（`IBV_QP_ACCESS_FLAGS`）打开 RemoteRead | RemoteWrite。INIT 时 QP 的访问位为 0（QP 的访问位只有 REMOTE_* 有意义，LocalWrite 是 MR 的属性）；MR 仍是 LocalWrite | RemoteRead | RemoteWrite（有效权限 = QP 与 MR 的交集），两侧的 max_dest_rd_atomic、max_rd_atomic 都是 16（C-10）。QP 在 0x0600 交换之后、3.3.2 握手之前就进了 RTS，原写法会让服务端的 QP 接受任何对端（包括未认证的）发来的 READ/WRITE，"不可信客户端碰不到服务端内存"只能靠 rkey 与池地址不被猜到；现在由网卡保证（semantics.md 第 8 节）。交给 modify_qp 的属性由纯函数 `plugins/rdma/qp_attr.rs`（init / rtr / rts / allow_remote / error）按 `params::qp_params(accept_side)` 生成，C-10 的回归测试 `rdma_qp_attributes_follow_c10_and_open_remote_access_only_after_auth`（`plugins/rdma/tests/qp.rs`）用 sideway 自带的 getter 读回这些属性逐项断言（不需要设备）；rq_psn 取对端的 psn、sq_psn 取本端的（与 libibverbs 的 C 版 rc_pingpong 相同，sideway 示例两端都反过来用）。② **推迟打开设备**：`[plugin.rdma] listen` 默认为空（`listen` 返回 None、不碰设备）；`register_region` 只记下区域（幂等）；第一次需要设备时（监听一个具体地址，或第一次 connect）才打开：每设备一个 PD、一个完成队列（`max_qps × (16 + 48) + 64` 项）、一个轮询线程，对已登记的每块区域注册一个 MR，并用 query_device 核对 max_qp、max_qp_wr、max_cqe 与并发 READ 数，不够就带中文原因拒绝。于是 rdma 默认编进二进制也不影响没有 RDMA 设备、没装 libibverbs 的机器（M6 起的 docker 测试镜像）上只用 tcp 的节点与一次性命令。③ **device、gid_index 都可以省略**：一次性命令（`wmfs ping rdma://…`）没有 node.toml，插件取引导 TCP 连接本端的 IPv4，在 sysfs 里找"RoCE v2、IPv4 映射、地址等于它、端口 ACTIVE"的 GID（`plugins/rdma/gid.rs`，只读 `/sys/class/infiniband`）；写了 device / gid_index 就只在符合的项里找；找不到时连接失败并在日志里列出候选。connect 在建 TCP 之前先用 UDP 套接字 connect 一下拿到本端地址（不发包）做这个预检，本机没有对应设备就立刻 `EINVAL`（RpcClient 不重试），所以 `wmfs ping rdma://127.0.0.1:…` 很快报错。只支持 RoCE v2 的 IPv4 地址。④ 每连接一个 RC QP：发送队列 48（一个请求的 16 个 READ + 一个回复的 16 个 WRITE 与 1 个 SEND，留余量）、接收队列 16、各 1 个 SGE；RTR 的 min_rnr_timer 12、地址向量 hop_limit 64。⑤ **建连**：accept 线程只做 accept；每条引导连接起一个 `rdma-setup` 线程，交换期间读写超时 5 s；活着的 QP（含建连中的）到 `max_qps` 之后新的引导连接直接关掉；失败时 QP 与它的槽位立刻释放。⑥ 设备不可用时 connect 记一行日志并返回错误：本机根本没有这个地址的 RoCE v2 GID → `EINVAL`（配置错了，不重试）；GID 在、只是端口链路不是 ACTIVE（例如交换机重启）→ `ECONNRESET`（按连接断开，重连重试）；其余 → 按连接断开。⑦ 本机没有可用链路，`verbs.rs`（选设备与 GID、建 QP、post 工作请求、改 QP 状态）与 `device.rs`（打开设备、注册内存、轮询完成队列）**未在本机验证**；调用 sideway 的只有这两个文件与 `qp_attr.rs`（纯函数，有单元测试）；其余逻辑由单元测试里的假网卡（`plugins/rdma/tests/fake.rs`）验证，`cargo test -p wmfs-plugins --features rdma rdma`。sideway 的错误在 Display 里只有一句英文外层说明，errno 在 `source()` 里，日志一律把整条 source 链拼上（`device::why`）。⑧ 不变量：一个 RdmaTransport 在进程的生命周期里只服务同一个内存池（每个进程一个池）；池被丢弃后在同一地址新建的池会沿用旧 MR（钉住的是旧页），不支持这种用法。

### 4.2 authn

- **shared-key**（110，≈ 150 行）。职责：连接建立时验证 `proof`，返回 `ConnIdentity { role, tenant, uid_ranges, root_squash, path_caps }`。实现：`hmac::Hmac<sha2::Sha256>`，常数时间比较（`verify_slice`）；`key_id == 0` → `role = Server`（不查 KEYMETA）；否则 role 取 `KEYMETA.role`（`Role::from_u8`）；key 元数据来自集群图（被撤销的 key 立即拒绝，已建立的连接在集群图更新时关闭）。core 随后强制"`ConnIdentity.role == Hello.role`"（3.3.2），并对每个请求声明的 uid 做"在 uid 范围内 + root_squash（uid 0 → 65534，除非 key 不 squash）"。位置：server 连接线程（握手阶段）。预算：每连接 20 µs。

### 4.3 authz

- **posix-mode**（210，≈ 180 行；实现注记（M4）：实际约 110 行，规则表见 `plugins/posix-mode/docs.md`；chown 的取值规则与 xattr 名字空间的规则由 core 检查，见 3.5.1 的注记；root 的放行在"有扩展 ACL 就弃权"之前——与 Linux 的 CAP_DAC_OVERRIDE 相同，带扩展 ACL 的 inode 对 root 也放行，M10 的 posix-acl 对 root 也要放行或弃权，否则按组合规则 root 会被拒绝）。职责：经典 POSIX 权限。实现：owner / group（含附加组）/ other 位；root（squash 后仍为 0）除执行位外全部放行；粘滞位约束 unlink/rename；chown 只有 root 能改 uid，owner 只能把 gid 改成自己所在的组；chmod 限 owner/root；时间设为"now"只需写权限；非属主写入时清除 suid/sgid（core 在 `CommitWrite` 里执行）。inode 有扩展 ACL 时 Abstain。位置：元数据 leader（执行器闭包内 / meta-read 线程）。预算 5 µs。
- **posix-acl**（220，≈ 260 行）。职责：POSIX.1e ACL 访问判定。实现：解析 Linux xattr 二进制格式（le32 version = 2，条目 `{le16 tag, le16 perm, le32 id}`；tag：USER_OBJ 1、USER 2、GROUP_OBJ 4、GROUP 8、MASK 0x10、OTHER 0x20），按 owner → 命名用户 → 属组/命名组（受 mask）→ other 判定；无 ACL 时 Abstain。继承与 `acl.default` 属性在 core 的 create 里做（3.7.3 末尾）。预算 5 µs。
- **path-caps**（250，≈ 90 行）。职责：把一把 key 限制在若干子树内（CephFS path 限制的等价物）。实现：规则来自**复制状态** `KEYMETA.path_caps`（`wmfs key create --path /proj3 [--path-ro /shared]` 写入时解析成 inode 号；目录被 rename 后限制仍跟着该目录）；`ConnIdentity.path_caps` 为空 → Abstain；目标（目录项操作时为父目录）本身或其 `ancestors` 里出现某个允许的根 → 放行（只读根上的写类操作 → `Deny(EROFS)`）；目标是某个允许的根的**祖先目录**且操作是 `Lookup/GetAttr` → 放行（只为路径遍历，不允许 ReadDir 与修改；允许根的祖先链由 core 按 `attr_epoch` 缓存）；其余 → `Deny(EACCES)`。预算 2 µs。

### 4.4 quota_admission

- **quota**（310，≈ 170 行，没有任何配置项：宽限期、quantum 的缺省值在属性注册表里）。职责：软 / 硬 / 宽限、文件数、按存储类。实现：对 core 给出的每个 scope：`delta_files > 0` 且 `used.files + used.dirs + Δ > hard_files` → Deny(EDQUOT)；`delta_bytes ≤ 0` → 放行；有效上限 = 宽限期已过 ? soft : hard；`used.bytes + Δ > 有效上限` → Deny(EDQUOT)；超软限：`soft_since == 0` → SoftWarn（core 记录起点并写事件）；`max_growth = min(各 scope 的 有效上限 − used − Δ)`。纯函数，状态由 core 写回。位置：执行器闭包内。预算 5 µs。

### 4.5 qos

- **mclock**（410，≈ 330 行）。职责：数据节点入口调度（第 6 章第 1 层）。实现：每个 `QueueKey (tenant, qos_id, class_id)` 一个队列，队列内按 `job_id` 分子队列**轮转**（理念文档 6.2"作业 id 参与分类"）；mClock 三个标签：入队时 `R = max(R + cost/r, now)`、`L = max(L + cost/l, now)`、`P = max(P + cost/w, now)`；出队先取"R ≤ now 的最小 R"（保底，与优先级无关）；否则进入权重阶段：**先只在 `low_priority = false` 的队列里**找"L ≤ now 且字节令牌桶足够"的最小 P，**没有可派发的才看 `low_priority = true` 的队列**（存储类 `priority = low`，例如 vHDD 类；约 10 行），选中后把该队列的 R 标签回退 cost/r；都没有则返回最早可再问的时刻；空闲 60 s 的队列回收、重新激活时标签重置为 now。`cost = 1 + bytes / 128 KiB`（4 KiB IO = 1，1 MiB = 9，4 MiB = 33），`r/l` 的单位是"成本单位/秒"——所以存储类里写的 `iops_*` 实际是"成本单位/秒"，这个换算写进 ADR-0010 与 semantics.md，`wmfs class show` 也直接显示换算结果（例如"iops_max = 300 ≈ 4 MiB 写 9 次/秒 ≈ 36 MiB/s，按 3 个节点均分后每节点 100"）。规格 `QosSpec` 由 core 从集群图计算：目录 `qos` 属性（`QOS` 表）或存储类的 QoS 包络，**按该存储类当前有 Up target 的数据节点数静态均分**到每个节点（没有 dmClock 式跨节点协调，【ADR-0010】）。后台请求（`background = true`）进一个固定的低优先级队列（`w = 1`、`l = 节点后台限速`、`low_priority = true`）。插件内部只有一个 `Mutex<State>`，**不阻塞、不睡眠**；选择时线性扫描活跃队列（小集群队列数 ≤ 几百）。位置：每个数据节点。预算 2 µs/次。
- **睡眠与唤醒归 core**（`core/data/service.rs`，两个插件共用）：core 持有一个 `Mutex<()> + Condvar` 和 `token → 请求` 的映射表；每次 `enqueue` 之后 `notify_one`；io 线程循环 `match qos.dequeue(now) { Ok(t) => 取出 t 对应的请求执行, Err(Some(t)) => cv.wait_timeout(t − now), Err(None) => cv.wait_timeout(100 ms) }`。所以插件写错也只会"派发不准"，不会忙等或丢失唤醒。
- **fifo**（401，≈ 40 行）。最简单的先进先出，给不需要 QoS 的 profile（`minimal`）用；"只允许一个活跃"规则不变。

### 4.6 placement

- **label-match**（510，≈ 200 行；M4 实现基础版：按 class/replicas 选链、容量排序、确定性散列；M13 补上 dedicated、locality、rebalance 用的 exclude）。职责：create、截断到 0 与迁移时选链。实现：候选 = `class_id` 与 `replicas` 都匹配、至少一个 Serving 成员、不在 `exclude` 里的链；排序键 =（`locality = prefer-client` 时 head 在客户端所在节点的链优先；客户端节点由会话的 `host_id` 与节点 `host_id` 匹配得出）→ serving 成员数降序 → `used / capacity` 升序 → `hash(seed, chain_id)`（seed = 新 ino，确定性分散）；core 取前 `k = min(stripe, 互不相同的链数)` 条（k < stripe 时照常创建并计入"条带数不足"，ADR-0016）。(class, replicas) 还没有链 → core 在同一闭包里 `gen_chains`（3.4.2.1），用它返回的新链作为候选重试一次；仍为空 → `ENOSPC`（`dedicated` 类没有专属 target、或要求 `label = hdd` 而本机没有这样的 target 时，**拒绝而不是静默放到别的设备**）。位置：执行器闭包内。预算 50 µs。

### 4.7 codec

- **replica**（610，≈ 50 行）。职责：数据 ↔ 分片。实现：`shards` = 每个成员一份 `Whole`；`read_order` = serving 成员，同节点优先（locality），其余按 `hash(chunk_id)` 轮转分散读负载。链式复制协议本身在 core（契约），codec 只决定"分几片、读哪个"。预算 1 µs。

### 4.8 transform

| 插件 | altitude / alg_id | 实现（API 均已在本机编译核对） | 预算（每 64 KiB 块） |
|---|---|---|---|
| crc32c | 701 / 1 | `crc32c::crc32c(&[u8]) -> u32`（aarch64 硬件指令）；encode 填 `meta.checksum[0..4]`、原样返回 Buf；decode 校验 | 20 µs |
| xxh3 | 702 / 2 | `xxhash_rust::xxh3::xxh3_64(&[u8]) -> u64` | 20 µs |
| blake3 | 703 / 3 | `blake3::hash(&[u8]).as_bytes()`（32 字节） | 60 µs |
| lz4 | 731 / 1 | `lz4_flex::block::compress_into(src, dst)` 写进新的 S 档 Buf；结果 > 明文 7/8 则返回原 Buf 且不置 `COMPRESSED`；decode `lz4_flex::block::decompress_into` | 150 µs |
| zstd | 732 / 2 | `zstd::bulk::compress_to_buffer(src, dst, level)` / `zstd::bulk::decompress_to_buffer(src, dst)`；每线程缓存一个上下文 | 600 µs |
| aes-gcm | 761 / 1 | `use aes_gcm::{Aes256Gcm, KeyInit, aead::{AeadInOut, Nonce}}`；`Aes256Gcm::new_from_slice(key)`（按 key_id 缓存）；nonce = `rand::fill` 的 12 个随机字节，`Nonce::<Aes256Gcm>::try_from(&nb[..])`；`let tag = cipher.encrypt_inout_detached(&nonce, aad, buf.as_mut_slice().into())?`（返回 `Result<Tag>`，**原地加密**，零拷贝），nonce 与 tag 写进块表；decode `decrypt_inout_detached(&nonce, aad, buf.into(), &tag)`，失败即 `CHECKSUM_MISMATCH` | 100 µs |

数据加密密钥存在 KV `DATAKEY`，用 `K_wrap` 以 AES-256-GCM（随机 nonce）包裹，经集群图下发，数据节点解包后只放在内存里。同一个密钥用随机 nonce 的安全上限约 2^32 次加密（约 256 TiB 的 64 KiB 块），`docs/ops.md` 建议超过 64 TiB 后轮换为新 key id。位置：数据节点 head 的 io/repl 线程（写）与被读成员的 io 线程（读）。每个都有 `transform.<名>.corrupt` 用例（篡改输出一字节，验证解码端能发现）。

### 4.9 chunk_store

- **local-fs**（≈ 420 行）。职责：把 core 编码好的 chunk 字节存成目录里的文件（ext4 或 XFS，本机 ext4【ADR-0008】）。实现：路径规则见 3.2.2；fd LRU 缓存 4096 个（同时缓存已提交版本的头 + 块表；本节点每次 commit/install/delete 时作废对应条目）；`write_pending` = `O_CREAT|O_TRUNC` 打开 `p/…p`、`pwrite` 各段、对 copies 用 `libc::copy_file_range`（`EXDEV/ENOSYS/EOPNOTSUPP` 时退回 `pread/pwrite`；在 XFS 上会变成 reflink）；`commit_pending` = （sync 时 `sync_data`）→ `rename` → 新建过目录时 `fsync` 目录；`install` = `t/` 临时文件 → `sync_data` → `rename` → `fsync` 目录；`sync` = `sync_data` + 目录 fsync；`list_chain` 按名字排序遍历；`delete_epoch` 删目录；`statfs` = `libc::statvfs` 与 `node.toml` 里的 `capacity` 上限取小（防止测试写满系统盘）。位置：io/repl 线程。预算 100 ms/次（3.7.4），`wmfs_chunkstore_op_seconds{op}`。fail point：`chunk.pending.eio`、`chunk.pending.torn`、`chunk.commit.before_rename`、`chunk.read.eio`、`chunk.read.corrupt`、`chunk.sync.eio`。写放大：小块随机覆盖写要复制该 chunk 其余块的存储字节（ext4 上是内核内拷贝，约 1 ms / 4 MiB）——首发接受并写进文档，客户端按 chunk 合并脏数据能消掉大多数【ADR-0021】；块级增量格式二期用 feature flag 引入。

### 4.10 meta_engine

- **embedded-kv**：raft-rs + redb（3.4.1、3.5.1）。因为"元数据 Raft"是契约层，实现放在 `core/src/meta/`，注册表里以 `embedded-kv` 名义注册为唯一的 `meta_engine`。理念文档 4.3 的要求照样满足：`plugins/embedded-kv/` 下有 `manifest.toml`（7 个必填字段）与 `docs.md`，core 里的 `Manifest` 常量由单元测试与它逐字段比对；fail point `plugin.embedded-kv.meta_engine` 包在 `read/txn` 外层。位置：raft 线程 + 执行器线程 + meta-read 线程。

### 4.11 cache_store（客户端）

- **memory**（≈ 140 行）：1 MiB 块的 LRU（容量默认 512 MiB，挂载参数 `--cache memory:SIZE`）；键含 `change_id`，文件一变旧条目自然失效；`invalidate_ino` 删除该 inode 全部条目。预算 20 µs + 拷贝。
- **local-nvme**（≈ 220 行）：缓存目录里每个 `(ino, change_id)` 一个稀疏文件 + 内存位图记录哪些 1 MiB 块已缓存；按总字节 LRU 淘汰（默认 50 GiB，`--cache local-nvme:DIR:SIZE`）；**挂载时清空整个缓存目录**（不做崩溃恢复，避免任何陈旧性推理）。预算 50 ms/次（本地盘 IO，3.7.4；命中页缓存时约 50 µs）。

### 4.12 bg_task

| 任务 | 运行位置 | 职责与最简实现 |
|---|---|---|
| reconciler（第一个实例，≈ 280 行） | MetaLeader | 3.10：`RECONCILE_Q` 子树遍历 + 每 24 h 全量扫描；声明 ≠ 实际 → `Migrator::migrate`；顺带校验 RSTAT |
| scrub（≈ 200 行） | EveryDataNode | 按 `list_chain` 顺序遍历本地已提交 chunk：读全部块 → 解码 → 校验；坏块 → 从同链另一个 Serving 成员 `FetchChunk` 并 `install`，发 `ChecksumRepaired` 事件；自己是唯一副本 → `DataLossDetected` 事件 + 指标（诚实上报，之后读该块返回 `EIO`）。默认 50 MiB/s |
| tiering（≈ 140 行） | MetaLeader | 每 24 h 遍历带 `tier` 属性的子树（先扫 `DIRATTR` 找规则根），对 `class == from` 且条件成立的文件 `migrate` 到 `to`；条件用 `PolicyPoint::TierCondition` 求值；与协调器用同一个 `declared()`，不会被迁回去 |
| rebalance（≈ 160 行） | MetaLeader | 同一存储类内最满与最空的链使用率相差 > 20% 时，从最满的链上抽样文件（placement 带 `exclude` 提示）迁到别的链，差距 < 10% 停止；受限速 |
| gc（≈ 220 行） | MetaLeader + EveryDataNode | leader：GC 记录 kind 2（废弃 epoch）宽限 `epoch_gc_grace`（默认 60 s，演示时可调成 0）后对其链 `DeleteEpoch`、删记录；kind 1（孤儿）按 3.5.6 条件删除；被驱逐/已关闭会话的去重记录分批点删；`EVICTED` 过期删除；changelog 修剪。数据节点：`list_epochs` → 批量 `CheckEpochs` → 删除已死且目录 mtime 早于 1 h 的 `(chain, ino, epoch)`；删除本 target 已不是成员、且超过 `rejoin_window` 的链目录 |
| trash-expire（≈ 70 行） | MetaLeader | 范围扫描 `TRASH_INDEX` 中 `expire_ms ≤ now` 的条目 → 以内部操作真正删除（进入普通孤儿/GC 流程） |
| ttl（≈ 90 行） | MetaLeader | 遍历带 `ttl` 属性的子树，mtime 早于期限的文件 → unlink（若该处 `trash` 生效则进回收站），changelog 原因 = ttl |

### 4.13 event_sink（进程外，`wmfs mgr` 内，`wmfs-mgr` crate）

mgr 宿主的规则（对 4.13、4.14 的五个插件都一样）：启动时读每个插件的 `manifest.toml`，校验理念文档 4.3.1 的 7 个必填字段与 `abi_version == MGR_ABI_VERSION`，不符合就**拒绝启动**；每次 `deliver/tick` 先过 `fail_point("plugin.<name>.<hook>")`，再计时、计数（`wmfs_hook_*` 指标，mgr 自己的 `/metrics` 输出）；changelog 只有宿主的一个长轮询循环在拉，按插件分发（3.11）。**同一集群只运行一个 mgr**（两个实例会重复投递、互相覆盖游标；`docs/ops.md` 写明）。

- **audit-log**（≈ 90 行）：只取 `audited` 记录 → JSON Lines 写入 `<mgr_dir>/audit/audit-YYYYMMDD.log`（按 100 MiB 轮转），每批 `fsync` 后才提交游标 `audit-log`（至少一次投递，带 seq 便于下游去重）。这个游标在 `audit` 第一次启用时已由 core 创建（3.8），所以 mgr 没运行期间的记录按游标保留（最多 7 天 / 1000 万条）；超出部分被修剪后，audit-log 启动时得到显式的 410 并告警，不是静默丢失。
- **webhook**（≈ 110 行）：每 100 条或 1 s 一批，用 ureq（`proxy(None)`，除非配置了代理）POST JSON 数组；非 2xx 指数退避（上限 5 min）；成功后提交游标。

### 4.14 mgmt_ext（进程外，`wmfs mgr` 内）

- **prometheus-exporter**（≈ 90 行）：mgr 的 `/metrics` = 抓取所有节点的 `/metrics`、加 `node="…"` 标签合并输出，Prometheus 只需配一个抓取目标；另附集群级指标（配额目录的用量/上限比、链健康、任务积压、声明 vs 实际计数）。
- **dashboard**（Rust ≈ 120 行 + 内嵌单页 HTML/JS ≈ 250 行，`include_str!`，无外部库）：mgr 代理到管理 API（mgr 持有只读或 admin key；浏览器访问需要启动日志里打印的 dashboard token，默认只监听 127.0.0.1）。页面：集群（节点、leader、链状态、降级/离线标红、集群警告：短链、"共享设备""故障域重叠"）、容量与配额（策略根的用量条、软限计时）、声明 vs 实际（调用 status API，输入路径，**逐个属性**显示声明、来源、实际、是否一致，3.10 的表）、QoS（每队列派发速率、等待 p99、命中上限次数）、后台任务（进度/积压）、事件与告警（changelog 尾部与策略违约）。每 5 s 轮询。
- **alert**（≈ 190 行）：规则文件 TOML，表达式只支持"指标{标签} 比较符 阈值"与"事件类型"两种；状态机 pending → firing → resolved（`for` 防抖）；动作 = 日志 + 可选 webhook；当前状态在 mgr 的 `GET /alerts`（JSON）里，`wmfs dev wait alert …` 用它。内置默认规则：target 离线 > 60 s、链降级 > 5 min、链不可用、无元数据 leader > 10 s、配额软限超出/硬限拒绝、scrub 发现坏块、协调器积压 1 h 内持续增长、内存池等待、插件超预算速率、`wmfs_meta_batch_stale_total > 0`、审计消费者 10 分钟没有前进、任何 `PolicyViolation` 事件（含"迁移因写打开被跳过超过 24 h""隔离降级"）。

---

## 5. 首发属性（理念文档 5.2；二期的 `ec`、`snapshot` 设置时返回 `EOPNOTSUPP`"二期功能"）

尺寸单位二进制（`K = 1024`）；时长 `s/m/h/d`。所有属性变更写 changelog。"新文件"在本表里一律包括**截断到 0 的文件**（截断到 0 时按当前生效属性重建 layout，3.4.2.9）；"新数据 + 协调器"类属性对新文件立即生效，已有文件由协调器迁移——**迁移完成前，已有文件里新写入的数据仍按旧 layout；一直写打开的文件会被跳过**（`wmfs set` 时警告、`wmfs status` 单独计数，偏离理念文档 5.2 的"新数据"，【ADR-0021，需确认】）。进度见 `wmfs status`。

| 属性 | 取值 / 解析 | 种类 | 校验 | 生效方式 | 执行者 |
|---|---|---|---|---|---|
| `class` | 存储类名 `[a-z0-9-]{1,32}` | Inherit | 类存在 | 新文件写进 inode.class_id 并按类选链；已有文件由协调器迁移 | placement / qos / bg（reconciler、tiering） |
| `replicas` | `1..=8` | Inherit | 范围；超过当前故障域数时警告（ADR-0016） | 新文件选对应长度的链；已有文件由协调器迁移；链缺成员由链管理器补齐 | codec（replica）+ placement |
| `chunk` | `64K..=16M`，2 的幂 | Inherit | 范围 | 新文件（layout.chunk_size）；不迁移 | core |
| `stripe` | `1..=64` | Inherit | 范围；超过可用的互不相同的链数时警告（ADR-0016） | 新文件（选 k = min(N, 可用链数) 条链，chunk i → chains[i % k]）；不迁移 | placement |
| `inline` | `0..=64K`（0 = 关闭） | Inherit | 范围 | 新文件（layout.inline_max）；内容 ≤ 阈值的文件放 `INLINE`，变大后单向转为 chunk 存储；`encrypt` 或 `consistency=strong` 生效的目录里新文件不 inline | core |
| `compress` | `none` / `lz4` / `zstd` / `zstd:1..=19` | Inherit | 算法已启用 | 新文件（layout 与 cap）；已有文件由协调器迁移 | transform、reconciler |
| `checksum` | `crc32c` / `xxh3` / `blake3` | Inherit | 算法已启用 | **新文件**；不迁移（理念文档写的是"新数据"：因为变换参数属于 layout，已有文件的新写入沿用它原来的算法，ADR-0021） | transform |
| `encrypt` | `none` / `tenant:<名字>`（存为数据密钥的数字 id） | Inherit | 数据密钥存在（`DATAKEY_NAME`）且属于设置者租户（admin 例外） | 新文件（`encrypt` 生效时新文件不 inline：inline 内容存在元数据库里、不加密）；已有文件由协调器迁移，已有 inline 文件被转成加密 chunk | transform（aes-gcm）、reconciler |
| `quota` | `<bytes>[,soft=<bytes>][,grace=<dur>][,files=<n>][,quantum=<bytes>]` 或 `off` | Stacked | soft ≤ hard；grace 需要 soft（缺省 7d）；quantum ∈ [1M, 1G]（缺省 64M） | 立即（下一次分配/提交即检查） | quota_admission（quota） |
| `quota.<class>` | 同上 | Stacked | 类存在 | 立即，只计该类的 per_class 用量 | quota_admission |
| `qos` | 存储类名（借用其 QoS 包络）或 `r=<n>,w=<n>,l=<n>[,bw=<bytes/s>][,burst=<dur>]`（`r/l` 单位是成本单位/秒，4.5） | Inherit（来源目录 = 队列键 qos_id） | 数值范围；`r ≤ l`（l ≠ 0 时）；`w ≥ 1` | 立即：集群图 ≤ 1 s 推到数据节点；已打开的文件按 cap 里的 qos_id 查最新规格 | qos（mclock） |
| `consistency` | `cto[,attr_ttl=<dur>]` / `strong`（`attr_ttl` 是子键，0–10 s，缺省 1 s，可继承） | Inherit | 枚举、范围 | 新打开的文件；目录项/属性 TTL 立即按新值回复（再受挂载参数 `--max-attr-ttl` 限制）；strong 目录里新文件不 inline | core（客户端 + 元数据） |
| `trash` | `off` / `<dur>`（≤ 365d） | Inherit | 范围 | 立即：之后的 unlink / 被覆盖的 rename 目标改为移入回收站 | core + bg（trash-expire） |
| `tier` | `<from>-><to>:<cond>`（也接受 `→`）；cond = `idle>Nd`（max(atime, mtime) 早于 N 天）/ `age>Nd`（btime）/ `size>N`，可用 `&` 连接 | Inherit | 两端类存在、条件可解析（`PolicyPoint::AttrValidate`） | 协调器（声明计算）+ tiering 任务 | bg |
| `ttl` | `off` / `<dur>`（按 mtime） | Inherit | 范围 | 协调器周期删除（ttl 任务） | bg |
| `acl.default` | `setfacl` 短文本，如 `u::rwx,g::r-x,o::---,u:1001:rwx,m::rwx` | Inherit | 能解析为合法 ACL | 新文件/目录：父目录没有 `system.posix_acl_default` 时，以它作为新对象的访问 ACL（目录同时作为默认 ACL）；有默认 ACL 时不应用 umask | core（继承）+ authz（posix-acl 判定） |
| `audit` | `off` / `on`（命名空间变更、setattr、xattr、属性变更）/ 过滤 `ops=create+unlink,uids=1000+1001`（首发不审计 open/read） | Inherit | 可解析 | 立即：写 changelog 时按 `PolicyPoint::AuditFilter` 计算 `audited` 标记；第一次启用时创建 `CURSOR(audit-log)`（3.8） | core（标记）+ event_sink（audit-log，进程外消费） |
| `locality` | `prefer-client` / `any` | Inherit | 枚举 | **新文件**选链（理念文档写的是"新数据"，原因同 checksum）；客户端读时立即优先本节点副本 | placement、客户端 |
| `inherit` | `false` | 只作用于本目录 | 只能是 false | 立即：解析到此目录时不再向上取 Inherit 键（Stacked 不受影响） | core |

**回收站细节**（`trash`）：回收站目录 `.wmfs-trash` 建在 `trash` 属性的**来源目录**下（第一次使用时创建，mode 1777，标 `F_TRASH_DIR`）；被删文件 rename 成 `<unix秒>-<ino>-<原名>`（超过 255 字节时截断原名），写 xattr `user.wmfs.origin = 原路径`，`inode.trash_expire_ms = now + 保留期` 并写 `TRASH_INDEX`；用户用普通 `mv` 移出回收站即恢复（rename 移出 `F_TRASH_DIR` 目录时清除 `trash_expire_ms` 并删除索引）；回收站里再删 = 真删；空目录的 rmdir 不进回收站（删除目录树时文件平铺进回收站，目录结构不保留，semantics ⑯）。

**"声明 vs 实际"**：每个属性的"实际"从哪来见 3.10 的表；协调器、tiering 与 `wmfs status` 用同一个函数 `declared(file) = resolve(主父目录) 再套用 tier 规则`。

---

## 6. 存储类与 QoS 四层执行（理念文档第 6 章）

### 6.1 存储类定义

`wmfs class create <name> --file class.toml`（TOML，不引入 YAML 依赖【ADR-0007】），存为 `CLASS` 记录并写 changelog：

```toml
name = "vssd-gold"
[devices]
label = "nvme"             # 匹配 target 的 labels（node.toml 的 [[target]] labels = ["nvme"]，部署参数）
dedicated = true           # 只用打了该标签、且不被其他类使用的 target
[redundancy]
replica = 2                # ec = "8+3" → 首发返回 FEATURE_UNSUPPORTED（二期）；目录上的 replicas 属性优先
[qos]
iops_min = 20000           # → mClock r（单位是"成本单位/秒"：cost = 1 + bytes/128KiB，4 KiB IO = 1、4 MiB = 33，4.5）
iops_max = 100000          # → mClock l
bw_min = "1G"              # 折算后加到 r
bw_max = "4G"              # 每队列字节令牌桶
burst = "30s"              # 桶深 = bw_max × burst
weight = 10                # → mClock w（缺省 1）
latency_target = "500us"   # 只测量与告警，不承诺（共享设备上不承诺尾延迟）
priority = "normal"        # normal | low；label 为 hdd 的类缺省 low：权重阶段先服务 normal 队列，没有可派发的才轮到 low（保底不受影响，4.5）
[placement]
failure_domain = "node"    # 首发只有 node
locality = "prefer-client"
[lifecycle]
tier_to = "vhdd-bulk"
when = "idle>30d"          # 等价于在绑定该类的目录上隐式设置 tier=vssd-gold->vhdd-bulk:idle>30d
```

`wmfs init` 预置 `default` 类（id 1，`label = "*"` 匹配任何 target，replica 3（minimal profile 为 1），priority normal，无 QoS 限制）。

**修改存储类** `wmfs class update <name> --file class.toml`（理念文档 6.2 第 4 层"类的变更触发后台迁移"）：
- 只改 `[qos]`（含 `priority`、`latency_target`）→ 只 `map_epoch += 1`，数据节点 ≤ 1 s 用上新份额，不迁移任何数据；
- 改 `replica` 或 `[lifecycle]` → 同批写一条 `RECONCILE_Q{dir: 1, reason: 3}`，协调器从根做一次整树协调（没单独设 `replicas` 的文件按新副本数迁移；新的分层规则开始生效）；
- 改 `label`、`dedicated`、`failure_domain` → **拒绝**（"请新建一个类，再把目录的 class 改过去，由协调器迁移"）——这样不需要"找出所有用这个类的链再换盘"这类复杂逻辑。
- `wmfs class show <name>` 同时显示 `iops_*` 换算成字节带宽的结果、按当前节点数均分后的每节点份额、以及由实际部署算出的隔离承诺（6.3）。

### 6.2 四层执行：本机能做到哪一步

| 层 | 理念文档要求 | 首发在本机做到哪一步 | 不承诺 / 推迟（理由） |
|---|---|---|---|
| 1 入口 | 每（租户, 存储类）一个 mClock 三元组 + 令牌桶；作业 id 参与分类 | **做**：每个数据节点按 `(tenant, qos_id, class)` 一个 mClock 队列 + 字节令牌桶，队列内按作业 id 轮转；三元组来自类的 `[qos]` 或目录 `qos` 属性；jobstats 指标 `wmfs_job_io_bytes_total{job}`（只保留前 100 个作业，其余记为 other） | ① 三元组按该类可用数据节点数静态均分，没有 dmClock 式跨节点协调：负载不均时总保底可能达不到、总上限偏保守（文档写明）；② 元数据请求首发不做 QoS（单 leader 串行执行，排队时间短）——【ADR-0010】 |
| 2 设备出口 | 每类一个 cgroup（io.weight / io.max）；同盘两类用 NVMe WRR | **不做**，只支持"每个存储类一个数据进程"的部署形态（一台机器可跑多个只带数据角色的 `wmfs server`，各自只挂某一类的 target），`docs/ops.md` 给出带 `IOWeight=` / `IOReadBandwidthMax=` 的 systemd unit 示例 | cgroup v2 的 io 控制器不是 threaded 控制器，同一进程的线程不能分属不同 io cgroup；本机用户 cgroup 只委派了 cpu/memory/pids（已检查），写 `io.max` 需要 root（未经人类同意不得 sudo）；Linux NVMe 驱动不提供按队列的 WRR 优先级——【ADR-0011】 |
| 3 硬件 | NVM Sets / FDP / 都不支持就整盘独占 | **只做"整盘独占"与检测**：`devices.dedicated = true` + 每块盘一个 target；target 启动时把目录的 `st_dev` 解析成**物理整盘或 namespace**（`core/base/devinfo.rs`，约 40 行纯 `std::fs`：读 `/sys/dev/block/<maj>:<min>`；是分区 → 取父设备；是 dm/md/LVM → 沿 `slaves/` 递归到底；loop 或解析失败 → unknown），上报 `phys_devs`，系统据此**计算**实际隔离状态；`wmfs doctor` 报告 `/sys/block/<dev>/queue/max_write_streams`（FDP/写流）与 namespace 数 | 本机单块系统盘 `max_write_streams = 0`（不支持 FDP，已检查）；NVM Sets 需要 NVMe admin 命令（root）——【ADR-0011】 |
| 4 协调器 | 类的变更触发后台迁移；配额与 QoS 违约进 dashboard | **做**：目录的 `class` 变化、或 `wmfs class update` 改了 `replica`/`lifecycle` → 协调器迁移；mClock 报告保底欠额（`wmfs_qos_reservation_deficit`，并经心跳汇总到 status）、类的 p99 延迟超过 `latency_target` → `PolicyViolation` 事件 → dashboard / alert | — |

### 6.3 诚实承诺（理念文档 6.3 [固定]，写进 semantics.md）

`wmfs status`、`wmfs class show` 与 dashboard 对每个类显示**由实际部署计算出的**隔离承诺，而不是用户声明：

| 实际情况（依据 target 上报的 `phys_devs`：物理整盘 / namespace，不是分区或 LV 的设备号） | 显示的承诺 |
|---|---|
| 该类所有 target 的 `phys_devs` 都非空（没有 unknown），且与其他类的 target 的 `phys_devs` **不相交** | `dedicated：份额 + 尾延迟（以设备能力为限）` |
| 有任何 target 与其他类共用一块物理盘/namespace（同一块 NVMe 的两个分区、同一个 PV 上的两个 LV 都算共用；本机必然如此），或有任何 target 是 unknown | `shared：只承诺份额，不承诺尾延迟`；若类声明了 `dedicated = true` → 同时产生 `isolation_downgraded` 告警 |
| `priority = low` 的类（label 为 hdd 的类缺省如此） | `只承诺容量、带宽份额与低优先级（mClock 权重阶段排在 normal 之后），不承诺延迟` |

系统**不会**按热度把随机 IO 自动挪到 NVMe（理念文档 8.1 禁止猜热度），需要随机 IO 的目录应显式绑定 NVMe 类。本机没有 HDD：可以定义 `label = "hdd"` 的类，但没有匹配的 target 时 placement 返回 `ENOSPC`（拒绝，而不是静默放到 NVMe）；功能测试时在同一块 NVMe 上建一个打 `hdd` 标签的测试 target，`status` 会如实标注"label hdd 位于 nvme 设备上（测试用途）"，性能数字没有意义。块设备（ublk 网关）首发不做。


---

## 7. 管理面

### 7.1 管理 API（替代 gRPC）【ADR-0005】

- 每个 `wmfs server` 在 `mgmt_port`（默认 9380；`wmfs dev` 下第 n 个节点为 `19n80`）上用 **tiny_http** 提供 HTTP/JSON；客户端（CLI、`wmfs mgr`）用 **ureq 3**。**每个端点的请求/响应结构都定义在 `wmfs-api` crate**（`types.rs`，只依赖 serde），core 的服务端与 `wmfs-mgr` 的 `ApiClient`（3.12）都用它，两边各有金样测试——字段名、类型、错误格式不靠猜。
- 为什么不用 gRPC：Rust 的 gRPC 实现要么基于 tokio（tonic），要么要编译 C++ 核心（grpcio），都违背"同步代码、零外部依赖"；HTTP/JSON 同步实现简单，新手可以直接 `curl` 看 `/metrics`。理念文档 [默认]"gRPC + 只读 REST"与 4.2"gRPC 管理 API + 事件总线" → 事件总线 = changelog 长轮询。
- **只有元数据 leader 回答依赖元数据的请求**；其他节点回 `409 {"error":"not_leader","leader":"http://<host:port>"}`，CLI 与 mgr 自动跟随（不做服务端代理，最简单）。节点本地的端点（`/metrics`、`/api/v1/health`、`/api/v1/node`、`/api/v1/trace/<id>`、`/api/v1/qos`、`/api/v1/failpoints`）每个节点都回答。`ApiClient` 接受多个地址：连不上就换下一个，遇到 409 就跟随。
- **认证**：`Authorization: WMFS-HMAC key=<id>,ts=<unix_ms>,sig=<hex>`，`sig = HMAC-SHA256(secret(key), METHOD ‖ "\n" ‖ path_and_query ‖ "\n" ‖ ts ‖ "\n" ‖ hex(sha256(body)))`，`|now − ts| ≤ 300 s`。写需要 admin 角色，读需要任一已认证 key；`/metrics` 与 `/api/v1/health` 免认证。错误统一为 `{"error": "<code>", "message": "<中文说明>"}`。**诚实说明**：服务端不记录见过的签名，所以 5 分钟窗口内被截获的写请求可以原样重放；超过 5 分钟就不行。首发只在可信网络中使用（ADR-0009）。
- **本机注意**：环境里设置了 `http_proxy`。所有内部 HTTP 客户端必须 `ureq::Agent::config_builder().proxy(None).build().into()`；演示命令里的 curl 一律加 `--noproxy '*'`（roadmap 的每终端准备步骤也设置了 `no_proxy`）。
- 实现注记（M1）：框架在 `core/src/mgmt/`（`http.rs`：tiny_http 监听与 4 个处理线程、路由；`auth.rs`：签名校验）；要签名的文本与请求头格式在 `wmfs-api` 的 `auth.rs`（服务端与 ApiClient 共用），hex 一律小写，两边各有同一组输入的签名金样。错误回复的 `error` 取错误码名的小写（`auth_failed`、`eacces`、`einval`、`enoent`、`not_leader`……）。`GET /api/v1/health` 的结构是 `wmfs_api::types::Health`：`{ok, version, cluster_id, node: {node_id, addrs, mgmt_addr}, meta_role, leader, audit_lag}`（M1：`meta_role = "none"`，`leader`、`audit_lag` 为 null）。fail point 接口另加了 **`GET /api/v1/failpoints`**（`wmfs failpoint list` 要用；读，任一已认证 key），GET/POST/DELETE 都回本节点当前的全部 fail point（`FailpointList`）；`failpoints_api = false` 时整组接口回 404。清除一个没设置过的名字回 `404 enoent`（拼错名字不会被静默忽略）；fail point 名字只能由字母、数字和 `.` `_` `-` 组成（环境变量、API 与 CLI 同一条规则，于是放进 `?name=` 不用转义）。**请求体**：先看请求头里声明的长度——超过 1 MiB 的请求既不读、也不回、也不交还给 tiny_http（整个"忘掉"，这条连接一直挂着）：tiny_http 0.12 丢弃没读完的 body 时会按剩余长度**一次分配内存**，`Content-Length` 写成几十 TB 就能让整个进程因分配失败而退出（读到一半对端断开也一样）。剩余风险（写进这里，不另做）：声明了 body 却不发的连接会一直占着一个处理线程——tiny_http 没有读超时，回复之后丢弃请求时也要把 body 读完，所以"读 body 之前先验签"挡不住它；首发只在可信网络中使用（ADR-0009）。
- 实现注记（M3）：health 的 `meta_role`：`"leader"` = **就绪**的 leader（已当选、本任期的空条目已应用、插件清单一致，可以服务请求——`wmfs dev wait leader` 等的就是它）；`"candidate"` = 正在选举，或已当选但还没就绪（新增的取值）；`"follower"`、`"learner"`。`leader` = 当前 leader 的地址（是自己就用本节点实际监听的地址，否则取已应用的 NodeRecV1；还不知道时为 null）。

| 方法 路径 | 作用 |
|---|---|
| `GET /metrics` | 本节点 Prometheus 文本（免认证） |
| `GET /api/v1/health` | 免认证：本节点健康、角色、版本、**本节点与 leader 的 `addrs`（rpc）和 mgmt 地址**——`wmfs fs`、`wmfs ping`、`wmfs mount` 用它从 mgmt 地址发现 rpc 地址；另含审计消费者落后条数 |
| `GET /api/v1/node` | 本节点 target、boot_id、内存池、线程数 |
| `GET /api/v1/cluster` | 集群概览：节点、元数据 leader/term/applied/voters/learners、target、链、会话数、集群警告（`ClusterMap.warnings`） |
| `GET /api/v1/chains` · `GET /api/v1/sessions`（含 24 h 内的 EVICTED 记录）· `GET /api/v1/tasks` · `GET /api/v1/classes` · `GET /api/v1/keys` · `GET /api/v1/features` | 列表 |
| `GET /api/v1/attr?path=&recursive=0\|1` · `POST /api/v1/attr {path, spec: "<原始字符串>"}`（服务端切分、解析、规范化，回 `{set: {k: v}, warnings: [...]}`）· `DELETE /api/v1/attr?path=&key=` | get / set / unset |
| `GET /api/v1/usage?path=` | 递归用量与配额（常量时间） |
| `GET /api/v1/status?path=&limit=` | 声明 vs 实际：`{warnings, attrs: [{key, declared, source, effect, actual, mismatched}], …}`（扫描，带上限，3.10） |
| `GET /api/v1/semantics` | 一致性等级、放松清单要点、当前集群参数 |
| `POST /api/v1/classes`（body = TOML）· `PUT /api/v1/classes/<name>`（update，6.1 的规则）· `DELETE /api/v1/classes/<name>` | 存储类 |
| `POST /api/v1/tasks/<kind>/{pause,resume,run}` · `POST /api/v1/tasks/<kind>/set {bw, items}` | 后台任务 |
| `POST /api/v1/sessions/<id>/evict` | 手动驱逐客户端（走两阶段） |
| `POST /api/v1/keys {name, role, tenant, uids, paths, paths_ro, root_squash}` · `POST /api/v1/keys/<id>/revoke` · `POST /api/v1/datakeys {name, tenant}`（回 `{id}`） | 认证 key（返回一次性的 key 文件内容）/ 数据加密 key |
| `GET /api/v1/changelog?after=&limit=&wait_ms=` · `POST /api/v1/changelog/cursor {name, seq}` · `GET /api/v1/changelog/cursor?name=` | changelog 流与消费者游标（同时最多 2 个长轮询，3.11） |
| `GET /api/v1/trace/<id>` · `GET /api/v1/trace/recent?n=` | 本节点关于该 trace 的 span / 最近的请求 |
| `POST /api/v1/nodes/<id>/{drain,remove,promote}` · `POST /api/v1/chains/<id>/force-serving?target=` | 运维操作（force-serving 需在 body 里带 `{"i_accept_data_loss": true}`） |
| `POST /api/v1/cluster/settings {name: value}` | 集群运行参数（3.2.8） |
| `GET /api/v1/cluster/plugins` · `POST /api/v1/cluster/plugins {add: [...], remove: [...]}` | 集群插件清单（3.7.2；修改后各节点需要用新配置重启） |
| `POST /api/v1/features/<name>/enable` | 启用 feature flag（不可逆） |
| `GET /api/v1/qos` | 本节点 QoS 队列快照 |
| `POST /api/v1/failpoints {name, action}` · `DELETE /api/v1/failpoints?name=` | 仅 `failpoints_api = true` 时存在（测试；M1 起就有） |

### 7.2 CLI 命令清单（单二进制 `wmfs`；理念文档的 `fs` 改名为 `wmfs`【ADR-0006】）

| 组 | 命令 |
|---|---|
| 部署 | `wmfs init …`（3.4.3）· `wmfs join …` · `wmfs server --data-dir D [--adopt-local-plugins]` · `wmfs mount <rpc 地址[,地址…]> <挂载点> [--key-file F] [--max-attr-ttl 10s] [--cache memory[:SIZE]\|local-nvme:DIR:SIZE\|none] [--transport tcp\|rdma] [--op-timeout 120s] [--job-id-var SLURM_JOB_ID]`（挂载成功后打印集群警告）· `wmfs umount <挂载点>`（封装 `fusermount3 -u`） |
| 策略（理念文档 5.1） | `wmfs set <path> k=v[,k=v…]`（打印服务端回的规范化结果与警告）· `wmfs get <path> [-r] [key]` · `wmfs unset <path> <key>` · `wmfs status [path]`（无参数 = 集群健康；第一行是集群警告）· `wmfs usage <path>` |
| 集群 | `wmfs class create\|update\|list\|show\|delete` · `wmfs node list\|drain\|remove\|promote` · `wmfs chain list\|show\|force-serving` · `wmfs cluster set <名>=<值>` · `wmfs cluster plugins show\|add\|remove <钩子:名字>` |
| 运维 | `wmfs task list\|show\|pause\|resume\|run\|set` · `wmfs session list\|evict` · `wmfs key create\|list\|revoke` · `wmfs datakey create <名字>`（打印数字 id）· `wmfs feature list\|enable` |
| 观测 | `wmfs changelog tail [-n N] [-f]` · `wmfs trace <trace_id>` · `wmfs trace --recent [N]`（列出各节点最近的请求及其 trace_id、操作、耗时）· `wmfs failpoint set\|clear\|list <node> …` |
| 进程外插件 | `wmfs mgr --cluster <mgmt 地址[,…]> --listen 127.0.0.1:9500 [--dir D] [--config mgr.toml]`（手动启动；`--dir` 默认取环境变量 `WMFS_MGR_DIR`，`wmfs dev env` 把它设为 `~/wmfs-dev/mgr`；审计日志写在 `<dir>/audit/`；dashboard 的 token 同时写进 `<dir>/token`） |
| 开发与体检 | `wmfs dev up [--nodes N] [--dir D] [--profile P] [--rdma IP]`（`--profile nas-safe` 在集群起好后再起一个 mgr，等同 `wmfs dev mgr`）· `wmfs dev down [--purge]` · `wmfs dev env\|status\|leader\|logs <n>` · `wmfs dev kill <n>`（SIGKILL）· `wmfs dev stop <n>`（SIGTERM）· `wmfs dev start <n>` · `wmfs dev restart <n> [--dylib F]…` · `wmfs dev restart-all [--dylib F]…`（用给出的列表改写 node.toml 的 `[plugins] dylib` 再重启；不给 `--dylib` = 清空列表）· `wmfs dev pause\|resume <n>`（SIGSTOP/SIGCONT）· `wmfs dev mount <挂载点> [--node n]（客户端冒充该节点所在的「主机」，供 locality 演示）[--key-file F] [--cache …]` · `wmfs dev umount <挂载点>` · `wmfs dev client-pid <挂载点>` · `wmfs dev mgr`（后台起一个 mgr，等它就绪后打印带 token 的 dashboard 网址）· `wmfs dev wait <条件> [--timeout 120]` · `wmfs doctor` · `wmfs ping <scheme://地址> [--count N] [--bulk 4M] [--key-file F]` |
| 调试客户端（不经 FUSE） | `wmfs fs [--as-uid UID[:GID]] ls\|stat\|mkdir\|rmdir\|rm\|mv\|ln [-s]\|touch\|cat\|put\|get\|truncate …`（`--as-uid` 只允许 admin key，用来在本机演示多用户权限，因为 `/etc/fuse.conf` 没开 `user_allow_other`） |
| 调试 | `wmfs debug kv put\|get\|bench`（需服务端 `debug_ops`；bench 用 `RpcClient::start` 保持 64 路并发）· `wmfs debug chunk-path <path> <idx> [--node n]` · `wmfs debug chunk-header <文件>` · `wmfs debug corrupt-chunk <path> <idx> --node n [--block b]`（按块表找到第 b 块的存储字节、翻转其中一个字节——演示与验收里制造坏块只用它，不手算偏移）· `wmfs debug rstat-check <data_dir>` · `wmfs debug meta-hash` · `wmfs debug gen-key --out F`（生成一把集群不认识的 key，演示认证失败） |

- **集群地址**：`--cluster` 与环境变量 `WMFS_CLUSTER` 接受**逗号分隔的多个 mgmt 地址**，CLI 依次尝试（连不上换下一个，409 就跟随）；`wmfs dev env` 导出全部节点的地址，所以杀掉任何一个节点（包括 leader）之后 CLI 照样能用。需要 rpc 地址的命令（`wmfs fs`、`wmfs mount`）先向某个 mgmt 地址的 `/api/v1/health` 要。
- **`<path>` 换算**：可以是本机 wmfs 挂载点下的路径——CLI 读 `/proc/self/mountinfo`，找到类型 `fuse.wmfs` 的挂载（FSName = `wmfs:<cluster_id 前 8 位 hex>`，不含逗号，因为 fusermount 用逗号分隔挂载选项），换算成文件系统内路径，再从挂载根目录的**虚拟 xattr `user.wmfs.mgmt`**（客户端本地应答，值为逗号分隔的当前 mgmt 地址；只在根 inode 上、只读、不出现在 listxattr 里）取得集群地址；也可以是文件系统内绝对路径加 `--cluster`。admin key 默认读环境变量 `WMFS_KEY_FILE`，否则 `~/.config/wmfs/admin.key`。`wmfs dev env` 打印这些 `export`，演示里用 `eval "$(wmfs dev env)"`。
- **`--json`**：所有查询类命令（`status`、`get`、`usage`、`node/chain/session/task list`、`changelog tail`、`dev status`）都支持 `--json`，输出就是管理 API 的 JSON；`scripts/accept.sh` **只解析 JSON**，不解析给人看的输出。
- **`wmfs dev wait <条件>`**（替代演示里的固定 `sleep`；条件满足立即返回 0，超时返回 1 并打印当前状态）：`leader`（有就绪的元数据 leader）、`chains-serving`（所有链长度达到期望且全部 Serving）、`task-idle <kind>`（该任务这一轮做完且积压为 0）、`session-evicted <挂载点>`、`alert <名字> firing|resolved`（查 mgr 的 `GET /alerts`）、`audit-caught-up`（`audit-log` 游标追上最新 changelog）。
- **`wmfs dev down --purge` 的顺序**（被冻住或崩溃的挂载点上 `fusermount3 -u` 会 EBUSY 或卡住）：对所有开发客户端进程依次 `SIGCONT` → `SIGTERM` → 3 s 后仍在则 `SIGKILL` → 对每个开发挂载点 `fusermount3 -uz`（lazy 卸载）→ 停服务端 → 最后删除开发目录。`wmfs doctor` 发现残留的 `fuse.wmfs` 挂载时给出这条命令。
- 实现注记（M1）：① `wmfs dev` 基础版只有 `up/down/env/status/kill/start/logs`，`up` 只支持 `--nodes 1`（多节点要 `wmfs join`，M8）；`--dir` 对所有 `dev` 子命令都有效，默认取环境变量 `WMFS_DEV_DIR`、否则 `~/wmfs-dev`，`wmfs dev env` 也导出 `WMFS_DEV_DIR`；节点在自己的进程组里后台运行，stdout/stderr 追加到 `<节点>/logs/server.log`；`up`/`start` 等到该节点的 health 有响应（且是本集群）才返回，进程退出或 30 s 超时则打印日志路径、非 0 退出；只对 pid 文件里、命令行确实是"`wmfs server --data-dir <该节点>`"的进程发信号；`down --purge` 只删除默认的 `~/wmfs-dev` 或带 `.wmfs-dev` 标记文件的目录（防止 `--dir` 写错删掉别的目录）；"就绪" = health 有响应**并且** pid 文件里就是刚起的那个进程（server 在管理 API 开始应答之前写 pid 文件），所以 `up` 之后紧接着的 `status`/`down` 一定找得到它；`down` 先发 SIGCONT（被冻住的节点也能处理 SIGTERM）、5 s 后仍在才 SIGKILL，并替被 SIGKILL 的进程删掉 pid 文件；开发目录一律换成规范的绝对路径（`./devC`、`/tmp//devC/`、符号链接都认得出是同一个）；起不来时把日志的最后一行也打印出来。② `wmfs failpoint <node>` 的 `<node>` 是节点号：依次问 `WMFS_CLUSTER`（或 `--cluster`）里每个地址的 health 找到它；也可以直接写管理地址。③ `wmfs ping` 另有 `--role admin|client|server` 与 `--json`。④ `wmfs debug gen-key` 生成的是 key 2（默认 client 的号）+ 随机秘密：号对、秘密不对，演示的是"证明不对"被拒。
- 实现注记（M3）：① `wmfs dev wait <条件…> [--timeout 秒]`（默认 120）：M3 只有 `leader`（某个节点的 health 报告 `meta_role = "leader"`），每 100 ms 问一遍开发目录里的每个节点；不认识的条件直接报错。② `wmfs debug kv put <键> <值>`、`get <键>`（不存在 → 退出码 1）、`bench [--n 100000]`（64 路并发、值 32 字节，都是常量），都有 `--json`（put：`{ok}`；get：`{key, found, value}`；bench：`{n, concurrency, ok, retries, elapsed_ms, ops_per_sec, batches, avg_batch}`）；先问 `--cluster`/`WMFS_CLUSTER` 的 health 找到 leader 的 rpc 地址（最多 30 s；自称 `candidate` 的节点再等一会儿，免得第一个请求碰上 `NOT_READY`），按 3.3.8 重试（总期限 30 s）；键值按 UTF-8 文本处理。bench 写 `bench-<8 位序号>`，平均批大小取 leader `/metrics` 里 `wmfs_meta_batch_ops` 直方图的 `_sum`/`_count` 增量（压测期间 `wmfs_raft_term` 变了 = leader 重启或换过、计数器重新开始，就把 `batches`/`avg_batch` 报为 0 并说明"不可用"）；可重试的失败按 3.3.8 退避，但故障时一起失败的那一批在途请求只退避一次（只有上次加大退避之后才发出的请求失败了才再加倍），退避期间不发新请求。`dev wait` 的 `--timeout` 换算成毫秒时饱和，不回绕。
- 实现注记（M4）：
  - ① `wmfs fs [--as-uid UID[:GID]] [--cluster A] [--key-file F] [--json] [--verbose] ls [-l]|stat|mkdir|rmdir|rm|mv|ln [-s]|touch <路径>…`（`cli/src/fs.rs`；`cat/put/get/truncate` 在 M5）：每条命令问 health 找到 leader → `MetaClient::connect`（GetClusterMap + SessionOpen）→ `ResolvePath` → 元数据请求 → SessionClose；单次 RPC 超时 10 s、总期限 30 s。
  - ② 身份默认取本进程的 uid/gid/附加组与 umask（读 `/proc/self/status`）；`--as-uid` 只允许 admin key（CLI 先拒绝，服务端对 client key 另按 uid 范围检查）。`mv` 等同 rename(2)；`touch` = O_EXCL 的 Create，已存在就把时间改成现在；`mkdir`/`touch` 的权限是 0777/0666 & ~umask。
  - ③ `--json`：成功时 `{ok, ino}`，`stat` 是属性 + `layout` + `chains`（每条链的 id、声明副本数、成员 target），`ls` 是 `{entries: [{name, ino, kind, mode, nlink, uid, gid, size, mtime_ms}]}`；失败时退出码 1、输出 `{error: "<错误码名>", errno, message, path}`。发请求之前的失败也输出 JSON（没有 `path`）：参数或密钥文件写错 `EINVAL`、client key 加 `--as-uid` 是 `EACCES`、30 s 内找不到元数据 leader 是 `ETIMEDOUT`、认证失败 `AUTH_FAILED`。`--verbose` 把重试（例如"超时重试 seq=… → 去重命中"）打印到 stderr。
  - ④ `wmfs init --target PATH[:label,label]`（3.4.3 的注记）；`wmfs init` 与 `wmfs dev up` 的输出里有集群警告。`wmfs dev up` 起好节点后取不到集群图（节点没有成为就绪的 leader——多半是旧里程碑建的开发集群、插件清单不一致）就打印原因与修法（`wmfs dev down --purge` 之后重新 `wmfs dev up`）并以非 0 退出；server.log 里插件清单不一致的错误、CLI 找 leader 超时（节点一直是 `candidate`）的报错也给出同样的修法。
  - ⑤ `wmfs debug kv` 与 `wmfs fs` 共用 `cli/src/leader.rs`（问 health 找 leader）。

### 7.3 `/metrics`（节选，前缀 `wmfs_`）

| 类别 | 指标 |
|---|---|
| RPC / 传输 | `rpc_requests_total{family,op,code}`、`rpc_latency_seconds{family,op}`（直方图）、`rpc_inflight`、`rpc_denied_total{role,family}`、`conn_write_timeout_total`、`conn_send_queue_full_total`、`tcp_bytes_total{dir}`、`rdma_ops_total{op}`（send/recv/read/write）、`req_dropped_total`、`transport_fallback_total{from,to}`（服务端之间数据族退回 control_scheme） |
| Raft / 元数据 | `raft_term`、`raft_is_leader`、`raft_commit_index`、`raft_applied_index`、`raft_persist_seconds`、`raft_msg_dropped_total`、`meta_ops_total{op,code}`、`meta_batch_ops`（直方图）、`meta_batch_bytes`（直方图）、`meta_commit_seconds`、`meta_batch_stale_total`、`meta_read_gate_seconds`、`meta_sessions`、`changelog_consumer_lag{name}` |
| 数据 | `chunk_read_bytes_total{target}`、`chunk_write_bytes_total{target}`、`chain_members{chain,state}`、`chains_degraded`、`chains_unavailable`、`target_used_bytes{target}`、`target_capacity_bytes{target}`、`craq_dirty_reads_total`、`craq_redrive_total`、`craq_need_full_total`、`resync_bytes_total`、`chunkstore_op_seconds{op}` |
| QoS | `qos_queue_depth{tenant,class}`、`qos_served_total{tenant,qos_id,phase}`（phase = reservation/weight）、`qos_wait_seconds{tenant,class}`、`qos_reservation_deficit{tenant,qos_id}`、`job_io_bytes_total{job}` |
| 配额 | `quota_used_bytes{dir}`、`quota_limit_bytes{dir,kind}`、`quota_denied_total{dir}`、`quota_grants_total` |
| 后台 | `task_items_total{task}`、`task_bytes_total{task}`、`task_backlog{task}`、`task_progress_ratio{task}`、`declared_mismatch_files{attr}`、`migration_skipped_open_files` |
| 插件 | `hook_calls_total{hook,plugin}`、`hook_seconds{hook,plugin}`、`hook_over_budget_total{hook,plugin}`、`hook_errors_total{hook,plugin}` |
| 基座 | `bufpool_free{class,lane}`、`bufpool_wait_seconds`、`failpoint_hits_total{name}`、`clock_skew_seconds`、`slow_requests_total{family}`、`threads` |

### 7.4 dashboard、alert、event_sink 的运行方式

dashboard、alert、audit-log、webhook、prometheus-exporter 都在 `wmfs mgr` 进程里（理念文档 4.2：进程外、只能通过管理 API / 指标流 / 事件流接触系统；崩溃不影响数据路径；`wmfs-mgr` crate 不依赖 core，边界由编译期保证）。**mgr 只手动启动**：server 不拉起它，也没有"跟着 leader 漂移"的逻辑——生产环境用 systemd 跑一个实例（`docs/ops.md` 给出 unit 示例，`Restart=on-failure`），开发环境用 `wmfs dev mgr`（`wmfs dev up --profile nas-safe` 会自动调用它，等同测试框架在集群起好之后启动一个 mgr）。同一集群只运行一个 mgr；游标存在 KV，mgr 重启后从游标继续——至少一次投递，事件带 seq 便于下游去重。五个插件合在一个进程是为了少一个要运维的进程【ADR-0017】，需要时可以拆开。

---

## 8. 客户端（FUSE）

### 8.1 挂载与内核能力

- `wmfs mount <rpc 地址[,地址…]> <挂载点>`：依次尝试这些地址，用 client key 握手 → `NodeOp::GetClusterMap`（client 角色得到**过滤后的**集群图：不含 keys、datakeys）→ 检查 `--transport` 指定的 data_scheme 每个数据节点都公布了（没有就报错并列出缺的节点）→ 与元数据 leader 及各数据节点建立 RPC 连接（每个 server 一条连接，请求复用）→ `SessionOpen` → 打印 `ClusterMap.warnings`（例如"default 类声明 3 副本，只有 1 个故障域：新文件实际为 1 副本"）。之后集群图经会话心跳的 `map_epoch` 刷新。
- fuser 0.18（`default-features = false`，经 `fusermount3` 非 root 挂载，已在本机实测）：`fuser::Config` 是 `#[non_exhaustive]`，只能 `Config::default()` 后改字段：`n_threads = Some(16)`、`clone_fd = true`、`mount_options = [FSName("wmfs:<cluster_id 前 8 位 hex>"), Subtype("wmfs"), DefaultPermissions]`（FSName 不能含逗号），`acl` 保持默认 `Owner`（本机没有 `user_allow_other`）。`Session::new(fs, 挂载点, &cfg)` 会拿走 fs，所以 notifier 事后塞进去：`fs` 持有 `Arc<OnceLock<Notifier>>`，`slot.set(session.notifier())` 后 `session.run()`。SIGTERM → 刷出全部脏数据 → 卸载。
- `Filesystem::init(&mut self, _req, cfg)`：`cfg.add_capabilities(want & cfg.capabilities())`，`want = FUSE_POSIX_LOCKS | FUSE_FLOCK_LOCKS | FUSE_POSIX_ACL | FUSE_DONT_MASK | FUSE_DO_READDIRPLUS | FUSE_READDIRPLUS_AUTO`；`set_max_write(1 MiB)`、`set_max_readahead(1 MiB)`、`set_max_background(64)`（签名已编译验证；`InitFlags::FUSE_DONT_MASK` 已在 fuser 0.18 源码确认；其余方法取 `&self`，ino/fh/owner 是 `INodeNo`/`FileHandle`/`LockOwner` 新类型）。
- `DefaultPermissions + FUSE_POSIX_ACL`：内核按缓存的属性与 ACL 先做一遍本地检查（包括路径遍历的执行位——否则同机其他用户可能借助内核 dentry 缓存绕过遍历检查）；**服务端 authz 仍是权威**（客户端不可信）。**umask**：fuse 总会给超级块置 `SB_POSIXACL`，所以 VFS 不剥 umask；但 fuse 的 create/mknod/mkdir 在没有协商 `FUSE_DONT_MASK` 时会自己 `mode &= ~umask`——那样父目录有默认 ACL 时服务端就拿不到原始 mode，没法按 POSIX"有默认 ACL 时不应用 umask"处理。所以必须协商 `FUSE_DONT_MASK`，由服务端决定：父目录有 `system.posix_acl_default`（或 `acl.default` 属性生效）→ 按 ACL 规则计算、不应用 umask；否则 `mode & !umask`（`umask` 是 fuser 回调参数，`MetaOp::Create/Mkdir/Mknod` 里已有）。M6 用一个约 20 行的 spike 打印内核传来的 mode/umask，实测确认。
- 根目录上的虚拟 xattr `user.wmfs.mgmt`：getxattr 由客户端本地应答（逗号分隔的当前 mgmt 地址），给 CLI 的路径换算用（7.2）；setxattr/removexattr 它 → `EPERM`；不出现在 listxattr 里。
- **故意不开**：`FUSE_WRITEBACK_CACHE`（开了以后内核在页缓存里吸收 write()，守护进程要到回写时才看到数据，`EDQUOT` 只能在 close/fsync 时报告，且内核掌管 i_size，与"长度由元数据提交"冲突）；`FUSE_DIRECT_IO_ALLOW_MMAP`（不开时 direct_io 文件上的任何 `MAP_SHARED` 被内核以 `ENODEV` 拒绝——诚实拒绝首发不支持的跨节点共享可写 mmap）；`FUSE_ATOMIC_O_TRUNC`（不开时，内核对 `open(O_TRUNC)` **先发 OPEN（flags 里去掉了 O_TRUNC），之后再发 SETATTR(size=0，不带 fh)**；`Open` 因此保持只读请求，而 SetAttr 的回复是 `Opened(OpenOut)`，带新 epoch 的 layout 与 cap，客户端用它更新 InodeState 和该 inode 的所有句柄——所以紧接着的写直接进新 epoch，不会白写一轮旧 epoch，3.1.4 C4）；passthrough 与 FUSE-over-io_uring（8.8）。

### 8.2 客户端状态（全部在进程内存里，重启即清空）

- `inodes: Mutex<HashMap<ino, Arc<Mutex<InodeState>>>>`，`InodeState { attr, seen_change_id, layout, cap, consistency, inline: Option<Vec<u8>>, quota_end, dirty: DirtyFile, write_err: Option<ErrCode>, writers: u32 }`。layout/cap 在 InodeState 里只有一份，所有句柄共用——`SetAttr` 改 size 的回复、`STALE_LAYOUT` 后的重新 `Open` 都直接更新它。
- `handles: Mutex<HashMap<fh, OpenFile { ino, flags, strong, direct, write }>>`；目录句柄 = 名字分页的只读快照（`readdir` 的 offset = 快照下标，POSIX 允许 readdir 反映某个快照）。
- `lock_waiters: Mutex<Vec<LockWaiter { reply: ReplyEmpty, ino, owner, start, end, kind, pid, next_try, backoff }>>`：被挂起的 F_SETLKW（8.6；fuser 的 `ReplyEmpty` 是 `Send + 'static`，可以交给别的线程回复）。
- 集群图缓存、leader 地址、会话（id、seq 计数器、在途修改型请求 ≤ 256）。

### 8.3 cto 缓存

| 缓存 | 位置 | 失效规则 |
|---|---|---|
| dentry / 属性 | 内核 | 回复 TTL = `min(AttrOut.ttl_ms, --max-attr-ttl)`：cto = 目录属性 `attr_ttl`（默认 1 s），strong 子树 = 0（C1） |
| inode 状态（属性、layout、cap、change_id、consistency） | 守护进程 | open 时总是向 leader 取新值（C2）；改 size 的 SetAttr 回复也更新它（C4）；cap 用到 80% 时 `RenewCap` |
| 文件数据页 | 内核页缓存 | open 时 change_id 不变才回 `FOPEN_KEEP_CACHE`，变了先 `inval_inode`（C2）；加锁时 `inval_inode`（C3） |
| 块缓存 | cache_store（memory / local-nvme / none） | 键含 change_id；`invalidate_ino` |
| 写回缓冲 | 守护进程 | 8.4 |
| 集群图（链、target、被驱逐会话） | 守护进程 | 心跳的 `map_epoch`、`STALE_CHAIN`/`NOT_SERVING`（C6） |

open 流程：`Open(ino, flags)`（只读请求，不写 raft）→ 得到最新属性、layout、cap、consistency、`quota_end`（写打开时）、inline 小文件的完整内容 → C2 → consistency = strong 或 `O_DIRECT` → 回 `FOPEN_DIRECT_IO`。**cto 下的 `O_DIRECT` 写与 strong 写走同一条同步路径**（`core/client/strong.rs`）：每次 write 先同步 `WriteChunk` 写链，再同步 `CommitWrite`；遇到 `STALE_LAYOUT` 用本次 write 回调里仍持有的数据按新 layout 重写。原因：如果 O_DIRECT 数据写进链后不在本地留副本、长度又要等 close 才提交，协调器在这期间切换 layout（打开集合最多滞后 3 s）就会让这批写悄悄丢失。测试：O_DIRECT 写的同时触发迁移，校验内容不丢。relatime：`atime_due = true` 时把 `(ino, now)` 放进 atime 队列，`atime` 线程每 5 s 批量发 `SetAtime`（尽力而为）。read：本 inode 的写回缓冲（读自己的写）→ inline → cache_store → 数据节点（按 1 MiB 对齐读，放进 cache_store）；副本选择 `codec.read_order`；`CHUNK_DIRTY` → 改问 last serving；`CHUNK_NOT_FOUND` → C5；`CHECKSUM_MISMATCH`/连接错误/`FENCED` → 下一个副本；`STALE_CHAIN` → 刷新集群图。结果截断到 `max(元数据 size, 本地脏区末尾)`。

### 8.4 写回与配额

- 每个 inode 一个 `DirtyFile = BTreeMap<chunk_idx, DirtyChunk { buf: Buf, extents: Vec<(u32, u32)> }>`；`write()` 把 `&[u8]` 拷进该 chunk 的 Buf（`chunk ≤ 128 KiB` 用 S 档，否则 M 档或 L 档，按 chunk_size）并合并 extent。上限按槽位计（3.6.1）：每挂载最多占 M/L 档槽位的 60%、每 inode 最多 16 个槽位，超过则 write() 阻塞等 flusher（背压），剩下的槽位留给读路径。
- **配额**：`quota_end != u64::MAX` 时，若 `offset + len > quota_end` → 先刷该 inode 的脏数据，再发 `CommitWrite{new_size, want_quota: true}`，用回复里新的 `quota_end`；仍不够 → 这次 `write()` 立即返回 `EDQUOT`（3.9）。
- **flush 触发**：某 chunk 脏数据已覆盖整个 chunk → 立即交给 flusher；脏数据存在超过 5 s；总量超阈值；`flush`（每次 close）；`fsync`；加锁/解锁前；进程退出。
- **flush 算法**：每个脏 chunk 一个 `WriteChunk{chunk, chain_ver, extents, sync: O_SYNC?}` 发往该链 head，bulk = 各 extent 对应的 `BufSlice`（同一个冻结的 Buf，不拷贝）；flusher 的 4 个写线程并发调用 `DataClient::write`（同一 chunk 同一时刻只有一个在途）→（fsync 时）`SyncChunks` → `CommitWrite{epoch, new_size = 已写最大偏移, mtime, close}`；**`CommitWrite` 成功后**才从脏区移除这些数据。
- `STALE_LAYOUT` → 重新 `Open` 取新 layout → 把仍在脏区里的数据按新 layout 重写 → 再次 `CommitWrite`（C4）。
- 失败处理：可重试错误在 `--op-timeout` 内重试；彻底失败时**保留脏数据并记下错误**，下一次 `write/flush/fsync/close` 返回该错误（类似内核的 errseq），不会悄悄丢弃。
- **inline**：layout 是 inline 时，文件最终大小 ≤ `layout.inline_max` → `CommitWrite` 携带 `inline_patches`（按区间打补丁，两个客户端写不相交区间互不覆盖；补丁越过当前长度时服务端中间补 0）；超过时 → `GetInline` 取最新内容 → 与脏数据合并写进 chunk（create 时链已分配好）→ `CommitWrite{uninline_expect_change_id}`（CAS 失败则重来）。
- 截断：`setattr(size=s)` → 先丢弃脏区里 s 之后的数据并刷出其余 → s == 0：直接 `SetAttr{size:0}`（服务端换 epoch 并按当前属性重建 layout）；s > 0：对受影响的 chunk 发 `TruncateChunk`（3.4.2.9；inline 文件没有 chunk，跳过这一步）后 `SetAttr{size: s}`。两种情况的回复都是 `Opened(OpenOut)`，用它更新 InodeState。

### 8.5 strong 模式

- open 回 `FOPEN_DIRECT_IO`，该子树的 entry/attr TTL = 0，不进 cache_store，不写回。strong 目录里新建的文件不 inline（新 layout 的 `inline_max = 0`）；**以写方式打开一个已有的 inline 文件时，先走 8.4 的 uninline 流程**（`GetInline` → 写进 chunk → `CommitWrite{uninline_expect_change_id}`），之后一律按 chunk 写——所以 strong 的写路径、`AppendReserve` 永远不会遇到 F_INLINE 文件（协调器也会把 strong 目录里残留的 inline 文件转成 chunk，3.10）。
- read：先 `GetAttr`（线性一致，拿 size 与 epoch；epoch 变了先刷新 layout）→ 按 CRAQ 规则读 chunk（只读打开的 inline 文件：`GetInline`，同样是线性一致读）。
- write：同步 `WriteChunk` 到链 → 同步 `CommitWrite`（校验 epoch、更新 size/mtime）→ 才返回；`STALE_LAYOUT` → 重新 `Open`，用本次 write 回调里仍持有的数据按新 layout 重写。代价：每次 write 一次元数据提交（本机 fsync 约 2–5 ms），写进文档；适合"协调为主"的负载（日志、共享状态），不适合大量小块随机写。
- `O_APPEND`：fuser 的 `write()` 能看到打开标志；strong 下忽略内核给的 offset，改为 `AppendReserve(ino, epoch, len)` 拿到 offset → 写数据 → `CommitWrite`；同一 inode 的追加在本客户端串行（一个 Mutex）。cto 下 `O_APPEND` 由内核用其缓存的大小计算偏移，跨节点不保证顺序（理念文档 8.2 原文）。M12 验收包含"strong 目录里两个挂载点并发追加同一个小文件"。
- 共享可写 mmap：内核返回 `ENODEV`（8.1）。

### 8.6 锁

- `getlk` → `GetLock`；`setlk(sleep = false)` → 一次 `SetLock`（冲突回 `EAGAIN`）。end 一律先归一（≥ `i64::MAX` → `LOCK_EOF`，3.5.3）。
- **`setlk(sleep = true)`（F_SETLKW）不在 FUSE 工作线程上等**：先试一次 `SetLock`；冲突 → 把 `ReplyEmpty` 连同 `(ino, owner, start, end, kind, pid)` 放进 `lock_waiters` 后**立即返回**（fuser 线程空出来，持锁进程的解锁请求照样有线程处理——否则 16 个进程同时等同一把锁就能把整个挂载点卡死）。`lock-wait` 线程按退避（50 ms 起、×2、上限 1 s）重试 `SetLock`；每次重试前读 `/proc/<pid>/status`（pid 是发起请求的线程）：进程已不存在，或 `SigPnd`/`ShdPnd` 里有 SIGKILL（任何致命信号都会让内核给它挂上 SIGKILL；这个进程正卡在内核里**等我们的回复才能退出**）→ 立即 `reply.error(EINTR)` 并丢弃这个等待者，绝不把锁授给正在死去的进程；拿到锁 → `inval_inode(ino, 0, 0)`、丢弃属性缓存 → `reply.ok()`；非 `EAGAIN` 的错误 → `reply.error(..)`。
- 解锁前先 flush 该 inode；`flush(lock_owner)` → 释放该 owner 在该文件上的 POSIX 锁；`release` 带 `lock_owner`（`FUSE_RELEASE_FLOCK_UNLOCK`）→ 释放该 flock 所有者的锁。
- M12 用测试确认"在 setlk 回调里（非阻塞锁）或 lock-wait 线程里（阻塞锁）调用 `inval_inode`"不与内核锁交互出问题（若有问题，改为"先回复成功、紧接着失效"并在 semantics.md 注明这一微小窗口）。

### 8.7 身份、会话与错误

- 每个 FUSE 请求的 uid/gid/pid 来自 `Request`；附加组读 `/proc/<pid>/status` 的 `Groups:`（按 pid 缓存 1 s）；作业 id 读 `/proc/<pid>/environ` 里 `--job-id-var` 指定的变量（默认依次尝试 `SLURM_JOB_ID`、`PBS_JOBID`、`WMFS_JOB_ID`，缓存 10 s，读不到为 0，参考 Lustre jobstats）。服务端按 key 策略处理（uid 范围、root_squash），客户端声明的 uid 只在其 key 允许的范围内被信任【ADR-0009】。
- 挂载时 `SessionOpen`；`session-hb` 线程每 3 s 心跳（打开集合增量；服务端要求时发全集）；租约自停与驱逐处理见 3.4.4。客户端从长时间停顿（例如 SIGSTOP）恢复后，若距上次成功心跳已超过 0.8 × lease，任何操作都先等一次续租结果：会话已被驱逐 → 旧句柄返回 `EIO`（即使数据在本地缓存里），并建立新会话。
- 错误码映射：POSIX 码原样转成 `fuser::Errno`；集群码在 `--op-timeout` 内重试，超时 → `EIO`；`SESSION_EVICTED` → `EIO`；`BAD_CAP` 二次失败 → `EACCES`；layout 无法刷新 → `ESTALE`；`FEATURE_UNSUPPORTED` → `EOPNOTSUPP`。

### 8.8 passthrough 与 io_uring：首发都不做

- **FUSE passthrough**【ADR-0004】：需要一个**本地后备文件** fd，且 `FUSE_DEV_IOC_BACKING_OPEN` 需要 `CAP_SYS_ADMIN`（我们以普通用户挂载）；我们的文件是"多个 chunk、带头部与块表、可能压缩/加密、分布在多个节点"，没有可直通的本地文件。预留：`CacheStore::local_file`（3.7.3）——二期在 local-nvme 完整缓存了只读文件的明文时对它做 passthrough；fuser 0.18 已提供 `open_backing/opened_passthrough`。
- **FUSE-over-io_uring 与服务端 io_uring**【ADR-0003】：本机内核模块参数 `enable_uring = N`（打开需要 root），fuser 0.18 只定义了标志位、没有实现；`io-uring` crate 与"只用同步代码"的承诺冲突。用 `n_threads = 16 + clone_fd` 多线程会话与服务端 IO 线程池代替。
- 理念文档要求"保留内核客户端与 LD_PRELOAD 拦截库的接口可能"：客户端库（`core/src/client/`）不依赖 fuser，FUSE 适配层在 `cli/src/fuse/`，将来的内核客户端协议适配或 LD_PRELOAD 库都可以复用客户端库与同一套线上协议。

---

## 9. 测试策略

### 9.1 单元测试（每个模块内，`cargo test`）

不引入 proptest 等新 crate，用自带的 xorshift 伪随机发生器写"带种子的随机测试"（失败时打印种子以便复现）。

| 对象 | 测什么 |
|---|---|
| 帧 / 握手 / 记录 / 消息编解码 | 往返；随机字节输入不 panic；`tests/golden/` 金样逐字节比对（防止无意改了格式） |
| chunk 格式 | 各种变换组合往返；压缩不划算存原文；加密 AAD 错位必须失败；块表与头 CRC；把 pending 文件在任意字节处截断后执行启动清理 → 不出现损坏读；翻转任意比特 → 被判为 Corrupt |
| raft Storage | 照 raft-rs `MemStorage` 的测试改写：entries/term/compact/snapshot 边界、max_size 至少一条、压缩后 hs.commit 不变式 |
| 执行器 + 覆盖层 | 随机操作序列 vs 一个"直接改 BTreeMap 的参考模型"，比对回复与最终状态；操作回滚；去重（同 seq 重放得到同结果）；错误也写去重记录；`exclusive` 与生成新链都结束批次；写集 ≥ 768 KiB 结束批次 |
| rstat | 随机操作序列（create/unlink/rename/link/truncate/commit/trash）之后，与"从头遍历重算"逐目录相等 |
| 配额 | quantum 模型：随机并发写者序列下，用量 ≤ hard + quantum × 并发写者数恒成立；护栏 16 × quantum；软限 + 宽限状态机；文件数精确 |
| 锁 | 随机加锁/解锁序列与暴力区间模型（逐字节记录持有者）对比；`LOCK_EOF` 与 `i64::MAX` 以上的 end 归一、`end + 1` 不溢出；客户端轮询的等待者最终拿到锁 |
| 属性 | 切分规则；继承、`inherit=false`、Stacked 键不被阻断、来源路径；无消费者的键被拒绝 |
| mClock | 虚拟时钟下：保底、上限、权重 1:3 的两队列长期份额误差 < 5%；作业轮转；空闲重置 |
| placement / 链生成 | 确定性（同 seed 同结果）、故障域约束、dedicated 独占 |
| CRAQ 状态机 | 3.4.2.4 的后继处理表逐格测试；head 的等待队列；版本分配含 incarnation；3.4.2.6 的四条前滚规则；3.4.2.7 的每一行 |
| cap | 签名、过期、篡改、chains 校验、被驱逐会话 |
| 握手与角色 | 金样；client key 申请 Server 角色被拒；key 0 只能是 Server；角色权限表（client 调 PeerCall → `EACCES`） |
| inline 与截断 | `len(INLINE) ≤ size` 不变式：`echo hello>f; echo x>f; truncate -s 6 f` 读到 `"x\n\0\0\0\0"`；补丁越过长度时补 0；截断到 0 按当前属性重建 layout |

### 9.2 确定性 raft 模拟（`tests/` 里的 `raft_sim.rs`）

同一进程里起 3–5 个 `RawNode<RedbStorage>` + 驱动器（driver 设计成"输入一步、处理一次 Ready"的结构体方法，测试直接调用，不起线程），消息走内存总线，可随机丢弃、延迟、重排、分区；tick 由测试驱动（不依赖真实时间）。**存储用测试专用的 redb 后端** `MemCrashBackend`（testkit，约 80 行，实现 redb 2.6 的 `StorageBackend`，用 `Builder::create_with_backend` 打开）：写入先放在"易失层"，只有 `sync_data` 之后才算落盘；随机"崩溃"= 丢掉易失层、只用已落盘的字节重建节点——这才真正模拟了掉电（`kill -9` 不丢页缓存，fsync 写错了也照样"不丢"，测不出来），而且不碰真实磁盘、不做真实 fsync，1000 个种子几分钟就能跑完。检查：① 所有存活副本在静止后 KV 哈希相同；② 每个被确认的写在任何崩溃序列后都还在；③ 单 key 读写历史的线性一致性（≤ 200 个操作的小历史用 Wing-Gong 回溯检查）；④ `expect_version` 作废的批次从未被部分应用；⑤ 附录 A 每个坑一个回归用例（含"单节点 propose → kill -9 → 重启"、快照安装、joint 退出兜底）。每次提交跑 1000 个随机种子，夜间跑 10 万个。

> 实现注记（M3）：单节点部分在 `tests/it/raft_sim.rs`，附录 A 的回归用例在 `raft_pitfalls.rs`（单节点之外的情形由测试手写对端的 raft 消息），压缩在 `raft_compact.rs`；`MemCrashBackend` 在 `tests/harness/mem_crash.rs`。除"崩溃 = 丢掉易失层"外还有 `freeze_after(k)`：在之后第 k 次 sync 处掉电、进程照常往下跑——重启后的 KV 必须等于"确认历史"里的某个状态，且不早于掉电点之前的最后一个确认（确认时的 sync 计数 ≤ 掉电点 ⇒ 它的条目已落盘），于是"在任意一次 fsync 处掉电"都被覆盖。普通 `cargo test` 跑 200 个种子（约 10 s，全工作区的测试别太慢），`scripts/ci.sh` 与 `scripts/accept.sh M3` 按"每次提交 1000 个"跑（约 1 分钟）。

### 9.3 CRAQ 组件测试（`tests/`，单进程）

在一个进程里起 3 个数据服务，通过 `plugin-mem-transport` 或回环 TCP 互连，集群图由测试直接下发（不需要元数据服务）。target 用测试专用的内存版 `MemTargetStore`（testkit，实现 `TargetStore` 这个插件边界）：文件内容、rename、目录项都先进"易失层"，`commit_pending(sync)`/`install`/`sync` 与目录 fsync 才让它们落盘，崩溃时丢掉易失层——用来验证 R1–R6 的 fdatasync 与目录 fsync 顺序。另有一组同样的用例跑在真实的 local-fs 上（只验证功能，不验证掉电）。注入：转发丢弃/延迟、链变更、重复转发、NeedFull、head 写完 pending 后崩溃、唯一幸存者重启（incarnation）、整链重启、同步期间并发写。不变式检查器：静止后所有 Serving 副本对每个 chunk 的已提交版本与内容哈希相同；读历史无回退。

### 9.4 fail point 测试

3.11 列出的每个 fail point 至少一个测试（理念文档 4.3.4、9 章"内建 fail points 覆盖每个钩子"）。重点：`raft.persist.before_commit=abort`（重启后不丢已确认写）、`meta.reply.drop`（客户端重试恰好一次）、`chunk.pending.torn`、`chain.head.crash_after_pending`（pending 前滚，链不分叉）、`chain.ack.drop`（重新驱动）、`sync.fetch.midway`、`clock.mono.rate_ppm`（租约在 20% 速率偏差内仍安全）、`client.heartbeat.stop`（驱逐两阶段：锁在数据节点确认后才释放、被驱逐客户端的写被拒绝）。

### 9.5 单机多进程集群测试框架（`tests/harness/`）

```rust
let c = Cluster::start(ClusterSpec { nodes: 3, profile: Profile::NasSafe, transport: "tcp", dir: tmp, failpoints: true })?;
let m1 = c.mount(1)?; let m2 = c.mount(2)?;            // 起 wmfs mount 子进程，等挂载点 st_dev 变化
c.kill(2)?; c.start(2)?; c.stop(3)?;                    // SIGKILL / 启动 / SIGTERM
c.pause(3)?; c.resume(3)?;                              // SIGSTOP / SIGCONT（模拟卡死、触发会话驱逐）
c.partition(&[1], &[2, 3])?; c.heal()?;                 // 经 net.drop.* fail point 实现，无需 root
c.failpoint(1, "chain.forward.delay", "sleep(200)")?;
c.wait_until(Duration::from_secs(60), |s| s.chains_degraded == 0)?;
c.rstat_check("/")?;
```

"多节点" = 本机多进程（不同端口、不同数据目录），不需要 docker。每个测试一个独立根目录，端口从测试名哈希出基址并用锁文件防冲突；**失败时保留目录与日志**，成功时清理。同样的能力以 `wmfs dev` 暴露给人。工作负载检查器：① 已确认写不丢（写入已知内容并 fsync，随机 kill -9，结束后逐一校验）；② rename 原子性（两个名字之间并发 rename，另一挂载点在 strong 子树循环 stat，永远恰好看到一个）；③ strong 追加（两挂载点各写 N 条 `(writer, seq)`，无重叠、无丢失；崩溃导致的零洞单独计数，符合文档）；④ 锁互斥（两挂载点在 F_SETLKW 下对同一计数器各加 1000 次，结果恰好 2000）；⑤ 配额（多写者并发写配额目录，后台每 100 ms 采样 RSTAT，永不超过 hard + quantum × 写者数）；⑥ 单点故障无错误（理念文档第 9 章成功标准：3 节点、replicas=3，写入中依次 kill 每个节点，客户端侧零错误）；⑦ Jepsen-lite：寄存器负载（8 字节读写）+ 随机 kill/SIGSTOP/分区/时钟偏移，WGL 线性一致性检查——M9 的版本由 testkit 直接用 `DataClient` 读写单个 chunk（绕过 FUSE 与各级缓存，那时还没有 strong）；M12 起再加 strong 目录里单文件当寄存器的 FUSE 版本。

**本机验证不了的事**（写进 ADR-0015）：真实设备上的掉电持久性（`fdatasync` 与目录 `fsync` 的顺序在 ext4/XFS 上是否真的够）需要 root 才能用的 dm-log-writes 或 LazyFS 环境补测；本机只能用上面两个模拟后端验证"我们按约定调用了 fsync"。

### 9.6 外部测试套件：能否在本机跑

本机没有 fio、mdtest、ior、pjdfstest（已检查），`/etc/fuse.conf` 没开 `user_allow_other`（root 也访问不了普通用户的 FUSE 挂载）。方案：`tests/docker/Dockerfile.tools`（ubuntu:24.04 + apt 装 fio + 源码编译 pjdfstest、fsx/fsstress、openmpi + IOR/mdtest），**客户端在容器里以 root 挂载**：`docker run --rm --device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor=unconfined --network host -v $PWD/target/release/wmfs:/usr/local/bin/wmfs:ro -v ~/wmfs-dev/n1/keys/client.key:/root/.config/wmfs/client.key:ro wmfs-tools …`（client key 也要挂进去），服务端跑在宿主机。容器里不需要装 rdma-core（sideway 在运行时才 dlopen libibverbs）。用户在 docker 组，不需要宿主机 sudo；**构建镜像需要联网，首次构建前征得人类同意**。注意代理：docker daemon 自己配了代理，但构建容器里的 `RUN apt/curl` 不会自动继承 shell 的 `http_proxy`，所以 `scripts/build-tools-image.sh` 用 `docker build --network host --build-arg http_proxy=$http_proxy --build-arg https_proxy=$https_proxy …`。

| 套件 | 首发 | 说明 |
|---|---|---|
| pjdfstest | ✅ 门槛 | 容器内 root 运行；与 semantics.md 放松清单对应的预期失败列在 `tests/pjdfstest-expected.txt`，**出现清单外的失败即不通过** |
| fio | ✅ 冒烟 + `verify=crc32c` | 顺序/随机读写；性能数字仅供参考（三个"节点"共用一块 NVMe） |
| fsx / fsstress | ✅ | 每个 profile fsx 10 万次无不一致 |
| mdtest / IOR（easy + hard 47008 字节） | ✅ 冒烟 | 单机多 rank（`mpirun --allow-run-as-root -np 8`）；hard 模式专门暴露锁与共享文件问题 |
| xfstests、LTP（除 fsx/fsstress） | ❌ 推迟 | 需要块设备、mount helper 或 root 测试框架【ADR-0015】 |
| dm-flakey | ❌ 用 fail point 替代 | 需要 root + device-mapper；`chunk.*` fail point 覆盖读错、写错、比特翻转、撕裂写 |
| Jepsen 风格 | ✅ 自研 | 9.5 框架 + fail point 分区/延迟/时钟 + kill -9 + SIGSTOP |
| IO500 / 与 BeeGFS 对比（RDMA ≥ 80%） | ❌ 本机无法验证 | 无物理 RDMA 链路（Soft-RoCE 性能不具代表性）、单盘、无 BeeGFS【ADR-0015】 |
| elbencho | ❌ 推迟 | 与 fio、IOR/mdtest 覆盖的场景重叠；源码构建要 boost 等一串额外依赖，首发不值得【ADR-0015】 |

### 9.7 三个 profile 怎么落地

`tests/profiles/<name>.rs` + 部署模板（node.toml 片段 + init 时写入的属性，3.4.3），`scripts/profiles.sh <name>` 一键运行，`wmfs dev up --profile <name>` 拉起同样的部署。

| profile | 部署 | 必过测试 |
|---|---|---|
| `minimal` | 1 节点、TCP、`[plugins] disable = ["quota", "mclock"]`（于是 `quota` 属性无人消费，`wmfs set quota=…` 被拒绝而不是被静默忽略；qos 用 fifo）、default 类 replica=1 | 冒烟；pjdfstest（容器）；fio verify；fsx；kill -9 / 重启后 fsync 过的数据完整；inline 与 chunk 文件；属性 set/get |
| `hpc-rdma` | 3 节点、RDMA（rxe，`data_scheme = "rdma"`）、quota、mclock、`/scratch replicas=2`、两个租户的 qos | IOR 运行中 kill leader 与一个数据节点；配额并发；QoS `l=` 上限生效（指标验证）；strong 追加；锁互斥；数据路径确实走 RDMA（`wmfs_rdma_ops_total{op="read"}` 增长）。**没有 RDMA 设备时整套明确输出 SKIP（不算通过）** |
| `nas-safe` | 3 节点、TCP、`/home replicas=3,trash=7d,audit=on`；集群起好之后由测试框架启动一个 mgr（`wmfs dev mgr`） | 9.5 检查器 ①–⑦；回收站恢复；审计日志在 mgr 重启后仍完整（每条修改恰好出现、可按 seq 去重）；scrub 发现注入损坏并修复 |

**分阶段执行**（理念文档 10.4 要求"三个 profile 的测试通过"才算完成；功能是逐个里程碑长出来的，所以按阶段收紧，【ADR-0015，需你决定】）：

| 里程碑 | 每次验收必须跑的 profile |
|---|---|
| M0–M5 | 只有单元测试与 fail point 测试（还没有挂载点，profile 无从谈起） |
| M6–M8 | `minimal`（M6 起有 FUSE） |
| M9–M11 | `minimal` + `nas-safe` 阶段版：3 节点、replicas=3，只设置已经有消费者的属性（不设 trash、audit） |
| M12–M13 | 同上，nas-safe 阶段版加上 `trash=7d` 与 `audit=on`（此时只检查 core 的 audited 标记与 changelog，mgr 还没有） |
| M14 起 | `minimal` + 完整 `nas-safe`（含 mgr 与审计日志） |
| 任何里程碑（M9 起） | 有 Soft-RoCE 时每次都跑 `hpc-rdma` 的阶段版（M9 起 RDMA + replicas=2，M10 起加配额，M13 起加 mclock）；没有就 SKIP |

CI（`scripts/ci.sh`）：`cargo fmt --check` → `cargo clippy --all-targets -- -D warnings` → 单元测试 → raft 模拟（1000 种子，内存后端）→ `scripts/loc.sh` 对账 → 上表规定的 profile。

---

## 10. 里程碑计划

见 [roadmap.md](roadmap.md)：16 个按依赖排序、每个都能运行与演示的里程碑（M0–M15），每个都有验收标准、可复制粘贴的演示命令、`scripts/accept.sh Mx` 自动验收、预计行数、"本阶段你需要理解的概念"。ADR 的确认方式：**技术契约在 M0 开工前一次确认**（批次 0，你回复一次"同意"即可），需要你做产品取舍的几条（含 0026 的配额模型）在用到它的里程碑开工前单独问你（roadmap 第 2 节）；所以没有任何契约是"先实现、后批准"。

---

## 11. 与理念文档的偏离清单

"批准"列：**需批准** = 触及 [固定] 或契约层或对外承诺，按理念文档第 0 章与 10.2，ADR 状态为 `proposed` 时不得实现；**需确认** = 偏离 [默认]，agent 起草 ADR 并标"待确认"；**已同意** = 人类已在任务需求中明确同意（仍记 ADR 留档）；**需决定** = 产品取舍。

| # | 条款（标记） | 偏离内容 | 理由 | ADR | 批准 |
|---|---|---|---|---|---|
| 1 | 7 章 语言 `tokio`（[默认]） | 全部同步：`std::thread` + `crossbeam-channel` + 阻塞 IO | 对新手的承诺；可调试；小集群足够 | 0001 同步并发模型 | 已同意 |
| 2 | 7 章 语言 `openraft`（[默认]） | raft-rs（`raft` 0.7） | openraft 依赖 async；raft-rs 同步核心、TiKV 生产验证，仍满足 8.1"不自研共识" | 0002 元数据共识库选用 raft-rs | 已同意 |
| 3 | 7 章 语言 `io-uring`；客户端"FUSE（… + io_uring）"（[默认]） | 阻塞 `pread/pwrite`；FUSE 多线程会话 | 不用 async；fuser 0.18 未实现 FUSE-over-io_uring；本机未开启 | 0003 首发不使用 io_uring | 需确认 |
| 4 | 7 章 客户端"FUSE（passthrough + …）"（[默认]） | 不做 passthrough（接口预留） | 需 `CAP_SYS_ADMIN`；没有可直通的本地后备文件 | 0004 首发不启用 FUSE passthrough | 需确认 |
| 5 | 7 章 管理 API"gRPC + 只读 REST"（[默认]）；4.2 进程外"gRPC 管理 API + 事件总线"；7 章线上协议"gRPC 仅用于控制面" | HTTP/JSON（tiny_http + ureq）读写都走它，HMAC 签名；事件总线 = changelog 长轮询（changelog 流本身是第 3 章的契约） | gRPC 需要 async 或 C++ 核心 | 0005 管理面采用同步 HTTP/JSON | **需批准**（触及契约层） |
| 6 | 5.1 / 7 章 CLI 名 `fs` | `wmfs` | 与二进制同名，避免与系统命令冲突 | 0006 CLI 命名 | 已同意 |
| 7 | 6.1 存储类示例为 YAML | TOML | 不新增 YAML 依赖 | 0007 存储类定义用 TOML | 需确认 |
| 8 | 4.1 chunk_store"XFS 上的 chunk 文件" | 任意满足 rename 原子替换的 POSIX 文件系统；本机在 ext4 上测试，推荐 XFS | 本机根分区是 ext4，格式化 XFS 需要 root | 0008 local-fs 支持 ext4 | 需确认 |
| 9 | 7 章 信任模型"服务端强制授权，客户端不可信"（[默认]）+ 11 章 [待定] | 服务端强制授权照做；但 uid 由**已认证的客户端主机**声明（类似 NFS AUTH_SYS），受 key 的 uid 范围 + root_squash + path-caps 约束；首发无 TLS，只在可信网络使用；RDMA 下服务端之间的请求会带上发起方的 rkey | crate 清单里没有 TLS/Kerberos；逐用户认证超出首发范围 | 0009 首发认证与信任边界 | **需批准**（安全边界） |
| 10 | 4.1 qos 钩子"请求进入执行队列"；6.2 第 1 层 | 只在数据节点做 mClock；三元组按节点数静态均分（无 dmClock）；元数据请求不做 QoS | 元数据由单 leader 串行执行，mClock 收益小、复杂度高；跨节点协调超出首发 | 0010 首发 QoS 语义 | 需确认 |
| 11 | 6.2 第 2、3 层（cgroup io、NVMe WRR、NVM Sets、FDP） | 不实现；第 3 层只做"整盘独占"与检测；第 2 层只给"一类一进程"部署指引 | io 控制器非 threaded；需 root；本机单盘无 FDP；Linux 无 WRR 接口 | 0011 设备出口与硬件隔离推迟 | 需确认（6.3 照做） |
| 12 | 3 章"钩子管线与插件 ABI"（[固定]）+ 4.2 热路径"编译期特性开关 + 动态库（稳定 C ABI）" | 动态库 C ABI 首发只覆盖 authz（推荐；备选再加压缩类 transform）；其余热路径钩子只有静态注册 | 一个钩子足以证明 ABI 规则；压缩类会连带 chunk 格式 feature 位与跨节点一致性规则；其余钩子的参数含 `Buf`/trait 对象，跨 FFI 风险大 | 0012 动态库 C ABI 首发范围 | **需批准** |
| 13 | 3 章 一致性 `strong`（租约 / 锁）（[固定]）；8.2"跨节点共享可写 mmap 仅在 strong 下支持" | strong = 零缓存 + 线性一致 GetAttr + 同步提交 + 集群锁 + 元数据分配追加偏移；**没有**读写委托租约；strong 下也**不支持**跨节点共享可写 mmap（`ENODEV`） | 能证明正确的最简实现；共享 mmap 需要页级租约与回调 | 0013 首发 strong 一致性的实现与限制 | **需批准** |
| 14 | 8.2 放松清单（对外承诺）+ 3 章命名空间语义（[固定]） | 新增放松 ①–⑯（3.1.1） | 来自 fuser/FUSE 限制与首发简化；全部写进 semantics.md | 0014 POSIX 放松清单补充 | **需批准** |
| 15 | 9 章 质量门槛（xfstests、LTP、dm-flakey、IO500、elbencho、RDMA ≥ BeeGFS 80%）；10.4"三个 profile 的测试通过"才算完成 | 首发门槛 = pjdfstest（容器）+ fsx + fio/mdtest/IOR 冒烟 + 自研故障注入与 Jepsen 式测试；其余推迟或替代；profile 测试按里程碑分阶段收紧（9.7） | 需要 root/块设备/RDMA 硬件/BeeGFS，本机不具备；早期里程碑还没有 profile 需要的功能 | 0015 首发质量门槛与分阶段 profile 规则 | **需批准**（发布门槛与完成的定义） |
| 16 | 原则 7"宁可拒绝…不可静默降级"与原则 4"单节点能跑"的冲突 | 声明无法满足（单节点 `replicas=3`）时创建成功、实际副本较少；`set` 时大声警告，`status`/dashboard/告警可见；节点加入后自动补齐 | 符合"声明 vs 实际"模型；若拒绝，单节点集群就用不了默认的 `/home` | 0016 无法满足的放置声明：可见降级 | **需决定**（备选：拒绝创建，`ENOSPC`） |
| 17 | 4.2 进程外插件"可独立升级与重启"；10.3 `plugins/<name>/` | 五个进程外插件合在一个 `wmfs mgr` 进程，代码在 `dashboard/`（`wmfs-mgr` crate，不依赖 core） | 少一个要运维的进程；仍与核心完全隔离；需要时可拆 | 0017 进程外控制面插件合并为 wmfs mgr | 需确认 |
| 18 | 11 章 [待定]"64 节点、数千客户端" | 首发目标 ≤ 64 节点、≤ 1000 个客户端挂载（RDMA 下每个连接固定占 2 MiB 接收槽，客户端很多时要调大内存池） | 每连接一个读线程 + 一个写线程的同步模型；更多客户端由将来的 epoll 传输插件解决（这正是传输插件化的意义） | 并入 0001 | 需确认 |
| 19 | 5.2 `compress`、`encrypt` 生效方式"新数据 + 协调器"，`checksum`、`locality`"新数据" | 变换参数与放置属于 layout：属性改动对**新文件与截断到 0 的文件**立即生效；已有文件的新写入沿用旧 layout，直到协调器迁移（写打开的文件被跳过、单独计数、24 h 告警）；checksum、locality 不迁移 | 按块切换 layout 需要块级元数据与混合解码，复杂度高；截断到 0 重建 layout 覆盖了最常见的"覆盖写" | 0021 chunk 文件格式 v1（附带的偏离） | 需确认 |
| 20 | 理念文档 4.3.1 只要求 manifest；首发另加"集群插件清单" | 不是偏离，是对原则 7 的落实：各节点会影响策略的插件集合必须一致，否则退出 | 否则 leader 切换会静默跳过授权/配额/QoS 策略 | 0025 | 随 0025 |

另有 11 条**不是偏离、但属于契约层首次定义**的设计 ADR（理念文档 10.1："这是契约吗？是 → 先写 ADR，等人类批准"），除 0026 外全部在 M0 开工前（批次 0）一次确认：0018 线上协议 v1、0019 元数据引擎与执行模型、0020 元数据 KV 布局 v1、0021 chunk 文件格式 v1 与数据侧崩溃一致性、0022 缓冲池与零拷贝所有权、0023 租约 / 能力令牌 / 两阶段驱逐、0024 CRAQ 链式复制（成员、版本、incarnation、重同步）、0025 钩子清单与插件 ABI 规则、0027 后台任务框架与可观测基座、0028 命名空间语义与 cto 缓存失效协议 v1。0026 目录属性与递归用量 / 配额 quantum 模型在 M7 开工前确认（它的实现从 M7 开始；其中"配额允许短暂超限"是要你决定的产品取舍）。实现中补齐的技术缺口（2026-10-21 起，主管按本文开头的例外条款决定）：0029 changelog 操作码表 v1、0030 M1 补齐的握手与管理 API 细节、0031 M2 补齐的 RDMA 插件细节、0032 M3 补齐的元数据引擎细节、0033 M4 补齐的命名空间与会话细节。实现中由人类拍板的：0034 inline 小文件转 chunk 由服务端完成（2026-10-22，改 8.4、8.5 的 uninline 流程）。

---

## 12. 主要风险与应对

| # | 风险 | 影响 | 应对 |
|---|---|---|---|
| 1 | 本机 fsync 慢（实测 ext4 上 redb Immediate 提交 ≈ 2.6 ms、4 KiB 追加 + fdatasync ≈ 5 ms），3 个"节点"还共用一块盘 | 单个元数据修改延迟 3–6 ms；strong 写同样慢 | 单飞批次天然攒批；文档写明延迟来源；企业级 NVMe（带掉电保护）上 fsync 约 20 µs；**绝不**关闭 fsync |
| 2 | raft-rs 集成细节出错（Ready 顺序、快照、conf change） | 静默的状态分叉 | 附录 A 坑清单逐条测试；确定性模拟 + 崩溃重建；`expect_version` 兜底；`wmfs debug meta-hash` 定期比对副本 KV 哈希 |
| 3 | 链式复制故障路径（前滚、同步与转发并发、last_serving、incarnation） | 数据不一致或不可用（最严重） | 3.4.2.4 的状态表逐格单测；每个转发/确认点都有 fail point；静止后不变式检查器；同步源固定为 last serving；Jepsen-lite |
| 4 | 单飞批次吞吐不足（元数据密集负载） | mdtest 数字偏低 | 先测量；`expect_version` 已为流水线准备好，可在不改契约的前提下改为多批在途（写 ADR） |
| 5 | 整 chunk 写时复制的写放大（小块随机覆盖写） | 数据库类负载慢 | 客户端按 chunk 合并脏数据；文档写明；XFS 上 `copy_file_range` 变成 reflink；块级增量格式二期 |
| 6 | FUSE/fuser 的限制（无 flock 标志、无 INTERRUPT、非 root 无 passthrough、处理函数里调用通知的边界） | 语义差异 | 全部写进 semantics.md；M6/M12 专门测试通知调用；必要时改为"先回复后失效"并记录窗口 |
| 7 | 本机无 RDMA 链路 | RDMA 插件只能做编译与不需设备的单元测试 | 人类决定本机不做 RDMA 实机测试（2026-10-20，不建 Soft-RoCE、不运行 sudo）；同一套传输一致性测试对 tcp/mem 同跑，rdma 需要设备的部分明确 SKIP；RDMA 标为"未在本机验证" |
| 8 | 每连接一个读线程 + 一个写线程 | 客户端很多时线程过多；RDMA 下每连接还固定占 2 MiB 接收槽 | 连接读/写线程栈 256 KiB；每客户端每节点只开 1 条连接；导出线程数指标；首发目标 ≤ 1000 客户端（ADR-0001）；RDMA 连接数受 `max_qps` 与启动校验约束 |
| 9 | redb 崩溃后修复时间随数据库大小增长；快照经 MVCC 读事务长时间持有 | 大元数据库重启慢、redb 文件增长 | M8 测量；必要时 `set_quick_repair(true)`（提交变慢）；pin 10 min 过期 |
| 10 | 租约/能力令牌的时钟假设 | 时钟异常时语义削弱 | 租约只用单调时钟 + 0.8 系数；cap 墙钟容差 5 min，文档要求 NTP；`clock.*` fail point 测试；`wmfs_clock_skew_seconds` 告警 |
| 11 | 未 fsync 的数据在所有副本同时掉电时丢失 | 用户可能误以为"有 3 副本就绝对安全" | semantics.md 与 `wmfs get` 的 `semantics:` 行明确写出；需要时用 `O_SYNC` / fsync |
| 12 | 范围与代码量膨胀（原定 ≤ 30k 行；2026-10-21 人类放宽到约 36k，功能不砍） | 新手难以验收、AI 难以保证质量 | 每个里程碑有行数预算与可运行演示；`scripts/loc.sh` 对账并按模块重估；审查禁止规格以外的功能与花哨写法；预计超过 36k 就停下来与人类商量（2.2） |
| 13 | 新手操作失误（端口冲突、残留挂载、代理变量、写满系统盘） | 演示失败 | `wmfs dev` 一键起停与清理；`wmfs umount`；所有 HTTP 客户端禁用代理；每个 target 有容量上限（dev 默认 20 GiB）；`wmfs doctor`；错误信息给出下一步命令 |
| 14 | 无 TLS、服务端之间的请求带 rkey、管理 API 5 分钟内可重放 | 不可信网络上不安全 | 文档明确"仅可信网络"（ADR-0009）；客户端永远拿不到服务端 rkey |
| 15 | 持久化格式不可逆 | 返工代价极高 | 第一天就带版本号与 feature flags；全部格式在 M0 前确认并由 M0 落成可编译的骨架与金样；金样测试防止无意修改 |
| 16 | 改了集群插件清单后忘了用新配置重启 | 节点因插件集合不一致而退出（宁可停，不静默跳过策略） | 错误日志列出差异与修复命令；`wmfs server --adopt-local-plugins` 作为逃生口（3.7.2） |

> 实现注记（M3，风险 1）：本机实测 `wmfs debug kv bench --n 100000`（64 路并发）约 12.6 s、约 7,900 条/秒、平均每批 32 个操作（闭环客户端下两批在 64 个在途请求之间轮流，每批约一半），每批约 4 ms（含一次 Immediate fsync）。

---

## 附录 A：raft-rs 0.7 集成的坑（实现与评审逐条打勾，每条一个回归测试）

| # | 坑 | 处理 |
|---|---|---|
| P1 | `ready()` 与 `advance()` 之间调用 `step/propose/campaign/read_index` 会破坏内部状态（`RawNode::ready` 文档："DO NOT call any function like step, propose"） | 只在 `raft_loop` 的输入阶段调用它们；leave-joint 兜底也经 `RaftInput` 在输入阶段提议 |
| P2 | follower 的消息在 `take_persisted_messages()`，必须 fsync 之后发；leader 的 `take_messages()` 可先发 | 3.4.1.3 ② ④ |
| P3 | 误以为 committed_entries 可能未持久化 | 源码：`next_entries_since` 的上界是 `min(committed, persisted)`，可与新条目同事务应用 |
| P4 | 照抄示例"跳过所有空 data 条目"会漏掉 leave-joint | 只有 `EntryNormal` 的空 data 是 no-op；空 data 的 `EntryConfChangeV2` 必须应用 |
| P5 | ConfState 未与条目同事务持久化 | `set_conf_state(tx, ..)` |
| P6 | Safe 模式下 leader 未在本任期提交日志时，`MsgReadIndex` 被静默丢弃（`commit_to_current_term` 检查） | leader 就绪后才发；500 ms 重发；2 s 回 RETRY |
| P7 | LeaseBased 不检查时间 | 不用 |
| P8 | `hs.commit < first_index − 1`（快照/压缩点之下）时重启 fatal | 规则 M3：只压缩到 durable_applied − 10 000；快照安装时 `commit = max(commit, index)` |
| P9 | `Config.applied` 未设置导致重复应用；或 applied > hs.commit 导致 fatal | 读 `raft_state["applied"]`；规则 M1 保证落盘的 applied ≤ hs.commit |
| P10 | `propose` 不返回 index | 比较 `last_index` 前后 + (index, term) 匹配 |
| P11 | 不 `report_snapshot` 会让 leader 卡在 Snapshot 状态 | snap-serve 报告成败；60 s 看门狗 |
| P12 | `entries()` 必须遵守 max_size 但至少返回 1 条；反过来，`limit_size` **总会保留第一条**，单条超过 `max_size_per_msg` 的条目照样整条进 MsgAppend | `raft::util::limit_size` + 单测；批次写集 768 KiB 上限 + RaftMsg 的 protobuf 放 bulk（3.4.1.4）；回归测试：3 节点提交 900 KiB 写集 |
| P13 | joint 状态可能无人退出（源码 TODO） | cluster-mgr 兜底提议空 ConfChangeV2 |
| P14 | 新节点自己 campaign 形成第二个集群；或用不同初始配置引导导致 joiner 学不到 voter | 空 ConfState 启动；`init` 只在全新目录上允许；按 `five_mem_node` 用 index = 1 的快照状态引导 |
| P15 | `step` 对未知对端的响应返回 `StepPeerNotFound` | 忽略（debug 日志） |
| P16 | 被隔离节点回来打断集群 | `pre_vote + check_quorum` |
| P17 | Ready 里快照与已提交条目同时出现 | raft-rs 断言二者互斥；安装代码不假设二者同时存在 |
| P18 | 大量 LightReady 条目撑爆内存 | `max_committed_size_per_ready = 16 MiB` |
| P19 | 降级成 follower 后，raft-rs 把收到的 MsgPropose 转发给新 leader | 提议前检查 `state == Leader && term == leader_term && apply_to_current_term()` |
| P20 | LightReady 推进 commit 后 `prev_hs.commit` 已更新，下一个 Ready 的 `hs()` 通常为 None | 第二个写事务写 `node.raft.hard_state()`（规则 M1） |
| P21 | 已有未应用 conf change 时，新的 conf change 被改写成空的 EntryNormal | 按 (index, term) 跟踪时把它判为 `Applied{ok:false}`，下一轮重试 |
| P22 | v1 `EntryConfChange` 与 V2 的 protobuf 字段不同，不能按 V2 解析 | wmfs 只提议 V2；遇 v1 条目 panic 并写明原因 |
| P23 | `is_empty_snap` 已弃用（会产生警告，破坏"零警告"） | 用 `rd.snapshot().is_empty()` |

> 实现注记（M3）：回归用例（`tests/it/` 下，单节点之外的情形由测试手写对端的 raft 消息）：P1 → 结构保证（step/propose/read_index/campaign 只在 Ready 处理之外调用——`Driver::handle_input`、`tick`、`process_ready` 开头的读闸门 pump、`open` 里的 campaign；`on_ready`（`ready()` 到 `advance()` 之间）从不调用）+ `raft_sim`（随机交错输入与 Ready）；P2 → `failpoint_raft_order`（提交前 panic：follower 的回复没发出、leader 的 MsgAppend 已发出）+ `raft_pitfalls::p2_p3…`；P3 → `raft_pitfalls::p2_p3…`（旧提交与新条目同一个 Immediate 事务落盘）；P4/P5/P21 → `raft_pitfalls::p4_p5_p21…`（joint → 自动 leave-joint 被应用、ConfState 与条目同事务、第二个 conf change 被改写成空条目 → `Applied{ok:false}`）；P6 → `raft_pitfalls::p6…`（未就绪不发、2 s 回 RETRY、500 ms 用新 ctx 重发）+ 读闸门单测；P7/P16/P18 → `raft_pitfalls::p7_p16_p18…`；P8 → `raft_compact`（20 万条后压缩、重启不 fatal）；P9/P20 → `raft_pitfalls::p9_p20…` + `raft_sim`（每步检查可见状态 applied ≤ hs.commit，每次掉电重启都不 fatal）；P10 → `raft_pitfalls::p10…`；P12 → `RedbStorage` 合约测试（max_size 至少一条；900 KiB 的 3 节点用例在 M8）；P14 → `raft_pitfalls::p14…` 与 init 拒绝非空目录；P15 → `raft_pitfalls::p15…`；P19 → `raft_pitfalls::p19…`（另有 3.4.1.4 规则 3 的提议前检查：插件清单不一致的 leader 当选了也不提议，`raft_pitfalls::proposals_to_a_leader_whose_plugins_differ…`）；P22 → `raft_pitfalls::p22…`（should_panic）；P23 → 零警告构建；另有 3.4.1.3 ① 的"不再是 leader → 在途提议 Lost、读者 NOT_LEADER"（`raft_pitfalls::stepping_down…`）。P11（report_snapshot）、P13（joint 无人退出的兜底）、P17（快照 + 已提交条目）在 M8 随快照与 cluster-mgr 实现。

## 附录 B：本机核对过的 API 与环境事实

以下在 `scratchpad/apicheck-final/`（edition 2024、rustc 1.99）中编译通过，或读源码确认：

| 对象 | 结论 |
|---|---|
| raft 0.7.0 | `Config` 字段（`election_tick`、`heartbeat_tick`、`applied`、`check_quorum`、`pre_vote`、`read_only_option`、`max_size_per_msg`、`max_inflight_msgs`、`max_uncommitted_size`、`max_committed_size_per_ready`）；`RawNode::{new, campaign, propose, propose_conf_change, apply_conf_change, step, ready, advance, advance_apply, read_index, report_snapshot, report_unreachable, transfer_leader, mut_store}`；`node.raft.{state, term, raft_log.last_index(), apply_to_current_term(), hard_state(), prs().conf().to_conf_state(), prs().get(id).matched}`；`Ready::{ss, hs, take_messages, take_persisted_messages, snapshot, entries, take_committed_entries, take_read_states, must_sync}`；`LightReady::{commit_index, take_committed_entries, take_messages}`；`advance()` 内部 = `advance_append` + `advance_apply_to(调用前的 commit_since_index)`，所以处理完 light 条目后必须再 `advance_apply()`；LeaseBased 分支直接用 committed；follower 转发 MsgPropose；commit_apply 自动 leave-joint（带 TODO）；`Storage` 六个方法签名；`five_mem_node` 用 index = 1 的快照初始化 leader |
| protobuf 2.28 | `use protobuf::Message as _;` 后 `write_to_bytes()` / `merge_from_bytes(&[u8])` 可用于 `eraftpb::{Message, ConfChangeV2}` |
| slog 2.8.2 | 用 `slog::Logger::root(drain, slog::o!())`；`slog::OwnedKV` 的字段是 `#[doc(hidden)]`、源码注释说明不是稳定 API，不要直接构造 |
| redb 2.6.4 | `ReadTransaction` 是 `Send + Sync`；`Durability::{None, Immediate}`、`set_durability(&mut self, ..)`；`retain_in`、`delete_table`、`set_quick_repair` 存在；None 提交对随后的读事务可见；`pub trait StorageBackend { len, read, set_len, sync_data(eventual), write }` 与 `Builder::create_with_backend` 存在（测试用的掉电模拟后端，9.2） |
| fuser 0.18.0 | `Config::default()` 后设 `mount_options`（含 `MountOption::{FSName, Subtype, DefaultPermissions}`）、`n_threads`、`clone_fd`；`Session::new(fs, path, &cfg)`、`session.notifier()`、`Notifier::inval_inode(INodeNo, i64, i64)`；`KernelConfig::{add_capabilities, capabilities, set_max_write, set_max_readahead, set_max_background}`；`InitFlags::{FUSE_POSIX_LOCKS, FUSE_FLOCK_LOCKS, FUSE_POSIX_ACL, FUSE_DONT_MASK, FUSE_ATOMIC_O_TRUNC, FUSE_DO_READDIRPLUS, FUSE_READDIRPLUS_AUTO, FUSE_DIRECT_IO_ALLOW_MMAP, FUSE_WRITEBACK_CACHE}`（库默认的 INIT 标志只有 `FUSE_ASYNC_READ \| FUSE_BIG_WRITES`，所以 `FUSE_DONT_MASK` 必须自己加）；`create/mkdir/mknod` 回调带 `mode` 与 `umask` 两个参数；`setlk(req, ino, fh, lock_owner, start, end, typ, pid, sleep, reply: ReplyEmpty)` 不暴露 `lk_flags`；`trait Reply: Send + 'static`，所以 `ReplyEmpty` 可以交给别的线程稍后回复；`release(.., lock_owner: Option<LockOwner>, flush, ..)`；`open_backing/opened_passthrough` 存在；非 root 经 fusermount3 挂载成功 |
| aes-gcm 0.11 / aead 0.6 | `use aes_gcm::{Aes256Gcm, KeyInit, aead::{AeadInOut, Nonce}}`；`Nonce::<Aes256Gcm>::try_from(&[u8])`；`encrypt_inout_detached(&nonce, aad, buf.into()) -> Result<Tag>`；`decrypt_inout_detached(&nonce, aad, buf.into(), &tag) -> Result<()>` |
| ureq 3 | `ureq::Agent::config_builder().proxy(None).http_status_as_error(false).build().into()` |
| libloading 0.9 | `unsafe { Library::new(path) }`；`unsafe { lib.get::<unsafe extern "C" fn() -> *const T>(b"sym\0") }` |
| Rust edition 2024 | `#[unsafe(no_mangle)] pub extern "C" fn …` 编译通过（裸 `#[no_mangle]` 报错） |
| sideway 0.4.3 | `ProtectionDomain::reg_mr(self: &Arc<Self>, ptr: usize, len: usize, access)` 是 `unsafe fn`，可注册外部内存；扩展 post 接口有 `setup_read/setup_write/setup_send`；`rc_pingpong` 的 RTR/RTS 用 `setup_max_dest_read_atomic(0)`/`setup_max_read_atomic(0)`、INIT 访问位无 `RemoteRead`——不能照抄；底层 rdma-mummy-sys 在运行时 dlopen libibverbs（不带 RDMA 的机器、容器不需要装 rdma-core） |
| ibverbs 0.9.2 | 只有 SEND/RECV、只能注册自己分配的内存、类型带生命周期——不采用 |
| 环境 | 根分区 ext4；`/etc/fuse.conf` 未开 `user_allow_other`；设置了 `http_proxy`/`https_proxy`、未设置 `no_proxy`；memlock ≈ 15 GiB；FUSE `enable_uring = N`；4 个 RoCE 设备链路 DOWN；用户 cgroup 只委派 `cpu memory pids`；`max_write_streams = 0`；无 fio/pjdfstest/mdtest/ior；20 核 |
| 内核 fuse 行为（来自对 `fs/fuse/` 源码的了解；本机只有 linux-headers，**M6、M12 用小 spike 实测确认**） | ① `fuse_fill_super_common` 总会置 `SB_POSIXACL`，VFS 不剥 umask；没协商 `FUSE_DONT_MASK` 时 fuse 的 create/mknod/mkdir 自己 `mode &= ~umask`（8.1）；② 不开 `FUSE_ATOMIC_O_TRUNC` 时 `open(O_TRUNC)` = 先 OPEN（去掉 O_TRUNC）、再 SETATTR(size=0，无 fh)（8.1）；③ 锁的"到文件尾"是 `OFFSET_MAX`（`i64::MAX`），getlk 回复里 `end > OFFSET_MAX` 或 `end < start` → `EIO`（3.5.3） |

## 附录 C：必须有回归测试的已知陷阱（raft 以外）

设计过程中发现并修掉的问题，每条对应一个回归测试，实现与复审时逐条打勾（raft-rs 的坑见附录 A）：

| # | 陷阱 | 处理（章节） |
|---|---|---|
| C-1 | AES-GCM nonce 从版本号推导会重用 | 每块随机 96-bit nonce，存块表；AAD 不含版本（3.2.4、4.8） |
| C-2 | 单阶段驱逐：被驱逐客户端的在途写在锁转交后落地 | 两阶段驱逐（3.4.4） |
| C-3 | path-caps 规则放在 node.toml，各节点不一致 | 存进复制的 `KEYMETA`（3.2.7、4.3） |
| C-4 | 降为 Syncing 时没移到末尾，破坏读正确性论证 | 降级必须同时移到 members 末尾（3.4.2.1 的 I0、3.4.2.7） |
| C-5 | 复制流量与客户端流量争同一个池 → 分布式死锁 | `Lane::Repl` 每档 25%（至少 1 个槽），连接读线程最多等 100 ms（3.6.1） |
| C-6 | 执行器超时后开新批 → 基于丢失提案的写集 | 执行器阻塞到 raft 线程给出确定结果 + `expect_version`（3.4.1.4） |
| C-7 | 转发与头写共用阻塞线程池 | 独立 repl 池 + `call_cb` 非阻塞转发 + 复制 lane（1.2、3.4.2.3） |
| C-8 | 头节点快速重启 / 截断删文件导致同版本号不同内容 | 任何重启以 Syncing 回链；截断写新版本不删文件；链 incarnation（3.4.2） |
| C-9 | 读尾失效、同步源取前驱、整链重启无规则 | 新 last serving 先本地提交 pending；同步源 = last serving；整链重启选"最后一个原 Serving 成员"（3.4.2.6–3.4.2.8） |
| C-10 | 照抄 rc_pingpong 的 `max_rd_atomic = 0` | RTR/RTS 都设 16、QP 访问位含 `RemoteRead`（4.1；实现注记（M2）：访问位按两侧分开，只有 connect 侧握手完成后开放远程读写；回归测试 `plugins/rdma/tests/qp.rs` 的 `rdma_qp_attributes_follow_c10_and_open_remote_access_only_after_auth`：读回交给 modify_qp 的属性逐项断言） |
| C-11 | `#[no_mangle]` 在 edition 2024 是硬错误 | 一律 `#[unsafe(no_mangle)]`（3.7.6） |
| C-12 | 角色有两套编号，client key 能冒充 server | 唯一的 `Role`；握手核对 `Hello.role == KEYMETA.role`、key 0 只能是 Server（3.3.2） |
| C-13 | 超过 1 MiB 的写集 → MsgAppend 超过帧 body 上限 → 连接被断、元数据永久卡死 | 批次写集 768 KiB 上限；RaftMsg 走 bulk（3.4.1.4） |
| C-14 | F_SETLKW 占住 FUSE 线程 → ≥ 16 个等待者卡死挂载点 | lock-wait 线程持有 `ReplyEmpty`，客户端轮询（8.6） |
| C-15 | 一个不读 socket 的客户端卡住执行器 | 每连接写线程 + 有界发送队列 + 5 s 写超时（1.2） |
| C-16 | inline 文件截断后旧字节在扩展时重新可见 | `len(INLINE) ≤ size` 不变式（3.2.7、3.5.1） |
| C-17 | cto 的 O_DIRECT 写在迁移切换 layout 后静默丢失 | O_DIRECT 走 strong 的同步写路径（8.3） |
| C-18 | 各节点插件集合不同 → leader 切换后策略被静默跳过 | 集群插件清单（3.7.2） |

