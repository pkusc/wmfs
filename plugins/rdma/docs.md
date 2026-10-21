# rdma 传输插件

**钩子**：transport（scheme `rdma`）　**热路径**：是，预算 100 ms（一次 `send`，或收到帧之后读完 body + bulk）
**fail point**：`plugin.rdma.transport`　**指标**：`wmfs_rdma_ops_total{op="send"|"recv"|"read"|"write"}`（插件构造时就注册，值从 0 起）

> **未在本机验证**：本机的 4 个 RoCE 端口链路都是 DOWN，按人类的决定（2026-10-20）不建 Soft-RoCE、不做 RDMA 实机测试。
> 和设备无关的逻辑（描述符表、信用控制、在途表、握手前禁止单边操作、收帧状态机、拆除顺序、引导连接、选 GID、QP 状态迁移
> 的属性）由单元测试测过（`cargo test -p wmfs-plugins --features rdma rdma`，大部分跑在进程内的"假网卡"上），真正调用
> libibverbs 的 `verbs.rs` 与 `device.rs` 没有在真设备上跑过。

## 它做什么

把 wmfs 的帧（48 字节帧头 + body + bulk，design.md 3.3.1）放在 RDMA 的 RC QP 上收发。和 tcp 插件一样只负责"能收发帧"：
握手认证、CRC 校验、重连重试都在本体。只有数据族（DataCall、PeerCall）与 Ping 会走 rdma；元数据、集群管理、raft、快照永远走
`[net] control_scheme`（默认 tcp），因为它们的 body 可能超过 rdma 的 120 KiB 上限（design.md 3.3.3）。

- **控制帧**（帧头 + body ≤ 120 KiB）用 SEND 发到对端预先准备好的接收槽：每条连接 16 个 S 档（128 KiB）槽位，另有 1 个 S 档
  槽位做发送暂存。**信用控制**：同一条连接上在途的请求帧最多 16 个，第 17 个 `send` 在写线程里等，直到有回复还回信用或连接关闭
  （不按写超时放弃："对端回得慢"不是"对端不读"，tcp 在这种情况下也不断开），所以对端永远有空槽、不会出现 RNR。按帧计、
  不按 req_id：握手消息、`req_id = 0` 的保活 Ping 都各占一个。
- **大块数据（bulk）用单边操作，永远由接收请求的一方发起**：请求带 bulk 时附一张描述符表（每个长度不为 0 的段一项
  `{地址, rkey, 长度}`），服务端按顺序 RDMA READ 进自己内存池里的**一个**缓冲；期望回复带 bulk 时，请求附上 `recv_into` 缓冲的
  描述符，服务端 RDMA WRITE 进去、再 SEND 回复帧（RC 保序：回复到达时数据已经可见）。发送方把交出去的缓冲留在"在途表"里，
  回复到达（或连接拆除）之后才放。
- **安全**：服务端（accept 侧）的 QP 不开放任何远程读写，客户端（connect 侧）也只在握手完成之后才开放——不可信的客户端碰不到
  服务端内存，由网卡保证；rkey 只随每个请求的描述符给出，建连时不交换。握手完成之前收到声明了 bulk 或带描述符的帧就断开；
  描述符表与帧头的 bulk_len 对不上、地址加长度溢出，也断开。
- **建连**：先在插件自己的引导 TCP 端口（`[plugin.rdma] listen`，`wmfs dev` 下是 19n01）上用插件私有消息 0x0600 交换
  `{qpn, psn, gid, mtu}`，QP 进 RTS 之后交给本体做握手。引导 TCP 一直开着：它断了 = 这条 RDMA 连接作废。一个连上来却不说话的
  对端最多占用一个名额 5 s；连接数到了 `max_qps` 之后新的引导连接直接关掉。
- **设备与 GID**：按引导连接本端的 IPv4 地址自动选——找"RoCE v2、IPv4 映射、地址等于它、端口 ACTIVE"的 GID（只读 sysfs）；
  写了 `device` / `gid_index` 就只在符合的里面找。本机根本没有这个地址的 GID 时连接失败（`EINVAL`，不重试）；GID 在、只是端口
  链路不是 ACTIVE（例如交换机重启）时按连接断开处理（重连重试）。日志里列出全部候选；`wmfs doctor` 也会列出。
- **推迟打开设备**：默认 `listen` 为空（不监听），注册内存只记下区域，所以没有 RDMA 设备、没装 libibverbs 的机器上只用 tcp 照常工作；
  第一次需要时（监听一个具体地址，或第一次连接）才打开设备：每设备一个 PD、一个完成队列（按 `max_qps` 定大小）、一个轮询线程
  `rdma-cq-<设备>`，对整个内存池注册一次（需要足够的 memlock，见 `wmfs doctor`）。传输被丢弃、连接全部拆除之后轮询线程退出。
- **接收槽启动校验**（只在监听的一侧）：`2 × max_qps × 16 × 128 KiB ≤ S 档`，不满足就拒绝启动，日志写明要把 `[mem] s_mb` 调到多大。
- **缓冲什么时候回池**：交给网卡的缓冲只在它的工作请求完成之后、或 QP 销毁之后才还给内存池（`close()` 只让 QP 出错并唤醒等待者；
  等不到完成事件的缓冲进"墓地"，QP 销毁之后才回池）。

## 限制（与 tcp 不同的地方）

- 帧头 + body ≤ 120 KiB，超过 → 这一个请求以 `FEATURE_UNSUPPORTED` 失败（不截断、不断开）。
- 一个请求的 bulk 最多 16 段（长度不为 0 的 `BufSlice`），超过 → `FEATURE_UNSUPPORTED`（tcp 没有这个限制）。
- 期望回复带 bulk 时必须给足够大的 `recv_into`：没给或不够大，服务端回 `FEATURE_UNSUPPORTED`（绝不越界写）。
- 发出的 bulk 与 `recv_into` 必须来自本进程注册过的内存池（不在里面 → `FEATURE_UNSUPPORTED`）。一个传输只服务同一个内存池
  （每个进程一个池）：池被丢弃后在同一地址新建的池会沿用旧的注册，不支持这种用法。
- 只支持请求与回复两种帧：流（SnapshotFetch）与 OneWay（RaftMsg）不走 rdma。
- 只支持 RoCE v2 的 IPv4 地址（InfiniBand 的 LID 寻址、IPv6 GID 首发不做）。
- 协议 feature 位 `BULK_REMOTE` 首发不在握手里协商：双方都是 rdma 时才会出现描述符，由插件两端自己处理。
- 同一条连接上 16 个请求都超过 15 s 还没回复时，保活 Ping 也拿不到信用，两端都会在 15 s 收不到帧后判对端已死、断开重连
  （它们的调用早已在 10 s 时超时；tcp 下保活照常，连接不断）。
- 多个线程同时对同一个对端发**第一次**调用时，core 会让每个线程各自建一条连接、只留先建好的那条：每条都要在两端各占一个
  `max_qps` 名额与 17 个 S 档槽位，名额不够的那几个调用以 `ECONNRESET` 结束（可重试）。先建好连接再并发就没有这个问题。

## 构建

rdma 在默认 feature 里。编译它需要 **cmake 与 libclang**（依赖 rdma-mummy-sys 的 build.rs 用它们生成并编译 libibverbs 的
dlopen 桩）；**运行**时不需要 rdma-core：没有 libibverbs 时列不出设备，只用 tcp 不受影响。不想编 rdma 时：

```bash
cargo build --release --no-default-features --features tcp,shared-key   # 在 cli/（wmfs）与 plugins/（wmfs-plugins）里同理
```

## 配置

```toml
[plugin.rdma]
listen = "10.77.0.1:19101"   # 引导 TCP 的监听地址（IPv4:端口）；默认空 = 不监听
max_qps = 32                 # 最多同时几条 RDMA 连接；默认 64（服务端），wmfs dev 写 32（它的 S 档只有 128 MiB）
# device = "rxe0"            # 默认空：按本端 IPv4 自动选
# gid_index = 1              # 默认 -1：自动选 RoCE v2 的 IPv4 映射 GID
```

`wmfs dev up --nodes 1 --rdma 10.77.0.1` 会写好上面两行，并设 `[net] data_scheme = "rdma"`（每次 `up` 都按这次的参数改写：
不带 `--rdma` 就改回只用 tcp）。客户端：`wmfs ping rdma://10.77.0.1:19101`。

## 故障注入

`WMFS_FAILPOINTS='plugin.rdma.transport=pct(30,return(EIO))'`：本体的写线程每次发送前过这个 fail point，触发 = 这条连接作废，
调用方走"连接断开 → 重连重试"。假网卡上的用例：`rdma_stack_failpoint_breaks_the_connection_and_retry_reconnects`
（plugins/rdma/tests/stack.rs，不需要设备，`scripts/accept.sh M2` 里单独列了一条）。
