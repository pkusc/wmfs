# tcp 传输插件

**钩子**：transport（scheme `tcp`）　**热路径**：是，预算 100 ms（一次 `send`，或收到帧头后读完 body + bulk）
**fail point**：`plugin.tcp.transport`　**指标**：`wmfs_tcp_bytes_total{dir="in"|"out"}`

## 它做什么

把 wmfs 的帧（48 字节帧头 + body + bulk，design.md 3.3.1）放在一条 TCP 连接上收发。它**只负责"能收发帧"**：
握手认证、body/bulk 的 CRC 校验、重连与重试、读写线程都在本体（core 的 `net/`），所以 RDMA 等别的传输插件和它共用同一套逻辑。

- **监听**：读自己的部署配置 `[plugin.tcp] listen`（`wmfs init` 写成 `--addr:--rpc-port`），返回实际绑定的地址，写进节点的地址表。
- **每条连接**：`TCP_NODELAY`、`SO_KEEPALIVE`、**写超时**（`[net] write_timeout_ms`，默认 5 s，按整帧算：每次写之前把 socket 的
  写超时设成这一帧剩下的时间，一帧 5 s 内写不完就报 `ETIMEDOUT`，core 断开这条连接并计数 `wmfs_conn_write_timeout_total`）、
  **读超时 15 s**（15 s 收不到任何字节就判对端已死；客户端连接握手完成后空闲 5 s 会由 core 发保活 Ping，所以正常的空闲连接不会被误判）。
- **发送**：帧头、body、各段 bulk 依次写出，bulk 的多段（`BufSlice`）逐段写，不拼接、不拷贝。
- **接收**：帧头先过 `FrameHeader::decode`（magic、帧头 CRC、版本、长度上限），不对就断开；bulk 直接读进内存池的缓冲：
  回复读进请求方预先给的 `recv_into`，请求按消息族选 lane（PeerCall 用复制专用 lane，其余普通 lane）。池里 100 ms 取不到缓冲时，
  把这段 bulk 读出来丢掉（字节流保持同步），交给 core 回 `BUSY`，发送方退避后重发——不会因为内存紧张卡住连接。
- **握手完成之前**只收握手帧：帧头声明了 bulk 就直接断开，不为它占内存池的槽位（握手帧从不带 bulk）。
- **计时**：发帧由 core 计时；收帧由本插件计时——读到帧头之后开始、读完 body + bulk 结束（空等帧头的时间不算），超过 100 ms 计入
  `wmfs_hook_over_budget_total{hook="transport",plugin="tcp"}`。
- 不支持单边描述符（`BULK_REMOTE`、`REPLY_REMOTE` 标志），收到即断开；注册内存是空操作。

## 配置

```toml
[plugin.tcp]
listen = "127.0.0.1:9300"   # 默认 0.0.0.0:9300
```

## 故障注入

`WMFS_FAILPOINTS='plugin.tcp.transport=pct(30,return(EIO))'`：core 的写线程每次发送前过这个 fail point，触发 = 这条连接作废，
调用方走"连接断开 → 重连重试"。演示：`wmfs ping tcp://127.0.0.1:19100 --count 100` 在 30% 发送失败下仍然 100/100 成功。
