# mem 传输插件（独立 crate 示例）

**钩子**：transport（scheme `mem`）　**热路径**：是，预算 100 ms　**fail point**：`plugin.mem.transport`
**指标**：`wmfs_mem_frames_total{dir="in"|"out"}`

## 它是什么

一个在**同一进程的内存里**收发帧的传输插件。它有两个用途：

1. **证明"新增一个传输插件不改本体一行"**（理念文档第 9 章成功标准）：它是一个独立的 crate，只依赖 `wmfs-core` 的公开接口
   （`Transport` / `Conn` trait、帧、内存池），握手、认证、重连重试、读写线程全部由本体提供。接进 `wmfs` 只需要
   `cli/src/registry.rs` 一行和 `cli/Cargo.toml` 一个 feature（`mem`，默认不编进来）。
2. **给单进程的组件测试用**：不占端口，测试可以起很多个互不干扰的服务端。

## 怎么工作

- `MemTransport::listening("名字")` 在进程内的一张表里登记这个名字；`connect("名字")` 造一对互相连通的连接，把服务端那一头交给
  监听方。`MemTransport::new()` 只连别人、不监听。
- 帧经有界通道（每个方向 256 帧）传过去；bulk 以 `BufSlice` 跟着走，**接收方**在自己的 `recv` 里把它拷进自己的内存池缓冲后才放掉
  发送方的缓冲——和 RDMA"接收方 READ 完、发送方才释放"的语义一样。池里 100 ms 取不到缓冲时交出 `bulk_dropped`（本体回 BUSY）。
  握手完成之前收到带 bulk 的帧就断开（与 tcp 相同）。
- `MemTransport::with_frame_limit(n)`：帧头 + body 超过 n 字节的帧发不出去（`send` 返回 `FEATURE_UNSUPPORTED`，模拟 RDMA 的
  120 KiB 上限），用来测试"帧超过传输的上限只让这一个请求失败、连接不断"。

## 试一下

```bash
cargo test --release -p plugin-mem-transport   # Ping、4 MiB bulk、错误密钥、client key 冒充 server、角色权限表、超限的帧、fail point
```
