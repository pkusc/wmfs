# ADR-0003 首发不使用 io_uring（服务端 IO 与 FUSE 会话）

- 状态：accepted（人类于 2026-10-20 批准）
- 日期：2026-10-20
- 偏离条款：理念文档第 7 章 [默认]"语言：Rust（…、`io-uring`、…）"与"客户端：FUSE（passthrough + io_uring）首发"中的 io_uring 部分
- 是否触及契约层：否
- 批准类别：需确认；确认批次 0（B 栏：知悉即可）
- 相关章节：design.md 8.8

## 背景

- fuser 0.18 只定义了 `FUSE_OVER_IO_URING` 标志位，会话循环仍是读写 `/dev/fuse`，没有实现 FUSE-over-io_uring。
- 本机内核模块参数 `/sys/module/fuse/parameters/enable_uring = N`，打开需要 root。
- `io-uring` crate 不在已验证依赖清单里，且其完成队列模型与"只用同步代码"的承诺冲突。

## 决定

- 服务端 chunk IO 用阻塞 `pread/pwrite/copy_file_range/fdatasync` + 固定大小的 IO 线程池。
- FUSE 用 fuser 的多线程会话（`n_threads = 16`、`clone_fd = true`）。

## 备选方案

- 服务端用 io-uring crate：系统调用更少，但引入 unsafe 与类 async 的完成处理，对新手不友好。推迟。
- 等待 fuser 支持 FUSE-over-io_uring：不可控。

## 后果

- 每次 IO 的系统调用开销更高；在小集群与本机硬件上不是瓶颈。
- 将来可以在 chunk_store 或 transport 插件内部改用 io_uring，不改协议与契约。
