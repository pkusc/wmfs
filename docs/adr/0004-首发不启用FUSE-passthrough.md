# ADR-0004 首发不启用 FUSE passthrough

- 状态：accepted（人类于 2026-10-20 批准）
- 日期：2026-10-20
- 偏离条款：理念文档第 7 章 [默认]"客户端：FUSE（passthrough + io_uring）首发"中的 passthrough 部分
- 是否触及契约层：否
- 批准类别：需确认；确认批次 0（B 栏：知悉即可）
- 相关章节：design.md 3.7.3（`CacheStore::local_file`）、8.8

## 背景

FUSE passthrough 让内核把读写直接转给一个**本地后备文件**，并且 `FUSE_DEV_IOC_BACKING_OPEN` 需要 `CAP_SYS_ADMIN`。wmfs 的文件被切成多个 chunk、带头部与块表、可能压缩/加密、分布在多个节点上——没有可以直通的本地文件；挂载也以普通用户运行。

## 决定

首发不使用 passthrough。预留接口：`CacheStore::local_file(ino, change_id)`——二期在 local-nvme 缓存完整缓存了某个只读文件的明文副本时，对该副本做 passthrough（fuser 0.18 已有 `open_backing/opened_passthrough`）。

## 备选方案

- 以 root 挂载并对缓存副本做 passthrough：需要 root 与完整的缓存一致性逻辑，首发收益小。推迟。

## 后果

读路径多一次用户态拷贝（FUSE 决定）；对首发目标负载可以接受。
