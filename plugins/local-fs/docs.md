# local-fs chunk_store 插件

**钩子**：chunk_store（唯一型）　**热路径**：是（每次读写 chunk），预算 100 ms/次（本职就是磁盘 IO，超了照样计数，dashboard 上看得到慢盘）
**fail point**：`plugin.local-fs.chunk_store`（本体包在每次调用外层，触发 = 这次调用以注入的码失败）、`chunk.pending.eio`、`chunk.pending.torn`、
`chunk.commit.before_rename`、`chunk.read.eio`、`chunk.read.corrupt`、`chunk.sync.eio`　**指标**：`wmfs_chunkstore_op_seconds{op}`

## 它做什么

把本体编好的 chunk 字节存成 target 目录里的普通文件（ext4 或 XFS；本机是 ext4，ADR-0008）。chunk 文件的格式（固定头、块表、
数据区）由本体定义和编解码，本插件只负责"按路径存取字节"，不解析头部（design.md 3.2.4、3.7.3）。

```
<target>/TARGET                                                   超级块（本体在 wmfs init 时写）
<target>/c/<链:8位hex>/<ino%256:2位hex>/<ino:16位hex>-<epoch:8位hex>/<chunk 号:8位hex>    已提交的 chunk 版本
<target>/p/<链>-<ino>-<epoch>-<chunk 号>.p                         正在写的新版本（pending，启动时清空）
<target>/t/<…>.t                                                   同步/修复用的临时文件（启动时清空）
```

- **写一个新版本**：先写 `p/` 下的 pending 文件（改动的块写新数据，没改动的块用 `copy_file_range` 从旧版本同位置复制，XFS 上
  就是 reflink），然后一次 `rename` 覆盖旧版本——断电时要么是旧版本、要么是新版本，不会有"写了一半"的文件（design.md 3.2.5 R1）。
- **不在写路径上 fsync**（持久性靠多副本）；客户端 `fsync` 时本体调 `sync`：对涉及的已提交文件 `fdatasync`，再 fsync 它所在的
  目录与各级父目录（rename 产生的目录项否则可能丢，R2）。O_SYNC 写在 rename 之前就 fdatasync。
- **同步/修复安装**：`t/` 临时文件 → fdatasync → rename → fsync 目录（R3）。**删除**：unlink 与删目录，不 fsync（R4）。
- **启动**：清空 `p/` 与 `t/`（R5）——写到一半的 pending 文件永远不会被读到。
- **容量**：`statfs` 报文件系统的大小与本 target 已提交文件实际占的块；本体再与 node.toml 的 `capacity` 取小（`wmfs dev` 默认 20 GiB，
  防止测试写满系统盘），已用到了上限时写入回 `ENOSPC`。

## 配置

没有部署参数：target 的目录与容量上限写在 node.toml 的 `[[target]]`（本体读）。

## 诚实说明

- 首发没有做 fd 缓存（每次读写打开一次文件；4 MiB 的 chunk 下打开文件的开销远小于读写本身）。
- 掉电时 fdatasync 与目录 fsync 的顺序是否在真实设备上足够，本机无法验证（需要 root 的 dm-log-writes 或 LazyFS，ADR-0015）；
  测试用模拟掉电的内存后端检查本体"该 fsync 的地方都调用了"。XFS 上可能出现"rename 已持久、数据未持久"的文件，本体的头 CRC 与
  块校验和会发现它（R5）。
