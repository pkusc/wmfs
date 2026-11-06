# xxh3 校验和插件

**钩子**：transform（校验和一类，altitude 702，算法编号 2）　**热路径**：是（每个 64 KiB 块），预算 20 µs/块
**fail point**：`plugin.xxh3.transform`（触发 = 这一块以注入的错误码失败）　**指标**：`wmfs_xxh3_blocks_total{dir}`、`wmfs_xxh3_mismatches_total`

## 它做什么

和 crc32c 一样，给每个 64 KiB 块的**明文**算一个指纹存进块表（xxh3-64，存在块表条目 `checksum` 的前 8 字节）；读的时候重算，
对不上就回 `CHECKSUM_MISMATCH`：本体把这个 chunk 标为损坏，客户端换一个副本读，全部副本都坏了才给应用 `EIO`，scrub 之后把它修好
（design.md 3.2.5 R5）。64 位的指纹比 crc32c 的 32 位更不容易"碰巧对上"。

## 配置

没有部署参数：`wmfs set <目录> checksum=xxh3`（M11 起可设；默认 crc32c）。只对新文件与截断到 0 的文件生效，已有文件不迁移
（design.md 第 5 章：校验算法属于文件的存储布局）。

## 诚实说明

xxh3 能发现意外损坏（位翻转、截断），**不防故意篡改**（防篡改要 aes-gcm 的 GCM tag）。
