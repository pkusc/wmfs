# blake3 校验和插件

**钩子**：transform（校验和一类，altitude 703，算法编号 3）　**热路径**：是（每个 64 KiB 块），预算 60 µs/块
**fail point**：`plugin.blake3.transform`（触发 = 这一块以注入的错误码失败）　**指标**：`wmfs_blake3_blocks_total{dir}`、`wmfs_blake3_mismatches_total`

## 它做什么

和 crc32c 一样，给每个 64 KiB 块的**明文**算一个指纹存进块表（blake3-256，32 字节全部写进块表条目的 `checksum`）；读的时候重算，
对不上就回 `CHECKSUM_MISMATCH`：本体把这个 chunk 标为损坏，客户端换一个副本读，全部副本都坏了才给应用 `EIO`，scrub 之后把它修好
（design.md 3.2.5 R5）。256 位的密码学哈希：比 crc32c、xxh3 慢一些，但几乎不可能"碰巧对上"。

## 配置

没有部署参数：`wmfs set <目录> checksum=blake3`（M11 起可设；默认 crc32c）。只对新文件与截断到 0 的文件生效，已有文件不迁移
（design.md 第 5 章：校验算法属于文件的存储布局）。

## 诚实说明

blake3 是密码学哈希，但哈希值与数据存在同一个文件里：能改数据的人也能改哈希，所以它同样**不防故意篡改**（防篡改要 aes-gcm 的 GCM tag）。
