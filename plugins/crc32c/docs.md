# crc32c 校验和插件

**钩子**：transform（校验和一类，altitude 701，算法编号 1）　**热路径**：是（每个 64 KiB 块），预算 20 µs/块
**fail point**：`plugin.crc32c.transform`（触发 = 这一块以注入的错误码失败）　**指标**：`wmfs_crc32c_blocks_total{dir}`、`wmfs_crc32c_mismatches_total`

## 它做什么

数据写进 chunk 文件之前，对每个 64 KiB 块的**明文**算一个 crc32c 指纹（aarch64 有硬件指令，很快），存进块表；读的时候重新算一遍，
对不上就说明数据坏了：回 `CHECKSUM_MISMATCH`，本体把这个 chunk 标为损坏（Corrupt），客户端换一个副本读，全部副本都坏了才给应用
`EIO`——**宁可报错，也不返回错误的数据**（design.md 3.2.5 R5）。

变换的顺序由本体固定：写 = 校验和 → 压缩 → 加密，读反过来（解密 → 解压 → 校验）。校验的是明文，所以压缩、加密之后再坏掉也查得出来。

## 配置

没有部署参数：用哪种校验和是目录属性 `checksum`（`crc32c` / `xxh3` / `blake3`，默认 crc32c；只对新文件与截断到 0 的文件生效）。

## 诚实说明

crc32c 能发现磁盘位翻转、截断这类意外损坏，**不防故意篡改**（防篡改要加密插件的 GCM tag）。
