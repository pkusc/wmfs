# ADR-0021 chunk 文件格式 v1 与数据侧崩溃一致性

- 状态：accepted（人类于 2026-10-20 批准）
- 日期：2026-10-20
- 性质：契约层首次定义（理念文档第 3 章"持久化格式"：chunk 头部含版本、校验、变换标记、所属 inode；8.1"快照二期，但 chunk 版本化从第一天设计"）；另附一条对理念文档 5.2"生效方式"的偏离（决定 7）
- 是否触及契约层：**是**（不可逆的磁盘格式）
- 批准类别：**需批准**（格式）+ 需确认（决定 7 的偏离）；确认批次 0（B 栏）
- 相关章节：design.md 3.2.2–3.2.5、3.4.2.9、3.10、4.9、第 5 章；semantics.md 第 4 节

## 背景

可选的格式有三种：日志结构记录（需要压实、启动时全盘扫描重建内存块索引、"撕裂尾部 vs 中段损坏"启发式）；原地打补丁（会破坏旧的已提交版本，回滚不安全）；"每个版本一个文件、rename 提交"（最简单）。

## 决定

1. **一个 chunk 版本 = 一个文件**：256 字节固定头（magic、格式版本、三组 feature 位、ino、epoch、chunk_idx、chain_id、block_size、**chunk_version**、chunk_size、logical_len、变换算法与密钥 id、snap_epoch 预留、cluster_id、write_time、header_crc）+ 每块 72 字节的块表（stored_len、plain_len、flags、随机 nonce、GCM tag、明文校验和）+ 数据区（第 i 块固定在 `header_len + i × block_size`，稀疏存放）。incompat bit2 与 `compress_alg` 128..=255 保留给二期的动态库压缩，首发不写。
2. 新版本先写在 `p/` 下（改动块写新数据，未改动块用 `copy_file_range` 从旧版本复制），**提交 = 一次同一文件系统内的 rename**；写路径不 fsync（持久性靠多副本），客户端 fsync 时沿链 `fdatasync` + 目录 fsync；同步/修复安装用 tmp → fdatasync → rename → 目录 fsync。启动时清空 `p/` 与 `t/`。
3. 变换参数属于 layout（同一 (ino, epoch) 的所有 chunk 一致）；**AES-GCM 每块随机 96-bit nonce**（绝不从版本号推导）；AAD 不含版本号（未改动块原样复制后仍可解）。
4. chunk 版本在 `(ino, epoch)` 的生命周期内单调：**截断写新版本、从不删除 chunk 文件**；只有整个废弃的 `(ino, epoch)` 才被删除（它永不复活）。截断到 0 换新 epoch，并在同一个元数据闭包里**按当前生效属性重建整个 layout**。
5. **Corrupt 标记**只在内存：读一律拒绝；不保留旧块的写（整块覆盖、截到 0）照常执行并清除标记；要保留旧块的写先从其他 Serving 成员修复，没有则 `EIO`。
6. 网络传输整 chunk 用 packed 形式（头 + 块表 + 所有非空块的存储字节）；快照预留：二期在快照点把已提交文件硬链接为 `<idx>@<version>`，启用 incompat 位 `chunk.snapshots`。
7. **偏离（需确认）**：因为变换参数与放置属于 layout，`compress`、`encrypt` 改动对**新文件与截断到 0 的文件**立即生效；已有文件里新写入的数据仍按它当前的 layout，直到协调器迁移（理念文档 5.2 写的是"新数据 + 协调器"）。一直写打开的文件会被协调器跳过：`wmfs set` 时警告，`wmfs status` 单独计数，跳过超过 24 h 发 `PolicyViolation`；semantics.md 写明。`checksum`、`locality`（理念文档写"新数据"）同理写成"新文件"，不迁移。

## 备选方案

- 日志结构记录 + 压实：小块覆盖写更省，但多约 1k 行与更难的崩溃判定。否决。
- 原地打补丁：不安全。否决。
- 按块记录变换参数、同一文件里新旧参数混存，使"新数据"立即生效：块表要多存算法与密钥、读路径要逐块分派、迁移规则更复杂。否决（截断到 0 重建 layout 已覆盖最常见的"整文件覆盖写"）。

## 后果

- 崩溃原子性只靠 rename，不需要日志、压实或常驻内存索引；按链分目录让重同步只需一次目录遍历。
- 代价：小块随机覆盖写要复制该 chunk 其余块的存储字节（ext4 上约 1 ms / 4 MiB；XFS 上是 reflink）。客户端按 chunk 合并脏数据能消掉大多数；块级增量格式二期用 feature flag 引入。
- 开启加密后，已有文件的新写入在转换完成前仍是明文——对外文档写明。
