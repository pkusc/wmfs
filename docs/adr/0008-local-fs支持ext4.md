# ADR-0008 local-fs 支持 ext4（本机测试在 ext4 上，推荐 XFS）

- 状态：accepted（人类于 2026-10-20 批准）
- 日期：2026-10-20
- 偏离条款：理念文档 4.1 `chunk_store` 首发插件"`local-fs`（XFS 上的 chunk 文件）"
- 是否触及契约层：否（chunk 格式本身见 ADR-0021）
- 批准类别：需确认；确认批次 0（B 栏：知悉即可）
- 相关章节：design.md 3.2.5、4.9

## 背景

本机根分区是 ext4；格式化一个 XFS 分区或 loop 设备需要 root（agent 不运行 sudo）。

## 决定

local-fs 只依赖 POSIX 语义：同一文件系统内 `rename` 原子替换、`fdatasync`、目录 `fsync`、`copy_file_range`（不支持时退回 `pread/pwrite`）。在 ext4 上开发与测试；文档推荐生产使用 XFS（`copy_file_range` 在 XFS 上会变成 reflink，小块覆盖写的写放大几乎为零）。XFS 没有 ext4 的 `auto_da_alloc` 启发式，掉电后可能出现"rename 已持久、数据未持久"的文件——chunk 头 CRC 与块校验和会发现它，"任何重启都以 Syncing 回链"保证它不会被当作好数据服务（ADR-0021、0024）。

## 备选方案

- 只支持 XFS：本机无法测试。否决。

## 后果

ext4 上小块覆盖写要真实复制未改动的块（约 1 ms / 4 MiB），写进文档；XFS 上的 reflink 行为需要在有 XFS 的环境里另行验证（可选：人类授权后用 loop 镜像测试）。
