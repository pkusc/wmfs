# ADR-0006 CLI 命令名为 `wmfs`（替代 `fs`）

- 状态：accepted（人类于 2026-10-20 批准）
- 日期：2026-10-20
- 偏离条款：理念文档 5.1 的命令示例 `fs set/get/unset/status`；第 7 章"部署：`fs init` / `fs join`"
- 是否触及契约层：否
- 批准类别：已同意（人类在需求中确认用 `wmfs set/get/unset/status/init/join/mount/...`）；确认批次 0（知悉）
- 相关章节：design.md 7.2

## 背景

`fs` 过于通用，容易与系统工具、shell 函数或其他软件冲突。理念文档要求单二进制。

## 决定

唯一的二进制叫 `wmfs`，所有功能是它的子命令：`wmfs init/join/server/mount/umount/set/get/unset/status/usage/…`。文档中出现的 `fs <子命令>` 一律对应 `wmfs <子命令>`，语义不变。

## 备选方案

- 保留 `fs` 作为 `wmfs` 的别名（符号链接）：可能与用户环境冲突，不默认安装；用户可以自己建别名。

## 后果

仅命名差异；文档与演示统一使用 `wmfs`。
