# ADR-0013 strong 一致性的首发实现与限制（不支持跨节点共享可写 mmap）

- 状态：accepted（人类于 2026-10-20 批准）
- 日期：2026-10-20
- 偏离条款：理念文档第 3 章 [固定]"一致性模型：`cto` 与 `strong`（租约 / 锁）"的实现方式；8.2"跨节点共享可写 `mmap` 仅在 `consistency=strong` 下支持"（对外承诺）
- 是否触及契约层：**是**
- 批准类别：**需批准**（A 栏：要你拍板）；确认批次 4（M12 开工前）
- 相关章节：design.md 3.1.3、3.5.3、3.5.5、8.1、8.3、8.5；semantics.md 2.3

## 背景

正确支持跨节点共享可写 mmap 需要页级租约与回调：服务端要能主动召回客户端的写权限、让对方刷出脏页并失效页缓存。FUSE 无法拦截已映射页上的写；首发没有服务端到客户端的回调通道。

## 决定

1. strong = 客户端零缓存（`FOPEN_DIRECT_IO`，entry/attr TTL 0，不进 cache_store）+ 每次 read 前一次线性一致 `GetAttr`（size 与 epoch）+ 每次 write 同步写链并同步 `CommitWrite`（校验 epoch；`STALE_LAYOUT` 时用本次 write 手里的数据按新 layout 重写）+ 集群范围的锁 + 元数据原子分配 `O_APPEND` 偏移（`AppendReserve`）。
2. strong 目录里新建的文件不 inline；以写方式打开已有的 inline 文件时先转成 chunk（复用 uninline 流程），协调器也会转换残留的 inline 文件——所以 strong 的写路径永远不处理 inline 文件。
3. cto 下用 `O_DIRECT` 打开的文件，写走同一条同步路径（否则不在本地留副本、长度到 close 才提交的 O_DIRECT 写，会在迁移切换 layout 后静默丢失）。这一条是 cto 的规则，随 ADR-0028 在批次 0 确认、M6 实现；这里只说明 strong 在 M12 复用同一份代码。
4. 没有读写委托（delegation）租约；租约原语只有会话、target 成员、能力令牌三种（ADR-0023）。
5. **跨节点共享可写 mmap 首发在任何等级都不支持**：不开 `FUSE_DIRECT_IO_ALLOW_MMAP`，于是 strong 文件上任何 `MAP_SHARED` 映射被内核以 `ENODEV` 拒绝；`MAP_PRIVATE` 可用；cto 文件的 `MAP_SHARED` 只在本节点内一致。

## 备选方案

- 实现页级缓存租约 + 回调召回（二期设计：独占写租约 + `inval_inode` 召回）：工作量大、正确性风险高。推迟。
- 允许 MAP_SHARED 但不保证跨节点一致：静默不一致，违反原则 7。否决。
- strong 下 inline 文件的写走同步的 `CommitWrite{inline_patches}`：要额外定义 O_APPEND 与 inline 补丁的组合规则。否决（选更简单的"strong 不 inline"）。

## 后果

依赖跨节点 MAP_SHARED 一致性的程序会得到明确的 `ENODEV`（诚实拒绝）；strong 写较慢（每次写一次元数据提交，本机约 3–6 ms），只建议用于协调类负载。
