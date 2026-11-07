# ADR-0043 M12 补齐的锁、回收站与 strong 细节

- 状态：accepted
  - 第 1 条由人类于 2026-11-07 决定。
  - 其余由主管于同日按 design.md 开头的例外条款决定：规格漏写的技术细节、落实规格已有规则的修补，或对外说明的澄清（其中两条是内核限制，没有别的做法，只能如实写明）。
- 日期：2026-11-07
- 性质：
  - 回收站的目录结构（第 1 条）；
  - 握手 feature 位按规格落实（第 3 条）；
  - 一个 M8 遗留的严重问题的修复（第 2 条）；
  - semantics.md 的几处澄清。
- 是否触及契约层：**是**。
  - 握手里 `features` 的取值（第 3 条，金样随之更新）；
  - 管理 API 只增的字段、DIRATTR 的新值（第 5 条）；
  - 快照 pin 的回收时机改了 design 3.4.1.7 的一句。
- 相关章节：design.md 3.2.11、3.3.2、3.4.1.7、第 5 章（回收站细节）、8.5–8.7；semantics.md 2.3、2.4、第 3 节（放松清单）、第 7 节；roadmap M12 演示；ADR-0013、ADR-0014、ADR-0023、ADR-0036、ADR-0037。

## 背景

M12 实现了这些东西：
- 集群锁（复制的锁表、lock-wait 线程）；
- strong（direct_io、读前 GetAttr、同步提交、AppendReserve）；
- 回收站、ttl、trash-expire、审计标记。

三路审查共 51 条意见，修复与验证都一次通过。下面是实现与修复时上报、要主管或人类定的事。

## 决定

### 人类决定（2026-11-07）

1. **回收站按用户分子目录**（审查发现的隐私问题）。
   - 问题：`.wmfs-trash` 原来是 1777、平铺的。文件原来靠父目录挡住别人（例如 700 的家目录），进了回收站之后能不能读只看文件自己的权限位；nas-safe 给 `/home` 设了 `trash=7d`，被删的 644 文件 7 天内所有人可读。
   - 改成：
     - 被删文件移进 `.wmfs-trash/<uid>/`，`<uid>` 是**文件的属主**（不是删它的人）。
     - 子目录第一次用时由服务端建，mode 0700、属主是这个 uid。
     - `.wmfs-trash` 本身 mode 0711（大家能走进自己的子目录，但列不出有哪些用户）。
     - 只有文件属主（和 root）能看到、恢复自己的文件，与 Linux 桌面回收站的 `.Trash-<uid>` 相同。
   - 其余不变：名字 `<unix秒>-<ino>-<原名>`、`user.wmfs.origin`、到期索引、用普通 `mv` 移出即恢复、仍计入该子树的配额。
   - 改动范围：semantics.md 放松 ⑯ 与第 7 节的说法、design.md 第 5 章的回收站细节、roadmap M12 演示（`ls ~/mntA/home/.wmfs-trash/$(id -u)/`）、trash-expire 遍历各用户子目录。

### 主管决定

2. **快照 pin 的回收**（M8 起就有的严重问题，M12 验收时发现）。
   - 问题：快照的 pin 是一个 redb 读事务。过期的 pin 只在下一次导出时才清理，follower 装好快照之后不再导出，pin 就一直挂着，leader 的 meta.redb 回收不了页面。实测每次写约涨 100 KiB，leader 涨到 1.4 GiB 时 follower 只有 28 MiB。
   - 修法：
     - 发送结束（成功或失败）就释放 pin；
     - raft-send 的看门狗释放超过 60 s、又没有在途发送的 pin。
   - 这改了 design 3.4.1.7 的"10 分钟过期"；m12.sh 加了"每个节点的 meta.redb ≤ 256 MiB"的检查。
3. **握手的 feature 位按规格落实**（design 3.2.11 第 3 条："消息枚举的新变体……只能在对端于握手中声明支持（feature 位）后发送"）。
   - 问题：M12 实现了 strong 与锁，但 `OUR_FEATURES` 仍是 0；M5 的 inline 也没声明。
   - 修法：
     - 节点与客户端在握手里声明已实现的 `FEAT_STRONG | FEAT_LOCKS | FEAT_INLINE`（`FEAT_BULK_REMOTE` 照 RDMA 传输的实际情况）。
     - 客户端发 `SetLock`/`GetLock`/`AppendReserve` 之前核对协商结果，对方没声明就回 `EOPNOTSUPP`、不发。
     - 握手金样随之更新。首发所有节点同版本，这只是把规格写明的规则做实。
4. **几处只能如实写明的限制**（semantics.md）：
   - **strong 下的 O_APPEND**：内核按 max_write（通常 1 MiB）把一次大的 `write()` 切成几个 FUSE WRITE，每段各自分配偏移。所以只有单次不超过这个大小的追加保证整条连续；更大的追加互不覆盖，但可能与别的节点的追加交错。另外 `pwritev2(RWF_APPEND)` 不经文件标志，FUSE 看不到，按内核给的偏移写。补进 2.3 第 3 条。
   - **目录上的锁只在本节点有效**：内核 FUSE 的目录操作没有 lock/flock 回调，请求到不了 wmfs。要跨节点互斥，请锁目录里的一个普通文件。补进第 3 节放松清单与 2.4。
   - **冻住挂载进程时的 close**：只读打开不再回 `FOPEN_NOFLUSH`，因为 POSIX 锁只能经 FLUSH 释放，实测只读 fd 上的读锁 close 后还在。所以挂载进程被冻住（SIGSTOP）时，这个挂载点上的任何 close 都等它恢复，包括子进程退出时关闭继承来的 fd。M10 只有写 fd 要等。
   - **F_GETLK 的 l_pid** 只在同一台机器上有意义。
   - **rm 有其他硬链接的文件**不进回收站，只去掉这个名字；数据还在，回收站只管最后一个链接。
   - **删除设了 `trash` 的那个目录本身**（`rm -r`）第一次会因为回收站被重新建出来而报 `ENOTEMPTY`。先 `wmfs unset <目录> trash` 再删。首发不另做处理。
5. **只增的取值确认**：
   - 管理 API：`AttrActual.bytes`（回收站里的字节数）、`AttrActual.lag`（审计游标落后的条数），`HealthReply.audit_lag` 开始填。
   - DIRATTR：`consistency=strong`；`trash`/`ttl` 存 `off` 或毫秒数；`audit` 存 `off`、`on` 或 `on,ops=A+B,uids=1+2`。金样在 attr_registry 的测试里。
   - 新指标 `wmfs_trash_expired_total`、`wmfs_ttl_deleted_total`，新 fail point `plugin.trash-expire.bg_task`、`plugin.ttl.bg_task`。
   - 锁与追加的取值：
     - `SetLock`/`GetLock` 按 `AuthzOp::Handle{read:false, write:false}` 授权；`AppendReserve` 按 `Handle{write:true}`。
     - kind 不在 1–3、或 start > end → `EINVAL`；end ≥ `i64::MAX` 一律记成到文件尾。
     - `AppendReserve` 回 `Offset{offset, attr}`，配额精确检查。
6. **等锁的超时**：放松 ③ 维持原样（有处理函数的普通信号打断不了 F_SETLKW），补一句"所以 `flock -w`、`alarm()` 这类等锁超时不起作用"。审查提的"lock-wait 线程看挂起信号、回 `EINTR`"是规格之外的改进，首发不做。

## 后果

- 回收站里的文件只有属主能看到。
- leader 的元数据库不再随写入一直变大。
- 锁与 strong 消息只在双方声明支持之后才发。
- 内核造成的几处限制如实写进 semantics.md。
