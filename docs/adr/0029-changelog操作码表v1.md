# ADR-0029 changelog 操作码表 v1

- 状态：accepted（主管于 2026-10-21 决定：补齐 M0 审查发现的技术缺口，不改变 semantics.md 的任何承诺、不增删功能；按人类 2026-10-20"技术契约全部同意""只在需要拍板或 sudo 时停"的授权处理，并在进度说明里告知人类）
- 日期：2026-10-21
- 性质：契约层补齐（持久化字段 `ChangeRecV1.op` 的取值）
- 是否触及契约层：**是**（写进 KV 的持久化数值）
- 相关章节：design.md 3.2.8、3.11、5.1（`audit`）；ADR-0005、ADR-0020

## 背景

`ChangeRecV1.op` 是写进 CHANGELOG 记录的 u16，`AuditSpec.ops`（`audit=ops=create+unlink`）也按这个数过滤；对外的 `ChangeEvent.op` 是名字。但 design.md 只零散地提到 Create、SetSize、SessionEvicted、AttrSet/AttrUnset、PolicyViolation、PluginListReset 等名字，没有"数 ↔ 名"的表。没有这张表，不同里程碑的 agent 会各编各的数。

## 决定

1. 表放在 `wmfs-api` 的 `api/src/ops.rs`（与 `codes.rs` 的错误码表同样的写法）：`pub const CHANGE_OPS: &[(u16, &str)]`、`op_name(u16) -> Option<&'static str>`、`op_code(&str) -> Option<u16>`（名字**不区分大小写**，所以属性里写 `ops=create+unlink`、`ops=setattr` 都能解析）。core 与 `wmfs-mgr` 共用。
2. 数值**显式写出**，不从任何 Rust 枚举的顺序推导；0 无效；**只追加、永不改号、永不复用**。追加新码不需要新 ADR（不改变已有记录的含义），但必须同时更新 design.md 3.2.8 的表和 `constants.golden`，并在提交说明里写明。
3. 名字就是 `ChangeEvent.op` 的 JSON 值。旧版 mgr 遇到不认识的数时显示 `op#<数>`，不报错。
4. v1 的表：

| 码 | 名字 | 由谁写 |
|---|---|---|
| 1 | SessionOpen | MetaOp::SessionOpen |
| 2 | SessionClose | MetaOp::SessionClose |
| 3 | Create | MetaOp::Create |
| 4 | Mkdir | MetaOp::Mkdir |
| 5 | Mknod | MetaOp::Mknod |
| 6 | Symlink | MetaOp::Symlink |
| 7 | Link | MetaOp::Link |
| 8 | Unlink | MetaOp::Unlink；ttl 删除也用它（`extra` 写 `reason=ttl`）；进了回收站时 `extra` 写 `trash=<回收站里的新名字>` |
| 9 | Rmdir | MetaOp::Rmdir |
| 10 | Rename | MetaOp::Rename |
| 11 | SetAttr | MetaOp::SetAttr（改 size 的也是它） |
| 12 | SetXattr | MetaOp::SetXattr |
| 13 | RemoveXattr | MetaOp::RemoveXattr |
| 14 | SetSize | 写入提交：MetaOp::CommitWrite、AppendReserve、Fallocate（`extra` 写 `via=<操作名>`） |
| 100 | AttrSet | `wmfs set` |
| 101 | AttrUnset | `wmfs unset` |
| 102 | SessionEvicted | 两阶段驱逐的第一阶段（自动或 `wmfs session evict`） |
| 103 | PolicyViolation | 3.11"策略违约可见"（`extra` 写 `kind=<种类>`） |
| 104 | SwitchLayout | 协调器迁移换布局 |
| 105 | ChecksumRepaired | scrub / 读路径修好坏块后，leader 按数据节点的上报写 |
| 106 | DataLossDetected | 唯一副本损坏，leader 按数据节点的上报写 |
| 107 | TrashExpire | 回收站过期条目被真正删除 |
| 200 | ClassCreate | `wmfs class create` |
| 201 | ClassUpdate | `wmfs class update` |
| 202 | ClassDelete | `wmfs class delete` |
| 203 | KeyCreate | `wmfs key create` |
| 204 | KeyRevoke | `wmfs key revoke` |
| 205 | DataKeyCreate | `wmfs datakey create` |
| 206 | NodeJoin | `wmfs join` |
| 207 | NodeDrain | `wmfs node drain` |
| 208 | NodeRemove | `wmfs node remove` |
| 209 | NodePromote | `wmfs node promote` |
| 210 | ChainForceServing | `wmfs chain force-serving` |
| 211 | ClusterSet | `wmfs cluster set` |
| 212 | PluginListChange | `wmfs cluster plugins add/remove` |
| 213 | PluginListReset | `wmfs server --adopt-local-plugins` 的逃生口 |
| 214 | FeatureEnable | `wmfs feature enable` |
| 215 | TaskControl | `wmfs task pause/resume/run/set`（`extra` 写 `action=<动作>`） |

5. **不写 changelog 的修改**：SessionHeartbeat（心跳）、SetAtime（尽力而为的 atime）、SetLock（锁是短暂状态，写进 changelog 只会刷屏）、DebugPut（调试）、消费者游标提交（否则会自己触发自己）、fail point。3.5.1 里 `changelog::record` 的注释"成功的修改都写"按这一条理解。
6. 码表在 M1 实现（`api/src/ops.rs` + `constants.golden` 金样）；M4 写第一条 changelog 时直接用。

## 备选方案

- 用 `MetaOp` 的变体序号当操作码：管理操作和事件（AttrSet、SessionEvicted 等）不是 MetaOp，还得另起一套；而且把持久化数值绑在线上协议的枚举顺序上，新手看不出两者的联系。否决。
- 每个里程碑用到时再定：正是 M0 审查担心的"各编各的"。否决。

## 后果

changelog 的 op 从第一条记录起就有固定含义；mgr、audit-log、alert 只认名字，不用关心数值。新增事件只需追加一行。
