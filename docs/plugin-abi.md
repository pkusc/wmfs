# wmfs 插件 ABI（动态库稳定 C ABI v1）

> 状态：**M6 先建了这个文件，只放 `AuthzOp` 的编号表**（ADR-0036 给 `AuthzOp` 追加了 `Handle`，编号要从现在起固定）；
> ABI 规则、`core/include/wmfs_plugin.h` 与加载流程在 M14 随 `core/hooks/cabi.rs` 写全（design.md 3.7.6）。

## `wmfs_authz_input_v1.op` 的编号（与 Rust 的 `AuthzOp` 一一对应，只追加、永不改号）

编号 = `core/src/hooks/traits.rs` 里 `AuthzOp` 的变体顺序。变体自带的布尔参数（`Open`、`SetAttr`、`Handle`）在 v1 输入结构里另有字段
（M14 定），不占编号。

| 编号 | AuthzOp | 含义 |
|---|---|---|
| 0 | `Lookup` | 在目录里查名字（对父目录要执行权限） |
| 1 | `GetAttr` | 取属性 |
| 2 | `ReadDir` | 列目录 |
| 3 | `ReadLink` | 读符号链接 |
| 4 | `Open { read, write }` | 打开（按打开方式要读 / 写） |
| 5 | `Create` | 建普通文件 |
| 6 | `Mkdir` | 建目录 |
| 7 | `Mknod` | 建设备、fifo、socket |
| 8 | `Symlink` | 建符号链接 |
| 9 | `Link` | 建硬链接 |
| 10 | `Unlink` | 删文件 |
| 11 | `Rmdir` | 删目录 |
| 12 | `RenameFrom` | 改名：源一侧 |
| 13 | `RenameTo` | 改名：目标一侧 |
| 14 | `SetAttr { mode, owner, size, times, times_now }` | chmod / chown / truncate / utimensat |
| 15 | `GetXattr` | 读 xattr |
| 16 | `SetXattr` | 写 xattr |
| 17 | `ListXattr` | 列 xattr |
| 18 | `RemoveXattr` | 删 xattr |
| 19 | `Lock` | 字节范围锁与 flock |
| 20 | `Admin` | 管理操作 |
| 21 | `Handle { read, write }` | **M6 追加（ADR-0036）**：经已经打开的句柄读 / 写（CommitWrite、Fallocate、Uninline、RenewCap、TruncateHandle；M12 的 AppendReserve）。posix-mode：root 与属主放行（属主不看权限位，学 NFS），其他人与 `Open` 相同，带扩展 ACL 的 inode 照旧弃权（交给 posix-acl）；不认识它的插件按组合规则弃权 |
