# wmfs 插件 ABI（动态库稳定 C ABI v1）

> 状态：**M14 写全**（design.md 3.7.6，ADR-0012：首发只覆盖 authz；ADR-0037 第 3 条：`AuthzOp::Handle` 的编号是 21）。
> 头文件：[`core/include/wmfs_plugin.h`](../core/include/wmfs_plugin.h)；加载代码：`core/src/hooks/cabi.rs`；示例：
> [`examples/dylib-deny-uid/`](../examples/dylib-deny-uid/)（`deny_uid.c`，以及一个 `abi_version = 2` 的坏插件 `bad_abi.c`）。

wmfs 的内置插件都是编译期静态注册的 Rust 代码（`cli/src/registry.rs`）。第三方想不重新编译 wmfs 就加一条**授权规则**时，用 C（或任何能导出
C 函数的语言）写一个 `.so`，按这一页的规则导出四个符号。首发只有 authz 这一种钩子走动态库（扁平结构进、允许 / 拒绝 / 弃权出，不碰缓冲区
所有权）；别的钩子要用 Rust 写进 wmfs 再编译（二期按需逐个加，每个新钩子 = 新的 `_v1` 结构与符号，不影响已有的）。

## 1. ABI 规则（永远成立）

1. **v1 冻结**：`wmfs_manifest_v1`、`wmfs_authz_input_v1` 两个结构与四个 `_v1` 符号一经发布永不修改（不加字段、不改含义、不改编号）。
   新能力 = 新版本号 + 新结构 + 带 `_v2` 后缀的新符号。
2. **版本列表**：wmfs 明确列出它支持的 C ABI 版本（首发 `[1]`，`wmfs --version` 末尾的 `c-abi 1`）。manifest 的 `abi_version` 不在列表里
   → **拒绝加载**，不"猜着适配"。
3. **加载失败 = 拒绝启动**：node.toml 里配置了、却加载不了的库（找不到文件、缺符号、ABI 版本不对、manifest 不合规、`init` 返回 NULL）
   → `wmfs server` 拒绝启动，日志写明原因（例如"动态库 ABI 版本 2 不受支持（支持 [1]），拒绝启动：/tmp/bad_abi.so"）。跳过一个授权插件
   等于静默少了一条授权规则（理念文档原则 7，ADR-0012 第 4 条）。库用 `RTLD_NOW` 打开：插件引用了不存在的函数（链错了库、依赖的库版本
   不符）在加载时就失败（日志里是 dlerror 的原文，例如 `undefined symbol: f`），不会等到第一次授权调用时才让进程崩溃。
4. **全集群一致**：动态库插件与静态插件一样进**集群插件清单**（`authz:<name>`，design.md 3.7.2）：每个节点都必须加载同名插件。
   加插件的流程是 `wmfs cluster plugins add authz:<name>` → 各节点因清单不一致退出 → 用新配置重启全部节点（开发集群：
   `wmfs dev restart-all --dylib /path/x.so`）。少装的节点会退出（退出码 3）并在日志里列出"本节点缺少 authz:<name>"。之后加入的新节点
   用 `wmfs join … --dylib /path/x.so` 带上它（写进新节点的 node.toml，并按它算 join 时比对的插件清单）；不带会被拒绝加入（退出码 3）。
   `wmfs dev up --nodes N` 的新节点沿用 node1 的动态库列表。
5. **常驻**：库在进程生命周期内保持加载，不支持热卸载。`wmfs_plugin_fini_v1` 在插件对象被丢弃时调用（测试里会这样）；server 退出时
   进程直接结束，**不保证**调到它——不要依赖 fini 做必须完成的收尾（例如落盘）。
6. **Rust 写的动态插件**通过 C 壳导出：`#[unsafe(no_mangle)] pub extern "C" fn …`（edition 2024 下裸 `#[no_mangle]` 是硬错误），
   函数体内 `std::panic::catch_unwind`：panic 一律当作拒绝（返回 2，`*err = EACCES`）并自己计数——panic 不得越过 FFI 边界。

头文件带 `extern "C"` 保护：用 C++ 写插件时直接 include 它，四个符号也不会被改名。

## 2. 四个符号

```c
const wmfs_manifest_v1 *wmfs_plugin_manifest_v1(void);
void   *wmfs_plugin_init_v1(const char *config_toml);   /* NULL = 初始化失败 → wmfs 拒绝启动 */
void    wmfs_plugin_fini_v1(void *state);
int32_t wmfs_authz_check_v1(void *state, const wmfs_authz_input_v1 *in, int32_t *err);
```

- **manifest**（插件里的静态常量，库加载期间一直有效）：`abi_version` 必须是 `WMFS_PLUGIN_ABI_V1`（= 1）；`name` 只能由小写字母、数字与
  `-` 组成，集群内唯一（集群插件清单里写 `authz:<name>`）；`hooks` 首发只能是 `WMFS_HOOK_AUTHZ`；`altitude` 在 authz 的区间 200–299，
  且与别的 authz 插件不同（posix-mode 210、posix-acl 220、path-caps 250 已被占用）；`hot_path` 必须为 1；`reserved0` 填 0；`budget_ns`
  是单次调用的时间预算（> 0，超出计入 `wmfs_hook_over_budget_total{hook="authz",plugin=<name>}`）；`config_schema` 是 TOML 文本，每个配置项
  写成 `{ type, default, doc }`。
- **init**：参数是 node.toml 里 `[plugin.<name>]` 表的 TOML 文本（没有这个表就是空串）；表里只能写 `config_schema` 里有的键（不认识的键 →
  拒绝启动）。返回插件自己的状态指针，之后原样交给 `check` 与 `fini`。
- **check**：每个需要授权的元数据请求调用一次（在 leader 的执行器线程上，**必须线程安全、不阻塞、不做大块分配**）。返回 0 = 弃权、
  1 = 允许、2 = 拒绝（`*err` 写 errno；不在 1–255 时按 `EACCES`）；别的返回值一律按拒绝（`EACCES`）——失败即关闭。`in` 里的指针只在这次
  调用期间有效。
- 组合规则与内置插件相同（design.md 3.7.2）：authz 插件按 altitude 升序串行，**任一拒绝即拒绝**；否则有允许就允许；全部弃权 → `EACCES`。
  所以"只拒绝某些情况、其余弃权"的插件（例如 deny-uid）不会放宽 POSIX 权限检查。
- 调用包装（design.md 3.7.4）照常：fail point `plugin.<name>.authz`（例如 `wmfs failpoint set 1 plugin.deny-uid.authz 'times(1,return(EPERM))'`）、
  计时与预算、`wmfs_hook_calls_total` / `wmfs_hook_errors_total` 等指标，与内置插件一样。

## 3. 输入结构 `wmfs_authz_input_v1`

| 字段 | 含义 |
|---|---|
| `uid`、`gid`、`tenant`、`job_id`、`groups`/`ngroups` | 发请求的人（uid 已按客户端密钥的 uid 范围校验、按 root_squash 映射） |
| `op` | 操作编号，见第 4 节 |
| `op_flags` | 操作自带的布尔参数（实现注记（M14）：v1 在 `op` 之后加了这个字段）：`OPEN`、`HANDLE` 用 `WMFS_OPF_READ`（bit0）、`WMFS_OPF_WRITE`（bit1）；`SETATTR` 用 `WMFS_OPF_MODE`（bit0）、`OWNER`（bit1）、`SIZE`（bit2）、`TIMES`（bit3）、`TIMES_NOW`（bit4）；别的操作为 0 |
| `ino`、`kind`、`mode`、`owner_uid`、`owner_gid` | 被访问的对象（没有对象时 `ino = 0`）；`kind` 1 普通文件、2 目录、3 符号链接、4 fifo、5 socket、6 字符设备、7 块设备 |
| `parent_ino`、`parent_mode`、`parent_uid`、`parent_gid` | 目录项操作的父目录（没有时 `parent_ino = 0`） |
| `ancestors`/`nancestors` | 根 → 父目录的 inode 号 |

## 4. `op` 的编号（与 Rust 的 `AuthzOp` 一一对应，只追加、永不改号）

编号 = `core/src/hooks/traits.rs` 里 `AuthzOp` 的变体顺序（`core/src/hooks/cabi.rs` 的单元测试逐个核对，头文件里是 `WMFS_OP_*`）。
两个结构的布局（`wmfs_manifest_v1` 48 字节、`wmfs_authz_input_v1` 112 字节）由 `tests/it/dylib.rs` 用 gcc 编出来的 offsetof/sizeof 与 Rust
的 `#[repr(C)]` 逐字段核对；"失败即关闭"（返回值、errno）与各项 manifest 检查也在那里各有一个用 gcc 编出来的测试插件。
变体自带的布尔参数（`Open`、`SetAttr`、`Handle`）在 `op_flags` 里，不占编号。

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

## 5. 例子：deny-uid

```bash
gcc -shared -fPIC -I core/include -o /tmp/deny_uid.so examples/dylib-deny-uid/deny_uid.c
wmfs cluster plugins add authz:deny-uid               # 各节点因插件集合不一致而退出
wmfs dev restart-all --dylib /tmp/deny_uid.so          # 开发集群：改写每个节点 node.toml 的 [plugins] dylib 再重启
wmfs fs --as-uid 4242 touch /home/zz                   # Permission denied：动态库插件拒绝了 uid 4242 的写
```

生产部署：在每个节点的 node.toml 里写

```toml
[plugins]
dylib = ["/opt/wmfs/plugins/deny_uid.so"]

[plugin.deny-uid]
uid = 4242
```

然后按第 1 节第 4 条的流程重启全部节点。
