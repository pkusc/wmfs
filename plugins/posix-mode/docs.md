# posix-mode 授权插件

**钩子**：authz（altitude 210）　**热路径**：是（每个元数据请求），预算 5 µs/次
**fail point**：`plugin.posix-mode.authz`（触发 = 以注入的错误码拒绝这次请求）　**指标**：`wmfs_posix_mode_decisions_total{result="allow"|"deny"|"abstain"}`

## 它做什么

经典的 POSIX 权限位检查（design.md 4.3）。每个元数据请求在元数据 leader 上执行时，本体把"谁（uid、gid、附加组）、要做什么、
对哪个 inode / 父目录 / 被删对象"交给它，它回答"允许 / 拒绝 / 弃权"：

| 操作 | 要求 |
|---|---|
| lookup、按路径解析 | 对所在目录有执行权限（x） |
| readdir | 对目录有读权限（r） |
| open | 按打开方式要 r / w |
| create、mkdir、mknod、symlink、link | 对父目录有写 + 执行（wx） |
| unlink、rmdir、rename | 对父目录有 wx；**粘滞目录**（例如 `/home`、`/scratch` 的 1777）里只有目录属主、被删对象的属主与 root 能删除或改名，否则 `EPERM` |
| rename 把目录移到别的父目录 | 还要对这个目录本身有写权限（它的 `..` 要改） |
| chmod、chown、把时间设成指定值 | 属主或 root，否则 `EPERM` |
| 把时间设成"现在"（`touch`） | 属主，或有写权限 |
| truncate | 写权限 |
| getxattr / setxattr、removexattr（`user.*`） | r / w |
| 经**已经打开的句柄**读写（提交写入的长度 CommitWrite、fallocate、小文件转 chunk 的 Uninline、令牌续期 RenewCap、经写句柄的 ftruncate） | root 放行；**文件的属主放行，不看权限位**（与 NFS 相同：git、cp 先建 0444 的文件再往里写）；其他人与 open 相同（看属组 / 其他人的位）；带扩展 ACL 的 inode 照旧弃权（ADR-0036） |

- 用哪一组位：调用者是属主 → 属主位；属组或附加组里有文件的组 → 属组位；否则 → 其他人位。权限不够 → `EACCES`。
- **root 全部放行**（uid 0；注意 `root_squash` 的 key 声明的 root 已经被本体映射成 65534，不再是 root），带扩展 ACL 的 inode 也一样
  （与 Linux 的 CAP_DAC_OVERRIDE 相同）。
- 不是 root 时，inode 带**扩展 ACL**（有命名用户/命名组条目）→ 对它弃权，由 posix-acl 插件判定（M10）。一个 authz 插件都没有放行
  （全部弃权）时，本体拒绝（`EACCES`，失败即关闭）——所以 M10 之前带扩展 ACL 的文件只有 root 能访问。
- **打开本身照常检查**：属主仍然不能以写方式打开一个没有写权限位的**已有**文件，按路径 `truncate` 属主自己的 0444 文件仍是 `EACCES`
  （与 POSIX 相同）；只有打开之后经句柄的操作才"属主不看权限位"。所以已经打开的文件被 `chmod` 去掉权限之后，属主的句柄照常可用，
  其他用户之前打开的句柄在下一次提交长度（写回、fsync、close）或令牌续期时可能报 `EACCES`（semantics.md 放松第 17 条）。
- chown 的取值规则（只有 root 能改 uid、属主只能把 gid 改成自己所在的组）、xattr 名字空间的规则（`trusted.*` 只给 root 等）、
  "非属主改长度时去掉 setuid"这些由本体执行——它们要看请求里的新值或 xattr 的名字，插件看不到。

## 配置

没有部署参数（权限是文件自己的属性）。

## 诚实说明

客户端声明的 uid 只在它的 key 允许的 uid 范围内被相信（本体在调用插件之前检查，ADR-0009）；被攻破的客户端机器可以冒充范围内的任何用户。
