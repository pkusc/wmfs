# posix-acl 授权插件

**钩子**：authz（altitude 220）　**热路径**：是（每个元数据请求），预算 5 µs/次
**fail point**：`plugin.posix-acl.authz`（触发 = 以注入的错误码拒绝这次请求）　**指标**：`wmfs_posix_acl_decisions_total{result="allow"|"deny"|"abstain"}`
**消费的目录属性**：`acl.default`（新文件继承在本体里做，本插件负责之后的访问判定）

## 它做什么

POSIX.1e ACL（`setfacl`/`getfacl`）的访问判定，规则与 Linux 本地文件系统相同（design.md 4.3）。文件或目录带**扩展 ACL**（有命名用户、
命名组或 MASK 条目）时，posix-mode 对"要看权限位"的检查弃权，由本插件按 ACL 判：

| 调用者 | 用哪一条 |
|---|---|
| 文件的属主 | `user::`（不受 `mask::` 限制） |
| 有一条 `user:<uid>:` | 这一条，再与 `mask::` 求交 |
| 在文件的属组或某个 `group:<gid>:` 里 | 其中有一条（与 `mask::` 求交后）够用就放行；在某个组里、可哪一条都不够 → 拒绝（不再看 `other::`） |
| 其他人 | `other::` |

- 操作要哪几位与 posix-mode 相同：查名字要 x、列目录要 r、建/删/改名要父目录的 wx、打开按方式要 r/w、截断要 w。
- **root 弃权**：root 的放行由 posix-mode 负责（它在"有扩展 ACL 就弃权"之前放行 root，与 Linux 的 CAP_DAC_OVERRIDE 相同）。
- **属主经已经打开的句柄读写**（提交写入的长度、fallocate、令牌续期……，ADR-0036）：放行，不看 ACL——与 posix-mode 相同（学 NFS）。
- 属主才能做的事（chmod、chown、把时间设成指定值）与粘滞目录的规则跟 ACL 无关，仍由 posix-mode 判。
- 没有扩展 ACL 的 inode：弃权。一个请求里判过的几个 inode：有一个拒绝就拒绝，有一个放行（其余没有 ACL）就放行。

## 新文件继承

父目录有默认 ACL（`setfacl -d`），或者生效的目录属性 `acl.default`（例如 `wmfs set /proj acl.default=u::rwx,g::r-x,o::---,u:1001:rwx,m::rwx`）
时，新建的文件与目录按 ACL 规则得到访问 ACL（目录同时带上默认 ACL），**不应用 umask**（POSIX）。这一步在本体的 create 里做。

## 配置

没有部署参数。
