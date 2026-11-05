# quota 配额插件

**钩子**：quota_admission（altitude 310）　**热路径**：是（受配额管着的写入要额度时、建文件时），预算 5 µs/次
**fail point**：`plugin.quota.quota_admission`（触发 = 以注入的错误码拒绝这次准入）　**指标**：`wmfs_quota_decisions_total{result="allow"|"soft_warn"|"deny"}`、`wmfs_quota_denied_total{dir}`（本体另有 `wmfs_quota_grants_total`）
**消费的目录属性**：`quota`、`quota.<存储类>`

## 它做什么

目录配额：软限 / 硬限 / 宽限期、文件数、按存储类（design.md 4.4、3.9，ADR-0026）。例如：

```bash
wmfs set ~/wmfs-mnt/home/q quota=100M,soft=80M,grace=1m,files=1000,quantum=1M
wmfs set ~/wmfs-mnt/proj quota.ssd=1T          # 只管存储类 ssd 的字节数
wmfs usage ~/wmfs-mnt/home/q                    # 用量、上限、软限与宽限剩余
```

| 值 | 意思 | 不写时 |
|---|---|---|
| `quota=<字节>` | 硬限（`off` = 不设） | — |
| `soft=<字节>` | 软限（≤ 硬限）：越过之后开始计时，宽限期内仍按硬限，过了宽限期按软限拒绝 | 没有软限 |
| `grace=<时长>` | 宽限期（要有 `soft`） | 7 天 |
| `files=<个数>` | 文件数上限（文件 + 子目录；`quota.<类>` 只数这个类的文件） | 不限 |
| `quantum=<字节>` | 每次给客户端的写入额度（1 MiB–1 GiB） | 64 MiB |

本体给出管着这次操作的每个 scope（每个祖先目录上的 `quota` 与 `quota.<类>`，Stacked：子目录的 `inherit=false` 挡不住），本插件逐个判：

1. 要新建对象、这个 scope 的文件数加上它超过 `files=` → `EDQUOT`；
2. 有效上限 = 宽限期已过 ? 软限 : 硬限；这次要增长、用量加上它超过有效上限 → `EDQUOT`；
3. 这次增长越过了软限、软限还没在计时 → `SoftWarn`（本体记下起点、写一条 changelog `PolicyViolation`，`kind=quota_soft`）；
4. 其余放行，同时给出 `max_growth` = 各 scope 的"有效上限 − 用量 − 这次的增长"里最小的：本体据此给客户端写入额度
   （`quota_end` = 长度 + min(quantum, max_growth)）。

## 精度（semantics.md 第 5 节）

- 守协议的客户端：用量最多超出硬限 `quantum × 同时向该子树写入的文件数`；单个写者精确不超；任何客户端最多超出 `16 × quantum`
  （本体的护栏）。文件数精确。
- 计量口径是逻辑大小；"删除后仍打开"的文件不计；硬链接只在主链接所在的目录计一次。

## 实现说明

纯函数：用量（递归计数 RSTAT）、软限的起点由本体给出，起点与事件也由本体写回（`core/src/meta/quota.rs`）；插件不保存任何状态，
元数据 leader 切换不影响它。

## 配置

没有部署参数：宽限期与 quantum 的缺省值在本体的 `core/src/meta/quota.rs`（`DEFAULT_GRACE_MS` = 7 天、`DEFAULT_QUANTUM` = 64 MiB；属性
注册表的 quota 项用它解析），只能用目录属性逐目录覆盖（行为策略一律是目录属性，3.8）。
