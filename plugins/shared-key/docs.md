# shared-key 认证插件

**钩子**：authn（altitude 110）　**热路径**：是（连接建立时），预算 20 µs/连接
**fail point**：`plugin.shared-key.authn`（触发 = 拒绝这次认证）　**指标**：`wmfs_shared_key_auth_total{result="accept"|"reject"}`

## 它做什么

连接建立时，验证客户端证明自己持有某把 key（design.md 3.3.2、4.2）。**密钥本身从不在网上传**：

1. 客户端发 `Hello`（key 号、声明的角色、随机数），服务端回 `Challenge`（自己的随机数）；
2. 客户端算 `proof = HMAC-SHA256(secret(key), "wmfs-auth-v1" ‖ 两个随机数 ‖ 角色 ‖ 集群 id ‖ key 号)` 发回；
3. 本插件用同一把 key 的秘密验算（常数时间比较），对上了就接受，并给出连接身份（角色、租户、uid 范围、root_squash、路径限制）；
4. 服务端再回 `server_proof`，客户端反过来确认服务端也有这把 key（双向认证，防止连到冒充的服务端）。

`secret(k) = HMAC-SHA256(cluster.key, "wmfs-key-v1" ‖ k)`：只有 `cluster.key` 是秘密根，key 文件可以随时由它重新算出。
key 0 固定是服务端之间用的（角色 Server）；其他 key 的角色等信息来自复制状态里的 KEYMETA（M1 还没有元数据，用 init 的初始值：
key 1 = admin、key 2 = 默认 client）。被撤销或不存在的 key 一律拒绝。

**角色规则由本体再强制一遍**（与本插件无关）：Hello 里声明的角色必须等于 KEYMETA 里的角色，key 0 只能是 Server——所以 client key
冒充不了服务端（演示：`wmfs ping … --key-file client.key --role server` 得到 `AUTH_FAILED`）。

## 配置

没有部署参数。

## 诚实说明

首发**不加密线上流量**（没有 TLS）：握手能证明身份、防止冒充，但不防窃听，只在可信网络中使用（ADR-0009）。
