# deny-uid：用 C 写的授权（authz）动态库插件示例

**钩子**：authz（altitude 290）　**热路径**：是，预算 5 µs/次　**fail point**：`plugin.deny-uid.authz`（wmfs 的调用包装自动给的）
**配置**：`[plugin.deny-uid] uid = 4242`（不写就是 4242）　**ABI**：wmfs 稳定 C ABI v1（`core/include/wmfs_plugin.h`、`docs/plugin-abi.md`）

## 它做什么

拒绝某一个 uid 的写操作（建、删、改名、改属性、写 xattr、以写方式打开、经句柄写），回 `EACCES`；别的一律弃权，交给 posix-mode 等内置插件按
POSIX 权限判定（任一拒绝即拒绝，所以它只会收紧、不会放宽权限）。`bad_abi.c` 是同一个样子、但 `abi_version = 2` 的坏插件：wmfs 首发只支持
C ABI 版本 `[1]`，加载它的节点拒绝启动，日志写明"动态库 ABI 版本 2 不受支持（支持 [1]），拒绝启动"。

## 用法（开发集群，roadmap M14 的演示）

```bash
gcc -shared -fPIC -I core/include -o /tmp/deny_uid.so examples/dylib-deny-uid/deny_uid.c
gcc -shared -fPIC -I core/include -o /tmp/bad_abi.so examples/dylib-deny-uid/bad_abi.c
wmfs cluster plugins add authz:deny-uid               # 集群插件清单加上它：各节点因插件集合不一致而退出
wmfs dev restart-all --dylib /tmp/deny_uid.so          # 改写每个节点 node.toml 的 [plugins] dylib，再重启全部节点
wmfs fs --as-uid 4242 touch /home/zz                   # Permission denied
wmfs dev restart 1; wmfs dev logs 1 | tail -3          # 不带 .so 重启 node1：它退出，日志列出"本节点缺少 authz:deny-uid"
wmfs dev restart 1 --dylib /tmp/deny_uid.so --dylib /tmp/bad_abi.so   # 拒绝启动：ABI 版本 2 不受支持
wmfs dev restart 1 --dylib /tmp/deny_uid.so            # 恢复
```

之后加入的新节点：`wmfs join <种子> … --dylib /path/deny_uid.so`（不带会因"本节点缺少 authz:deny-uid"被拒绝加入）；`wmfs dev up --nodes N`
加的新节点沿用 node1 的动态库列表。用 C++ 写也行：头文件有 `extern "C"` 保护，四个符号不会被改名。

测试：`tests/it/dylib.rs`（gcc 编这两个文件、经 `core::hooks::cabi::load` 加载：判定、配置、进集群插件清单、fail point、坏 ABI 被拒绝；
另有失败即关闭、manifest 检查、缺符号、两个结构 C/Rust 同布局的用例）；
真进程的流程在 `scripts/accept.d/m14.sh`。
