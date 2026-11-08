/*
 * deny_uid.c：用 C 写的 authz 动态库插件示例（wmfs 稳定 C ABI v1，design.md 3.7.6、docs/plugin-abi.md）。
 * 拒绝某一个 uid 的写操作（建、删、改名、改属性、写 xattr、以写方式打开、经句柄写）；别的一律弃权（交给 posix-mode 等插件判定）。
 *
 * 编译：gcc -shared -fPIC -I core/include -o /tmp/deny_uid.so examples/dylib-deny-uid/deny_uid.c
 * 使用：node.toml 的 [plugins] dylib = ["/tmp/deny_uid.so"]（开发集群：wmfs dev restart-all --dylib /tmp/deny_uid.so），
 *       并把 authz:deny-uid 加进集群插件清单（wmfs cluster plugins add authz:deny-uid）——每个节点都必须加载它。
 * 配置：[plugin.deny-uid] uid = 4242（不写就是 4242）。
 */
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "wmfs_plugin.h"

static const wmfs_manifest_v1 MANIFEST = {
    .abi_version = WMFS_PLUGIN_ABI_V1,
    .name = "deny-uid",
    .version = "0.1.0",
    .hooks = WMFS_HOOK_AUTHZ,
    .altitude = 290,
    .hot_path = 1,
    .reserved0 = 0,
    .budget_ns = 5000,
    .config_schema = "uid = { type = \"u32\", default = 4242, doc = \"拒绝这个 uid 的写操作\" }\n",
};

typedef struct {
    uint32_t uid;
} deny_state;

const wmfs_manifest_v1 *wmfs_plugin_manifest_v1(void) { return &MANIFEST; }

/* config_toml 是 [plugin.deny-uid] 表的 TOML 文本，例如 "uid = 4242\n"（示例只做最简单的解析）。 */
void *wmfs_plugin_init_v1(const char *config_toml) {
    deny_state *s = malloc(sizeof *s);
    if (s == NULL) {
        return NULL;
    }
    s->uid = 4242;
    const char *p = config_toml != NULL ? strstr(config_toml, "uid") : NULL;
    if (p != NULL && (p = strchr(p, '=')) != NULL) {
        s->uid = (uint32_t)strtoul(p + 1, NULL, 10);
    }
    return s;
}

void wmfs_plugin_fini_v1(void *state) { free(state); }

static int is_write(uint32_t op, uint32_t flags) {
    switch (op) {
    case WMFS_OP_OPEN:
    case WMFS_OP_HANDLE:
        return (flags & WMFS_OPF_WRITE) != 0;
    case WMFS_OP_CREATE:
    case WMFS_OP_MKDIR:
    case WMFS_OP_MKNOD:
    case WMFS_OP_SYMLINK:
    case WMFS_OP_LINK:
    case WMFS_OP_UNLINK:
    case WMFS_OP_RMDIR:
    case WMFS_OP_RENAME_FROM:
    case WMFS_OP_RENAME_TO:
    case WMFS_OP_SETATTR:
    case WMFS_OP_SETXATTR:
    case WMFS_OP_REMOVEXATTR:
        return 1;
    default:
        return 0;
    }
}

/* 线程安全：只读 state。 */
int32_t wmfs_authz_check_v1(void *state, const wmfs_authz_input_v1 *in, int32_t *err) {
    const deny_state *s = state;
    if (in->uid == s->uid && is_write(in->op, in->op_flags)) {
        *err = EACCES;
        return WMFS_AUTHZ_DENY;
    }
    return WMFS_AUTHZ_ABSTAIN;
}
