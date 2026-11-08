/*
 * bad_abi.c：一个 abi_version = 2 的"坏"插件（design.md 3.7.6 的验收用）。wmfs 首发只支持 C ABI 版本 [1]：加载它时服务端拒绝启动，
 * 日志写明"动态库 ABI 版本 2 不受支持（支持 [1]），拒绝启动"——绝不"猜着适配"，也不跳过它（跳过一个授权插件等于静默少一条规则）。
 *
 * 编译：gcc -shared -fPIC -I core/include -o /tmp/bad_abi.so examples/dylib-deny-uid/bad_abi.c
 */
#include "wmfs_plugin.h"

static const wmfs_manifest_v1 MANIFEST = {
    .abi_version = 2, /* 不受支持的版本 */
    .name = "bad-abi",
    .version = "0.1.0",
    .hooks = WMFS_HOOK_AUTHZ,
    .altitude = 291,
    .hot_path = 1,
    .reserved0 = 0,
    .budget_ns = 5000,
    .config_schema = "",
};

const wmfs_manifest_v1 *wmfs_plugin_manifest_v1(void) { return &MANIFEST; }

void *wmfs_plugin_init_v1(const char *config_toml) {
    (void)config_toml;
    return (void *)&MANIFEST;
}

void wmfs_plugin_fini_v1(void *state) { (void)state; }

int32_t wmfs_authz_check_v1(void *state, const wmfs_authz_input_v1 *in, int32_t *err) {
    (void)state;
    (void)in;
    (void)err;
    return WMFS_AUTHZ_ABSTAIN;
}
