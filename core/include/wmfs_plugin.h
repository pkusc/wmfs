/*
 * wmfs 动态库插件的稳定 C ABI v1（design.md 3.7.6，ADR-0012：首发只覆盖 authz）。规则与编号表见 docs/plugin-abi.md。
 *
 * 一个 .so 导出四个符号：wmfs_plugin_manifest_v1、wmfs_plugin_init_v1、wmfs_plugin_fini_v1、wmfs_authz_check_v1。
 * node.toml 的 [plugins] dylib = ["/path/x.so"] 列出要加载的库；[plugin.<名字>] 表原样（TOML 文本）交给 init。
 * v1 的结构与符号一经发布永不修改：新能力 = 新版本号 + 新结构 + 带 _v2 后缀的新符号。
 * 版本号不在 wmfs 支持的列表里（首发 [1]）、或加载 / 初始化失败 → wmfs server 拒绝启动并写明原因。
 */
#ifndef WMFS_PLUGIN_H
#define WMFS_PLUGIN_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" { /* 用 C++ 写插件时符号不改名（wmfs 按 C 的名字找它们） */
#endif

#define WMFS_PLUGIN_ABI_V1 1u
#define WMFS_HOOK_AUTHZ (1u << 2)

typedef struct {
    uint32_t abi_version;      /* 必须 == WMFS_PLUGIN_ABI_V1，否则 wmfs 拒绝加载 */
    const char *name;          /* NUL 结尾，小写字母、数字与 '-'，集群内唯一；进集群插件清单（authz:<name>） */
    const char *version;
    uint32_t hooks;            /* 首发只能是 WMFS_HOOK_AUTHZ */
    uint16_t altitude;         /* authz：200..=299，与别的 authz 插件不同 */
    uint8_t hot_path;          /* 必须为 1 */
    uint8_t reserved0;         /* 填 0 */
    uint64_t budget_ns;        /* 单次调用的时间预算（> 0）；超出计入 wmfs_hook_over_budget_total */
    const char *config_schema; /* TOML 文本：每个配置项 { type, default, doc }；[plugin.<name>] 里只能写这些键 */
} wmfs_manifest_v1;

const wmfs_manifest_v1 *wmfs_plugin_manifest_v1(void);
void *wmfs_plugin_init_v1(const char *config_toml); /* NULL = 初始化失败，wmfs 拒绝启动 */
void wmfs_plugin_fini_v1(void *state);

/* wmfs_authz_input_v1.op 的编号（= Rust AuthzOp 的变体顺序，只追加、永不改号；docs/plugin-abi.md） */
#define WMFS_OP_LOOKUP 0u
#define WMFS_OP_GETATTR 1u
#define WMFS_OP_READDIR 2u
#define WMFS_OP_READLINK 3u
#define WMFS_OP_OPEN 4u
#define WMFS_OP_CREATE 5u
#define WMFS_OP_MKDIR 6u
#define WMFS_OP_MKNOD 7u
#define WMFS_OP_SYMLINK 8u
#define WMFS_OP_LINK 9u
#define WMFS_OP_UNLINK 10u
#define WMFS_OP_RMDIR 11u
#define WMFS_OP_RENAME_FROM 12u
#define WMFS_OP_RENAME_TO 13u
#define WMFS_OP_SETATTR 14u
#define WMFS_OP_GETXATTR 15u
#define WMFS_OP_SETXATTR 16u
#define WMFS_OP_LISTXATTR 17u
#define WMFS_OP_REMOVEXATTR 18u
#define WMFS_OP_LOCK 19u
#define WMFS_OP_ADMIN 20u
#define WMFS_OP_HANDLE 21u

/* wmfs_authz_input_v1.op_flags：OPEN、HANDLE 用 READ/WRITE；SETATTR 用 MODE…TIMES_NOW；别的操作为 0 */
#define WMFS_OPF_READ (1u << 0)
#define WMFS_OPF_WRITE (1u << 1)
#define WMFS_OPF_MODE (1u << 0)
#define WMFS_OPF_OWNER (1u << 1)
#define WMFS_OPF_SIZE (1u << 2)
#define WMFS_OPF_TIMES (1u << 3)
#define WMFS_OPF_TIMES_NOW (1u << 4)

typedef struct {
    uint32_t uid, gid, tenant;
    uint64_t job_id;
    const uint32_t *groups;
    uint32_t ngroups;
    uint32_t op;       /* WMFS_OP_* */
    uint32_t op_flags; /* WMFS_OPF_*（实现注记（M14）：AuthzOp 变体自带的布尔参数） */
    uint64_t ino;      /* 无目标时 ino = 0 */
    uint32_t kind, mode, owner_uid, owner_gid;
    uint64_t parent_ino; /* 无父目录时 = 0 */
    uint32_t parent_mode, parent_uid, parent_gid;
    const uint64_t *ancestors; /* 根 → 父 */
    uint32_t nancestors;
} wmfs_authz_input_v1;

#define WMFS_AUTHZ_ABSTAIN 0
#define WMFS_AUTHZ_ALLOW 1
#define WMFS_AUTHZ_DENY 2

/* 返回 0 = abstain，1 = allow，2 = deny（*err 写 errno，不在 1..=255 时按 EACCES）；别的返回值按 deny（EACCES）。
 * 必须线程安全、不阻塞、不做大块分配；输入里的指针只在这次调用期间有效。 */
int32_t wmfs_authz_check_v1(void *state, const wmfs_authz_input_v1 *in, int32_t *err);

#ifdef __cplusplus
}
#endif

#endif
