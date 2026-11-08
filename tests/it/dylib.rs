//! 动态库插件的稳定 C ABI v1（design.md 3.7.6，ADR-0012；M14）：用 gcc 把 `examples/dylib-deny-uid/` 里的两个 C 插件编进临时目录，
//! 经 `core::hooks::cabi::load` 加载——deny-uid 拒绝某个 uid 的写操作（配置 `[plugin.deny-uid] uid` 生效）、别的一律弃权；进了管线之后
//! 与 posix-mode 一起按组合规则判定（任一 Deny 即拒绝）；fail point `plugin.deny-uid.authz` 触发 = 以注入的码拒绝（调用包装照常计数）；
//! `abi_version = 2` 的坏插件被拒绝加载并写明原因；`[plugin.deny-uid]` 里写了 config_schema 之外的键 → `validate` 拒绝。
//! 没有 gcc 时打印 SKIP 并通过（验收脚本里另外用 gcc 编、真进程验证）。fail point 是进程级的：单独一个测试程序。
//! M14 审查补上：失败即关闭（check 返回 0/1/2 之外的值、2 但 errno 不在 1–255 → EACCES）、manifest 与名字不合规、init 返回 NULL、缺符号
//! （RTLD_NOW）都拒绝加载；两个 v1 结构在 gcc 与 Rust 的 `#[repr(C)]` 里逐字段同布局（v1 一经发布就冻结）。

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use wmfs_core::api::{failpoint, metrics};
use wmfs_core::base::ErrCode;
use wmfs_core::base::config::PluginTable;
use wmfs_core::base::types::{Principal, Role};
use wmfs_core::hooks::cabi;
use wmfs_core::hooks::pipeline::Pipeline;
use wmfs_core::hooks::registry::PluginSet;
use wmfs_core::hooks::traits::{AuthzInput, AuthzOp, AuthzPlugin, ConnIdentity, Decision, InodeView};
use wmfs_core::init::HOME_INO;
use wmfs_core::meta::schema::keys::ROOT_INO;
use wmfs_testkit::harness::ns::{Ns, user};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

/// 编一个示例插件；没有 gcc → None。
fn build(dir: &Path, name: &str) -> Option<String> {
    let (src, out) = (root().join(format!("examples/dylib-deny-uid/{name}.c")), dir.join(format!("{name}.so")));
    let inc = root().join("core/include");
    let mut gcc = Command::new("gcc");
    gcc.args(["-shared", "-fPIC", "-Wall", "-Werror", "-I"]).arg(&inc).arg("-o").arg(&out).arg(&src);
    match gcc.status() {
        Ok(s) if s.success() => Some(out.display().to_string()),
        Ok(s) => panic!("gcc 编 {name}.c 失败：{s}"),
        Err(_) => None,
    }
}

fn who(uid: u32) -> Principal {
    let conn = Arc::new(ConnIdentity {
        key_id: 1,
        role: Role::Admin,
        tenant: 0,
        uid_ranges: vec![(0, u32::MAX)],
        root_squash: false,
        path_caps: vec![],
    });
    Principal { uid, gid: uid, groups: vec![5, 6], tenant: 0, job_id: 0, conn }
}

fn decide(p: &dyn AuthzPlugin, uid: u32, op: AuthzOp) -> Decision {
    let dir = InodeView { ino: ROOT_INO, kind: 2, mode: 0o1777, uid: 0, gid: 0, acl_access: None };
    let w = who(uid);
    let i = AuthzInput { who: &w, op, target: None, parent: Some(&dir), victim: None, ancestors: &[1] };
    p.check(&i)
}

fn table(text: &str) -> std::collections::BTreeMap<String, PluginTable> {
    std::collections::BTreeMap::from([("deny-uid".to_string(), text.parse::<PluginTable>().unwrap())])
}

#[test]
fn c_authz_plugins_load_decide_and_bad_abi_is_refused() {
    let dir = std::env::temp_dir().join(format!("wmfs-dylib-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let Some(deny) = build(&dir, "deny_uid") else {
        println!("SKIP：本机没有 gcc");
        return;
    };
    let bad = build(&dir, "bad_abi").unwrap();

    let p = cabi::load(&deny, &Default::default()).unwrap();
    assert_eq!((p.manifest().name, p.manifest().altitude, p.manifest().abi_version), ("deny-uid", 290, 1));
    assert_eq!(p.manifest().fail_points, ["plugin.deny-uid.authz"]);
    assert_eq!(decide(&p, 4242, AuthzOp::Create), Decision::Deny(ErrCode::EACCES), "默认拒绝 uid 4242 的写");
    assert_eq!(decide(&p, 4242, AuthzOp::Open { read: false, write: true }), Decision::Deny(ErrCode::EACCES));
    assert_eq!(decide(&p, 4242, AuthzOp::Open { read: true, write: false }), Decision::Abstain, "读不管");
    assert_eq!(decide(&p, 4242, AuthzOp::GetAttr), Decision::Abstain);
    assert_eq!(decide(&p, 1000, AuthzOp::Create), Decision::Abstain, "别人弃权");
    let p7 = cabi::load(&deny, &table("uid = 7")).unwrap();
    assert_eq!(decide(&p7, 7, AuthzOp::Unlink), Decision::Deny(ErrCode::EACCES), "[plugin.deny-uid] uid = 7 生效");
    assert_eq!(decide(&p7, 4242, AuthzOp::Unlink), Decision::Abstain);

    let e = cabi::load(&bad, &Default::default()).err().unwrap();
    assert!(e.contains("动态库 ABI 版本 2 不受支持（支持 [1]），拒绝启动") && e.contains("bad_abi.so"), "{e}");

    let mut s = PluginSet::new(&[]);
    s.authz(Arc::new(cabi::load(&deny, &Default::default()).unwrap()));
    assert!(s.validate(&table("bogus = 1")).unwrap_err().contains("不认识的键 bogus"), "配置键按 config_schema 校验");
    s.validate(&table("uid = 4242")).unwrap();
    assert_eq!(s.plugin_list().plugins, ["authz:deny-uid"], "进集群插件清单");

    in_the_pipeline(&deny);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 进了管线：posix-mode 放行（/ 下随便建）也挡不住 deny-uid 的拒绝；fail point `plugin.deny-uid.authz` 以注入的码拒绝。
fn in_the_pipeline(deny: &str) {
    let mut set = PluginSet::new(&[]);
    set.authz(Arc::new(wmfs_plugins::posix_mode::PosixMode::new()));
    set.authz(Arc::new(cabi::load(deny, &Default::default()).unwrap()));
    set.placement(Arc::new(wmfs_plugins::label_match::LabelMatch::new()));
    let mut ns = Ns::with_pipeline(Pipeline::new(set));
    let d = HOME_INO; // 1777：posix-mode 让谁都能在这里建
    assert!(ns.create(&user(1000, 1000), d, "ok").is_ok(), "别的 uid 照常");
    assert_eq!(ns.create(&user(4242, 4242), d, "no"), Err(ErrCode::EACCES), "动态库插件拒绝了 uid 4242");
    failpoint::set("plugin.deny-uid.authz", "times(1,return(EPERM))").unwrap();
    assert_eq!(ns.create(&user(1000, 1000), d, "fp"), Err(ErrCode::EPERM), "fail point：以注入的码拒绝");
    assert!(ns.create(&user(1000, 1000), d, "fp").is_ok());
    let errs = metrics::counter("wmfs_hook_errors_total", "", &[("hook", "authz"), ("plugin", "deny-uid")]).get();
    assert_eq!(errs, 1, "调用包装把 fail point 计为插件错误");
}

/// 用来测"失败即关闭"与 manifest 检查的 C 插件：`-D` 换掉 manifest 的某一项，check 按 uid 返回各种结果（M14 审查）。
const ODD_C: &str = r#"
#include <errno.h>
#include "wmfs_plugin.h"
#ifndef HOOKS
#define HOOKS WMFS_HOOK_AUTHZ
#endif
#ifndef HOT
#define HOT 1
#endif
#ifndef NAME
#define NAME "odd"
#endif
#ifndef INIT_NULL
#define INIT_NULL 0
#endif
static const wmfs_manifest_v1 M = {WMFS_PLUGIN_ABI_V1, NAME, "0.1.0", HOOKS, 280, HOT, 0, 5000, ""};
static int st;
const wmfs_manifest_v1 *wmfs_plugin_manifest_v1(void) { return &M; }
void *wmfs_plugin_init_v1(const char *c) { (void)c; return INIT_NULL ? (void *)0 : (void *)&st; }
void wmfs_plugin_fini_v1(void *s) { (void)s; }
int32_t wmfs_authz_check_v1(void *s, const wmfs_authz_input_v1 *in, int32_t *err) {
    (void)s;
    switch (in->uid) {
    case 1: return 7;
    case 2: *err = 0; return 2;
    case 3: *err = 1000; return 2;
    case 4: *err = EROFS; return 2;
    default: return 0;
    }
}
"#;

/// 把 `src` 编成 `<tag>.so`（`defs` 是 gcc 的 -D 参数）；没有 gcc → None。
fn build_src(dir: &Path, tag: &str, src: &str, defs: &[&str]) -> Option<String> {
    let (c, out) = (dir.join(format!("{tag}.c")), dir.join(format!("{tag}.so")));
    std::fs::write(&c, src).unwrap();
    let mut gcc = Command::new("gcc");
    gcc.args(["-shared", "-fPIC", "-Wall", "-Werror", "-I"]).arg(root().join("core/include")).args(defs);
    match gcc.arg("-o").arg(&out).arg(&c).status() {
        Ok(s) if s.success() => Some(out.display().to_string()),
        Ok(s) => panic!("gcc 编 {tag} 失败：{s}"),
        Err(_) => None,
    }
}

/// 失败即关闭：check 返回 0/1/2 之外的值、返回 2 但 errno 不在 1–255 → EACCES；errno 原样；manifest 不合规、名字不合规、init 返回 NULL
/// → 拒绝加载（服务端据此拒绝启动）。
#[test]
fn odd_plugins_fail_closed_and_bad_manifests_are_refused() {
    let dir = std::env::temp_dir().join(format!("wmfs-dylib-odd-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let Some(ok) = build_src(&dir, "odd", ODD_C, &[]) else {
        println!("SKIP：本机没有 gcc");
        return;
    };
    let p = cabi::load(&ok, &Default::default()).unwrap();
    assert_eq!(decide(&p, 1, AuthzOp::GetAttr), Decision::Deny(ErrCode::EACCES), "返回 7 → 拒绝");
    assert_eq!(decide(&p, 2, AuthzOp::GetAttr), Decision::Deny(ErrCode::EACCES), "2 但 errno = 0 → EACCES");
    assert_eq!(decide(&p, 3, AuthzOp::GetAttr), Decision::Deny(ErrCode::EACCES), "2 但 errno = 1000 → EACCES");
    assert_eq!(decide(&p, 4, AuthzOp::GetAttr), Decision::Deny(ErrCode::EROFS), "errno 原样");
    assert_eq!(decide(&p, 5, AuthzOp::GetAttr), Decision::Abstain);
    for (tag, def, want) in [
        ("hooks", "-DHOOKS=5u", "manifest 不合规"),
        ("hot", "-DHOT=0", "manifest 不合规"),
        ("name", "-DNAME=\"Bad_Name\"", "只能由小写字母"),
        ("init", "-DINIT_NULL=1", "初始化失败"),
    ] {
        let so = build_src(&dir, tag, ODD_C, &[def]).unwrap();
        let e = cabi::load(&so, &Default::default()).err().unwrap();
        assert!(e.contains(want) && e.contains("拒绝启动"), "{tag}：{e}");
    }
    let missing = build_src(&dir, "missing", "void f(void);\nvoid g(void) { f(); }\n", &[]).unwrap();
    let e = cabi::load(&missing, &Default::default()).err().unwrap();
    assert!(e.contains("undefined symbol: f") && e.contains("拒绝启动"), "RTLD_NOW：缺符号在加载时就失败：{e}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 打印两个 v1 结构每个字段的 offsetof、sizeof（gcc 编出来跑）。
const LAYOUT_C: &str = r#"
#include <stdio.h>
#include "wmfs_plugin.h"
#define F(T, f) printf("%s.%s %zu %zu\n", #T, #f, offsetof(T, f), sizeof(((T *)0)->f))
int main(void) {
    printf("wmfs_manifest_v1 %zu\nwmfs_authz_input_v1 %zu\n", sizeof(wmfs_manifest_v1), sizeof(wmfs_authz_input_v1));
    F(wmfs_manifest_v1, abi_version); F(wmfs_manifest_v1, name); F(wmfs_manifest_v1, version);
    F(wmfs_manifest_v1, hooks); F(wmfs_manifest_v1, altitude); F(wmfs_manifest_v1, hot_path);
    F(wmfs_manifest_v1, reserved0); F(wmfs_manifest_v1, budget_ns); F(wmfs_manifest_v1, config_schema);
    F(wmfs_authz_input_v1, uid); F(wmfs_authz_input_v1, gid); F(wmfs_authz_input_v1, tenant);
    F(wmfs_authz_input_v1, job_id); F(wmfs_authz_input_v1, groups); F(wmfs_authz_input_v1, ngroups);
    F(wmfs_authz_input_v1, op); F(wmfs_authz_input_v1, op_flags); F(wmfs_authz_input_v1, ino);
    F(wmfs_authz_input_v1, kind); F(wmfs_authz_input_v1, mode); F(wmfs_authz_input_v1, owner_uid);
    F(wmfs_authz_input_v1, owner_gid); F(wmfs_authz_input_v1, parent_ino); F(wmfs_authz_input_v1, parent_mode);
    F(wmfs_authz_input_v1, parent_uid); F(wmfs_authz_input_v1, parent_gid); F(wmfs_authz_input_v1, ancestors);
    F(wmfs_authz_input_v1, nancestors);
    return 0;
}
"#;

/// v1 一经发布就冻结：gcc 编出来的两个结构的大小、每个字段的偏移与大小，与 Rust 这边 `#[repr(C)]` 的逐个相同（design.md 3.7.6 的
/// M14 注记）。
#[test]
fn the_v1_structs_have_the_same_layout_in_c_and_rust() {
    use std::mem::{offset_of, size_of};

    use wmfs_core::hooks::cabi::{AuthzInputV1 as I, ManifestV1 as M};
    let dir = std::env::temp_dir().join(format!("wmfs-dylib-layout-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (src, exe) = (dir.join("layout.c"), dir.join("layout"));
    std::fs::write(&src, LAYOUT_C).unwrap();
    let mut gcc = Command::new("gcc");
    match gcc.args(["-Wall", "-Werror", "-I"]).arg(root().join("core/include")).arg("-o").arg(&exe).arg(&src).status() {
        Ok(s) => assert!(s.success(), "gcc 编 layout.c 失败：{s}"),
        Err(_) => return println!("SKIP：本机没有 gcc"),
    }
    let out = String::from_utf8(Command::new(&exe).output().unwrap().stdout).unwrap();
    let (p, w) = (size_of::<usize>(), |n: usize| n.to_string()); // 指针的大小；数 → 字符串
    let m = "wmfs_manifest_v1";
    let i = "wmfs_authz_input_v1";
    let mut want = vec![format!("{m} {}", size_of::<M>()), format!("{i} {}", size_of::<I>())];
    let fields: [(&str, &str, usize, usize); 28] = [
        (m, "abi_version", offset_of!(M, abi_version), 4),
        (m, "name", offset_of!(M, name), p),
        (m, "version", offset_of!(M, version), p),
        (m, "hooks", offset_of!(M, hooks), 4),
        (m, "altitude", offset_of!(M, altitude), 2),
        (m, "hot_path", offset_of!(M, hot_path), 1),
        (m, "reserved0", offset_of!(M, reserved0), 1),
        (m, "budget_ns", offset_of!(M, budget_ns), 8),
        (m, "config_schema", offset_of!(M, config_schema), p),
        (i, "uid", offset_of!(I, uid), 4),
        (i, "gid", offset_of!(I, gid), 4),
        (i, "tenant", offset_of!(I, tenant), 4),
        (i, "job_id", offset_of!(I, job_id), 8),
        (i, "groups", offset_of!(I, groups), p),
        (i, "ngroups", offset_of!(I, ngroups), 4),
        (i, "op", offset_of!(I, op), 4),
        (i, "op_flags", offset_of!(I, op_flags), 4),
        (i, "ino", offset_of!(I, ino), 8),
        (i, "kind", offset_of!(I, kind), 4),
        (i, "mode", offset_of!(I, mode), 4),
        (i, "owner_uid", offset_of!(I, owner_uid), 4),
        (i, "owner_gid", offset_of!(I, owner_gid), 4),
        (i, "parent_ino", offset_of!(I, parent_ino), 8),
        (i, "parent_mode", offset_of!(I, parent_mode), 4),
        (i, "parent_uid", offset_of!(I, parent_uid), 4),
        (i, "parent_gid", offset_of!(I, parent_gid), 4),
        (i, "ancestors", offset_of!(I, ancestors), p),
        (i, "nancestors", offset_of!(I, nancestors), 4),
    ];
    want.extend(fields.iter().map(|(s, f, off, size)| format!("{s}.{f} {} {}", w(*off), w(*size))));
    assert_eq!(out.lines().collect::<Vec<_>>(), want, "C 与 Rust 的布局逐字段相同");
    assert_eq!((size_of::<M>(), size_of::<I>()), (48, 112), "aarch64 / x86_64 上的大小（design.md 3.7.6）");
    let _ = std::fs::remove_dir_all(&dir);
}
