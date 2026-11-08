//! 动态库插件的稳定 C ABI v1（design.md 3.7.6，ADR-0012：首发只覆盖 authz）。头文件 `core/include/wmfs_plugin.h`，规则与编号表
//! 见 `docs/plugin-abi.md`。全仓库只有这个文件（连同 buf/pool.rs、base/sys.rs、plugins/rdma/）可以写 `unsafe`（0.2 第 4 条）。
//!
//! 加载（服务端装配插件时，`cli/src/registry.rs` 对 node.toml `[plugins] dylib` 里的每个路径调用 [`load`]）：dlopen（`RTLD_NOW`：
//! 插件引用了不存在的符号——链错了库、依赖版本不符——在加载时就失败、拒绝启动，而不是在第一次授权调用时被动态链接器杀掉进程）→ 取
//! `wmfs_plugin_manifest_v1` → abi_version 要在 [`C_ABI_VERSIONS`] 里（不在就拒绝加载，不尝试适配）→ hooks 只能是 AUTHZ、hot_path = 1、
//! reserved0 = 0、名字是小写字母数字与 `-` → 取 init / fini / check 三个符号 → 用 `[plugin.<名字>]` 表的 TOML 文本调 init（NULL = 初始化
//! 失败）。altitude 区间、预算、fail point 声明、配置键由随后的 `PluginSet::validate` 照常检查（生成的 `Manifest` 的 `abi_version` 填
//! `CORE_ABI_VERSION`：C ABI 版本这里已按 `C_ABI_VERSIONS` 核对过，冻结的 v1 库在本体的钩子 trait 改版之后照样能用）。任何一步失败 → 返回给人看的原因，服务端
//! **拒绝启动**：配置了却加载不了的授权插件被跳过，就等于静默少了一条授权规则（原则 7、ADR-0012 第 4 条）。库在进程生命周期内保持加载
//! （`Library` 放在插件里，插件随管线活到进程结束；不支持热卸载）。
//!
//! 调用：[`AuthzPlugin::check`] 把 `AuthzInput` 摊平成 `wmfs_authz_input_v1`（指针只在这次调用期间有效），返回 0 弃权、1 允许、2 拒绝
//! （`*err` 是 errno，不在 1–255 → EACCES）；别的返回值按拒绝（EACCES）——失败即关闭。fail point `plugin.<名字>.authz`、计时与预算照常由
//! `Pipeline::authz` 的调用包装做（3.7.4）。实现注记（M14）：v1 输入结构在 `op` 之后加了 `op_flags`（`AuthzOp` 变体自带的布尔参数，
//! `docs/plugin-abi.md` 原写"M14 定"；v1 在 M14 才发布，此前没有任何插件用过这个结构）。

use std::ffi::{CStr, CString, c_char, c_void};

use libloading::Library;
use libloading::os::unix::{self as dl, RTLD_LOCAL, RTLD_NOW};

use crate::base::ErrCode;
use crate::base::config::PluginTable;
use crate::hooks::manifest::{C_ABI_VERSIONS, CORE_ABI_VERSION, Hook, Manifest};
use crate::hooks::traits::{AuthzInput, AuthzOp, AuthzPlugin, Decision};

/// `wmfs_plugin.h` 的 `WMFS_HOOK_AUTHZ`。
const HOOK_AUTHZ: u32 = 1 << 2;

/// `wmfs_manifest_v1`（与头文件逐字段一致；`tests/it/dylib.rs` 用 gcc 编出来的 offsetof/sizeof 逐个核对）。
#[repr(C)]
pub struct ManifestV1 {
    pub abi_version: u32,
    pub name: *const c_char,
    pub version: *const c_char,
    pub hooks: u32,
    pub altitude: u16,
    pub hot_path: u8,
    pub reserved0: u8,
    pub budget_ns: u64,
    pub config_schema: *const c_char,
}

/// `wmfs_authz_input_v1`（同上）。
#[repr(C)]
pub struct AuthzInputV1 {
    pub uid: u32,
    pub gid: u32,
    pub tenant: u32,
    pub job_id: u64,
    pub groups: *const u32,
    pub ngroups: u32,
    pub op: u32,
    pub op_flags: u32,
    pub ino: u64,
    pub kind: u32,
    pub mode: u32,
    pub owner_uid: u32,
    pub owner_gid: u32,
    pub parent_ino: u64,
    pub parent_mode: u32,
    pub parent_uid: u32,
    pub parent_gid: u32,
    pub ancestors: *const u64,
    pub nancestors: u32,
}

type ManifestFn = unsafe extern "C" fn() -> *const ManifestV1;
type InitFn = unsafe extern "C" fn(*const c_char) -> *mut c_void;
type FiniFn = unsafe extern "C" fn(*mut c_void);
type CheckFn = unsafe extern "C" fn(*mut c_void, *const AuthzInputV1, *mut i32) -> i32;

/// 一个加载好的 authz 动态库插件。`state` 是 init 返回的不透明指针，只交还给插件自己的 check 与 fini。
pub struct DylibAuthz {
    manifest: &'static Manifest,
    state: *mut c_void,
    check: CheckFn,
    fini: FiniFn,
    _lib: Library, // 放在最后：Drop 先调 fini、再卸载库
}

// SAFETY: 按 ABI 规则（docs/plugin-abi.md 第 2 节、头文件）check 必须线程安全；state 只交还给插件自己（check 可以从任何线程并发调用，
// fini 只在丢弃时调一次）；函数指针与 Library 本身可以跨线程。
unsafe impl Send for DylibAuthz {}
// SAFETY: 同上——&DylibAuthz 只用来调 check（插件保证线程安全）。
unsafe impl Sync for DylibAuthz {}

/// 加载 `path` 这个动态库插件（见文件头）；`cfg` = node.toml 的全部 `[plugin.<名字>]` 表。错误是给人看的中文（服务端据此拒绝启动）。
pub fn load(path: &str, cfg: &std::collections::BTreeMap<String, PluginTable>) -> Result<DylibAuthz, String> {
    let bad = |why: String| format!("动态库 {path}：{why}，拒绝启动");
    // SAFETY: dlopen 会运行库的初始化代码——加载哪些库是管理员在 node.toml 里写明的（等同于信任它们的代码）；路径只是字符串。
    // RTLD_NOW（与 design.md 3.7.6 代码块里的 Library::new = RTLD_LAZY 不同，见文件头）：缺符号在这里就失败。
    let lib: Library =
        unsafe { dl::Library::open(Some(path), RTLD_NOW | RTLD_LOCAL) }.map(Library::from).map_err(|e| {
            let why = std::error::Error::source(&e).map_or_else(|| e.to_string(), |s| s.to_string()); // dlerror 的原文在 source 里
            bad(format!("加载失败（{why}）"))
        })?;
    // SAFETY: 符号的类型就是 wmfs_plugin.h 里声明的 `const wmfs_manifest_v1 *wmfs_plugin_manifest_v1(void)`。
    let get_manifest: ManifestFn = unsafe { lib.get::<ManifestFn>(b"wmfs_plugin_manifest_v1\0".as_slice()) }
        .map(|s| *s)
        .map_err(|_| bad("没有导出 wmfs_plugin_manifest_v1（不是 wmfs 插件，或者是不支持的新版本）".into()))?;
    // SAFETY: 按 ABI 调用一个无参数、只返回指针的函数；返回值在下面检查非空之后才读。
    let mp = unsafe { get_manifest() };
    if mp.is_null() {
        return Err(bad("wmfs_plugin_manifest_v1 返回了 NULL".into()));
    }
    // SAFETY: mp 非空，指向插件的 manifest（插件保证它在库加载期间一直有效）；任何版本的 manifest 第一个字段都是 abi_version，
    // 版本不受支持时只读这一个字段。
    let v = unsafe { (*mp).abi_version };
    if !C_ABI_VERSIONS.contains(&v) {
        return Err(format!("动态库 ABI 版本 {v} 不受支持（支持 {C_ABI_VERSIONS:?}），拒绝启动：{path}"));
    }
    // SAFETY: 版本是 1，结构就是 wmfs_manifest_v1（#[repr(C)] 与头文件一致）；字符串指针在 [`cstr`] 里判空。
    let m = unsafe { &*mp };
    if m.hooks != HOOK_AUTHZ || m.hot_path != 1 || m.reserved0 != 0 {
        return Err(bad(format!(
            "manifest 不合规（hooks = {}，首发只能是 WMFS_HOOK_AUTHZ；hot_path 必须为 1、reserved0 必须为 0）",
            m.hooks
        )));
    }
    let name = cstr(m.name).ok_or_else(|| bad("manifest 的 name 不是 UTF-8 字符串".into()))?;
    if name.is_empty() || !name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-') {
        return Err(bad(format!("插件名 {name:?} 只能由小写字母、数字与 - 组成")));
    }
    let version = cstr(m.version).ok_or_else(|| bad("manifest 的 version 不是 UTF-8 字符串".into()))?;
    let schema = cstr(m.config_schema).ok_or_else(|| bad("manifest 的 config_schema 不是 UTF-8 字符串".into()))?;
    let sym = |n: &str| bad(format!("没有导出 {n}"));
    // SAFETY: 三个符号的类型就是 wmfs_plugin.h 里声明的函数类型；取出的函数指针只在 `_lib` 还加载着时调用（它们放在同一个结构里）。
    let (init, fini, check) = unsafe {
        let init = lib.get::<InitFn>(b"wmfs_plugin_init_v1\0".as_slice()).map_err(|_| sym("wmfs_plugin_init_v1"))?;
        let fini = lib.get::<FiniFn>(b"wmfs_plugin_fini_v1\0".as_slice()).map_err(|_| sym("wmfs_plugin_fini_v1"))?;
        let chk = lib.get::<CheckFn>(b"wmfs_authz_check_v1\0".as_slice()).map_err(|_| sym("wmfs_authz_check_v1"))?;
        (*init, *fini, *chk)
    };
    let table = cfg.get(name).cloned().unwrap_or_default();
    let text = toml::to_string(&table).map_err(|e| bad(format!("[plugin.{name}] 无法写成 TOML（{e}）")))?;
    let text = CString::new(text).map_err(|_| bad(format!("[plugin.{name}] 里有 NUL 字符")))?;
    // SAFETY: 按 ABI 调用 init：参数是 NUL 结尾的字符串，调用期间有效；返回的指针只交还给这个插件自己的 check 与 fini。
    let state = unsafe { init(text.as_ptr()) };
    if state.is_null() {
        return Err(bad(format!("{name} 初始化失败（wmfs_plugin_init_v1 返回 NULL）")));
    }
    let fp = format!("plugin.{name}.authz");
    let manifest = Box::leak(Box::new(Manifest {
        name: leak(name),
        version: leak(version),
        hooks: &[Hook::Authz],
        altitude: m.altitude,
        hot_path: true,
        config_schema: leak(schema),
        abi_version: CORE_ABI_VERSION, // C ABI 版本上面已核对过（见文件头）
        attrs: &[],
        budget_ns: m.budget_ns,
        fail_points: Box::leak(Box::new([leak(&fp)])),
    }));
    Ok(DylibAuthz { manifest, state, check, fini, _lib: lib })
}

/// 进程生命周期内只加载一次（启动时），泄漏这几个字符串换来 `&'static`（`Manifest` 的字段都是 `&'static`）。
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// C 字符串 → `&str`（NULL 或不是 UTF-8 → None）。
fn cstr<'a>(p: *const c_char) -> Option<&'a str> {
    if p.is_null() {
        return None;
    }
    // SAFETY: p 非空；按 ABI 它指向插件里 NUL 结尾、在库加载期间一直有效的字符串。
    unsafe { CStr::from_ptr(p) }.to_str().ok()
}

/// `AuthzOp` → (`op` 编号, `op_flags`)：编号 = 变体顺序（docs/plugin-abi.md 的表，只追加、永不改号）。
fn op_code(op: &AuthzOp) -> (u32, u32) {
    let b = |x: bool, bit: u32| u32::from(x) << bit;
    match op {
        AuthzOp::Lookup => (0, 0),
        AuthzOp::GetAttr => (1, 0),
        AuthzOp::ReadDir => (2, 0),
        AuthzOp::ReadLink => (3, 0),
        AuthzOp::Open { read, write } => (4, b(*read, 0) | b(*write, 1)),
        AuthzOp::Create => (5, 0),
        AuthzOp::Mkdir => (6, 0),
        AuthzOp::Mknod => (7, 0),
        AuthzOp::Symlink => (8, 0),
        AuthzOp::Link => (9, 0),
        AuthzOp::Unlink => (10, 0),
        AuthzOp::Rmdir => (11, 0),
        AuthzOp::RenameFrom => (12, 0),
        AuthzOp::RenameTo => (13, 0),
        AuthzOp::SetAttr { mode, owner, size, times, times_now } => {
            (14, b(*mode, 0) | b(*owner, 1) | b(*size, 2) | b(*times, 3) | b(*times_now, 4))
        }
        AuthzOp::GetXattr => (15, 0),
        AuthzOp::SetXattr => (16, 0),
        AuthzOp::ListXattr => (17, 0),
        AuthzOp::RemoveXattr => (18, 0),
        AuthzOp::Lock => (19, 0),
        AuthzOp::Admin => (20, 0),
        AuthzOp::Handle { read, write } => (21, b(*read, 0) | b(*write, 1)),
    }
}

impl AuthzPlugin for DylibAuthz {
    fn manifest(&self) -> &'static Manifest {
        self.manifest
    }

    fn check(&self, i: &AuthzInput) -> Decision {
        let (op, op_flags) = op_code(&i.op);
        let (t, p) = (i.target, i.parent);
        let input = AuthzInputV1 {
            uid: i.who.uid,
            gid: i.who.gid,
            tenant: i.who.tenant,
            job_id: i.who.job_id,
            groups: i.who.groups.as_ptr(),
            ngroups: i.who.groups.len() as u32,
            op,
            op_flags,
            ino: t.map_or(0, |t| t.ino),
            kind: t.map_or(0, |t| t.kind as u32),
            mode: t.map_or(0, |t| t.mode),
            owner_uid: t.map_or(0, |t| t.uid),
            owner_gid: t.map_or(0, |t| t.gid),
            parent_ino: p.map_or(0, |p| p.ino),
            parent_mode: p.map_or(0, |p| p.mode),
            parent_uid: p.map_or(0, |p| p.uid),
            parent_gid: p.map_or(0, |p| p.gid),
            ancestors: i.ancestors.as_ptr(),
            nancestors: i.ancestors.len() as u32,
        };
        let mut err = 0i32;
        // SAFETY: 按 ABI 调用：state 是这个插件的 init 返回的；input 与它指向的切片、err 在调用期间有效；库还加载着（`_lib`）。
        let r = unsafe { (self.check)(self.state, &input, &mut err) };
        match r {
            0 => Decision::Abstain,
            1 => Decision::Allow,
            2 if (1..=255).contains(&err) => Decision::Deny(ErrCode(err as u16)),
            _ => Decision::Deny(ErrCode::EACCES), // 失败即关闭
        }
    }
}

impl Drop for DylibAuthz {
    fn drop(&mut self) {
        // SAFETY: state 是 init 返回的、只交还一次；库还加载着（`_lib` 在这之后才被丢弃）。
        unsafe { (self.fini)(self.state) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 编号表（docs/plugin-abi.md、wmfs_plugin.h 的 WMFS_OP_*）：只追加、永不改号；变体的布尔参数在 op_flags 里。
    #[test]
    fn op_numbers_follow_the_published_table() {
        let all = [
            AuthzOp::Lookup,
            AuthzOp::GetAttr,
            AuthzOp::ReadDir,
            AuthzOp::ReadLink,
            AuthzOp::Open { read: true, write: false },
            AuthzOp::Create,
            AuthzOp::Mkdir,
            AuthzOp::Mknod,
            AuthzOp::Symlink,
            AuthzOp::Link,
            AuthzOp::Unlink,
            AuthzOp::Rmdir,
            AuthzOp::RenameFrom,
            AuthzOp::RenameTo,
            AuthzOp::SetAttr { mode: false, owner: false, size: true, times: false, times_now: false },
            AuthzOp::GetXattr,
            AuthzOp::SetXattr,
            AuthzOp::ListXattr,
            AuthzOp::RemoveXattr,
            AuthzOp::Lock,
            AuthzOp::Admin,
            AuthzOp::Handle { read: false, write: true },
        ];
        for (i, op) in all.iter().enumerate() {
            assert_eq!(op_code(op).0, i as u32, "{i}");
        }
        assert_eq!(op_code(&all[4]).1, 1, "Open：bit0 = read");
        assert_eq!(op_code(&all[14]).1, 1 << 2, "SetAttr：bit2 = size");
        assert_eq!(op_code(&all[21]).1, 1 << 1, "Handle：bit1 = write");
        let h = include_str!("../../include/wmfs_plugin.h");
        assert!(h.contains("#define WMFS_OP_HANDLE 21u") && h.contains("#define WMFS_OP_ADMIN 20u"), "头文件与表一致");
        assert_eq!(std::mem::size_of::<AuthzInputV1>(), 112, "tests/it/dylib.rs 用 gcc 逐字段核对 offsetof/sizeof");
    }

    #[test]
    fn a_missing_library_is_a_readable_error() {
        let e = load("/nonexistent/x.so", &Default::default()).err().unwrap();
        assert!(e.contains("/nonexistent/x.so") && e.contains("拒绝启动"), "{e}");
        assert!(e.contains("No such file"), "带上 dlerror 的原文，而不只是 dlopen failed：{e}");
    }
}
