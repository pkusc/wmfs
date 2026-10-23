//! 目录属性的设置、删除与查询，递归用量与"声明 vs 实际"（design.md 3.8、3.1.5、3.9、3.10，ADR-0026、ADR-0016）：管理 API
//! （`mgmt/api.rs`）把修改包成执行器闭包、把查询包成线性一致读。路径是文件系统内的绝对路径（逐段查名字，与 ResolvePath 相同）。
//!
//! - **set**（执行器闭包里，全部是点查）：目标必须是目录（属性只挂在目录上，文件 → `EINVAL`）→ 每个键要有消费者（"core" 或已启用
//!   插件的 manifest 声明了它，3.8）→ 交叉检查（存储类存在、校验算法的插件已启用）→ 写 DIRATTR、每个键一条 changelog `AttrSet`
//!   （extra = key/old/new，显示形式）、"新数据 + 协调器"类的键写一条 `RECONCILE_Q(dir, 1)`（协调器在 M11）→ 最后 `attr_epoch += 1`
//!   （之后这个闭包里不再解析属性，见 `attrs.rs`）。警告（成功但要说的话）：副本数大于存储类可用的故障域数、条带数大于可用的链数
//!   （ADR-0016）、改了 class/replicas 时已有文件不会马上转换。
//! - **unset**：删一个键（后面里程碑才有的键与 set 一样回 `EOPNOTSUPP`；目录上没设这个键 → `ENOENT`）、changelog `AttrUnset`、
//!   同样 `attr_epoch += 1`。
//! - **get**：生效值 + 来源 + 生效方式（每个注册表里的键一行）+ `semantics:` 一行（3.1.5）；文件 = 它所在目录的生效属性；
//!   `recursive` 时另列出子树里显式设置了属性的目录（扫描 DIRATTR，最多 1000 个）。
//! - **status**：每个键的声明、来源、实际、是否一致；实现注记（M7）：实际 = 这个目录下的新文件此刻实际得到的值（replicas 是
//!   min(声明, 可用故障域数)、stripe 是 min(声明, 可用链数)，其余就是声明值）；逐文件扫描已有文件在 M11 的 `bg/status.rs`。
//!   `warnings` = 集群警告 + 这棵子树里显式写着、当前满足不了的 replicas/stripe 声明（例如单节点上 init 写的 `/home replicas=3`；
//!   最多查 1000 个这样的目录，多了明确说一句）+ 这个目录自己的。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use crate::api::types::{
    AttrActual, AttrEntry, AttrGetReply, AttrSetReply, AttrUnsetReply, ClassUsage, DEFAULT_SOURCE, DirAttrs,
    StatusAttr, StatusReply, UsageReply,
};
use crate::base::units::{fmt_duration, fmt_size};
use crate::base::{ErrCode, Failed};
use crate::data::format::checksum_name;
use crate::hooks::traits::TransformKind;
use crate::meta::attr_registry::{AttrKind, Effect, Parsed, REGISTRY, find, spec_of};
use crate::meta::attrs::{
    CONSISTENCY_STRONG, EffectiveAttrs, MAX_DEPTH, bump_attr_epoch, checksum_id, queue_reconcile,
};
use crate::meta::changelog::{self, Change};
use crate::meta::cluster::chains::{SERVING, candidates};
use crate::meta::engine::txn::{KvRead, KvScan, KvTxn, OpCtx};
use crate::meta::ns::common::{self, Node};
use crate::meta::ns::read_ops;
use crate::meta::rstat;
use crate::meta::schema::keys::{self, ROOT_INO, T_DIRATTR};
use crate::meta::schema::kv;
use crate::meta::schema::records::{AttrMapV1, ChainRecV1, ClassIdV1, decode_v1};
use crate::proto::meta::MetaReply;
use crate::proto::peer::ClusterMap;

/// `-r` 最多列出、status 最多检查多少个目录。
const SUBTREE_MAX: usize = 1000;

/// 按文件系统内的绝对路径找 inode（与 `ResolvePath` 相同：逐段查名字、每段要执行权限）。
pub fn lookup(v: &dyn KvRead, c: &OpCtx, path: &str) -> Result<Node, Failed> {
    match read_ops::resolve(v, c, path.as_bytes()) {
        Ok(MetaReply::Resolved { ino, .. }) => Ok(common::get(v, ino)?),
        Ok(_) => Err(ErrCode::INTERNAL.into()),
        Err(ErrCode::ENOENT) => Err(Failed::new(ErrCode::ENOENT, format!("路径 {path} 不存在"))),
        Err(ErrCode::EINVAL) => {
            Err(Failed::new(ErrCode::EINVAL, format!("路径应是文件系统内的绝对路径（以 / 开头）：{path}")))
        }
        Err(e) => Err(e.into()),
    }
}

/// 一个 inode 的路径（沿主链接的 parent/name 拼到根；走了 `MAX_DEPTH` 步还没到根 = parent 指针损坏）。
pub fn path_of(v: &dyn KvRead, ino: u64) -> Result<String, ErrCode> {
    let mut names = Vec::new();
    let mut cur = common::get(v, ino)?;
    while cur.ino != ROOT_INO {
        if names.len() >= MAX_DEPTH {
            return Err(ErrCode::INTERNAL);
        }
        names.push(String::from_utf8_lossy(&cur.rec.name).into_owned());
        cur = common::get(v, cur.rec.parent)?;
    }
    names.reverse();
    Ok(format!("/{}", names.join("/")))
}

fn dir_at(v: &dyn KvRead, c: &OpCtx, path: &str) -> Result<Node, Failed> {
    let n = lookup(v, c, path)?;
    if !n.is_dir() {
        return Err(Failed::new(ErrCode::EINVAL, format!("属性只能挂在目录上：{path} 不是目录")));
    }
    Ok(n)
}

/// 目录自己的 DIRATTR（没有 = 空）。
fn own_attrs(v: &dyn KvRead, dir: u64) -> Result<AttrMapV1, ErrCode> {
    Ok(kv::get(v, &keys::dirattr(dir))?.unwrap_or(AttrMapV1 { entries: BTreeMap::new() }))
}

/// 3.8 的"必须有插件消费"与交叉检查（存储类存在、校验算法的插件已启用）。
fn check(v: &dyn KvRead, c: &OpCtx, p: &Parsed) -> Result<(), Failed> {
    let (k, s) = (p.spec.key, p.spec);
    if !s.consumers.contains(&"core") && !c.pipeline.consumes(k) {
        let who = s.consumers.join("、");
        let msg =
            format!("属性 {k} 没有已启用的插件消费（消费它的插件：{who}；node.toml 的 [plugins] disable 里停掉了？）");
        return Err(Failed::new(ErrCode::EINVAL, msg));
    }
    if k == "class" && kv::get::<ClassIdV1>(v, &keys::class_name(&p.norm))?.is_none() {
        return Err(Failed::new(ErrCode::EINVAL, format!("存储类 {} 不存在", p.norm)));
    }
    let alg = checksum_id(&p.norm).unwrap_or(0);
    if k == "checksum" && c.pipeline.transform(TransformKind::Checksum, alg).is_none() {
        return Err(Failed::new(ErrCode::EINVAL, format!("校验算法 {} 的插件没有启用", p.norm)));
    }
    Ok(())
}

pub fn set(v: &mut dyn KvTxn, c: &OpCtx, path: &str, items: &[Parsed]) -> Result<AttrSetReply, Failed> {
    let d = dir_at(v, c, path)?;
    for p in items {
        check(v, c, p)?;
    }
    let before = c.attrs.resolve(v, d.ino)?; // 在 attr_epoch 加一之前解析（警告要用）
    let mut m = own_attrs(v, d.ino)?;
    let mut out = AttrSetReply { path: path.into(), ..Default::default() };
    for p in items {
        let (k, show) = (p.spec.key, p.spec.show);
        let old = m.entries.insert(k.into(), p.norm.clone()).map(|o| show(&o)).unwrap_or_default();
        let extra = vec![("key".into(), k.into()), ("old".into(), old), ("new".into(), show(&p.norm))];
        changelog::append(v, c, Change { op: "AttrSet", ino: d.ino, extra, ..Default::default() })?;
        out.set.insert(k.into(), show(&p.norm));
    }
    if items.iter().any(|p| p.spec.effect == Effect::NewDataAndReconcile) {
        queue_reconcile(v, d.ino, 1)?;
    }
    kv::put(v, &keys::dirattr(d.ino), &m);
    bump_attr_epoch(v)?; // 最后一步（见文件头）
    // 这次给出的键的规范化值（没给 → None）
    let given = |k: &str| items.iter().find(|p| p.spec.key == k).map(|p| p.norm.as_str());
    let class = match given("class") {
        Some(name) => kv::get::<ClassIdV1>(v, &keys::class_name(name))?.map_or(before.class_id, |x| x.id),
        None => before.class_id,
    };
    let replicas = given("replicas").and_then(|r| r.parse().ok()).unwrap_or(before.replicas);
    let stripe = given("stripe").and_then(|s| s.parse().ok()).unwrap_or(before.stripe);
    let topo = given("replicas").is_some() || given("class").is_some();
    let check_stripe = topo || given("stripe").is_some();
    out.warnings = unmet(c.map, class, replicas, stripe, topo, check_stripe, "");
    if topo {
        out.warnings.push(
            "class、replicas 是\"新数据 + 协调器\"类的属性：已有文件在协调器转换完成前，新写入仍按旧设置（协调器在 M11 实现，现在不会\
             转换已有文件）；一直写打开的文件不会被转换"
                .into(),
        );
    }
    Ok(out)
}

pub fn unset(v: &mut dyn KvTxn, c: &OpCtx, path: &str, key: &str) -> Result<AttrUnsetReply, Failed> {
    let s = spec_of(key)?; // 后面里程碑才有的键：与 set 相同的 EOPNOTSUPP；不认识的：未知属性
    let d = dir_at(v, c, path)?;
    let mut m = own_attrs(v, d.ino)?;
    let not_set = || Failed::new(ErrCode::ENOENT, format!("{path} 上没有设置 {key}"));
    let old = (s.show)(&m.entries.remove(key).ok_or_else(not_set)?);
    if m.entries.is_empty() {
        v.delete(&keys::dirattr(d.ino));
    } else {
        kv::put(v, &keys::dirattr(d.ino), &m);
    }
    let extra = vec![("key".into(), key.into()), ("old".into(), old.clone())];
    changelog::append(v, c, Change { op: "AttrUnset", ino: d.ino, extra, ..Default::default() })?;
    if s.effect == Effect::NewDataAndReconcile {
        queue_reconcile(v, d.ino, 1)?;
    }
    bump_attr_epoch(v)?;
    Ok(AttrUnsetReply { path: path.into(), key: key.into(), old })
}

/// 存储类可用的故障域数（不同节点上的可用 target，3.4.2.1）。
fn domains(m: &ClusterMap, class: u16) -> usize {
    let k = m.classes.iter().find(|k| k.id == class);
    k.map_or(0, |k| candidates(m, k).iter().map(|(_, node)| *node).collect::<BTreeSet<u32>>().len())
}

/// (class, replicas) 可用的互不相同的链数：已有的（至少一个 Serving 成员）；一条都还没有 → 会生成的条数（= 候选 target 数）。
fn usable_chains(m: &ClusterMap, class: u16, replicas: u8) -> usize {
    let serving = |c: &ChainRecV1| c.members.iter().any(|(_, s)| *s == SERVING);
    let n = m.chains.iter().filter(|(_, c)| c.class_id == class && c.replicas == replicas && serving(c)).count();
    if n > 0 { n } else { m.classes.iter().find(|k| k.id == class).map_or(0, |k| candidates(m, k).len()) }
}

/// 满足不了的声明（ADR-0016）：`check_replicas`、`check_stripe` = 查不查副本数、条带数；`who` 是消息开头（例如"目录 /home "）。
fn unmet(
    m: &ClusterMap,
    class: u16,
    replicas: u8,
    stripe: u16,
    check_replicas: bool,
    check_stripe: bool,
    who: &str,
) -> Vec<String> {
    let domains_n = domains(m, class);
    let chains_n = usable_chains(m, class, replicas);
    let mut w = Vec::new();
    if check_replicas && domains_n == 0 {
        w.push(format!("{who}没有可用的 target：新文件无法创建（ENOSPC）"));
    } else if check_replicas && replicas as usize > domains_n {
        w.push(format!(
            "{who}声明 {replicas} 副本，当前只有 {domains_n} 个故障域，新文件实际为 {domains_n} 副本；wmfs status 会显示差距；\
             加入节点后自动补齐"
        ));
    }
    if check_stripe && domains_n > 0 && stripe as usize > chains_n {
        w.push(format!(
            "{who}声明 stripe={stripe}，只有 {chains_n} 条可用的链，新文件实际只有 {chains_n} 个条带（已有文件不迁移）"
        ));
    }
    w
}

/// 一个键的生效值（显示形式）。加了键要在这里加一行（`attr_registry.rs` 文件头的清单）。
fn display(c: &OpCtx, e: &EffectiveAttrs, own: &AttrMapV1, key: &str) -> String {
    match key {
        "class" => {
            c.map.classes.iter().find(|k| k.id == e.class_id).map_or(format!("#{}", e.class_id), |k| k.name.clone())
        }
        "replicas" => e.replicas.to_string(),
        "chunk" => fmt_size(e.chunk as u64),
        "stripe" => e.stripe.to_string(),
        "inline" => fmt_size(e.inline_max as u64),
        "checksum" => checksum_name(e.checksum).into(),
        "consistency" if e.consistency == CONSISTENCY_STRONG => "strong".into(),
        "consistency" => format!("cto,attr_ttl={}", ttl(e)),
        "inherit" => own.entries.get(key).cloned().unwrap_or_else(|| "true".into()), // 只看目录自己的
        // 这里还没处理的键（不该有）：目录自己存的值（显示形式），没有就空着——不冒充成某个生效值
        _ => own.entries.get(key).map(|x| find(key).map_or(x.clone(), |s| (s.show)(x))).unwrap_or_default(),
    }
}

fn ttl(e: &EffectiveAttrs) -> String {
    fmt_duration(Duration::from_millis(e.attr_ttl_ms as u64))
}

/// 一个键的来源：来源目录的路径，内置默认值是 [`DEFAULT_SOURCE`]；Local 键（inherit）只看目录自己的。
fn source(v: &dyn KvRead, e: &EffectiveAttrs, own: &AttrMapV1, dir: u64, key: &str) -> Result<String, ErrCode> {
    let local = find(key).is_some_and(|s| s.kind == AttrKind::Local);
    let src = if !local {
        e.source(key)
    } else if own.entries.contains_key(key) {
        dir
    } else {
        0
    };
    if src == 0 { Ok(DEFAULT_SOURCE.into()) } else { path_of(v, src) }
}

/// `path` 的生效属性所属的目录（文件 = 它的主父目录）、它的解析结果、它自己的 DIRATTR。
fn dir_attrs_of_path(v: &dyn KvRead, c: &OpCtx, path: &str) -> Result<(u64, Arc<EffectiveAttrs>, AttrMapV1), Failed> {
    let n = lookup(v, c, path)?;
    let dir = if n.is_dir() { n.ino } else { n.rec.parent };
    Ok((dir, c.attrs.resolve(v, dir)?, own_attrs(v, dir)?))
}

pub fn get(v: &dyn KvScan, c: &OpCtx, path: &str, recursive: bool) -> Result<AttrGetReply, Failed> {
    let (dir, e, own) = dir_attrs_of_path(v, c, path)?;
    let mut attrs = Vec::new();
    for s in REGISTRY.iter() {
        let value = display(c, &e, &own, s.key);
        let source = source(v, &e, &own, dir, s.key)?;
        attrs.push(AttrEntry { key: s.key.into(), value, source, effect: s.effect.name().into() });
    }
    let warnings = unmet(c.map, e.class_id, e.replicas, e.stripe, true, true, "这个目录：");
    let actual = domains(c.map, e.class_id).min(e.replicas as usize);
    let iso = "shared-device(先进先出,不承诺份额与尾延迟)"; // qos 插件现在只有 fifo（mclock 在 M13，到时按插件分开写）
    let from = match source(v, &e, &own, dir, "consistency")? {
        s if s == DEFAULT_SOURCE => s,
        s => format!("(来源 {s})"),
    };
    let semantics = format!(
        "consistency=cto attr_ttl={} {from}; quota=none; durability=replicas:声明{}/实际{actual} fsync:all-replicas; \
         isolation={iso}",
        ttl(&e),
        e.replicas
    );
    let (subtree, truncated) = if recursive { subtree(v, c, dir)? } else { (vec![], false) };
    let dir = path_of(v, dir)?;
    Ok(AttrGetReply { path: path.into(), dir, attrs, warnings, semantics, subtree, truncated })
}

/// 子树（含 `root` 自己）里显式设置了属性的目录：(目录, 它的 DIRATTR)。`only` 不空时只要含其中某个键的；最多 `SUBTREE_MAX`
/// 个，多了第二项为 true。
fn dirattrs_under(
    v: &dyn KvScan,
    c: &OpCtx,
    root: u64,
    only: &[&str],
) -> Result<(Vec<(u64, AttrMapV1)>, bool), ErrCode> {
    let mut out = Vec::new();
    for (k, b) in kv::scan_tag(v, T_DIRATTR)? {
        let d = kv::key_id(&k);
        let m: AttrMapV1 = decode_v1(&b)?;
        let wanted = only.is_empty() || only.iter().any(|x| m.entries.contains_key(*x));
        if wanted && (d == root || c.attrs.ancestors(v, d)?.contains(&root)) {
            if out.len() == SUBTREE_MAX {
                return Ok((out, true));
            }
            out.push((d, m));
        }
    }
    Ok((out, false))
}

fn subtree(v: &dyn KvScan, c: &OpCtx, root: u64) -> Result<(Vec<DirAttrs>, bool), ErrCode> {
    let (dirs, truncated) = dirattrs_under(v, c, root, &[])?;
    let mut out = Vec::new();
    for (d, m) in dirs {
        let attrs = m.entries.iter().map(|(k, x)| (k.clone(), find(k).map_or(x.clone(), |s| (s.show)(x)))).collect();
        out.push(DirAttrs { path: path_of(v, d)?, attrs });
    }
    Ok((out, truncated))
}

pub fn status(v: &dyn KvScan, c: &OpCtx, path: &str) -> Result<StatusReply, Failed> {
    let (dir, e, own) = dir_attrs_of_path(v, c, path)?;
    let mut warnings = c.map.warnings.clone();
    // 子树里显式写着、当前满足不了的 replicas/stripe 声明；这个目录自己的在后面按全部生效值查一遍，这里跳过
    let (dirs, truncated) = dirattrs_under(v, c, dir, &["replicas", "stripe"])?;
    for (d, m) in dirs.into_iter().filter(|(d, _)| *d != dir) {
        let x = c.attrs.resolve(v, d)?;
        let (r, s) = (m.entries.contains_key("replicas"), m.entries.contains_key("stripe"));
        let who = format!("目录 {} ", path_of(v, d)?);
        warnings.extend(unmet(c.map, x.class_id, x.replicas, x.stripe, r, s, &who));
    }
    if truncated {
        warnings.push(format!(
            "设置了副本数或条带数的目录超过 {SUBTREE_MAX} 个，只检查了前 {SUBTREE_MAX} 个；其余的用 wmfs status <子目录> 查看"
        ));
    }
    warnings.extend(unmet(c.map, e.class_id, e.replicas, e.stripe, true, true, "这个目录："));
    let mut attrs = Vec::new();
    for s in REGISTRY.iter() {
        let declared = display(c, &e, &own, s.key);
        let actual = match s.key {
            "replicas" => domains(c.map, e.class_id).min(e.replicas as usize).to_string(),
            "stripe" => usable_chains(c.map, e.class_id, e.replicas).min(e.stripe as usize).to_string(),
            _ => declared.clone(), // 每次 open/create 由 core 按生效属性执行，新文件就是声明值
        };
        let source = source(v, &e, &own, dir, s.key)?;
        let effect = s.effect.name().to_string();
        let mismatched = actual != declared;
        let actual = AttrActual { new_files: actual };
        attrs.push(StatusAttr { key: s.key.into(), declared, source, effect, actual, mismatched });
    }
    Ok(StatusReply { path: path.into(), warnings, attrs })
}

pub fn usage(v: &dyn KvRead, c: &OpCtx, path: &str) -> Result<UsageReply, Failed> {
    let n = lookup(v, c, path)?;
    if !n.is_dir() {
        return Err(Failed::new(ErrCode::ENOTDIR, format!("递归用量只对目录：{path} 不是目录")));
    }
    let r = rstat::get(v, n.ino)?;
    let name = |id: u16| c.map.classes.iter().find(|k| k.id == id).map_or(format!("#{id}"), |k| k.name.clone());
    let per_class = r.per_class.iter().map(|(k, b, f)| ClassUsage { class: name(*k), bytes: *b, files: *f }).collect();
    Ok(UsageReply { path: path.into(), bytes: r.bytes, files: r.files, dirs: r.dirs, per_class })
}
