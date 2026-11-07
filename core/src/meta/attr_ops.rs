//! 目录属性的设置、删除与查询，"声明 vs 实际"（design.md 3.8、3.1.5、3.10，ADR-0026、ADR-0016）：管理 API（`mgmt/api.rs`）把
//! 修改包成执行器闭包、把查询包成线性一致读。路径是文件系统内的绝对路径（`ns::read_ops::node_at`：逐段查名字，与 ResolvePath 相同）。
//! 递归用量与配额的显示 M10 起在 `meta/quota.rs`（`wmfs usage`、get 的 semantics 行里配额的一段）；set 写回配额键之前调
//! `quota::note_set`、unset 之后调 `quota::forget`（3.8 变更通知 ⑤：软限的起点）。
//!
//! - **set**（执行器闭包里，全部是点查）：目标必须是目录（属性只挂在目录上，文件 → `EINVAL`）→ 每个键要有消费者（"core" 或已启用
//!   插件的 manifest 声明了它，3.8）→ 交叉检查（存储类存在、校验算法的插件已启用）→ 写 DIRATTR、每个键一条 changelog `AttrSet`
//!   （extra = key/old/new，显示形式）、"新数据 + 协调器"类的键写一条 `RECONCILE_Q(dir, 1)`（协调器消费，M11）→ 最后 `attr_epoch += 1`
//!   （之后这个闭包里不再解析属性，见 `attrs.rs`）。警告（成功但要说的话）：副本数大于存储类可用的故障域数、条带数大于可用的链数
//!   （ADR-0016）、改了 class/replicas 时已有文件不会马上转换。
//! - **unset**：删一个键（后面里程碑才有的键与 set 一样回 `EOPNOTSUPP`；目录上没设这个键 → `ENOENT`）、changelog `AttrUnset`、
//!   同样 `attr_epoch += 1`。
//! - **get**：生效值 + 来源 + 生效方式（每个注册表里的键一行）+ `semantics:` 一行（3.1.5）；文件 = 它所在目录的生效属性；
//!   `recursive` 时另列出子树里显式设置了属性的目录（扫描 DIRATTR，最多 1000 个）。
//! - **status**（M12 起在 `attr_ops/status.rs`）：每个键的声明、来源、实际、是否一致；实现注记（M7）：实际 = 这个目录下的新文件此刻实际得到的值（replicas 是
//!   min(声明, 可用故障域数)、stripe 是 min(声明, 可用链数)，inline 是 `ns::layout::inline_of`（加密生效时为 0），其余就是声明值）；
//!   实现注记（M11）：逐文件统计（已有文件里与声明不同的、写打开的、副本或条带不足的、降级链上的）在这个读之外由 `bg/status.rs` 受限速地
//!   扫子树、再填进回复（`mgmt/api.rs`）；`quota`、`quota.<类>` 的实际是管着这个目录的每个配额此刻的用量与上限（`quota::quotas_of`，
//!   超了软限、宽限已过或超了硬限算不一致）。
//!   `warnings` = 集群警告 + 这棵子树里显式写着、当前满足不了的 replicas/stripe 声明（例如单节点上 init 写的 `/home replicas=3`；
//!   最多查 1000 个这样的目录，多了明确说一句）+ 这个目录自己的。实现注记（M9）：replicas 另给 `short_files`——子树里已有文件中
//!   实际副本数少于声明的个数（M11 起由 `bg/status.rs` 数）。实现注记（M12）：trash、ttl、audit 的实际见 `attr_ops/status.rs`。
//! - **audit 游标**（M12，3.8 变更通知 ⑥）：set 把 `audit` 设成非 off 时，`CURSOR(audit-log)` 还不存在就同批创建，已存在却
//!   10 分钟没前进、又落后了 → 警告（[`ensure_audit_cursor`]）。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use crate::api::types::{AttrEntry, AttrGetReply, AttrSetReply, AttrUnsetReply, DEFAULT_SOURCE, DirAttrs};
use crate::base::types::Role;
use crate::base::units::{fmt_duration, fmt_size};
use crate::base::{ErrCode, Failed};
use crate::data::format::{CMP_NONE, CMP_ZSTD, ENC_AES256GCM, checksum_name, compress_name};
use crate::hooks::traits::TransformKind;
use crate::meta::attr_registry::trash_ttl_audit::{audit_text, fmt_ms};
use crate::meta::attr_registry::{AttrKind, Effect, Parsed, REGISTRY, find, spec_of};
use crate::meta::attrs::{
    CONSISTENCY_STRONG, EffectiveAttrs, bump_attr_epoch, checksum_id, compress_id, queue_reconcile,
};
use crate::meta::changelog::{self, AUDIT_CURSOR, Change};
use crate::meta::cluster::chains::{SERVING, candidates};
use crate::meta::engine::txn::{KvRead, KvScan, KvTxn, OpCtx};
use crate::meta::ns::acl;
use crate::meta::ns::common::Node;
use crate::meta::ns::read_ops::{node_at, path_of};
use crate::meta::quota;
use crate::meta::schema::keys::{self, T_DIRATTR};
use crate::meta::schema::kv;
use crate::meta::schema::records::{AttrMapV1, ChainRecV1, ClassIdV1, DataKeyIdV1, DataKeyV1, decode_v1};
use crate::proto::peer::ClusterMap;

mod status;
pub use status::status;

/// `-r` 最多列出、status 最多检查多少个目录。
const SUBTREE_MAX: usize = 1000;
/// audit-log 游标多久不前进算"没在消费"（3.8 警告 ③）。
const AUDIT_STALE_MS: u64 = 10 * 60_000;

fn dir_at(v: &dyn KvRead, c: &OpCtx, path: &str) -> Result<Node, Failed> {
    let n = node_at(v, c, path)?;
    if !n.is_dir() {
        return Err(Failed::new(ErrCode::EINVAL, format!("属性只能挂在目录上：{path} 不是目录")));
    }
    Ok(n)
}

/// 目录自己的 DIRATTR（没有 = 空）。
fn own_attrs(v: &dyn KvRead, dir: u64) -> Result<AttrMapV1, ErrCode> {
    Ok(kv::get(v, &keys::dirattr(dir))?.unwrap_or(AttrMapV1 { entries: BTreeMap::new() }))
}

/// 3.8 的"必须有插件消费"与交叉检查（存储类存在、校验与压缩算法的插件已启用、数据密钥存在且属于设置者的租户）；回要存进 DIRATTR 的
/// 规范化值（`encrypt=tenant:<名字>` 在这里换成数据密钥的数字 id，3.8）。
fn check(v: &dyn KvRead, c: &OpCtx, p: &Parsed) -> Result<String, Failed> {
    let (k, s) = (p.spec.key, p.spec);
    if !s.consumers.contains(&"core") && !c.pipeline.consumes(k) {
        let who = s.consumers.join("、");
        let msg =
            format!("属性 {k} 没有已启用的插件消费（消费它的插件：{who}；node.toml 的 [plugins] disable 里停掉了？）");
        return Err(Failed::new(ErrCode::EINVAL, msg));
    }
    let class = if k == "class" { Some(p.norm.as_str()) } else { p.key.strip_prefix("quota.") }; // quota.<类> 的类也要存在
    if let Some(name) = class
        && kv::get::<ClassIdV1>(v, &keys::class_name(name))?.is_none()
    {
        return Err(Failed::new(ErrCode::EINVAL, format!("存储类 {name} 不存在")));
    }
    let off = |kind, alg| c.pipeline.transform(kind, alg).is_none();
    let bad = |what: &str| Err(Failed::new(ErrCode::EINVAL, format!("{what} {} 的插件没有启用", p.norm)));
    match k {
        "checksum" if off(TransformKind::Checksum, checksum_id(&p.norm).unwrap_or(0)) => bad("校验算法"),
        "compress" if compress_id(&p.norm).is_some_and(|(a, _)| a != CMP_NONE && off(TransformKind::Compress, a)) => {
            bad("压缩算法")
        }
        "encrypt" if p.norm != "none" => datakey_of(v, c, &p.norm),
        _ => Ok(p.norm.clone()),
    }
}

/// `encrypt=tenant:<名字>`：加密插件已启用、数据密钥存在（DATAKEY_NAME 点查）、属于设置者的租户（admin 例外）→ 它的数字 id。
fn datakey_of(v: &dyn KvRead, c: &OpCtx, norm: &str) -> Result<String, Failed> {
    let einval = |m: String| Failed::new(ErrCode::EINVAL, m);
    if c.pipeline.transform(TransformKind::Encrypt, ENC_AES256GCM).is_none() {
        return Err(einval("加密插件 aes-gcm 没有启用".into()));
    }
    let name = norm.strip_prefix("tenant:").unwrap_or(norm);
    let hint = format!("数据密钥 {name} 不存在（先 wmfs datakey create {name}）");
    let id = kv::get::<DataKeyIdV1>(v, &keys::datakey_name(name))?.ok_or_else(|| einval(hint))?.id;
    let k = kv::get::<DataKeyV1>(v, &keys::datakey(id))?.ok_or(ErrCode::INTERNAL)?;
    let who = &c.req.who;
    if k.tenant != who.tenant && who.conn.role != Role::Admin {
        return Err(Failed::new(ErrCode::EACCES, format!("数据密钥 {name} 属于租户 {}，不是你的", k.tenant)));
    }
    Ok(id.to_string())
}

/// 一个键的存储形式 → 显示形式：`encrypt` 存的是数据密钥的 id，按集群图显示成 `tenant:<名字>`；其余按注册表的 `show`。
fn shown(c: &OpCtx, key: &str, norm: &str) -> String {
    match (key, norm.parse::<u32>()) {
        ("encrypt", Ok(id)) => format!("tenant:{}", datakey_name(c, id)),
        _ => find(key).map_or(norm.to_string(), |s| (s.show)(norm)),
    }
}

fn datakey_name(c: &OpCtx, id: u32) -> String {
    c.map.datakeys.iter().find(|(i, _)| *i == id).map_or(format!("#{id}"), |(_, k)| k.name.clone())
}

pub fn set(v: &mut dyn KvTxn, c: &OpCtx, path: &str, items: &[Parsed]) -> Result<AttrSetReply, Failed> {
    let d = dir_at(v, c, path)?;
    let mut norms = Vec::new();
    for p in items {
        norms.push(check(v, c, p)?);
    }
    let before = c.attrs.resolve(v, d.ino)?; // 在 attr_epoch 加一之前解析（警告要用）
    let mut m = own_attrs(v, d.ino)?;
    let mut out = AttrSetReply { path: path.into(), ..Default::default() };
    for (p, norm) in items.iter().zip(norms) {
        let (k, new) = (p.key.as_str(), shown(c, p.spec.key, &norm));
        let old = m.entries.insert(k.into(), norm).map(|o| shown(c, p.spec.key, &o)).unwrap_or_default();
        let extra = vec![("key".into(), k.into()), ("old".into(), old), ("new".into(), new.clone())];
        changelog::append(v, c, Change { op: "AttrSet", ino: d.ino, extra, ..Default::default() })?;
        out.set.insert(k.into(), new);
    }
    if items.iter().any(|p| p.spec.effect == Effect::NewDataAndReconcile) {
        queue_reconcile(v, d.ino, 1)?;
    }
    quota::note_set(v, c, d.ino, items)?; // 配额键的软限起点（3.8 变更通知 ⑤）：要在写回之前（它看设置之前的值）
    let audit_lag = match given_norm(items, "audit") {
        Some(a) if a != "off" => ensure_audit_cursor(v, c)?,
        _ => None,
    };
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
    let moved: Vec<&str> =
        items.iter().filter(|p| p.spec.effect == Effect::NewDataAndReconcile).map(|p| p.spec.key).collect();
    if let Some(n) = audit_lag {
        out.warnings.push(format!(
            "审计记录要靠 wmfs mgr 的 audit-log 写进文件，它现在没有在消费（落后 {n} 条；它的游标 10 分钟没有前进）"
        ));
    }
    if !moved.is_empty() {
        out.warnings.push(format!(
            "{} 是\"新数据 + 协调器\"类的属性：新文件立即按新设置；已有文件由协调器在后台转换（进度见 wmfs status、wmfs task list），\
             转换完成前往已有文件里新写的数据仍按旧设置；一直写打开的文件会被跳过（wmfs status 单独计数）",
            moved.join("、")
        ));
    }
    Ok(out)
}

fn given_norm<'a>(items: &'a [Parsed], key: &str) -> Option<&'a str> {
    items.iter().find(|p| p.spec.key == key).map(|p| p.norm.as_str())
}

/// `audit` 设成非 off（3.8 变更通知 ⑥，M12）：`CURSOR(audit-log)` 还不存在 → 同批创建（= 当前最大的 seq：从此 changelog 按它保留），回 None；
/// 已经在、却 [`AUDIT_STALE_MS`] 没有前进、又落后了 → 回落后的条数（提醒：mgr 的 audit-log 没在消费）。
fn ensure_audit_cursor(v: &mut dyn KvTxn, c: &OpCtx) -> Result<Option<u64>, Failed> {
    let last = changelog::last_seq(v)?;
    match changelog::get_cursor(v, AUDIT_CURSOR)? {
        None => changelog::set_cursor(v, c, AUDIT_CURSOR, last).map(|_| None),
        Some(r) if r.updated_ms + AUDIT_STALE_MS < c.req.now_ms && r.seq < last => Ok(Some(last - r.seq)),
        Some(_) => Ok(None),
    }
}

pub fn unset(v: &mut dyn KvTxn, c: &OpCtx, path: &str, key: &str) -> Result<AttrUnsetReply, Failed> {
    let s = spec_of(key)?; // 后面里程碑才有的键：与 set 相同的 EOPNOTSUPP；不认识的：未知属性
    let d = dir_at(v, c, path)?;
    let mut m = own_attrs(v, d.ino)?;
    let not_set = || Failed::new(ErrCode::ENOENT, format!("{path} 上没有设置 {key}"));
    let old = shown(c, s.key, &m.entries.remove(key).ok_or_else(not_set)?);
    if m.entries.is_empty() {
        v.delete(&keys::dirattr(d.ino));
    } else {
        kv::put(v, &keys::dirattr(d.ino), &m);
    }
    let extra = vec![("key".into(), key.into()), ("old".into(), old.clone())];
    changelog::append(v, c, Change { op: "AttrUnset", ino: d.ino, extra, ..Default::default() })?;
    quota::forget(v, d.ino, key)?; // 配额键：软限的起点一并删掉
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
        "class" => c.map.class_name(e.class_id),
        "replicas" => e.replicas.to_string(),
        "chunk" => fmt_size(e.chunk as u64),
        "stripe" => e.stripe.to_string(),
        "inline" => fmt_size(e.inline_max as u64),
        "checksum" => checksum_name(e.checksum).into(),
        "compress" if e.compress == CMP_ZSTD => format!("zstd:{}", e.compress_level),
        "compress" => compress_name(e.compress).into(),
        "encrypt" if e.encrypt_key == 0 => "none".into(),
        "encrypt" => format!("tenant:{}", datakey_name(c, e.encrypt_key)),
        "consistency" if e.consistency == CONSISTENCY_STRONG => "strong".into(),
        "consistency" => format!("cto,attr_ttl={}", ttl(e)),
        "inherit" => own.entries.get(key).cloned().unwrap_or_else(|| "true".into()), // 只看目录自己的
        // Stacked：最近的那个（来源一栏是它的目录）；没有 → none
        "quota" | "quota.*" => {
            let q = e.quotas.iter().find(|(_, q)| q.class.is_some() == (key == "quota.*"));
            q.map_or("none".into(), |(_, q)| quota::decl_text(q, q.class.map(|k| c.map.class_name(k)).as_deref()))
        }
        "acl.default" => {
            e.acl_default.as_deref().and_then(|b| acl::parse(b).ok()).map_or("none".into(), |a| acl::to_text(&a))
        }
        "trash" => fmt_ms(e.trash_ms), // M12：0 显示成 off
        "ttl" => fmt_ms(e.ttl_ms),
        "audit" => audit_text(&e.audit),
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

/// `n` 的生效属性所属的目录（文件 = 它的主父目录）、它的解析结果、它自己的 DIRATTR。
fn dir_attrs_of(v: &dyn KvRead, c: &OpCtx, n: &Node) -> Result<(u64, Arc<EffectiveAttrs>, AttrMapV1), ErrCode> {
    let dir = if n.is_dir() { n.ino } else { n.rec.parent };
    Ok((dir, c.attrs.resolve(v, dir)?, own_attrs(v, dir)?))
}

pub fn get(v: &dyn KvScan, c: &OpCtx, path: &str, recursive: bool) -> Result<AttrGetReply, Failed> {
    let (dir, e, own) = dir_attrs_of(v, c, &node_at(v, c, path)?)?;
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
    let q = quota::semantics(v, &e.quotas)?;
    let cons = if e.consistency == CONSISTENCY_STRONG { "strong".into() } else { format!("cto attr_ttl={}", ttl(&e)) };
    let semantics = format!(
        "consistency={cons} {from}; {q}; durability=replicas:声明{}/实际{actual} fsync:all-replicas; isolation={iso}",
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
        let attrs = m.entries.iter().map(|(k, x)| (k.clone(), shown(c, k, x))).collect();
        out.push(DirAttrs { path: path_of(v, d)?, attrs });
    }
    Ok((out, truncated))
}
