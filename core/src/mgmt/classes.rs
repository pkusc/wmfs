//! M13 的管理端点：存储类（design.md 6.1、6.3、7.1）。TOML 的解析、修改规则、QoS 包络与隔离承诺在 `meta/cluster/classes.rs`。
//!
//! - `GET /api/v1/classes`：全部存储类，带着由实际部署算出的节点数、每节点份额与隔离承诺（`ClassInfo`）。
//! - `POST /api/v1/classes?name=<名字>`（admin，body = TOML）：一个 exclusive 批次（存储类改变集群图，3.4.1.4 规则 5）——名字没被用过、
//!   `[lifecycle] tier_to` 的类存在、`next_class` 取号、写 CLASS 与 CLASS_NAME、`map_epoch += 1`、changelog `ClassCreate`（`extra` =
//!   `name`/`id`）。
//! - `PUT /api/v1/classes/<名字>`（admin，body = 完整的新定义）：6.1 的规则——改 label、dedicated、failure_domain → 拒绝；改 replica 或
//!   [lifecycle] → 同批写 `RECONCILE_Q{dir: 1, reason: 3}`（协调器从根整树协调）；只改 [qos]（与 locality）→ 只下发。都 `map_epoch += 1`、
//!   `attr_epoch += 1`（没设 replicas、tier、locality 的目录的生效值来自存储类，解析缓存要作废，ADR-0038 第 7 条 ⑤）；changelog
//!   `ClassUpdate`（`extra` = `name`/`id`/`reconcile`）。`qos=<这个类>` 是活的借用（ADR-0044 第 2 条）：QoS 包络变了 → 同批把借用它的
//!   目录的 QOS 记录换成新的包络（借用它的目录先在只读阶段扫出来，与删除同一个扫描、同一个 `attr_epoch` CAS）。
//! - `DELETE /api/v1/classes/<名字>`（admin）：只删没用过的类——default 类、已经有链的类（建过文件）、被目录属性提到的（`class=`、
//!   `quota.<类>`、`tier` 的两端、`qos=<类>`）、被别的类的 `[lifecycle] tier_to` 提到的都拒绝（目录属性要扫描：先在只读阶段扫，同时记下
//!   `attr_epoch`；批次里点查其余的，并核对 `attr_epoch` 没变——变了就是期间有人改了目录属性（可能刚设了 `class=<这个类>`），回 `EBUSY`
//!   请重试：3.4.1.4 规则 1 不许在闭包里扫描，所以用它做 CAS，M13 审查）。changelog `ClassDelete`（`extra` = `name`/`id`）。新建也令
//!   `attr_epoch += 1`（删了又建同名的类时，旧的解析缓存作废）。
//! - 声明了 `dedicated = true`、按实际部署算出来却是 shared 的类（新建、修改时）：同批写一条 `PolicyViolation`（`extra` =
//!   `kind=isolation_downgraded`/`class`/`isolation`，6.3）。新建的 dedicated 类占走 target 之后会让别的类一个可用的 target 也没有（例如
//!   label `*`）→ `EINVAL`（M13 审查：不然整个集群的新文件都 `ENOSPC`，类又删不掉、改不了 dedicated）。
//! - qos 插件不执行份额时（fifo，minimal profile：没有插件消费 `qos` 属性）：带 [qos] 份额、上限、权重的定义 → `EINVAL`（与 `qos` 属性同一个
//!   判据，3.8"没有插件消费就拒绝"）；`ClassInfo` 的承诺写"先进先出，不承诺份额与尾延迟"（与 `wmfs get` 的 semantics 行一致）。
//!   label 是 hdd、盘却是 nvme 的类，承诺后面接上"label hdd 位于 nvme 设备上（测试用途）"（ADR-0044 第 7 条）。

use super::api::{failed_at, read, write_exclusive};
use super::http::{Reply, json};
use crate::api::types::{ClassInfo, ClassQos, ClassReply, ClassesReply};
use crate::api::url::param;
use crate::base::types::ReqCtx;
use crate::base::{ErrCode, Failed};
use crate::meta::attr_registry::qos_tier::{locality_name, split_tier};
use crate::meta::attrs::{DEFAULT_CLASS, attr_epoch, bump_attr_epoch, queue_reconcile};
use crate::meta::changelog::{self, Change};
use crate::meta::cluster::classes::{
    self, FIFO_PROMISE, HDD_ON_NVME, Isolation, PRIORITY_LOW, downgraded, envelope, hdd_on_nvme, isolation, nodes,
    starved, with_class,
};
use crate::meta::engine::EmbeddedEngine;
use crate::meta::engine::txn::{KvRead, KvScan, KvTxn, OpCtx};
use crate::meta::ns::read_ops::path_of;
use crate::meta::schema::keys::{self, G_NEXT_CLASS, ROOT_INO, T_DIRATTR};
use crate::meta::schema::kv;
use crate::meta::schema::records::{AttrMapV1, ClassIdV1, StorageClassV1, decode_v1};
use crate::proto::peer::ClusterMap;

/// `RECONCILE_Q.reason` 3 = 存储类变化（3.2.8）。
const REASON_CLASS: u8 = 3;

pub fn handles(path: &str) -> bool {
    path == "/api/v1/classes" || path.starts_with("/api/v1/classes/")
}

fn bad(m: String) -> Failed {
    Failed::new(ErrCode::EINVAL, m)
}

/// leader_gate 之后的分派。
pub(super) fn serve(
    eng: &EmbeddedEngine,
    req: &ReqCtx,
    (method, path, query): (&str, &str, &str),
    body: &[u8],
) -> Reply {
    let name = path.strip_prefix("/api/v1/classes/").unwrap_or_default().to_string();
    let text = || std::str::from_utf8(body).map(str::to_string).map_err(|_| bad("存储类定义不是 UTF-8".into()));
    let r = match method {
        "GET" if name.is_empty() => {
            let all = |_v: &dyn KvScan, c: &OpCtx| {
                Ok(ClassesReply { classes: c.map.classes.iter().map(|k| info(c, c.map, k)).collect() })
            };
            read(eng, req, all).map(|x| json(200, &x))
        }
        "POST" if name.is_empty() => {
            text().and_then(|t| create(eng, req, param(query, "name"), t)).map(|x| json(200, &x))
        }
        "PUT" if !name.is_empty() => text().and_then(|t| update(eng, req, name, t)).map(|x| json(200, &x)),
        "DELETE" if !name.is_empty() => delete(eng, req, name).map(|x| json(200, &x)),
        _ => Err(bad(format!("{path} 不支持 {method}"))),
    };
    r.unwrap_or_else(|f| failed_at(eng, f))
}

/// 管理 API 里的样子（`m` 里要有 `k` 自己；`c.pipeline` 看 qos 插件执不执行份额）。`per_node` 的各项向下取整（给人看的；数据节点执行与
/// `wmfs status` 显示的是小数，`classes::per_node`）。
fn info(c: &OpCtx, m: &ClusterMap, k: &StorageClassV1) -> ClassInfo {
    let n = nodes(m, k);
    let qos = ClassQos {
        iops_min: k.iops_min,
        iops_max: k.iops_max,
        bw_min: k.bw_min,
        bw_max: k.bw_max,
        burst_ms: k.burst_ms,
        weight: k.weight,
    };
    let per_node = ClassQos {
        iops_min: k.iops_min / n,
        iops_max: k.iops_max / n,
        bw_min: k.bw_min / n as u64,
        bw_max: k.bw_max / n as u64,
        ..qos.clone()
    };
    let qos_on = c.pipeline.consumes("qos"); // fifo：什么份额也不执行，与 wmfs get 的 semantics 行一样说（见文件头）
    let iso = if qos_on { isolation(m, k) } else { Isolation::Shared };
    let mut promise = if qos_on { iso.promise() } else { FIFO_PROMISE }.to_string();
    if hdd_on_nvme(m, k) {
        promise += &format!("；{HDD_ON_NVME}"); // ADR-0044 第 7 条
    }
    ClassInfo {
        id: k.id,
        name: k.name.clone(),
        label: k.label.clone(),
        dedicated: k.dedicated,
        replica: k.replica,
        priority: if k.priority == PRIORITY_LOW { "low" } else { "normal" }.into(),
        locality: locality_name(k.locality).into(),
        latency_target_us: k.latency_target_us,
        tier_to: k.tier_to.clone(),
        tier_when: k.tier_when.clone(),
        qos,
        nodes: n,
        per_node,
        isolation: iso.name().into(),
        promise,
        downgraded: downgraded(m, k),
    }
}

fn class_id(v: &dyn KvRead, name: &str) -> Result<Option<u16>, ErrCode> {
    Ok(kv::get::<ClassIdV1>(v, &keys::class_name(name))?.map(|c| c.id))
}

/// 新建、修改的公共部分：① `[lifecycle] tier_to` 的类要存在；② qos 插件不执行份额时不收 [qos] 的份额（见文件头）；③ 写 CLASS、
/// `map_epoch += 1`；④ changelog（`op`）；⑤ 声明了 dedicated、实际做不到 → 同批写 `PolicyViolation`（isolation_downgraded）。回给调用者的样子。
fn save_class(
    v: &mut dyn KvTxn,
    c: &OpCtx,
    k: &StorageClassV1,
    op: &'static str,
    extra: Vec<(String, String)>,
) -> Result<ClassInfo, Failed> {
    if let Some(to) = &k.tier_to
        && class_id(v, to)?.is_none()
    {
        return Err(bad(format!("[lifecycle] tier_to 的存储类 {to} 不存在")));
    }
    let shares = k.iops_min > 0 || k.iops_max > 0 || k.bw_min > 0 || k.bw_max > 0 || k.weight != 1;
    if shares && !c.pipeline.consumes("qos") {
        return Err(bad(
            "[qos] 的份额、上限与权重要 mclock 执行，这个集群的 qos 插件是 fifo（minimal profile）：去掉它们".into(),
        ));
    }
    kv::put(v, &keys::class(k.id), k);
    kv::bump_map_epoch(v)?; // 存储类在集群图里（3.3.6）
    changelog::append(v, c, Change { op, extra, ..Default::default() })?;
    let k = info(c, &with_class(c.map, k), k);
    if k.downgraded {
        let extra = vec![
            ("kind".into(), "isolation_downgraded".into()),
            ("class".into(), k.name.clone()),
            ("isolation".into(), k.isolation.clone()),
        ];
        changelog::append(v, c, Change { op: "PolicyViolation", extra, ..Default::default() })?;
    }
    Ok(k)
}

fn create(eng: &EmbeddedEngine, req: &ReqCtx, name: Option<String>, text: String) -> Result<ClassReply, Failed> {
    let k = classes::parse(&text, name.as_deref())?;
    write_exclusive(eng, req, move |v, c| {
        if class_id(v, &k.name)?.is_some() {
            return Err(Failed::new(
                ErrCode::EEXIST,
                format!("存储类 {} 已经存在（改它用 wmfs class update）", k.name),
            ));
        }
        let id = u16::try_from(kv::next_id(v, G_NEXT_CLASS)?).map_err(|_| ErrCode::ENOSPC)?;
        let k = StorageClassV1 { id, ..k.clone() };
        if let Some(other) = starved(c.map, &k) {
            return Err(bad(format!(
                "dedicated 的存储类 {} 会占走 label {} 的全部 target，存储类 {other} 就一个可用的 target 也没有了（新文件 ENOSPC）：\
                 请给要独占的 target 打单独的标签",
                k.name, k.label
            )));
        }
        kv::put(v, &keys::class_name(&k.name), &ClassIdV1 { id });
        let extra = vec![("name".into(), k.name.clone()), ("id".into(), id.to_string())];
        let class = save_class(v, c, &k, "ClassCreate", extra)?;
        bump_attr_epoch(v)?; // 最后一步（见文件头）
        Ok(ClassReply { class, reconcile: false })
    })
}

fn update(eng: &EmbeddedEngine, req: &ReqCtx, name: String, text: String) -> Result<ClassReply, Failed> {
    let k = classes::parse(&text, Some(&name))?;
    let (_, borrowers, epoch) = mentions(eng, req, &name)?;
    write_exclusive(eng, req, move |v, c| {
        let id = class_id(v, &name)?.ok_or_else(|| Failed::new(ErrCode::ENOENT, format!("没有存储类 {name}")))?;
        let old = kv::get::<StorageClassV1>(v, &keys::class(id))?.ok_or(ErrCode::INTERNAL)?;
        let reconcile = classes::changes(&old, &k)?;
        let k = StorageClassV1 { id, ..k.clone() };
        let q = envelope(&k);
        if q != envelope(&old) {
            unchanged(v, epoch, "update")?; // 借用它的目录还是扫到的那些（ADR-0044 第 2 条）
            for d in &borrowers {
                kv::put(v, &keys::qos(*d), &q); // 活的借用：放新的包络（map_epoch 由 save_class 加一）
            }
        }
        if reconcile {
            queue_reconcile(v, ROOT_INO, REASON_CLASS)?; // 从根整树协调（6.1；协调器消费，3.10）
        }
        let extra = vec![
            ("name".into(), name.clone()),
            ("id".into(), id.to_string()),
            ("reconcile".into(), reconcile.to_string()),
        ];
        let class = save_class(v, c, &k, "ClassUpdate", extra)?;
        bump_attr_epoch(v)?; // 最后一步：之后这个闭包里不再解析属性（attrs.rs）
        Ok(ClassReply { class, reconcile })
    })
}

/// (提到这个类的第一个目录的路径, 用 `qos=` 借用它的目录, 扫描时的 `attr_epoch`)。
type Mentions = (Option<String>, Vec<u64>, u64);

/// 只读阶段扫一遍 DIRATTR（3.4.1.4 规则 1 不许在批次里扫描；删除与修改共用，ADR-0044 第 2 条）：提到存储类 `name` 的目录（`class=`、
/// `quota.<类>`、`tier` 的两端、`qos=<类>`）与借用它的 qos 的目录，同时记下 `attr_epoch`（批次里用 [`unchanged`] 核对）。
fn mentions(eng: &EmbeddedEngine, req: &ReqCtx, name: &str) -> Result<Mentions, Failed> {
    let name = name.to_string();
    read(eng, req, move |v, _c| {
        let (mut first, mut borrowers) = (None, Vec::new());
        for (key, b) in kv::scan_tag(v, T_DIRATTR)? {
            let (d, m) = (kv::key_id(&key), decode_v1::<AttrMapV1>(&b)?);
            let tier =
                m.entries.get("tier").and_then(|t| split_tier(t)).is_some_and(|(a, b, _)| a == name || b == name);
            let is = |k: &str| m.entries.get(k).is_some_and(|x| *x == name);
            if is("qos") {
                borrowers.push(d);
            }
            if tier || is("class") || is("qos") || m.entries.contains_key(&format!("quota.{name}")) {
                first = first.or(Some(d));
            }
        }
        let first = first.map(|d| path_of(v, d).unwrap_or(format!("inode {d}")));
        Ok((first, borrowers, attr_epoch(v)?))
    })
}

/// 批次里核对只读阶段之后目录属性没变（每次 set/unset、目录 rename、删有 DIRATTR 的目录都令 `attr_epoch` 加一）：变了 → `EBUSY` 请重试。
fn unchanged(v: &dyn KvRead, epoch: u64, cmd: &str) -> Result<(), Failed> {
    if attr_epoch(v)? != epoch {
        return Err(Failed::new(ErrCode::EBUSY, format!("wmfs class {cmd} 的同时有目录属性被修改了：请重试")));
    }
    Ok(())
}

fn delete(eng: &EmbeddedEngine, req: &ReqCtx, name: String) -> Result<ClassReply, Failed> {
    let (dir, _, epoch) = mentions(eng, req, &name)?;
    if let Some(dir) = dir {
        return Err(Failed::new(ErrCode::EBUSY, format!("目录 {dir} 的属性还提到存储类 {name}：先改掉它")));
    }
    write_exclusive(eng, req, move |v, c| {
        unchanged(v, epoch, "delete")?;
        let id = class_id(v, &name)?.ok_or_else(|| Failed::new(ErrCode::ENOENT, format!("没有存储类 {name}")))?;
        let k = kv::get::<StorageClassV1>(v, &keys::class(id))?.ok_or(ErrCode::INTERNAL)?;
        let busy = |why: &str| Err(Failed::new(ErrCode::EBUSY, format!("存储类 {name} {why}，首发不能删除")));
        if name == DEFAULT_CLASS {
            return busy("是内置的 default 类");
        }
        if c.map.chains.iter().any(|(_, ch)| ch.class_id == id) {
            return busy("已经用过（有它的数据链）");
        }
        if c.map.classes.iter().any(|o| o.tier_to.as_deref() == Some(name.as_str())) {
            return busy("是别的类 [lifecycle] tier_to 的目标");
        }
        let class = info(c, c.map, &k);
        v.delete(&keys::class(id));
        v.delete(&keys::class_name(&name));
        kv::bump_map_epoch(v)?;
        let extra = vec![("name".into(), name.clone()), ("id".into(), id.to_string())];
        changelog::append(v, c, Change { op: "ClassDelete", extra, ..Default::default() })?;
        bump_attr_epoch(v)?;
        Ok(ClassReply { class, reconcile: false })
    })
}
