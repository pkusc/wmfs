//! 属性注册表的单元测试：服务端切分（3.8 的规则）、各键的解析与规范化、显示、还不能设置的键、manifest 对账。

use super::*;

fn ok(text: &str) -> Vec<(&'static str, String)> {
    split(text).unwrap_or_else(|e| panic!("{text}：{e:?}")).into_iter().map(|p| (p.spec.key, p.norm)).collect()
}

fn err(text: &str) -> (ErrCode, String) {
    let f = split(text).expect_err(text);
    (f.code, f.msg)
}

#[test]
fn split_follows_the_rules_of_3_8() {
    assert_eq!(ok("chunk=1M,stripe=2"), vec![("chunk", "1048576".into()), ("stripe", "2".into())]);
    assert_eq!(ok(" consistency = cto , attr_ttl = 3s "), vec![("consistency", "cto,attr_ttl=3000".into())]);
    assert_eq!(ok("consistency=cto"), vec![("consistency", "cto".into())], "没写 attr_ttl：默认 1 s");
    assert_eq!(ok("replicas=3,class=default,inherit=false,inline=0,checksum=crc32c").len(), 5);
    let (c, m) = err("chunck=4M");
    assert_eq!((c, m.as_str()), (ErrCode::EINVAL, "未知属性 chunck"), "拼错的键");
    assert_eq!(err("attr_ttl=3s").1, "未知属性 attr_ttl", "子键只能跟在它的主键后面");
    assert_eq!(err("chunk=1M,attr_ttl=3s").1, "未知属性 attr_ttl", "attr_ttl 不是 chunk 的子键");
    assert!(err("chunk=1M,chunk=2M").1.contains("写了两次"));
    assert!(err("chunk=1M,").1.contains("空的片段"));
    assert!(err("").1.contains("空的"));
    assert!(err("stripe").1.contains("没有给值"));
    assert!(err("chunk=1M,bogus").1.contains("未知属性 bogus"));
}

#[test]
fn values_are_validated_and_normalized() {
    for bad in ["chunk=3M", "chunk=32K", "chunk=32M", "chunk=x", "inline=65537", "stripe=0", "stripe=65", "replicas=0"]
    {
        let (c, m) = err(bad);
        assert_eq!(c, ErrCode::EINVAL, "{bad}");
        assert!(m.starts_with(bad), "原因里带上写错的那一段：{m}");
    }
    for bad in ["replicas=9", "class=Gold", "class=", "checksum=md5", "consistency=x", "inherit=true"] {
        assert_eq!(err(bad).0, ErrCode::EINVAL, "{bad}");
    }
    assert_eq!(err("consistency=cto,attr_ttl=11s").0, ErrCode::EINVAL, "attr_ttl 0–10 s");
    assert_eq!(err("consistency=cto,attr_ttl=1s,attr_ttl=2s").0, ErrCode::EINVAL);
    assert_eq!(ok("chunk=64K,inline=64K"), vec![("chunk", "65536".into()), ("inline", "65536".into())]);
    assert_eq!(ok("chunk=16M")[0].1, "16777216");
    assert_eq!(ok("consistency=cto,attr_ttl=0s")[0].1, "cto,attr_ttl=0");
}

#[test]
fn keys_that_cannot_be_set_yet_are_rejected_explicitly() {
    for (text, code) in [
        ("ec=4+2", ErrCode::EOPNOTSUPP),
        ("chunk=1M,snapshot=on", ErrCode::EOPNOTSUPP),
        ("class=wasm:policy", ErrCode::FEATURE_UNSUPPORTED),
    ] {
        assert_eq!(err(text).0, code, "{text}");
    }
    assert!(err("snapshot=on").1.contains("二期"));
    // 单独给出的键（wmfs unset）与 set 的说法相同
    assert_eq!(spec_of("chunk").map(|s| s.key).ok(), Some("chunk"));
    assert_eq!(spec_of("qos").map(|s| s.key).ok(), Some("qos"), "M13");
    assert_eq!(spec_of("quota.ssd").map(|s| s.key).ok(), Some("quota.*"), "M10：quota.<类> 按前缀匹配");
    assert_eq!(spec_of("ec").unwrap_err().code, ErrCode::EOPNOTSUPP);
    let f = spec_of("chunck").unwrap_err();
    assert_eq!((f.code, f.msg.as_str()), (ErrCode::EINVAL, "未知属性 chunck"));
}

/// M13：qos、tier、locality 的切分、校验与 DIRATTR 里存的形式（只增的持久取值：qos 五项都写，或 `qos=<类>` 存类名原样——活的借用，
/// QOS 表里放那个类此刻的包络，ADR-0044 第 2 条；tier 存原样、`→` 换成 `->`）。
#[test]
fn qos_tier_and_locality_are_parsed() {
    assert_eq!(ok("qos=r=0,w=1,l=3000"), vec![("qos", "r=0,w=1,l=3000,bw=0,burst=0s".into())]);
    assert_eq!(ok("qos=l=300,bw=4M,burst=30s"), vec![("qos", "r=0,w=1,l=300,bw=4194304,burst=30s".into())]);
    assert_eq!(ok("qos=gold"), vec![("qos", "gold".into())], "借用存储类的包络：存类名");
    assert_eq!(
        (qos_tier::qos_class("gold"), qos_tier::qos_spec("gold"), show_qos("gold")),
        (Some("gold"), None, "gold".into())
    );
    assert_eq!(qos_tier::qos_class("r=0,w=1,l=3000,bw=0,burst=0s"), None, "数值的形式不是类名");
    assert_eq!(show_qos("r=0,w=1,l=300,bw=4194304,burst=30s"), "r=0,w=1,l=300,bw=4M,burst=30s");
    let q = qos_tier::qos_spec("r=1,w=2,l=3,bw=4,burst=5ms").unwrap();
    assert_eq!((q.r, q.w, q.l, q.bw_max, q.burst_ms), (1, 2, 3, 4, 5), "存的形式解回数值");
    for bad in ["qos=r=10,l=5", "qos=w=0", "qos=x=1", "qos=r=1,r=2", "qos=Gold", "qos=l=-1"] {
        assert_eq!(err(bad).0, ErrCode::EINVAL, "{bad}");
    }
    assert_eq!(ok("tier=gold->silver:idle>30d"), vec![("tier", "gold->silver:idle>30d".into())]);
    assert_eq!(ok("tier=gold→silver:age>2s&size>1M"), vec![("tier", "gold->silver:age>2s&size>1M".into())]);
    for bad in ["tier=gold", "tier=gold->gold:idle>1d", "tier=a->b:hot>1d", "tier=a->b:idle>x", "tier=A->b:size>1"] {
        assert_eq!(err(bad).0, ErrCode::EINVAL, "{bad}");
    }
    assert_eq!(ok("locality=prefer-client"), vec![("locality", "prefer-client".into())]);
    assert_eq!(err("locality=near").0, ErrCode::EINVAL);
    assert_eq!(find("tier").unwrap().effect.name(), "协调器");
    let f = |s: u64, a: i64, m: i64, b: i64| crate::hooks::policy::FileFacts {
        ino: 1,
        size: s,
        atime: crate::base::types::Ts { secs: a, nanos: 0 },
        mtime: crate::base::types::Ts { secs: m, nanos: 0 },
        btime: crate::base::types::Ts { secs: b, nanos: 0 },
        class_id: 1,
        uid: 0,
    };
    let now = 100_000; // 第 100 秒（毫秒）
    assert!(qos_tier::holds("idle>30s", &f(0, 60, 50, 0), now), "最后一次读写在第 60 秒：闲了 40 s");
    assert!(!qos_tier::holds("idle>30s", &f(0, 50, 80, 0), now), "mtime 在第 80 秒：只闲了 20 s");
    assert!(qos_tier::holds("age>90s&size>1K", &f(2048, 99, 99, 5), now));
    assert!(!qos_tier::holds("age>90s&size>1K", &f(1024, 99, 99, 5), now), "size 要大于");
    assert!(!qos_tier::holds("bogus", &f(1, 0, 0, 0), now), "解不开 = 不成立");
}

/// M11：compress 规范化成 none / lz4 / zstd:<级别>（只写 zstd = 级别 3）；encrypt 先原样留着名字（设置时换成数据密钥的 id）。
#[test]
fn compress_and_encrypt_are_parsed() {
    assert_eq!(ok("compress=zstd"), vec![("compress", "zstd:3".into())]);
    assert_eq!(ok("compress=zstd:19")[0].1, "zstd:19");
    assert_eq!(ok("compress=lz4")[0].1, "lz4");
    assert_eq!(ok("compress=none")[0].1, "none");
    for bad in ["compress=zstd:0", "compress=zstd:20", "compress=gzip", "compress=lz4:1"] {
        assert_eq!(err(bad).0, ErrCode::EINVAL, "{bad}");
    }
    assert_eq!(ok("encrypt=tenant:k1"), vec![("encrypt", "tenant:k1".into())]);
    assert_eq!(ok("encrypt=none")[0].1, "none");
    for bad in ["encrypt=k1", "encrypt=tenant:", "encrypt=tenant:a b"] {
        assert_eq!(err(bad).0, ErrCode::EINVAL, "{bad}");
    }
    let show = |v: &str| (find("encrypt").unwrap().show)(v);
    assert_eq!((show("none"), show("3")), ("none".into(), "tenant:#3".into()), "没有集群图时显示 id");
}

#[test]
fn show_turns_the_stored_form_back() {
    let show = |k: &str, v: &str| (find(k).unwrap().show)(v);
    assert_eq!(show("chunk", "1048576"), "1M");
    assert_eq!(show("inline", "0"), "0");
    assert_eq!(show("inline", "65536"), "64K");
    assert_eq!(show("consistency", "cto,attr_ttl=3000"), "cto,attr_ttl=3s");
    assert_eq!(show("consistency", "cto,attr_ttl=0"), "cto,attr_ttl=0s");
    assert_eq!(show("consistency", "cto"), "cto");
    assert_eq!(attr_ttl_ms("cto,attr_ttl=1500"), Some(1500));
    assert_eq!(find("inherit").unwrap().kind, AttrKind::Local);
    assert_eq!(find("class").unwrap().effect.name(), "新文件 + 协调器");
}

#[test]
fn plugin_manifests_must_name_known_attributes() {
    assert!(check_manifest_attrs("label-match", &["class", "replicas", "stripe", "locality"]).is_ok());
    assert!(check_manifest_attrs("mclock", &["qos", "class"]).is_ok());
    assert!(check_manifest_attrs("crc32c", &["checksum"]).is_ok());
    let e = check_manifest_attrs("x", &["chunck"]).unwrap_err();
    assert!(e.contains("chunck"), "{e}");
}

/// M10：配额与 acl.default（3.8、第 5 章）。
#[test]
fn quota_and_acl_default_are_split_parsed_and_shown() {
    let p = split("quota=100M,soft=80M,grace=1m,files=1000,quantum=1M,replicas=2").unwrap();
    assert_eq!(
        (p[0].key.as_str(), p[0].norm.as_str()),
        ("quota", "104857600,soft=83886080,grace=60000,files=1000,quantum=1048576")
    );
    assert_eq!((p[1].key.as_str(), p[1].spec.kind), ("replicas", AttrKind::Inherit), "子键之后照样能开始新属性");
    assert_eq!((find("quota").unwrap().show)(&p[0].norm), "100M,soft=80M,grace=1m,files=1000,quantum=1M");
    let c = split("quota.ssd=1G").unwrap();
    assert_eq!((c[0].key.as_str(), c[0].spec.key, c[0].spec.kind), ("quota.ssd", "quota.*", AttrKind::Stacked));
    assert_eq!(ok("quota=off")[0].1, "off");
    for bad in [
        "quota=0",
        "quota=1G,soft=2G",
        "quota=1G,grace=1d",
        "quota=1G,quantum=512K",
        "quota=1G,quantum=2G",
        "quota=off,soft=1M",
    ] {
        assert_eq!(err(bad).0, ErrCode::EINVAL, "{bad}");
    }
    assert!(err("quota=1G,soft=1M,soft=2M").1.contains("两次"));
    let a = split("acl.default=u::rwx,g::r-x,o::---,u:1001:rwx,m::rwx").unwrap();
    assert_eq!(a[0].norm, "u::rwx,u:1001:rwx,g::r-x,m::rwx,o::---", "takes_rest：值里的逗号并进来，存规范的写法");
    assert_eq!(err("acl.default=u::rwx").0, ErrCode::EINVAL);
    assert!(
        check_manifest_attrs("quota", &["quota", "quota.*"]).is_ok()
            && check_manifest_attrs("posix-acl", &["acl.default"]).is_ok()
    );
}

/// M12：consistency=strong、trash、ttl、audit 可以设置了（规范化形式见 `attr_registry/policy.rs`）。
#[test]
fn strong_trash_ttl_and_audit_are_parsed() {
    assert_eq!(ok("consistency=strong"), vec![("consistency", "strong".into())]);
    assert_eq!(err("consistency=strong,attr_ttl=1s").0, ErrCode::EINVAL, "strong 不缓存属性");
    assert_eq!(ok("trash=7d,ttl=90s"), vec![("trash", "604800000".into()), ("ttl", "90000".into())]);
    assert_eq!(ok("trash=off")[0].1, "off");
    for bad in ["trash=366d", "trash=0s", "ttl=0s", "ttl=x", "trash=7d,soft=1"] {
        assert_eq!(err(bad).0, ErrCode::EINVAL, "{bad}");
    }
    assert_eq!((trash_ttl_audit::show_ms("604800000"), trash_ttl_audit::show_ms("off")), ("7d".into(), "off".into()));
    assert_eq!(ok("audit=on")[0].1, "on");
    assert_eq!(ok("audit=off")[0].1, "off");
    let a = ok("audit=on,ops=create+UNLINK,uids=1000+1001");
    assert_eq!(a, vec![("audit", "on,ops=Create+Unlink,uids=1000+1001".into())], "操作名按表里的写法存");
    assert_eq!(ok("audit=ops=setattr,uids=7")[0].1, "on,ops=SetAttr,uids=7", "过滤直接写成值 = on 加过滤");
    for bad in [
        "audit=maybe",
        "audit=on,ops=bogus",
        "audit=on,uids=x",
        "audit=off,ops=create",
        "audit=on,ops=create,ops=unlink",
    ] {
        assert_eq!(err(bad).0, ErrCode::EINVAL, "{bad}");
    }
    let spec = trash_ttl_audit::audit_spec("on,ops=Create+Unlink,uids=1000").unwrap();
    assert_eq!((spec.on, spec.ops, spec.uids), (true, vec![3, 8], vec![1000]));
    assert_eq!(trash_ttl_audit::audit_text(&trash_ttl_audit::audit_spec("on").unwrap()), "on");
    assert_eq!(trash_ttl_audit::audit_spec("off"), Some(Default::default()));
}
