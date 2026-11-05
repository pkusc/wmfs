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
    assert_eq!(err("consistency=strong").0, ErrCode::EOPNOTSUPP, "strong 在 M12");
    for (text, code) in [
        ("chunk=1M,compress=zstd:3", ErrCode::EOPNOTSUPP),
        ("qos=r=100,w=10", ErrCode::EOPNOTSUPP),
        ("ec=4+2", ErrCode::EOPNOTSUPP),
        ("class=wasm:policy", ErrCode::FEATURE_UNSUPPORTED),
    ] {
        assert_eq!(err(text).0, code, "{text}");
    }
    assert!(err("snapshot=on").1.contains("二期"));
    assert!(err("trash=7d").1.contains("M12"));
    // 单独给出的键（wmfs unset）与 set 的说法相同
    assert_eq!(spec_of("chunk").map(|s| s.key).ok(), Some("chunk"));
    let f = spec_of("compress").unwrap_err();
    assert!(f.code == ErrCode::EOPNOTSUPP && f.msg.contains("M11"), "{f:?}");
    assert_eq!(spec_of("quota.ssd").map(|s| s.key).ok(), Some("quota.*"), "M10：quota.<类> 按前缀匹配");
    assert_eq!(spec_of("ec").unwrap_err().code, ErrCode::EOPNOTSUPP);
    let f = spec_of("chunck").unwrap_err();
    assert_eq!((f.code, f.msg.as_str()), (ErrCode::EINVAL, "未知属性 chunck"));
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
    assert!(
        check_manifest_attrs("label-match", &["class", "replicas", "stripe", "locality"]).is_ok(),
        "locality 在 M13"
    );
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
