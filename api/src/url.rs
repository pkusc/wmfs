//! 管理 API 的查询参数（design.md 7.1）：文件系统内的路径这类参数按百分号编码放进 `?path=…`。签名覆盖原样的
//! `path_and_query`（`auth.rs`），所以客户端编码、服务端解码都用这里的同一对函数。
//!
//! 规则（实现注记（M7），规格没写）：`A–Z a–z 0–9 - . _ ~ /` 原样，其余字节一律 `%XX`（大写 hex）；解码只认 `%XX`（`+` 原样，
//! 不当空格），坏的编码或解出来不是 UTF-8 → `None`。

/// 编码一个参数值。
pub fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~/".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// 查询串 `query`（`?` 之后的部分）里参数 `name` 的值（解码后）；没有这个参数或编码坏了 → `None`。
pub fn param(query: &str, name: &str) -> Option<String> {
    let raw = query.split('&').find_map(|kv| kv.strip_prefix(name)?.strip_prefix('='))?;
    let b = raw.as_bytes();
    let (mut out, mut i) = (Vec::with_capacity(b.len()), 0);
    while i < b.len() {
        if b[i] == b'%' {
            let h = raw.get(i + 1..i + 3).filter(|h| h.bytes().all(|x| x.is_ascii_hexdigit()))?; // from_str_radix 还认前导 +
            out.push(u8::from_str_radix(h, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_bad_input() {
        for s in ["/proj/a b", "/中文/x&y=z", "/%41", "", "/a+b"] {
            let q = format!("x=1&path={}&k=v", encode(s));
            assert_eq!(param(&q, "path").as_deref(), Some(s), "{q}");
        }
        assert_eq!(encode("/a b&c"), "/a%20b%26c");
        assert_eq!(param("path=%zz", "path"), None, "坏的编码");
        assert_eq!(param("path=%4", "path"), None, "截断的编码");
        assert_eq!(param("path=%+4", "path"), None, "只认两位 hex（from_str_radix 会把 +4 当成 4）");
        assert_eq!(param("path=%FF", "path"), None, "不是 UTF-8");
        assert_eq!(param("p=1", "path"), None);
        assert_eq!(param("pathx=1&path=2", "path").as_deref(), Some("2"), "按完整的参数名找");
    }
}
