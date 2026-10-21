//! 字节 ↔ 小写 hex：cluster_id、密钥文件、管理 API 签名都用它（全仓库只此一份）。

/// 字节 → 小写 hex。
pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// hex → 字节，写进 `out`。长度必须正好是 `2 × out.len()`、全是 ASCII hex 字符（大小写都行），否则返回 false。
/// 输入可能来自网络或被手工改坏的文件：任何输入都只返回 false，不 panic。
pub fn unhex(s: &str, out: &mut [u8]) -> bool {
    if s.len() != out.len() * 2 || !s.bytes().all(|c| c.is_ascii_hexdigit()) {
        return false;
    }
    for (i, b) in out.iter_mut().enumerate() {
        // 上面已确认全是 ASCII hex 字符，按字节下标切分不会落在字符中间
        let Ok(v) = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16) else { return false };
        *b = v;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_garbage() {
        assert_eq!(hex(&[0x00, 0xab, 0x5f]), "00ab5f");
        let mut b = [0u8; 3];
        assert!(unhex("00AB5f", &mut b));
        assert_eq!(b, [0x00, 0xab, 0x5f]);
        for bad in ["", "00ab5", "00ab5f00", "00ab5g", "+0ab5f", "é0ab5"] {
            assert!(!unhex(bad, &mut b), "应拒绝：{bad:?}");
        }
    }
}
