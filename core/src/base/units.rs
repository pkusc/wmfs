//! 尺寸与时长的解析和显示（design.md 第 5 章："尺寸单位二进制（K = 1024）；时长 s/m/h/d"）。
//!
//! 属性、node.toml、命令行参数都用这里的函数：`4M`、`64K`、`1G`、`16MiB` → 字节数；
//! `30s`、`500ms`、`7d`、`500us` → 时长（不带单位的整数按秒）。显示时取能整除的最大单位，
//! 例如 4194304 → `4M`（目录属性的值存规范化后的数字，显示时再换回来）。

use std::time::Duration;

const SIZE_UNITS: [(char, u32); 5] = [('K', 10), ('M', 20), ('G', 30), ('T', 40), ('P', 50)];

/// 解析尺寸：整数 + 可选单位（K/M/G/T/P，可带 `i`/`iB`/`B`，大小写不敏感；单独的 `B` 表示字节）。
pub fn parse_size(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let digits = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    if digits == 0 {
        return Err(format!("无法解析尺寸 {s:?}：应以数字开头，例如 4M、64K、1G"));
    }
    let n: u64 = t[..digits].parse().map_err(|_| format!("尺寸 {s:?} 的数字太大"))?;
    let unit = t[digits..].trim().to_ascii_uppercase();
    let shift = match unit.as_str() {
        "" | "B" => 0,
        u => {
            let letter = u.chars().next().unwrap_or(' ');
            let rest = &u[letter.len_utf8()..];
            let sh = SIZE_UNITS.iter().find(|(c, _)| *c == letter).map(|(_, sh)| *sh);
            match (sh, rest) {
                (Some(sh), "" | "I" | "IB" | "B") => sh,
                _ => {
                    return Err(format!("无法解析尺寸 {s:?}：不认识的单位 {unit}（可用 K、M、G、T、P）"));
                }
            }
        }
    };
    n.checked_mul(1u64 << shift).ok_or_else(|| format!("尺寸 {s:?} 超出范围"))
}

/// 显示尺寸：取能整除的最大单位（0 → "0"，1536 → "1536"，67108864 → "64M"）。
pub fn fmt_size(n: u64) -> String {
    for (c, sh) in SIZE_UNITS.iter().rev() {
        let u = 1u64 << sh;
        if n != 0 && n.is_multiple_of(u) {
            return format!("{}{}", n / u, c);
        }
    }
    n.to_string()
}

const DUR_UNITS: [(&str, u64); 6] =
    [("d", 86_400_000_000), ("h", 3_600_000_000), ("m", 60_000_000), ("s", 1_000_000), ("ms", 1_000), ("us", 1)];

/// 解析时长：整数 + 单位（us、ms、s、m、h、d）；不带单位的整数按秒。
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let t = s.trim();
    let digits = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    if digits == 0 {
        return Err(format!("无法解析时长 {s:?}：应以数字开头，例如 30s、500ms、7d"));
    }
    let n: u64 = t[..digits].parse().map_err(|_| format!("时长 {s:?} 的数字太大"))?;
    let unit = t[digits..].trim();
    let us_per = if unit.is_empty() {
        1_000_000
    } else {
        DUR_UNITS
            .iter()
            .find(|(u, _)| *u == unit)
            .map(|(_, f)| *f)
            .ok_or_else(|| format!("无法解析时长 {s:?}：不认识的单位 {unit}（可用 us、ms、s、m、h、d）"))?
    };
    n.checked_mul(us_per).map(Duration::from_micros).ok_or_else(|| format!("时长 {s:?} 超出范围"))
}

/// 显示时长：取能整除的最大单位（0 → "0s"，1500 ms → "1500ms"，7 天 → "7d"）。
pub fn fmt_duration(d: Duration) -> String {
    let us = d.as_micros() as u64;
    if us == 0 {
        return "0s".to_string();
    }
    for (u, f) in DUR_UNITS {
        if us.is_multiple_of(f) {
            return format!("{}{}", us / f, u);
        }
    }
    format!("{us}us")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes() {
        assert_eq!(parse_size("4M"), Ok(4 << 20));
        assert_eq!(parse_size("64K"), Ok(65536));
        assert_eq!(parse_size("1G"), Ok(1 << 30));
        assert_eq!(parse_size("16MiB"), Ok(16 << 20));
        assert_eq!(parse_size("2kb"), Ok(2048), "单位一律二进制、大小写不敏感");
        assert_eq!(parse_size("512"), Ok(512));
        assert_eq!(parse_size(" 512B "), Ok(512));
        for bad in ["", "M", "4X", "4MX", "-1K", "99999999999999999999", "16E"] {
            assert!(parse_size(bad).is_err(), "应拒绝 {bad:?}");
        }
        assert!(parse_size("20000000P").is_err(), "溢出");
    }

    #[test]
    fn size_display() {
        assert_eq!(fmt_size(0), "0");
        assert_eq!(fmt_size(1536), "1536");
        assert_eq!(fmt_size(4 << 20), "4M");
        assert_eq!(fmt_size(64 << 20), "64M");
        assert_eq!(fmt_size(3 << 30), "3G");
        for n in [1u64, 1023, 4096, 4 << 20, 7 << 40] {
            assert_eq!(parse_size(&fmt_size(n)), Ok(n));
        }
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("30s"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse_duration("500us"), Ok(Duration::from_micros(500)));
        assert_eq!(parse_duration("7d"), Ok(Duration::from_secs(7 * 86400)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(parse_duration("1h"), Ok(Duration::from_secs(3600)));
        assert_eq!(parse_duration("120"), Ok(Duration::from_secs(120)), "不带单位按秒");
        assert_eq!(parse_duration("0s"), Ok(Duration::ZERO));
        for bad in ["", "s", "5x", "5 sec", "1.5s"] {
            assert!(parse_duration(bad).is_err(), "应拒绝 {bad:?}");
        }
    }

    #[test]
    fn duration_display() {
        assert_eq!(fmt_duration(Duration::ZERO), "0s");
        assert_eq!(fmt_duration(Duration::from_secs(1)), "1s");
        assert_eq!(fmt_duration(Duration::from_millis(1500)), "1500ms");
        assert_eq!(fmt_duration(Duration::from_secs(7 * 86400)), "7d");
        assert_eq!(fmt_duration(Duration::from_secs(90)), "90s");
        assert_eq!(fmt_duration(Duration::from_micros(500)), "500us");
    }
}
