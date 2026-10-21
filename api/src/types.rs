//! 管理 API 与进程外插件共用的数据结构（design.md 7.1、3.12）。
//!
//! M0 只有密钥文件与 changelog 事件；各管理端点的请求/响应结构随所在里程碑加入
//! （core 的服务端与 wmfs-mgr 的 ApiClient 都用这里的定义，每个都要有金样测试）。

use serde::{Deserialize, Serialize};

/// 放在 wmfs-api（core 与 wmfs-mgr 都要读密钥文件）：一行 `wmfs-key-v1:<key_id>:<64 位 hex secret>` 的解析结果（3.3.2）。
#[derive(Clone, PartialEq, Eq)]
pub struct KeyFile {
    pub key_id: u32,
    pub secret: [u8; 32],
}

impl KeyFile {
    /// 解析密钥文件内容（允许前后空白与末尾换行）。文件来自磁盘、可能被手工改坏：任何输入都只返回错误，不 panic。
    pub fn parse(text: &str) -> Result<KeyFile, String> {
        let line = text.trim();
        let rest = line.strip_prefix("wmfs-key-v1:").ok_or("密钥文件应以 wmfs-key-v1: 开头")?;
        let (id, hex) = rest.split_once(':').ok_or("密钥文件格式应为 wmfs-key-v1:<key_id>:<64 位 hex>")?;
        // 先逐字节检查字符集：标准库的数字解析会接受前导 '+'，而按字节下标切分含多字节字符的字符串会 panic
        if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
            return Err(format!("key_id 应是十进制数字：{id:?}"));
        }
        let key_id: u32 = id.parse().map_err(|_| format!("key_id 超出范围：{id}"))?;
        if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("secret 里有非 hex 字符（只能是 0-9、a-f、A-F）".into());
        }
        if hex.len() != 64 {
            return Err(format!("secret 应是 64 位 hex，实际 {} 位", hex.len()));
        }
        let mut secret = [0u8; 32];
        for (i, b) in secret.iter_mut().enumerate() {
            // 上面已确认全是 ASCII hex 字符，所以按字节下标切分不会落在字符中间
            *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).map_err(|_| "secret 里有非 hex 字符".to_string())?;
        }
        Ok(KeyFile { key_id, secret })
    }

    /// 写回文件用的一行文本（末尾带换行）。
    pub fn to_line(&self) -> String {
        let hex: String = self.secret.iter().map(|b| format!("{b:02x}")).collect();
        format!("wmfs-key-v1:{}:{}\n", self.key_id, hex)
    }
}

/// 打印时不泄露 secret。
impl std::fmt::Debug for KeyFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyFile {{ key_id: {}, secret: <隐藏> }}", self.key_id)
    }
}

/// changelog 记录的 JSON 形态：`GET /api/v1/changelog` 的每一项，也是 `EventSink::deliver` 的输入（3.7.3、3.11）。
/// 字段与 core 的 `ChangeRecV1` 一一对应，另加 `seq`；`op` 用操作名（例如 "Create"）；
/// 名字按 UTF-8 显示（非法字节替换为 U+FFFD）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct ChangeEvent {
    pub seq: u64,
    pub time_ms: u64,
    pub op: String,
    pub result: u16,
    pub trace_id: u64,
    pub uid: u32,
    pub gid: u32,
    pub tenant: u32,
    pub session: u64,
    pub job_id: u64,
    pub ino: u64,
    pub parent: u64,
    pub name: String,
    pub new_parent: u64,
    pub new_name: String,
    pub size: u64,
    pub extra: Vec<(String, String)>,
    pub audited: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_file_round_trip() {
        let k = KeyFile { key_id: 7, secret: [0xab; 32] };
        let line = k.to_line();
        assert_eq!(line, format!("wmfs-key-v1:7:{}\n", "ab".repeat(32)));
        assert_eq!(KeyFile::parse(&line).unwrap(), k);
        assert_eq!(KeyFile::parse(&line.to_uppercase().replace("WMFS-KEY-V1", "wmfs-key-v1")).unwrap(), k);
    }

    #[test]
    fn key_file_rejects_garbage() {
        let hex = "ab".repeat(32);
        for bad in [
            "",
            "wmfs-key-v2:1:00",
            "wmfs-key-v1:x:00",
            "wmfs-key-v1:1:abcd",
            &format!("wmfs-key-v1:1:{}", "zz".repeat(32)),
            // 多字节字符：hex 部分正好 64 字节，按字节下标切分会落在字符中间（以前会 panic）
            &format!("wmfs-key-v1:1:{}a", "€".repeat(21)),
            &format!("wmfs-key-v1:1:a{}b", "é".repeat(31)),
            // 标准库的数字解析接受前导 '+'，这里必须拒绝
            &format!("wmfs-key-v1:1:{}", "+f".repeat(32)),
            &format!("wmfs-key-v1:+7:{hex}"),
            &format!("wmfs-key-v1::{hex}"),
            &format!("wmfs-key-v1: 7:{hex}"),
            &format!("wmfs-key-v1:4294967296:{hex}"),
        ] {
            assert!(KeyFile::parse(bad).is_err(), "应拒绝：{bad}");
        }
        assert_eq!(KeyFile::parse(&format!("wmfs-key-v1:4294967295:{hex}")).unwrap().key_id, u32::MAX);
    }

    #[test]
    fn key_file_debug_hides_secret() {
        let k = KeyFile { key_id: 1, secret: [0x5a; 32] };
        assert!(!format!("{k:?}").contains("5a"));
    }

    #[test]
    fn change_event_json_field_names() {
        let e = ChangeEvent { seq: 3, op: "Mkdir".into(), name: "a".into(), ..Default::default() };
        let j = serde_json::to_value(&e).unwrap();
        assert_eq!(j["seq"], 3);
        assert_eq!(j["op"], "Mkdir");
        assert_eq!(j["name"], "a");
        let back: ChangeEvent = serde_json::from_value(j).unwrap();
        assert_eq!(back, e);
    }
}
