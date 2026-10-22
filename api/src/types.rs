//! 管理 API 与进程外插件共用的数据结构（design.md 7.1、3.12）。
//!
//! 密钥文件、changelog 事件，以及各管理端点的请求/响应结构（随所在里程碑加入；M1：health、failpoints、错误回复）。
//! core 的服务端与 wmfs-mgr 的 ApiClient 都用这里的定义；每个结构的 JSON 字段名都有逐字的金样测试（本文件末尾）。
//! 演进规则：路径带 `/v1`，字段只增不改（ADR-0005）。

use serde::{Deserialize, Serialize};

/// 所有错误回复的 body：`{"error": "<code>", "message": "<中文说明>"}`（7.1）。
/// `error` 是错误码名的小写（例如 `auth_failed`、`eacces`、`einval`、`enoent`、`not_leader`）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ErrorReply {
    pub error: String,
    pub message: String,
}

/// 一个节点的地址：rpc 地址按传输分开（与 `NodeRecV1.addrs` 相同的 `(scheme, 地址)`）与管理 API 地址。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct NodeAddrs {
    pub node_id: u32,
    pub addrs: Vec<(String, String)>, // 例如 [("tcp", "127.0.0.1:19100")]
    pub mgmt_addr: String,            // 例如 "127.0.0.1:19180"
}

/// `GET /api/v1/health`（免认证）：本节点健康、角色、版本、本节点与 leader 的地址；
/// `wmfs fs`、`wmfs ping`、`wmfs mount`、`wmfs failpoint <node>` 用它从管理地址找到 rpc 地址与节点号。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct Health {
    pub ok: bool,
    pub version: String,    // 与 `wmfs --version` 相同，例如 "0.1.0 (proto 1, core-abi 1, c-abi 1)"
    pub cluster_id: String, // 32 位小写 hex
    pub node: NodeAddrs,
    // 元数据角色："none"（M1 还没有元数据）；M3 起："leader"（**就绪**的 leader：已当选、本任期的空条目已应用、插件清单一致，
    // 可以服务请求）、"candidate"（正在选举，或已当选但还没就绪）、"follower"、"learner"（7.1 的 M3 注记）
    pub meta_role: String,
    pub leader: Option<NodeAddrs>, // 元数据 leader 的地址（M3 起；不知道时为 null）
    pub audit_lag: Option<u64>,    // 审计消费者落后的 changelog 条数（M12 起；没有审计消费者时为 null）
}

/// 一个 fail point：`POST /api/v1/failpoints` 的请求，也是列表里的一项（3.11）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Failpoint {
    pub name: String,
    pub action: String, // 例如 "times(1,return(EIO))"
}

/// `GET/POST/DELETE /api/v1/failpoints` 的回复：本节点当前的全部 fail point（按名字排序）。
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct FailpointList {
    pub failpoints: Vec<Failpoint>,
}

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
        let mut secret = [0u8; 32];
        if !crate::hex::unhex(hex, &mut secret) {
            return Err(format!("secret 应是 64 位 hex，实际 {} 位", hex.len()));
        }
        Ok(KeyFile { key_id, secret })
    }

    /// 写回文件用的一行文本（末尾带换行）。
    pub fn to_line(&self) -> String {
        format!("wmfs-key-v1:{}:{}\n", self.key_id, crate::hex::hex(&self.secret))
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

    /// 金样：管理 API 的 JSON 逐字固定（服务端与 ApiClient 都用这些结构，字段名不靠猜）。
    #[test]
    fn mgmt_json_golden() {
        let n = NodeAddrs {
            node_id: 1,
            addrs: vec![("tcp".into(), "127.0.0.1:19100".into())],
            mgmt_addr: "127.0.0.1:19180".into(),
        };
        let h = Health {
            ok: true,
            version: "0.1.0 (proto 1, core-abi 1, c-abi 1)".into(),
            cluster_id: "c1".repeat(16),
            node: n,
            meta_role: "none".into(),
            leader: None,
            audit_lag: None,
        };
        let want = format!(
            r#"{{"ok":true,"version":"0.1.0 (proto 1, core-abi 1, c-abi 1)","cluster_id":"{}","node":{{"node_id":1,"addrs":[["tcp","127.0.0.1:19100"]],"mgmt_addr":"127.0.0.1:19180"}},"meta_role":"none","leader":null,"audit_lag":null}}"#,
            "c1".repeat(16)
        );
        assert_eq!(serde_json::to_string(&h).unwrap(), want);
        assert_eq!(serde_json::from_str::<Health>(&want).unwrap(), h);
        let l = FailpointList { failpoints: vec![Failpoint { name: "a.b".into(), action: "off".into() }] };
        assert_eq!(serde_json::to_string(&l).unwrap(), r#"{"failpoints":[{"name":"a.b","action":"off"}]}"#);
        let e = ErrorReply { error: "auth_failed".into(), message: "签名不对".into() };
        assert_eq!(serde_json::to_string(&e).unwrap(), r#"{"error":"auth_failed","message":"签名不对"}"#);
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
