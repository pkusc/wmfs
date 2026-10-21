//! 带 HMAC 签名的管理 API 客户端（design.md 3.12、7.1），CLI 与 mgr 共用。
//!
//! 接口骨架（M0）：签名以这里为准，函数体在 M1 实现。实现要点：ureq 的 agent 必须
//! `proxy(None)`（本机设置了 http_proxy）；依次尝试地址，连不上换下一个；遇到 409 跟随 leader。

use wmfs_api::types::KeyFile;

pub struct ApiClient {/* ureq::Agent（proxy(None)）、地址列表、key */}

#[allow(unused_variables)] // 骨架：函数体在 M1 实现，实现后删除这一行
impl ApiClient {
    pub fn new(addrs: Vec<String>, key: KeyFile) -> ApiClient {
        // addrs：逗号分隔的 WMFS_CLUSTER 拆开
        unimplemented!("M1：ApiClient::new")
    }
    pub fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, ApiError> {
        // 依次尝试地址；409 跟随 leader
        unimplemented!("M1：ApiClient::get")
    }
    pub fn post<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, ApiError> {
        unimplemented!("M1：ApiClient::post")
    }
    pub fn delete<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, ApiError> {
        unimplemented!("M1：ApiClient::delete")
    }
}

/// = 服务端的 `{"error","message"}`，另加 HTTP 状态码。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiError {
    pub status: u16,
    pub code: String,
    pub message: String,
}
