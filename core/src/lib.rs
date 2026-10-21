//! wmfs-core：本体（design.md 2.1、第 3 章契约层）。
//!
//! M0 落成**接口骨架**：design.md 3.2.7–3.2.9、3.3、3.5.1、3.6.2、3.6.4、3.7.1、3.7.3、3.7.7、3.10、3.12 的全部类型、
//! 枚举、trait 都在这里，从 M0 起以源码为唯一真源；还没实现的函数体是 `unimplemented!("Mx：…")`，写明哪个里程碑实现。
//! `tests/golden/` 有每种消息、记录、帧头、chunk 头的金样（`cargo test -p wmfs-core golden`）。
//!
//! - [`base`]：错误码、单位、时钟、随机数、日志、node.toml、libc 封装、物理盘解析
//! - [`proto`]：帧头与全部线上消息（3.3）
//! - [`buf`]：预注册内存池与缓冲区所有权（3.6）
//! - [`net`]：连接包装、RpcClient、Responder（3.12）
//! - [`hooks`]：manifest、钩子 trait、管线、策略求值点（3.7）
//! - [`meta`]：KV 布局与记录（3.2.7–3.2.9）、元数据事务模型（3.5.1）、目录属性（3.12）
//! - [`data`]：chunk 文件格式与 target 超级块（3.2.3、3.2.4）
//! - [`client`]：MetaClient / DataClient（3.12）
//! - [`bg`]：后台任务框架（3.10）

pub mod base;
pub mod bg;
pub mod buf;
pub mod client;
pub mod data;
pub mod hooks;
pub mod meta;
pub mod net;
pub mod proto;
