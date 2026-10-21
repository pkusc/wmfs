//! 数据面（design.md 3.2.2–3.2.5、3.4.2）。
//!
//! M0 只有 [`format`]（chunk 文件固定头与块表，3.2.4）与 [`superblock`]（target 超级块，3.2.3）的字节布局与编解码。
//! 读改写 build.rs、chunk 状态表、写/读/截断/同步、链式复制等在 M5、M9 加入。

pub mod format;
pub mod superblock;
