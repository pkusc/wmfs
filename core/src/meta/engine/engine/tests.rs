//! embedded-kv 的 manifest：`plugins/embedded-kv/manifest.toml` 与代码里的常量逐字段相等（design.md 3.7.1、4.10）。

use super::*;
use crate::hooks::manifest::check_manifest_toml;

#[test]
fn manifest_toml_matches_code() {
    let text = include_str!("../../../../../plugins/embedded-kv/manifest.toml");
    check_manifest_toml(&MANIFEST, text).unwrap();
}
