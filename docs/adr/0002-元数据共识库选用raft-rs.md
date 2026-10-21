# ADR-0002 元数据共识库选用 raft-rs（替代 openraft）

- 状态：accepted（人类于 2026-10-20 批准）
- 日期：2026-10-20
- 偏离条款：理念文档第 7 章 [默认]"语言：Rust（…、`openraft`、…）"
- 是否触及契约层：否（共识算法的选择；元数据 Raft 的使用方式见 ADR-0019）
- 批准类别：已同意（人类在需求中确认"只用成熟 Raft 库、不用 async"）；确认批次 0（知悉）
- 相关章节：design.md 2.3、3.4.1、附录 A、附录 B

## 背景

理念文档 8.1 要求"不自研共识算法，使用成熟 Raft 库"，第 7 章默认 openraft。openraft 建立在 async 之上，与 ADR-0001 冲突。raft-rs（crate `raft`）是 TiKV 生产验证的同步核心实现。

## 决定

1. 使用 `raft = { version = "0.7", default-features = false, features = ["protobuf-codec"] }`。
2. 直接依赖 `protobuf = "2.28"`（raft 不 re-export，消息编解码需要 `protobuf::Message` trait；版本与 raft-proto 0.7 一致）与 `slog = "2"`（`RawNode::new` 需要 `slog::Logger`；logger 用 `slog::Logger::root(SlogBridge, slog::o!())` 构造，约 40 行的 `SlogBridge` 转到我们自己的日志，不引入 slog-term/envlogger）。
3. 集成规则严格按 design.md 3.4.1（Ready 处理顺序、两个写事务都写硬状态、提议前检查任期、只提议 ConfChangeV2、Safe ReadIndex、`limit_size` 总保留第一条所以要限制批次大小等），附录 A 的每个坑都有回归测试。

## 备选方案

- **openraft**：API 更高层，但依赖 async。否决（ADR-0001）。
- **通过 FFI 用 etcd/raft（Go）或其他 C 库**：跨语言、构建复杂。否决。
- **自研**：理念文档 8.1 禁止。

## 后果

- 好处：成熟、同步、被大规模生产验证；我们完全掌控持久化与线程模型。
- 代价：raft-rs 的 API 很底层，Ready/LightReady、快照、成员变更都要自己正确处理——设计里把每个已知的坑写成清单并要求回归测试与确定性模拟（design.md 9.2）。
- 依赖较旧的 protobuf 2。
