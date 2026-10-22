# ADR-0034 inline 小文件转成 chunk 由服务端完成

- 状态：accepted（人类于 2026-10-22 决定："让服务端去搬"）
- 日期：2026-10-22
- 性质：改 design.md 8.4、8.5 的 uninline 流程；线上协议只增不改（`MetaOp` 末尾追加一个变体）
- 是否触及契约层：**是**（新增 `MetaOp::Uninline`，金样随 M5 更新）
- 相关章节：design.md 3.3.4（`GetInline`、`CommitWrite.uninline_expect_change_id`）、3.4.2.9、3.10（迁移原语第 3、4 步）、8.4、8.5；ADR-0021、ADR-0023、ADR-0028、ADR-0033 第 11 条

## 背景

≤ `inline_max`（默认 64 KiB）的文件，内容存在元数据的 `INLINE` 里。文件长过阈值时要转成 chunk 存储。design.md 8.4 原来让**客户端**做：`GetInline` 取旧内容 → 与脏数据合并 → 写进 chunk → `CommitWrite{uninline_expect_change_id}`。

M4 定下"文件内容只给能读这个文件的人"（ADR-0033 第 11 条）之后，出现了一个缺口：对文件只有写权限、没有读权限的人（例如权限 0622 的公共日志，别人只能往里追加），客户端拿不到旧内容，没法转。可选的做法有三种：① 这次写返回 `EACCES`（不符合 POSIX）；② 让只写的客户端也能拿到旧内容（削弱"服务端强制授权"的承诺）；③ 由服务端自己把旧内容写进 chunk。人类选了 ③。

## 决定

1. **只保留一条路：uninline 一律由服务端完成。** 客户端永远不需要旧的 inline 内容就能让文件变大，所以只写权限的人也能正常写（完全符合 POSIX），`GetInline` 仍然只给能读的人（不泄露内容）。8.4 里客户端"取旧内容、合并"的那一段不再实现。
2. **新请求 `MetaOp::Uninline { ino }`**（追加在 `MetaOp` 末尾，只增不改）：
   - 权限：调用者对文件要有写权限（与 `CommitWrite` 相同）。
   - 幂等：文件已经不是 inline（没有 `F_INLINE`）→ 直接回成功。所以它不需要 seq 去重；按只读请求的方式处理（seq 0），重试安全。
   - 回复：`Opened(OpenOut)`（新的 layout 与属性，客户端用它更新 InodeState）。
3. **服务端怎么做**（在 leader 上、处理这个请求的 `meta-read` 线程里做，不占执行器与 raft 线程）：
   1. 线性一致地读出 inode（记下 change_id c）和 `INLINE` 内容。
   2. 用服务端身份（Server 角色、leader 自己签的写 cap）把内容按文件当前的 layout（同一个 epoch、create 时就分好的链）写进 chunk，再 `SyncChunks`（**清掉 INLINE 之前数据必须已持久**）。
   3. 在执行器里提交 `CommitWrite{uninline_expect_change_id: Some(c)}`：change_id 仍是 c 才清除 `F_INLINE` 与 `INLINE`（CAS，原有逻辑）。
   4. CAS 失败（期间有人改了 inline 内容）→ 从第 1 步重做，最多 3 次，仍失败回 `RETRY`（客户端按 3.3.8 重试）。
   - 内容与大小都没变，只换了存放位置，所以不写 changelog。
   - 崩溃发生在第 2、3 步之间：INLINE 还在，读照旧读 INLINE；chunk 里写了一半的数据没人读，下一次 Uninline 会覆盖它，文件删掉或截断时由 gc 按 epoch 清理。
4. **客户端什么时候调用**：
   - 8.4 写回：inline 文件这次的结果会超过 `inline_max` → 先 `Uninline`，再把脏数据当成普通 chunk 文件写出。
   - 8.5 strong：以写方式打开一个已有的 inline 文件时，先 `Uninline`。
5. **复用**：3.10 迁移原语第 3、4 步的"把 INLINE 内容按 L′ 写进 chunk"与第 3 条用同一个服务端函数。`CommitWrite.uninline_expect_change_id` 保留（服务端内部用它做 CAS）。
6. **体量**：约 150 行（服务端写 chunk 与 CAS 重试约 100 行，客户端调用与新变体约 50 行）。客户端少了"合并旧内容"的代码，抵掉一部分。

## 备选方案

- ① 返回 `EACCES`：最简单，但这个罕见情况不符合 POSIX。人类没有选。
- ② 让只写的客户端拿到旧内容：代码最少，但被攻破的客户端主机能读到本来不该读的小文件。人类没有选。
- 两条路都保留（能读的客户端自己转、只写的交给服务端）：代码更多，没有好处。否决。

## 后果

semantics.md 不需要新增放松条目：只写权限的人也能把小文件写大。元数据服务从 M5 起会经数据面写 chunk（只在 uninline 时，一个文件一辈子最多一次）。
