# M5 单节点数据路径：逐条对应 roadmap §3 M5 的"验收标准"与"演示命令"。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：100 MiB 文件往返一致；小文件走 inline；人为损坏一个块后读返回 EIO（单副本无法修复，诚实报错）而不是错误数据；
#           之后截断到 10 MiB 照常成功；cap 被篡改/过期时数据节点拒绝；截断到 s > 0 与到 0 都正确；
#           `echo hello>f; echo x>f; truncate -s 6 f` 读到 x\n 加 4 个 0 字节；注入"pending 文件写到一半崩溃"后重启无损坏读；
#           内存后端下模拟掉电，fsync 过的数据不丢；废弃 epoch 在宽限期后被删除。另有 chunk 格式（头、块表、版本号、任意比特翻转）、
#           Corrupt 标记的规则、cap 的签发与校验、M5 的钩子规则、插件（local-fs、crc32c、replica、fifo、gc）的 manifest 与 fail point、
#           ADR-0034 的三个用例（只能写的人把 inline 文件写过 64 KiB、Uninline 与 inline 补丁并发、提交之前崩溃）、后台任务框架的
#           fail point、target 心跳、target 一级的检查（cluster_id、ro_compat、容量上限）、并发（Uninline 的并发上限、同一 chunk
#           的写串行），以及演示命令。
#
# 开发集群只用本次验收自己的目录（$LOGDIR/dev，端口照常是 19100/19180）：通过时随日志一起删除，失败时保留现场；
# 演示用的临时文件（100 MiB 的随机文件等）放在 $LOGDIR/files，无论成败退出前都删除（不占用户的空间），并停掉本次起的节点。
# 只解析 --json 输出；例外是演示要求"看得见"的给人看的文字（stat 的「layout：chunk=4M、epoch=1、1 条链」与「inline=yes」、
# chunk-header 的「checksum=crc32c」「logical_len=4194304」、get 坏块时的「Input/output error」），各有对应的 --json 检查作为确凿证据。
# 等待一律用 wmfs dev wait leader，不用 sleep。公共函数 dev_ports_free、expect_error、dev_metric 在 scripts/accept.sh 里。

accept_m5() {
    check "开发集群的端口 19100、19180 空闲（演示用的开发集群要先 wmfs dev down）" dev_ports_free M5
    check "cargo build --release 成功且零警告" build_no_warnings
    check "chunk 格式：固定头与块表的编解码、偏移、坏头被拒绝、随机字节当头解析不 panic（core 单元测试）" \
        tests_at_least 5 --release -p wmfs-core --lib data::format
    check "cap：签名与校验、篡改被发现、有效期（5 分钟容差）、权限位、chunk 所在的链（core 单元测试）" \
        tests_at_least 2 --release -p wmfs-core --lib proto::cap
    check "金样：CapV1 的签名字节与 MetaOp::Uninline（追加在末尾）逐字节一致" \
        cargo test --release -p wmfs-core --test golden_proto
    check "M5 的钩子规则：qos 与 chunk_store 只能各一个、transform 按类别的 altitude 子区间、算法与 codec 编号唯一" \
        one_test --release -p wmfs-core --lib hooks::registry::tests::data_hooks_one_qos_transform_subranges_and_unique_ids
    check "chunk 状态表：同一 chunk 的写一个一个做、等待队列最多 8 个（满了 BUSY）、Corrupt 标记清除之前一直在（core 单元测试）" \
        tests_at_least 2 --release -p wmfs-core --lib data::state
    check "后台任务的限速：任务级与节点级取更严的、这一步已经花掉的时间算进去" \
        one_test --release -p wmfs-core --lib bg::limiter::tests::the_stricter_limit_wins_and_time_already_spent_counts
    check "local-fs：manifest、写 pending/提交/读/拷贝区间、提交之后已打开的读者仍读旧版本、列举/删除/用量/启动清理" \
        tests_at_least 3 --release -p wmfs-plugins --lib local_fs::
    check "crc32c：manifest、往返、翻转任意一个字节都被发现" tests_at_least 2 --release -p wmfs-plugins --lib crc32c::
    check "replica：manifest、每个成员存整份、读副本的顺序" tests_at_least 2 --release -p wmfs-plugins --lib replica::
    check "fifo：manifest、先进先出" tests_at_least 2 --release -p wmfs-plugins --lib fifo::
    check "gc：manifest、在元数据 leader 上每秒一轮" tests_at_least 2 --release -p wmfs-plugins --lib gc::
    check "wmfs 默认装的插件里有 local-fs、crc32c、replica、fifo、gc（集群插件清单）" \
        one_test --release -p wmfs --bin wmfs tests::registry_builds_the_default_plugin_set
    check "随机的写（多段、跨块、部分块）、截断与读和参考模型逐字节一致：MemTargetStore 与真实目录上的 local-fs 各 1000 个种子" \
        m5_model
    check "chunk 头：版本号从 incarnation<<40 起单调增加、logical_len、块大小、洞、校验算法；cap 的参数与 chunk 对不上 → EINVAL" \
        one_test --release -p wmfs-testkit --test data_chunk header_versions_holes_and_layout_params
    check "pending 文件写到一半崩溃（在任意字节处截断、掉电后还在 p/ 里）→ 启动清理把它扔掉，读到的仍是旧版本，之后的写照常（MemTargetStore）" \
        one_test --release -p wmfs-testkit --test data_chunk a_pending_file_cut_at_any_byte_is_cleaned_up_and_never_read
    check "已提交的文件只有一部分落了盘 → 判为损坏、报错，绝不返回错误的数据" \
        one_test --release -p wmfs-testkit --test data_chunk a_committed_file_that_only_partly_reached_the_disk_is_reported_not_returned
    check "翻转头、块表、数据区里的任意一个比特 → 一律被发现（Corrupt），不返回错误的数据" \
        one_test --release -p wmfs-testkit --test data_chunk every_flipped_bit_in_header_table_or_data_is_caught
    check "Corrupt 标记（R5）：读一律拒绝；要保留任何旧块的写与截断 → EIO；覆盖整个 chunk、截断到 0 照常执行并清除标记" \
        one_test --release -p wmfs-testkit --test data_chunk corrupt_chunks_refuse_reads_and_keep_old_block_writes_until_rewritten
    check "内存后端下模拟掉电：fsync 过的数据不丢，没 fsync 的写要么整个没有、要么整个在" \
        one_test --release -p wmfs-testkit --test data_chunk power_loss_keeps_exactly_what_was_fsynced
    check "fail point chunk.pending.torn：pending 文件写到一半整个进程 abort（子进程）→ 重启清空 p/ → 读到的正好是旧版本" \
        tests_at_least 1 --release -p wmfs-testkit --test failpoint_chunk_torn
    check "CommitWrite / inline 补丁 / Uninline 的 change_id CAS（不写 changelog）/ Fallocate / GetInline / RenewCap / 按打开方式签的 cap" \
        tests_at_least 6 --release -p wmfs-testkit --test meta_file_ops
    check "echo hello>f; echo x>f; truncate -s 6 f → 读到 x\\n 加 4 个 0 字节（len(INLINE) ≤ size 的不变式，元数据层）" \
        one_test --release -p wmfs-testkit --test meta_file_ops echo_hello_then_echo_x_then_truncate_6_reads_x_newline_and_four_zeros
    check "完整节点上：3 个 chunk 的文件往返一致、小文件走 inline、覆盖写换 epoch、截断到 s>0（再扩回来读为 0）与到 0 都正确" \
        one_test --release -p wmfs-testkit --test data_e2e files_round_trip_inline_small_files_and_truncate
    check "cap 被篡改、过期（超过 5 分钟容差）、chains 不符、权限位不够、会话被驱逐 → 数据节点拒绝；DeleteEpoch 要管理权限位" \
        one_test --release -p wmfs-testkit --test data_e2e data_nodes_reject_bad_caps
    check "损坏一个块 → 读回 EIO 而不是错误的数据；之后把坏 chunk 整块截掉照常成功" \
        one_test --release -p wmfs-testkit --test data_e2e a_corrupt_block_reads_as_eio_and_truncating_it_away_still_works
    check "target 心跳把 phys_devs、boot_id 写进 TargetRecV1，StatFs 报出容量" \
        one_test --release -p wmfs-testkit --test data_e2e heartbeats_fill_in_the_target_record_and_statfs
    check "ADR-0034 ①：只有写权限的人（0622、O_WRONLY 追加）把非空的 inline 文件写过 64 KiB → 成功、内容逐字节正确，GetInline 仍 EACCES" \
        one_test --release -p wmfs-testkit --test data_e2e a_write_only_user_can_grow_an_inline_file_past_64k_without_seeing_it
    check "ADR-0034 ②：Uninline 与另一个客户端的 inline 补丁并发 → CAS 失败、从头重做一次 → chunk 里是补丁之后的内容；同一个 inode 的两个 Uninline 并发 → 只有一个写 chunk" \
        tests_at_least 1 --release -p wmfs-testkit --test failpoint_uninline_race
    check "ADR-0034 ③：写完 chunk、提交之前崩溃（meta.uninline.before_commit = abort，子进程）→ 重启后仍是原 inline 内容，再 Uninline 正常、不冒出旧内容" \
        tests_at_least 1 --release -p wmfs-testkit --test failpoint_uninline_crash
    check "新集群上 12 个并发 Uninline（数据节点还不认识刚生成的链）几秒内全部完成，期间无关的 GetAttr 照常应答" \
        one_test --release -p wmfs-testkit --test data_e2e concurrent_uninlines_on_a_new_cluster_finish_quickly_and_reads_stay_responsive
    check "16 个写者同时写同一个 chunk 里互不相交的块：一个块也不丢（同一 chunk 的写串行，I1）" \
        one_test --release -p wmfs-testkit --test data_e2e sixteen_writers_on_one_chunk_lose_nothing
    check "target：cluster_id 不符拒绝打开、不认识的 ro_compat 位 → 只读（写与截断 EROFS、读照常）、写到容量上限 → ENOSPC（截断照常）" \
        tests_at_least 3 --release -p wmfs-testkit --test data_target
    check "废弃 epoch 在宽限期（这里调成 2 s）之内不删、过了宽限期被 gc 删除（数据与 GC 记录）" \
        one_test --release -p wmfs-testkit --test data_gc obsolete_epochs_are_deleted_after_the_grace_period
    check "孤儿：有会话打开着时不删；都关掉之后 gc 先删数据、再删 inode" \
        one_test --release -p wmfs-testkit --test data_gc orphans_wait_until_nobody_has_them_open
    check "数据路径的 fail point：chunk.pending.eio / commit.before_rename / read.eio / read.corrupt / sync.eio，以及 plugin.local-fs.chunk_store、plugin.crc32c.transform、plugin.replica.codec、plugin.fifo.qos" \
        tests_at_least 1 --release -p wmfs-testkit --test failpoint_chunk
    check "后台任务的 fail point：plugin.gc.bg_task 与 bg.step.crash_after_effect → 记错误、从同一个游标重试，重放无害，旧 epoch 最终删除" \
        tests_at_least 1 --release -p wmfs-testkit --test failpoint_bg

    export WMFS_DEV_DIR="$LOGDIR/dev"
    F="$LOGDIR/files"
    mkdir -p "$F"
    trap 'wmfs dev down >/dev/null 2>&1; rm -rf "$F"' EXIT
    check "wmfs dev up --nodes 1" wmfs dev up --nodes 1
    eval "$(wmfs dev env)"
    check "wmfs dev wait leader" wmfs dev wait leader --timeout 30
    check "head -c 100M /dev/urandom > r100" m5_random
    check_json "wmfs fs put r100 /big：104857600 字节、25 个 chunk" \
        '.ok == true and .size == 104857600 and .inline == false and .chunks == 25' wmfs fs put "$F/r100" /big --json
    check_json "wmfs fs get /big r100.back" '.ok == true and .size == 104857600' wmfs fs get /big "$F/r100.back" --json
    check "cmp r100 r100.back：SAME（100 MiB 文件往返一致）" cmp "$F/r100" "$F/r100.back"
    check_json "wmfs fs stat /big：size=104857600，layout：chunk=4M、epoch=1、1 条链，不是 inline" \
        '.size == 104857600 and .layout.chunk_size == 4194304 and .layout.epoch == 1 and (.layout.chains | length) == 1 and .inline == false' \
        wmfs fs stat /big --json
    check "echo hello | wmfs fs put - /small：内容只存在元数据里（inline）" m5_put_small
    check_json "wmfs fs stat /small：inline=yes、size=6（阈值 64 KiB）" \
        '.inline == true and .size == 6 and .layout.inline_max == 65536' wmfs fs stat /small --json
    check "wmfs fs cat /small：hello" m5_cat_small
    check "chunk-header \"\$(chunk-path /big 3)\"：version、checksum=crc32c、logical_len=4194304、chunk_idx=3、64 个数据块" m5_chunk_header
    check "演示里给人看的输出：stat /big 有「layout：chunk=4M、epoch=1、1 条链」，stat /small 有「inline=yes」" m5_demo_text
    check_json "wmfs debug corrupt-chunk /big 3 --node 1 --block 0：按块表找到第 0 块的数据，翻转一个字节" \
        '.ok == true and .block == 0' wmfs debug corrupt-chunk /big 3 --node 1 --block 0 --json
    check "wmfs fs get /big x：EIO（Input/output error：校验和不符，单副本修不了，诚实报错），也没有写出错误的数据" m5_get_eio
    check_json "wmfs fs truncate /big 10M" '.ok == true and .size == 10485760' wmfs fs truncate /big 10M --json
    check_json "wmfs fs stat /big：size=10485760（坏掉的 chunk 3 被整块截掉，不需要读它；epoch 不变）" \
        '.size == 10485760 and .layout.epoch == 1' wmfs fs stat /big --json
    check "截断之后读回来的 10 MiB 与原文件的前 10 MiB 一致" m5_cmp_10m
    check "wmfs fs truncate /big 12M：扩回来的 2 MiB 读为 0（截断到 s > 0 时被截掉的部分清零，不会冒出旧数据）" m5_extend
    check "wmfs fs truncate /big 0：size=0、换了新 epoch（2），读回 0 字节" m5_trunc0
    check "echo hello>f; echo x>f; truncate -s 6 f（经 CLI）→ 读到 x\\n 加 4 个 0 字节" m5_inline_invariant
    check "inline 文件写过 64 KiB（覆盖 /small 为 70000 字节）：服务端先 Uninline，再按 chunk 写，内容一致、不再是 inline" m5_grow
    check "server.log 里没有 panic / fatal；wmfs_meta_batch_stale_total 恒为 0" m5_log_clean
    check "wmfs dev down 停掉全部开发节点" wmfs dev down
    rm -rf "$F"
}

m5_model() {
    WMFS_DATA_SEEDS=1000 tests_at_least 2 --release -p wmfs-testkit --test data_chunk model
}

m5_random() {
    head -c 100M /dev/urandom >"$F/r100" || return 1
    ls -l "$F/r100"
    [ "$(stat -c %s "$F/r100")" -eq 104857600 ]
}

m5_put_small() {
    echo hello | json_expect '.ok == true and .size == 6 and .inline == true' wmfs fs put - /small --json
}

m5_cat_small() {
    local got
    got="$(wmfs fs cat /small)" || return 1
    printf '读到：%s\n' "$got"
    [ "$got" = "hello" ]
}

m5_chunk_header() {
    local p
    p="$(wmfs debug chunk-path /big 3 --json | jq -r .path)" || return 1
    echo "chunk 3 的路径：$p"
    [ -f "$p" ] || { echo "这个文件不存在"; return 1; }
    [ "$(wmfs debug chunk-path /big 3)" = "$p" ] || { echo "不带 --json 时只打印同一个路径（演示里 \$(…) 直接用它）"; return 1; }
    json_expect '.checksum == "crc32c" and .logical_len == 4194304 and .chunk_idx == 3 and .version > 0 and .data_blocks == 64 and .epoch == 1' \
        wmfs debug chunk-header "$p" --json
}

m5_demo_text() {
    local big small hdr
    big="$(wmfs fs stat /big)" || return 1
    small="$(wmfs fs stat /small)" || return 1
    hdr="$(wmfs debug chunk-header "$(wmfs debug chunk-path /big 3)")" || return 1
    printf '%s\n%s\n%s\n' "$big" "$small" "$hdr"
    printf '%s' "$big" | grep -q 'size=104857600' || return 1
    printf '%s' "$big" | grep -q 'layout：chunk=4M、epoch=1、1 条链' || return 1
    printf '%s' "$small" | grep -q 'inline=yes' || return 1
    printf '%s' "$hdr" | grep -q 'checksum=crc32c' || return 1
    printf '%s' "$hdr" | grep -q 'logical_len=4194304'
}

m5_get_eio() {
    local err="$F/get.stderr"
    expect_error EIO wmfs fs get /big "$F/x" --json || return 1
    if wmfs fs get /big "$F/x" 2>"$err"; then
        echo "不带 --json 时也应该失败"
        return 1
    fi
    cat "$err"
    grep -q 'Input/output error' "$err" || { echo "没有打印 Input/output error"; return 1; }
    # 坏的是 chunk 3（[12 MiB, 16 MiB)）：读到它之前写出去的前 12 MiB 必须是对的，坏块的内容绝不能出现在输出里
    local n
    n="$(stat -c %s "$F/x" 2>/dev/null || echo 0)"
    echo "失败之前写出的字节数：$n"
    [ "$n" -le $((12 << 20)) ] || { echo "坏 chunk 的内容被写了出去"; return 1; }
    [ "$n" -eq 0 ] || cmp -n "$n" "$F/r100" "$F/x"
}

m5_cmp_10m() {
    json_expect '.ok == true and .size == 10485760' wmfs fs get /big "$F/b10" --json || return 1
    cmp -n 10485760 "$F/r100" "$F/b10" && [ "$(stat -c %s "$F/b10")" -eq 10485760 ]
}

m5_extend() {
    json_expect '.ok == true and .size == 12582912' wmfs fs truncate /big 12M --json || return 1
    json_expect '.ok == true and .size == 12582912' wmfs fs get /big "$F/b12" --json || return 1
    cmp -n 10485760 "$F/r100" "$F/b12" || return 1
    tail -c 2097152 "$F/b12" | cmp -n 2097152 - /dev/zero && echo "后 2 MiB 全是 0"
}

m5_trunc0() {
    json_expect '.ok == true and .size == 0' wmfs fs truncate /big 0 --json || return 1
    json_expect '.size == 0 and .layout.epoch == 2' wmfs fs stat /big --json || return 1
    json_expect '.ok == true and .size == 0' wmfs fs get /big "$F/b0" --json || return 1
    [ "$(stat -c %s "$F/b0")" -eq 0 ]
}

m5_inline_invariant() {
    echo hello | json_expect '.ok == true and .inline == true' wmfs fs put - /f --json || return 1
    echo x | json_expect '.ok == true and .size == 2' wmfs fs put - /f --json || return 1
    json_expect '.ok == true and .size == 6' wmfs fs truncate /f 6 --json || return 1
    json_expect '.ok == true and .size == 6' wmfs fs get /f "$F/f.out" --json || return 1
    od -c "$F/f.out"
    printf 'x\n\0\0\0\0' | cmp - "$F/f.out"
}

m5_grow() {
    head -c 70000 "$F/r100" >"$F/s70k" || return 1
    json_expect '.ok == true and .size == 70000 and .inline == false and .chunks == 1' wmfs fs put "$F/s70k" /small --json || return 1
    json_expect '.inline == false and .size == 70000' wmfs fs stat /small --json || return 1
    json_expect '.ok == true and .size == 70000' wmfs fs get /small "$F/s70k.back" --json || return 1
    cmp "$F/s70k" "$F/s70k.back"
}

m5_log_clean() {
    local stale log="$WMFS_DEV_DIR/n1/logs/server.log"
    stale="$(dev_metric wmfs_meta_batch_stale_total)"
    echo "wmfs_meta_batch_stale_total = $stale"
    [ "$stale" = "0" ] || return 1
    if grep -nE 'panicked|fatal|is out of range' "$log"; then
        echo "server.log 里有 panic / fatal（见上面）"
        return 1
    fi
    echo "server.log 里没有 panic / fatal"
}
