# M3 单节点元数据引擎：逐条对应 roadmap §3 M3 的"验收标准"与"演示命令"。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：raft Storage 合约测试；回归测试"单节点写入 → kill -9 → 重启"不 fatal；raft.persist.before_commit=abort 后重启，
#           已确认的写一个不丢；写 20 万条后日志被压缩；执行器"写集 ≥ 768 KiB 就结束本批"的单测；确定性 raft 模拟（单节点部分，
#           内存后端模拟掉电）通过；wmfs_meta_batch_stale_total 恒为 0。另有 M2 遗留的 core 修复（ADR-0031 第 11 条：单飞建连）、
#           附录 A 的回归用例、fail point 用例与演示命令（dev up、debug kv put/get/bench、kill -9 后重启、dev wait leader、/metrics）。
#
# 开发集群只用本次验收自己的目录（$LOGDIR/dev，端口照常是 19100/19180）：通过时随日志一起删除，失败时保留现场；
# 无论成败，退出前都停掉本次起的节点（不留后台进程）。演示用的开发集群（~/wmfs-dev）还占着这两个端口时第一条就失败，
# 提示先 wmfs dev down（免得白跑几分钟测试，到起节点时才 bind 失败）。只解析 --json 输出（/metrics 是 Prometheus 文本格式）；
# 等待一律用 wmfs dev wait leader，不用 sleep。

M3_HTTP=http://127.0.0.1:19180

accept_m3() {
    check "开发集群的端口 19100、19180 空闲（演示用的开发集群要先 wmfs dev down）" m3_ports_free
    check "cargo build --release 成功且零警告" build_no_warnings
    check "单飞建连（M2 遗留的 core 修复，ADR-0031 第 11 条）：16 个线程同时第一次调用，tcp 与 mem 的服务端都只看到 1 条连接" \
        one_test --release -p wmfs-testkit --test net_rpc first_calls_from_16_threads_share_one_connection
    check "raft Storage 合约测试（entries/term/compact 的边界、max_size 至少一条、覆盖写、压缩点与硬状态落盘、拒绝不认识的格式）" \
        tests_at_least 10 --release -p wmfs-core --lib meta::raft::storage
    check "执行器「写集 ≥ 768 KiB 就结束本批」的单测" \
        one_test --release -p wmfs-core --lib meta::engine::executor::tests::write_set_of_768_kib_ends_the_batch
    check "执行器的其余单测：256 个操作、独占操作与新链结束本批、操作级回滚、去重（错误也记录、点删 seq−256）、被驱逐的会话、批次结局到回复的映射、随机操作序列 vs 参考模型" \
        tests_at_least 8 --release -p wmfs-core --lib meta::engine::executor
    check "读闸门（批量 ReadIndex）：之后到达的读者等下一轮、500 ms 重发、2 s 回 RETRY、角色变化全部 NOT_LEADER" \
        tests_at_least 4 --release -p wmfs-core --lib meta::engine::read_gate
    check "wmfs init 写 index = 1 的初始状态（3.4.1.9、3.4.3）；初始 KV 与 raft_state 的金样逐字节一致" m3_init_state
    check "embedded-kv 的 manifest.toml 与代码一致；meta_engine 是唯一型钩子" m3_manifest
    check "附录 A 的回归用例（P2/P3 follower 落盘后才回复、P4/P5/P21 成员变更、P6 读闸门、P7/P16/P18 参数、P9/P20 重启不 fatal、P10、P14、P15、P19、P22，插件清单不一致的 leader 不提议）" \
        tests_at_least 12 --release -p wmfs-testkit --test raft_pitfalls
    check "确定性 raft 模拟（单节点部分，内存后端在随机的 fsync 处掉电）：1000 个种子，确认过的写一个不丢、过期批次从不部分应用" \
        env WMFS_SIM_SEEDS=1000 cargo test --release -p wmfs-testkit --test raft_sim
    check "写 20 万条后日志被压缩（压缩点 = durable_applied − 10 000），之后掉电重启不 fatal、数据都在（坑 P8）" \
        cargo test --release -p wmfs-testkit --test raft_compact
    check "fail point：raft.persist.before_commit / raft.persist.after_commit / raft.apply.before = abort（子进程、真实 redb 文件）后重启，确认过的写一个不丢" \
        cargo test --release -p wmfs-testkit --test failpoint_raft_abort
    check "fail point：follower 的回复在落盘之后、leader 的消息在落盘之前（坑 P2 的严格先后）" \
        cargo test --release -p wmfs-testkit --test failpoint_raft_order
    check "fail point：meta.exec.before_propose 与 plugin.embedded-kv.meta_engine 生效、清除后恢复" \
        cargo test --release -p wmfs-testkit --test failpoint_meta
    check "元数据引擎（raft + 执行器线程）：掉电重启不 fatal、64 路并发写、认证读已应用的 KEYMETA、插件清单不一致的 leader 不就绪、不是 leader 时读写都回 NOT_LEADER" \
        cargo test --release -p wmfs-testkit --test meta_engine
    check "MetaCall 服务（经 tcp）：DebugPut/DebugGet、只给 admin key 且要 debug_ops、未实现的操作回 FEATURE_UNSUPPORTED、撤销的 key 被拒" \
        cargo test --release -p wmfs-testkit --test meta_service

    export WMFS_DEV_DIR="$LOGDIR/dev"
    trap 'wmfs dev down >/dev/null 2>&1' EXIT
    check "wmfs dev up --nodes 1：init 写元数据的初始状态并自动 campaign" wmfs dev up --nodes 1
    eval "$(wmfs dev env)"
    check "wmfs dev wait leader：有就绪的元数据 leader 就立即返回 0" wmfs dev wait leader --timeout 30
    check_json "GET /api/v1/health：meta_role = leader，leader 是 node1" \
        '.meta_role == "leader" and .leader.node_id == 1 and .leader.addrs == [["tcp","127.0.0.1:19100"]]' \
        curl --noproxy '*' -s "$M3_HTTP/api/v1/health"
    check_json "wmfs debug kv put hello world" '.ok == true' wmfs debug kv put hello world --json
    check_json "wmfs debug kv get hello → world" '.found and .value == "world"' wmfs debug kv get hello --json
    check "wmfs debug kv get 不存在的键：退出码 1、found = false" m3_missing_key
    check_json "wmfs debug kv bench --n 100000：64 路并发全部写成，批会自然变大（平均每批 ≥ 8 个操作；本机实测约 32）" \
        '.ok == 100000 and .n == 100000 and .concurrency == 64 and .avg_batch >= 8 and .batches > 0' \
        wmfs debug kv bench --n 100000 --json
    check "/metrics：wmfs_raft_applied_index > 0，wmfs_meta_batch_stale_total = 0" m3_metrics
    check "单节点写入 → kill -9 → 重启 → wmfs dev wait leader：不 fatal，hello 与压测的最后一个键都还在" m3_kill9
    check "raft.persist.before_commit=abort：先确认 20 个写，节点在下一次落盘前 abort，重启后 20 个一个不丢" m3_abort_before_commit
    check "重启之后 wmfs_meta_batch_stale_total 仍为 0；server.log 里没有 panic / fatal" m3_after_restarts
    check "wmfs dev down 停掉全部开发节点" wmfs dev down
}

# 开发集群的 rpc 与管理端口上已经有人在听（多半是演示用的开发集群还在运行）→ 失败并提示 wmfs dev down。
m3_ports_free() {
    local p
    for p in 19100 19180; do
        if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
            echo "127.0.0.1:$p 已被占用：演示用的开发集群还在运行？请先 wmfs dev down，再跑 scripts/accept.sh M3"
            return 1
        fi
    done
    echo "19100、19180 都空闲"
}

m3_init_state() {
    tests_at_least 2 --release -p wmfs-core --lib init::tests || return 1
    cargo test --release -p wmfs-core --test golden_init 2>&1 | tee /dev/stderr | grep -q 'test result: ok. 1 passed'
}

m3_manifest() {
    one_test --release -p wmfs-core --lib meta::engine::engine::tests::manifest_toml_matches_code || return 1
    one_test --release -p wmfs-core --lib hooks::registry::tests::meta_engine_is_a_unique_hook_outside_the_cluster_list
}

m3_missing_key() {
    local out rc
    out="$(wmfs debug kv get no-such-key --json)"
    rc=$?
    printf '%s\n' "$out"
    echo "退出码 $rc"
    [ "$rc" -eq 1 ] && printf '%s' "$out" | jq -e '.found == false and .value == null'
}

# /metrics 里一个不带标签的指标的值（没有就是空）。
m3_metric() {
    curl --noproxy '*' -s "$M3_HTTP/metrics" | awk -v n="$1" '$1 == n { print $2 }'
}

m3_metrics() {
    local applied stale
    applied="$(m3_metric wmfs_raft_applied_index)"
    stale="$(m3_metric wmfs_meta_batch_stale_total)"
    echo "wmfs_raft_applied_index = $applied，wmfs_meta_batch_stale_total = $stale"
    [ -n "$applied" ] && [ "${applied%.*}" -gt 0 ] && [ "$stale" = "0" ]
}

m3_kill9() {
    wmfs dev kill 1 || return 1
    wmfs dev start 1 || return 1
    wmfs dev wait leader --timeout 30 || return 1
    json_expect '.found and .value == "world"' wmfs debug kv get hello --json || return 1
    json_expect '.found and (.value | startswith("v99999-"))' wmfs debug kv get bench-00099999 --json
}

m3_abort_before_commit() {
    local i
    for i in $(seq 1 20); do
        wmfs debug kv put "k$i" "v$i" --json | jq -e '.ok' >/dev/null || { echo "第 $i 个写没有确认"; return 1; }
    done
    echo "20 个写都已确认"
    wmfs failpoint set 1 raft.persist.before_commit abort || return 1
    # 这一次写让 raft 线程在第一个写事务提交之前 abort（CLI 会一直重试，所以只给它 5 s）
    timeout 5 wmfs debug kv put k21 v21 --json
    json_expect '.nodes[0].running == false' wmfs dev status --json || return 1
    wmfs dev start 1 || return 1
    wmfs dev wait leader --timeout 30 || return 1
    for i in $(seq 1 20); do
        wmfs debug kv get "k$i" --json | jq -e --arg v "v$i" '.found and .value == $v' >/dev/null ||
            { echo "确认过的 k$i 丢了"; return 1; }
    done
    echo "20 个确认过的写都在（k21 没有确认过，在不在都可以）"
    json_expect '.found and .value == "world"' wmfs debug kv get hello --json
}

m3_after_restarts() {
    local stale log
    stale="$(m3_metric wmfs_meta_batch_stale_total)"
    echo "wmfs_meta_batch_stale_total = $stale"
    [ "$stale" = "0" ] || return 1
    log="$WMFS_DEV_DIR/n1/logs/server.log"
    if grep -nE 'panicked|fatal|hs.commit .* is out of range|applied\(.*\) is out of range' "$log"; then
        echo "server.log 里有 panic / fatal（见上面）"
        return 1
    fi
    echo "server.log 里没有 panic / fatal"
}
