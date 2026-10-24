# M9 链式复制与故障恢复：逐条对应 roadmap §3 M9 的"验收标准"与"演示命令"，以及主管交代的第一步与留给 M9 的事项。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：写 2 GiB 过程中 kill -9 一个节点 → 写成功、md5 一致；该节点的 target 先被移出链，回来后 Syncing → Serving，wmfs status 的
#           差距归零；不变式检查器（静止后所有 Serving 副本的已提交版本与内容哈希一致）；寄存器负载的线性一致性检查通过；chain.*/sync.*
#           fail point 测试；整链重启与唯一幸存者（incarnation）测试；minimal 与 nas-safe 阶段版通过。另有第一步（ADR-0039 第 6 条的
#           逃生口规则、wmfs dev restart-all）、settings_prev（ADR-0038 第 5 条）、join 的新节点补进链与 drain 迁走链、gc 跳过失败的记录、
#           semantics.md 第 7 节的承诺（数据链重配约 8–10 s、最后的副本离线时链不可用并告警、force-serving）。
#
# 开发集群只用本次验收自己的目录（$LOGDIR/dev，节点 n 的端口 19n00/19n80，第 4 个节点从 join/drain 那一条起），挂载点 $LOGDIR/mnt，
# 2 GiB 的源文件也放在 $LOGDIR 里（不放 /tmp）。无论成败，退出前都 wmfs dev down（先卸载挂载点、再停节点），不留挂载、不留进程，
# 并删掉全部大文件——源文件、读回的副本、各节点 target 里的 chunk；失败时只留下日志（各条检查的输出与节点日志）作现场（m9_cleanup）。
# 只解析 --json 输出与退出码，例外三处：/metrics 的文本（重同步取回的字节数、链的指标）与节点日志里 fail point 触发的那一行；等待一律
# 按条件（wmfs dev wait、有上限的轮询）。演示里的 "sleep 1"（让写先开始）在这里是"等被看的节点已提交 400 MB"。
# 客户端停顿量的是数据面：一个活着的节点上已提交的数据多久不长（挂载点上看到的文件长度先在本地缓冲里涨，测出来偏小）。

# shellcheck source=/dev/null
source "$ROOT/scripts/accept.d/m6.sh"

accept_m9() {
    check "开发集群的端口 19100–19480 都空闲（演示用的开发集群要先 wmfs dev down）" m9_ports_free
    check "没有残留的 wmfs FUSE 挂载（有的话先 wmfs dev down --purge）" m6_no_mounts
    check "cargo build --release 成功且零警告" build_no_warnings
    check "第一步 ①（ADR-0039 第 6 条，真进程）：3 个节点带 --adopt-local-plugins、配置相同 → 收敛后都照常运行、清单等于本地清单；配置不同的那个在清单改好之后以退出码 3 退出、不会来回改清单" \
        tests_at_least 3 --release -p wmfs --test plugin_list
    check "单元：chunk 状态表（pending 占着 chunk）、链管理器的每一行（离线、重启降级、唯一幸存者、整链重启、last_serving、rejoin、10 s 转发失败、drain、修复、新 target、SyncDone、计时重置——听全了才处理重启、随机输入下 I0）、集群警告、I5 的 target 租约（持有方 0.8 倍先自停）、复制 lane（C-5）" m9_unit
    check "CRAQ 组件测试（3 个数据服务、mem 传输）：写到每个成员、NeedFull 与重同步、唯一幸存者 incarnation（C-8）、整链重启、3.4.2.6 的四条前滚规则、3.4.2.4 的后继处理表、同步源列出 pending 的版本、退回 control_scheme 时警告" \
        tests_at_least 8 --release -p wmfs-testkit --test craq
    check "CRAQ 组件测试（续，审查发现的边界情况）：同步期间写到中间的 Syncing 成员不丢 chunk、新成员等 0.8 × 租约才第一轮同步、规则 2 之后的重新驱动等后面的成员（C-9）、head 本地提交失败不让出版本号（I6）、I5 自我隔离（读与转发）、DeleteEpoch 沿链、多块 chunk 的部分覆盖与截断" \
        tests_at_least 7 --release -p wmfs-testkit --test craq_edges
    check "M1 交接 ②：call_cb 在后台建连（调用方立即返回，回调在别的线程上）" \
        one_test --release -p wmfs-testkit --test net_rpc call_cb_dials_in_the_background
    check "fail point chain.forward.drop（CHUNK_DIRTY 重定向到 last serving + 3 s 后重新驱动）、chain.ack.drop、chain.forward.delay 期间链变了（规则 4）、一直丢转发时等确认的写不占住 io 线程（C-7）" \
        one_test --release -p wmfs-testkit --test failpoint_chain chain_fail_points_are_rolled_forward
    check "fail point sync.fetch.midway：同步中途失败 → 重跑 → 一致" \
        one_test --release -p wmfs-testkit --test failpoint_sync a_sync_that_fails_midway_is_rerun_and_converges
    check "金样：settings_prev 与链/target 状态常量、链与 status（short_files）的管理 API JSON" m9_golden
    check "ADR-0038 第 5 条：调小租约时同一批次记下 settings_prev（每项取较大的），一个旧租约周期之后删掉" \
        one_test --release -p wmfs-testkit --test data_target shrinking_a_lease_records_the_previous_settings_until_one_old_lease_passes
    check "M5 留下的：gc 里一条记录的 DeleteEpoch 失败只跳过这一条、下一轮再试（不挡住整页；不可用的链不发请求、不等满总期限）" \
        one_test --release -p wmfs-testkit --test data_gc a_failing_delete_epoch_skips_only_its_own_record
    check "回归：单节点数据路径与进程内的多节点集群（4 个节点的 8 个文件现在都按 3 副本写）照样通过" m9_regress

    export WMFS_DEV_DIR="$LOGDIR/dev"
    M="$LOGDIR/mnt"
    mkdir -p "$M"
    CP_PID=""
    WRITER_PID=""
    trap m9_cleanup EXIT
    check "wmfs dev up --nodes 3（3 个元数据 voter）" wmfs dev up --nodes 3
    eval "$(wmfs dev env)"
    check "wmfs dev mount 一个挂载点" wmfs dev mount "$M"
    check_json "wmfs get ~/wmfs-mnt/home：replicas=3（来源 /home）" \
        '[.attrs[] | select(.key == "replicas")][0] | .value == "3" and .source == "/home"' wmfs get "$M/home" --json
    check "第一次建文件时生成链；wmfs chain list：每条链 3 个成员、全部 Serving（期望 3）" m9_chains_full
    check "演示：写 2 GiB 的途中 kill -9 节点 2 → WRITE-OK、md5 一致；最长停顿 ≤ 11 s（semantics.md：数据链重配约 8–10 s）" \
        m9_write_kill f 2 1 11000
    check_json "wmfs chain list：节点 2 的 target 已从各链移除（链降级为 2 个成员，记在 removed 里）" \
        'all(.chains[]; (.members | length) == 2 and all(.members[]; .target != 2) and (.removed | index([2])))' \
        wmfs chain list --json
    check "leader 的 /metrics：wmfs_chains_degraded = 3（3 条链都降级）" m9_leader_metric wmfs_chains_degraded 3
    check_json "wmfs status ~/wmfs-mnt/home：第一行起警告链降级；replicas 声明 3、实际 2 的文件数 > 0" \
        '(.warnings | any(test("少于声明副本数"))) and ([.attrs[] | select(.key == "replicas")][0] | .actual.short_files > 0 and .mismatched)' \
        wmfs status "$M/home" --json
    check "wmfs dev start 2：它的 target 先 Syncing、后 Serving（wmfs dev wait chains-serving），重同步从 last serving 取回了数据" m9_resync
    check_json "wmfs status ~/wmfs-mnt/home：差距归零" \
        '([.attrs[] | select(.key == "replicas")][0] | .actual.short_files == 0 and (.mismatched | not)) and (.warnings | any(test("少于声明副本数")) | not)' \
        wmfs status "$M/home" --json
    check "leader 的 /metrics：wmfs_chains_degraded = 0" m9_leader_metric wmfs_chains_degraded 0
    check "不变式检查器：静止后每条链所有 Serving 副本的已提交版本与内容（packed 形式的 crc32c）一致" \
        m9_ignored --release -p wmfs-testkit --test replica_check every_serving_replica_has_the_same_committed_versions_and_bytes
    check "md5 再核对一次（wmfs fs get 绕过页缓存，直接从数据节点读）" m9_md5_direct
    check "写 2 GiB 的途中 kill -9 元数据 leader 所在的节点 → WRITE-OK、md5 一致；最长停顿 ≤ 15 s（选举 1–3 s + 新 leader 就绪后一个 target 租约的计时重置 + 集群图传播；超出 semantics.md 的约 8–10 s，已上报）" \
        m9_kill_leader
    check "写 2 GiB 的途中冻住（SIGSTOP）元数据 leader 所在的节点 20 s → WRITE-OK、md5 一致；最长停顿 ≤ 16 s（客户端的元数据请求超时之后换节点，不干等冻住的 leader）" \
        m9_pause_leader
    check "ADR-0038 第 5 条：调小 target_lease 之后立刻冻结一个数据节点（SIGSTOP），它在旧租约（8 s）期满之前不被移出链" m9_settings_prev
    check "fail point chain.head.crash_after_pending（abort）：head 写完 pending 之后崩溃 → 写照样成功（存活成员前滚）；它重启之后以 Syncing 回链、再 Serving；副本一致" m9_head_crash
    check "Jepsen-lite：寄存器（单个 chunk，DataClient 直接读写）在随机 kill -9 / SIGSTOP 下线性一致（Wing-Gong；失败时打印种子）" \
        m9_ignored --release -p wmfs-testkit --test jepsen_register a_register_on_one_chunk_stays_linearizable_while_nodes_die_and_freeze
    check "join 的新节点补进链：dev up --nodes 4 → 生成以 t4 为 head 的链；1 副本的链最后的副本离线 → 链不可用、告警「等待最后的副本」、不拿别的顶替；回来之后可用；wmfs node drain 4 → 先追加替代成员、Serving 之后再移除 t4" \
        m9_join_drain
    check "wmfs chain force-serving：不加 --i-accept-data-loss 拒绝；加了 → 指定的 target 成为唯一 Serving、incarnation + 1、写 changelog ChainForceServing；其余重新同步" \
        m9_force_serving
    check "整链重启（wmfs dev down + up：全部节点同时重启）：按链序最后一个原 Serving 成员保持 Serving、incarnation + 1，其余重新同步（链上两个 Syncing 成员，重同步期间一直写新 chunk）；不变式检查器一致；文件内容不变" \
        m9_whole_restart
    check "第一步 ②（3.7.2 第 4 条）：cluster plugins remove 之后改好 node.toml，wmfs dev restart-all 之后集群恢复；add 回来、改回 node.toml，再 restart-all 又恢复" \
        m9_restart_all
    check "wmfs dev down（先卸载挂载点，再停全部节点）" wmfs dev down
    check "结束：本次验收自己的挂载都已卸载（$LOGDIR 下没有 fuse.wmfs 挂载）" m6_no_own_mounts
    m9_rm_big
    check "profile minimal（scripts/profiles.sh minimal）" scripts/profiles.sh minimal
    check "profile nas-safe 阶段版（3 节点、replicas=3：写入中依次 kill -9 每个节点 0 个错误、fsync 过的文件全部完好；Jepsen-lite）" \
        scripts/profiles.sh nas-safe
    skip "profile hpc-rdma 阶段版（RDMA + replicas=2）" "本机没有可用的 RDMA 设备（人类决定本机不做 RDMA 实机测试）"
}

# 不论成败：结束后台的 cp 与写者、停开发集群（它先卸载 dev mount 的挂载点）、删掉全部大文件。
m9_cleanup() {
    [ -n "${CP_PID:-}" ] && kill "$CP_PID" 2>/dev/null
    [ -n "${WRITER_PID:-}" ] && kill "$WRITER_PID" 2>/dev/null
    wmfs dev down >/dev/null 2>&1
    m9_rm_big
}

# 删掉大文件：源文件与读回的副本、本次验收自己的开发集群各节点 target 里的 chunk（n*/logs 留着作现场）。
m9_rm_big() {
    local f
    for f in src2g f.back w.back crash.src crash.back single.src single.back; do
        rm -f "$LOGDIR/$f"
    done
    if [ -n "$LOGDIR" ] && [ "${WMFS_DEV_DIR:-}" = "$LOGDIR/dev" ]; then
        rm -rf "$WMFS_DEV_DIR"/n*/targets
    fi
}

m9_ports_free() {
    local p
    for p in 19100 19180 19200 19280 19300 19380 19400 19480; do
        if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
            echo "127.0.0.1:$p 已被占用：演示用的开发集群还在运行？请先 wmfs dev down，再跑 scripts/accept.sh M9"
            return 1
        fi
    done
    echo "19100–19480 都空闲"
}

m9_unit() {
    tests_at_least 5 --release -p wmfs-core --lib data::state || return 1
    tests_at_least 14 --release -p wmfs-core --lib meta::cluster::chain_plan || return 1
    one_test --release -p wmfs-core --lib meta::cluster::chains::tests::warnings_cover_degraded_and_unavailable_chains_and_transport_fallback || return 1
    one_test --release -p wmfs-core --lib data::chain::tests::the_target_lease_fences_itself_first || return 1
    one_test --release -p wmfs-core --lib proto::frame::tests::peer_call_bulk_comes_from_the_replication_lane
}

m9_golden() {
    cargo test --release -p wmfs-core --test golden_constants || return 1
    tests_at_least 1 --release -p wmfs-api types::cluster::tests::chains_json_golden || return 1
    tests_at_least 1 --release -p wmfs-api types::mgmt
}

m9_regress() {
    tests_at_least 7 --release -p wmfs-testkit --test data_e2e || return 1
    tests_at_least 8 --release -p wmfs-testkit --test meta_cluster
}

# 跑一个 #[ignore] 的用例（对着本次验收的开发集群），并确认它真的跑了。
m9_ignored() {
    local out
    out="$(cargo test "$@" -- --exact --ignored --nocapture 2>&1)"
    printf '%s\n' "$out"
    printf '%s' "$out" | grep -q 'test result: ok. 1 passed'
}

m9_chains_full() {
    echo hello >"$M/home/hello" || return 1
    json_expect '(.chains | length) == 3 and all(.chains[]; (.members | length) == 3 and .want == 3 and all(.members[]; .state == "serving"))' \
        wmfs chain list --json
}

# 节点 n 的 target 目录里已提交数据的字节数。
m9_bytes() {
    du -sb "$WMFS_DEV_DIR/n$1/targets/1/c" 2>/dev/null | awk '{print $1}'
}

# 就绪的元数据 leader 的 /metrics 里不带标签的指标 $1 变成 $2（链管理器每 200 ms 刷新一次：最多等 5 s）。
m9_leader_metric() {
    local l v i
    for i in $(seq 1 50); do
        l="$(wmfs dev leader)" || return 1
        v="$(curl --noproxy '*' -s "http://127.0.0.1:19${l}80/metrics" | awk -v n="$1" '$1 == n { print $2 }')"
        [ "${v:-}" = "$2" ] && break
        sleep 0.1
    done
    echo "node$l（leader）：$1 = $v（期望 $2）"
    [ "${v:-}" = "$2" ]
}

# 后台把 2 GiB 的源文件写到挂载点上的 /home/$1，写到一半时对节点 $2 做 $5（kill：kill -9；pause：冻住 $6 秒再让它继续）；在节点 $3 上量
# 已提交的数据多久不长（见文件头），最长停顿超过 $4 ms 判失败；最后 md5 要一致。
m9_write_fault() {
    local f="$1" victim="$2" watch="$3" limit="$4" how="$5" secs="${6:-0}" out="$LOGDIR/cp-$1.out" base b now prev last max=0 i t0
    [ -s "$LOGDIR/src2g" ] || { head -c 2G /dev/urandom >"$LOGDIR/src2g" && md5sum "$LOGDIR/src2g" | awk '{print $1}' >"$LOGDIR/src2g.md5"; } || return 1
    base="$(m9_bytes "$watch")"
    (cp "$LOGDIR/src2g" "$M/home/$f" && echo WRITE-OK) >"$out" 2>&1 &
    CP_PID=$!
    for i in $(seq 1 600); do # 等写先开始：被看的节点已提交 400 MB，最多 60 s
        [ $(($(m9_bytes "$watch") - base)) -gt 400000000 ] && break
        sleep 0.1
    done
    echo "$how 节点 $victim（这时节点 $watch 已提交了 $((($(m9_bytes "$watch") - base) >> 20)) MiB）"
    if [ "$how" = kill ]; then wmfs dev kill "$victim" || return 1; else wmfs dev pause "$victim" || return 1; fi
    t0="${EPOCHREALTIME//[!0-9]/}"
    prev="$(m9_bytes "$watch")"
    last="$t0"
    while kill -0 "$CP_PID" 2>/dev/null; do
        b="$(m9_bytes "$watch")"
        now="${EPOCHREALTIME//[!0-9]/}"
        if [ "$how" = pause ] && [ -n "$t0" ] && [ $((now - t0)) -ge $((secs * 1000000)) ]; then
            wmfs dev resume "$victim" || return 1
            t0=""
        fi
        if [ "$b" -gt "$prev" ]; then
            [ $((now - last)) -gt "$max" ] && max=$((now - last))
            last="$now"
            prev="$b"
        fi
        sleep 0.05
    done
    [ -n "$t0" ] && [ "$how" = pause ] && wmfs dev resume "$victim"
    wait "$CP_PID"
    CP_PID=""
    cat "$out"
    echo "最长停顿 $((max / 1000)) ms（节点 $watch 上已提交的数据多久不长；判据 ≤ $limit ms）"
    grep -qx WRITE-OK "$out" && [ "$max" -le $((limit * 1000)) ] || return 1
    echo "md5：源 $(cat "$LOGDIR/src2g.md5")，挂载点上 $(md5sum "$M/home/$f" | awk '{print $1}')"
    [ "$(md5sum "$M/home/$f" | awk '{print $1}')" = "$(cat "$LOGDIR/src2g.md5")" ]
}

# 演示的那一条：杀的是 follower（节点 2 恰好是元数据 leader 时先让它重启一次——SIGTERM 时它把 leader 让出去——杀 leader 另有一条）。
m9_write_kill() {
    if [ "$(wmfs dev leader)" = "$2" ]; then
        wmfs dev stop "$2" && wmfs dev start "$2" && wmfs dev wait leader && wmfs dev wait chains-serving --timeout 120 || return 1
    fi
    m9_write_fault "$1" "$2" "$3" "$4" kill
}

# 杀元数据 leader 所在的节点（它也在每条链上）；回来之后链恢复。写完删掉这个文件（省磁盘）。
m9_kill_leader() {
    local l
    l="$(wmfs dev leader)" || return 1
    m9_write_fault g "$l" $((l % 3 + 1)) 15000 kill || return 1
    rm -f "$M/home/g"
    wmfs dev start "$l" && wmfs dev wait chains-serving --timeout 120
}

m9_pause_leader() {
    local l
    l="$(wmfs dev leader)" || return 1
    m9_write_fault g "$l" $((l % 3 + 1)) 16000 pause 20 || return 1
    rm -f "$M/home/g"
    wmfs dev wait chains-serving --timeout 120
}

m9_resync() {
    local i seen=0 st
    wmfs dev start 2 || return 1
    for i in $(seq 1 600); do # 最多 60 s：先看到 Syncing
        st="$(wmfs chain list --json | jq -r '[.chains[].members[] | select(.target == 2) | .state] | join(",")')"
        echo "$st" | grep -q syncing && { seen=1; break; }
        sleep 0.1
    done
    echo "节点 2 的 target 在各链上：$st"
    [ "$seen" = 1 ] || return 1
    wmfs dev wait chains-serving --timeout 120 || return 1
    json_expect 'all(.chains[]; (.members | length) == 3 and all(.members[]; .state == "serving"))' wmfs chain list --json || return 1
    local b
    b="$(curl --noproxy '*' -s http://127.0.0.1:19280/metrics | awk '$1 == "wmfs_resync_bytes_total" { print $2 }')"
    echo "节点 2 重同步取回 $b 字节"
    [ "${b:-0}" -gt 1000000000 ]
}

m9_md5_direct() {
    wmfs fs get /home/f "$LOGDIR/f.back" || return 1
    [ "$(md5sum "$LOGDIR/f.back" | awk '{print $1}')" = "$(cat "$LOGDIR/src2g.md5")" ] || return 1
    rm -f "$LOGDIR/f.back"
}

# 开发节点 n 的 target 还在不在某条链上（json：true/false）。
m9_in_chains() {
    wmfs chain list --json | jq --argjson t "$1" '[.chains[].members[] | select(.target == $t)] | length > 0'
}

m9_settings_prev() {
    local L F t0 i gone=""
    L="$(wmfs dev leader)" || return 1
    F=$((L % 3 + 1)) # 冻一个不是 leader 的节点（冻 leader 会先换 leader，不是这里要看的）
    wmfs cluster set target_lease=3s --json || return 1
    t0="${EPOCHREALTIME//[!0-9]/}"
    wmfs dev pause "$F" || return 1
    for i in $(seq 1 300); do # 最多 30 s
        if [ "$(m9_in_chains "$F")" = "false" ]; then
            gone=$(((${EPOCHREALTIME//[!0-9]/} - t0) / 1000))
            break
        fi
        sleep 0.1
    done
    wmfs dev resume "$F" || return 1
    echo "调小 target_lease 8s → 3s 之后冻住节点 $F：$gone ms 后被移出链（新租约 3 s；旧租约 8 s 期满之前不移出）"
    [ -n "$gone" ] && [ "$gone" -ge 7000 ] || return 1
    wmfs cluster set target_lease=8s --json || return 1
    wmfs dev wait chains-serving --timeout 120
}

m9_head_crash() {
    local t n hits
    rm -f "$M/home/crash"
    touch "$M/home/crash" || return 1
    t="$(wmfs fs stat /home/crash --json | jq -r '.chains[0].members[0]')" # 这个文件所在链的 head（开发集群里 target n 在节点 n 上）
    n="$t"
    echo "文件 /home/crash 的链的 head 是 target $t（节点 $n）"
    wmfs failpoint set "$n" chain.head.crash_after_pending 'times(1,abort)' || return 1
    head -c 20M /dev/urandom >"$LOGDIR/crash.src" || return 1
    cp "$LOGDIR/crash.src" "$M/home/crash" || return 1 # head 写完第一个 pending 就 abort：客户端等链重配、换 head 重试
    wmfs dev status --json | jq -e --argjson n "$n" '.nodes[] | select(.node == $n) | .running | not' || return 1
    grep -q "chain.head.crash_after_pending" "$WMFS_DEV_DIR/n$n/logs/server.log" || return 1
    cmp "$LOGDIR/crash.src" "$M/home/crash" || return 1
    wmfs dev start "$n" || return 1
    wmfs dev wait chains-serving --timeout 120 || return 1
    wmfs fs get /home/crash "$LOGDIR/crash.back" && cmp "$LOGDIR/crash.src" "$LOGDIR/crash.back" || return 1
    rm -f "$LOGDIR/crash.src" "$LOGDIR/crash.back"
    m9_ignored --release -p wmfs-testkit --test replica_check every_serving_replica_has_the_same_committed_versions_and_bytes
}

m9_join_drain() {
    local c4 f i chain
    wmfs dev up --nodes 4 || return 1
    for i in $(seq 1 300); do # 最多 30 s：node4 的第一次心跳到了 leader、链管理器下一轮（200 ms）就为它生成链
        c4="$(wmfs chain list --json | jq '[.chains[] | select(.members[0].target == 4)] | length')"
        [ "${c4:-0}" -ge 1 ] && break
        sleep 0.1
    done
    echo "以 t4 为 head 的链：$c4 条"
    [ "${c4:-0}" -ge 1 ] || return 1
    wmfs dev wait chains-serving --timeout 120 || return 1
    # 1 副本的链：最后的副本离线 → 链不可用（不拿别的副本顶替）、告警；回来之后可用
    mkdir -p "$M/single" && wmfs set "$M/single" replicas=1 --json || return 1
    head -c 200K /dev/urandom >"$LOGDIR/single.src" || return 1 # 超过 64 KiB：不是 inline，内容在链上
    for i in $(seq 1 16); do
        cp "$LOGDIR/single.src" "$M/single/s$i" || return 1
    done
    for i in $(seq 1 16); do
        chain="$(wmfs fs stat "/single/s$i" --json | jq -r '.chains[0] | select(.members == [4]) | .id')"
        [ -n "$chain" ] && { f="s$i"; break; }
    done
    if [ -z "${f:-}" ]; then
        echo "16 个 1 副本的文件都没落在 [t4] 上（放置按 ino 打散）"
        return 1
    fi
    echo "文件 /single/$f 在 1 副本的链 $chain = [t4] 上"
    wmfs dev kill 4 || return 1
    for i in $(seq 1 300); do
        wmfs chain list --json | jq -e --argjson c "$chain" '.chains[] | select(.id == $c) | (.members | length) == 0 and .last_serving == 4' >/dev/null && break
        sleep 0.1
    done
    json_expect "(.warnings | any(test(\"链 $chain 等待最后的副本 target 4\")))" wmfs status /single --json || return 1
    if timeout 5 wmfs fs cat "/single/$f" >/dev/null 2>&1; then
        echo "最后的副本不在：读应该等待重试，不能成功"
        return 1
    fi
    wmfs dev start 4 || return 1
    wmfs dev wait chains-serving --timeout 120 || return 1
    wmfs fs get "/single/$f" "$LOGDIR/single.back" && cmp "$LOGDIR/single.src" "$LOGDIR/single.back" || return 1
    json_expect ".chains[] | select(.id == $chain) | .incarnation == 2 and .last_serving == null" wmfs chain list --json || return 1
    # drain：先为相关链追加替代成员（Syncing），等它们 Serving 后再移除 t4
    wmfs node drain 4 --json || return 1
    for i in $(seq 1 1200); do # 最多 120 s
        [ "$(m9_in_chains 4)" = "false" ] && break
        sleep 0.1
    done
    json_expect '[.chains[].members[] | select(.target == 4)] | length == 0' wmfs chain list --json || return 1
    json_expect '[.chains[] | select(.replicas == 3) | select((.members | length) < 3)] | length == 0' wmfs chain list --json || return 1
    [ "$(wmfs fs cat /home/hello)" = "hello" ]
}

m9_force_serving() {
    local before after
    before="$(wmfs chain show 1 --json | jq '.incarnation')"
    expect_error einval wmfs chain force-serving 1 1 --json || return 1
    wmfs chain force-serving 1 1 --i-accept-data-loss --json || return 1
    after="$(wmfs chain show 1 --json | jq '.incarnation')"
    echo "chain 1 的 incarnation：$before → $after"
    [ "$after" -eq $((before + 1)) ] || return 1
    json_expect '.members[0] == {"target": 1, "node": 1, "state": "serving"}' wmfs chain show 1 --json || return 1
    wmfs changelog tail -n 1000 --json | jq -e -s '[.[].events[] | select(.op == "ChainForceServing")] | length >= 1' || return 1
    wmfs dev wait chains-serving --timeout 120
}

# 整链重启之后链是 [幸存者 Serving, Syncing, Syncing]：重同步期间一直往 /home/w 写新的 4 MiB chunk（中间那个 Syncing 成员收到后继的
# 确认、本地提交的新 chunk 不能被这一轮同步删掉，审查发现的 blocker），链全部 Serving 之后停写、跑不变式检查器、读回比对。
m9_whole_restart() {
    local inc_before inc_after n
    inc_before="$(wmfs chain list --json | jq '[.chains[].incarnation] | add')"
    wmfs dev down || return 1
    wmfs dev up --nodes 4 || return 1
    eval "$(wmfs dev env)"
    wmfs dev mount "$M" || return 1
    rm -f "$LOGDIR/stop-writer"
    (
        i=0
        while [ ! -e "$LOGDIR/stop-writer" ] && [ "$i" -lt 400 ]; do
            dd if="$LOGDIR/src2g" of="$M/home/w" bs=4M count=1 skip="$i" seek="$i" conv=notrunc status=none || exit 1
            i=$((i + 1))
        done
    ) &
    WRITER_PID=$!
    wmfs dev wait chains-serving --timeout 120 || return 1
    touch "$LOGDIR/stop-writer"
    wait "$WRITER_PID" || { WRITER_PID=""; echo "重同步期间的写失败了"; return 1; }
    WRITER_PID=""
    inc_after="$(wmfs chain list --json | jq '[.chains[].incarnation] | add')"
    echo "incarnation 之和：$inc_before → $inc_after"
    [ "$inc_after" -gt "$inc_before" ] || return 1
    m9_ignored --release -p wmfs-testkit --test replica_check every_serving_replica_has_the_same_committed_versions_and_bytes || return 1
    n="$(stat -c %s "$M/home/w")"
    wmfs fs get /home/w "$LOGDIR/w.back" || return 1
    echo "重同步期间写了 $((n >> 20)) MiB；读回比对"
    cmp -n "$n" "$LOGDIR/src2g" "$LOGDIR/w.back" || return 1
    rm -f "$LOGDIR/w.back"
    [ "$(md5sum "$M/home/f" | awk '{print $1}')" = "$(cat "$LOGDIR/src2g.md5")" ]
}

# 改各节点 node.toml 的 [plugins] disable（$1 是 TOML 数组的内容，例如 '"gc"' 或 ''）。
m9_disable() {
    local n
    for n in 1 2 3 4; do
        sed -i "s/^disable = \[.*\]$/disable = [$1]/" "$WMFS_DEV_DIR/n$n/node.toml" || return 1
    done
}

# 等 3 个元数据节点都退出（插件清单不一致：退出码 3），最多 20 s。只做数据的 node4 不一定退出：元数据节点清单不一致时不再是就绪的
# leader，它拿不到新的集群图（也续不了租约：自我隔离），restart-all 照样按新的 node.toml 重启它。
m9_all_exited() {
    local i
    for i in $(seq 1 200); do
        wmfs dev status --json | jq -e 'all(.nodes[] | select(.node <= 3); .running | not)' >/dev/null && return 0
        sleep 0.1
    done
    wmfs dev status --json
    return 1
}

m9_restart_all() {
    wmfs dev umount "$M" || return 1
    wmfs cluster plugins remove bg_task:gc --json || return 1
    m9_all_exited || return 1
    m9_disable '"gc"' || return 1
    wmfs dev restart-all || return 1
    json_expect '.plugins | index("bg_task:gc") | not' wmfs cluster plugins show --json || return 1
    wmfs dev wait chains-serving --timeout 120 || return 1
    wmfs cluster plugins add bg_task:gc --json || return 1
    m9_all_exited || return 1
    m9_disable '' || return 1
    wmfs dev restart-all || return 1
    json_expect '.plugins | index("bg_task:gc")' wmfs cluster plugins show --json || return 1
    wmfs dev status --json | jq -e 'all(.nodes[]; .running and .healthy)' || return 1
    wmfs dev wait chains-serving --timeout 120
}
