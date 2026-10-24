# M8 元数据三副本：逐条对应 roadmap §3 M8 的"验收标准"与"演示命令"。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：3 个 voter；3000 次 mkdir 循环中 kill -9 leader → 全部成功；被杀节点重启后追平；强制小压缩阈值触发快照安装；连续 10 轮
#           杀 leader；3 节点下提交 900 KiB 写集的批次成功；插件配置不同的节点 join 被拒绝并列出差异；确定性 raft 模拟全套（1000 个种子，
#           内存后端）。另有第一步（ADR-0038 第 7 条：读句柄重新取 layout 用 ReopenHandle）、留给 M8 的几件（uninline 的数据面 fencing、
#           插件清单不一致时退出码 3 与逃生口、管理 API 只在 leader 上回答、changelog 长轮询失去 leader 时立即返回、SIGTERM 时先让出
#           leader、附录 A 的 P11/P13/P17）与演示命令。
#
# 开发集群只用本次验收自己的目录（$LOGDIR/dev，3 个节点，端口 19100/19180、19200/19280、19300/19380），挂载点 $LOGDIR/mnt：通过时随
# 日志一起删除，失败时保留；无论成败，退出前都 wmfs dev down（先卸载 dev mount 的挂载点、再停节点），不留挂载、不留进程（m8_cleanup）。
# 只解析 --json 输出与退出码，例外四处（检查的正是给人看的输出）：join 被拒时列出的插件差异、节点日志里"日志压缩到""安装了快照"
# "下载快照失败""进程退出（退出码 3）"的行、演示里 mkdir 循环打印的 DONE/FAIL（以及它自己量的最长停顿 MAXGAP_US）。
# 等待一律按条件：wmfs dev up/start（等就绪）、wmfs dev wait leader、wmfs debug meta-hash（等各节点 applied 相同再比，最多 30 s）、
# 有上限的轮询（目录出现、指标变化）。演示里的 "sleep 2"（让 mkdir 循环先跑起来）在这里是"等循环建出第 100 个目录"。

# 复用 M6 的两个挂载检查（m6_no_mounts、m6_no_own_mounts）：只跑 M8 时 accept.sh 不会加载 m6.sh
# shellcheck source=/dev/null
source "$ROOT/scripts/accept.d/m6.sh"

accept_m8() {
    check "开发集群的端口 19100/19180、19200/19280、19300/19380 都空闲（演示用的开发集群要先 wmfs dev down）" m8_ports_free
    check "没有残留的 wmfs FUSE 挂载（有的话先 wmfs dev down --purge，或 fusermount3 -uz <挂载点>）" m6_no_mounts
    check "cargo build --release 成功且零警告" build_no_warnings
    check "第一步（ADR-0038 第 7 条）：读句柄碰上 layout 变化用 ReopenHandle{write:false}——属主 chmod 0200 之后照常读到新内容（chunk 与 inline）；非属主 chmod o-r 之后 EACCES" \
        one_test --release -p wmfs-testkit --test client_reopen read_handles_reopen_by_handle_after_a_layout_change
    check "单元：快照的导出与 pin 复用（MVCC；接收方要在 pin 的成员配置里）、发送记录与 60 s 超时、快照文件的往返/损坏检测/字节布局" \
        tests_at_least 4 --release -p wmfs-core --lib meta::raft::snapshot
    check "单元：成员变更的决定函数（learner 加入、一次 joint 提升为 3 voter——追上要求确认过数据、移除、drain 让出 leader——接替的人要活着且追上、记录同步、joint 超过 5 s 的兜底）与插件清单的比对" m8_unit
    check "金样：快照文件 WMFSSNP1…WMFSEND!（小端）、快照流消息、集群/节点/插件清单/meta-hash 的管理 API JSON" m8_golden
    check "附录 A：P11（快照下载失败 → 报告 → 重发、装上）、P13（Explicit 的 joint 无人退出 → 空 ConfChangeV2 兜底）、P17/规则 M5（装完快照掉电）" \
        tests_at_least 3 --release -p wmfs-testkit --test raft_pitfalls_m8
    check "附录 A 以前的回归（单节点与成员变更的坑）照样通过" tests_at_least 12 --release -p wmfs-testkit --test raft_pitfalls
    check "确定性 raft 模拟（单节点部分，1000 个种子，内存后端）" \
        env WMFS_SIM_SEEDS=1000 cargo test --release -p wmfs-testkit --test raft_sim
    check "确定性 raft 模拟（3 节点，1000 个种子 × 400 步：掉电、丢消息、分区、重排、换 leader、成员变更、小压缩阈值下的快照；用时见日志）" m8_sim
    check "3 节点（同一进程）：P12 900 KiB 写集提交、插件不同的 join 被拒绝、follower 回 409 与长轮询在失去 leader 时立即返回（409 带 leader 地址）、停 leader 时先让位且客户端跟随、promote/remove、只做数据的节点找得到 leader、drain 的 leader 不让给死掉的节点、join 的重试拿回原来的节点" \
        tests_at_least 8 --release -p wmfs-testkit --test meta_cluster
    check "ADR-0035 第 11 条：旧 leader 迟到的 uninline 写落进作废的 epoch，换 leader 之后写的新数据不被覆盖，旧意图有 GC 记录" \
        one_test --release -p wmfs-testkit --test failpoint_uninline_fence a_stale_leaders_late_uninline_write_lands_in_a_dead_epoch
    check "uninline 崩溃在意图之后、切换之前：读照旧读 INLINE，下次 uninline 换新 epoch、旧意图记为垃圾" \
        tests_at_least 1 --release -p wmfs-testkit --test failpoint_uninline_crash
    check "插件清单（真的 wmfs server 进程）：cluster plugins add 之后退出码 3、日志列出差异与修法；照原样再启动也是 3；--adopt-local-plugins 改回本地清单" \
        one_test --release -p wmfs --test plugin_list a_changed_plugin_list_stops_the_node_with_exit_code_3_until_it_adopts_its_local_plugins

    export WMFS_DEV_DIR="$LOGDIR/dev"
    M="$LOGDIR/mnt"
    mkdir -p "$M"
    LOOP_PID=""
    trap m8_cleanup EXIT
    check "wmfs dev up --nodes 3（第 2、3 个节点 join 进来，等到 3 个元数据 voter）" wmfs dev up --nodes 3
    eval "$(wmfs dev env)"
    check_json "wmfs node list：3 个 voter（不在 joint 里）、恰好一个 leader、3 个节点都追平" \
        '(.voters | length) == 3 and .voters_outgoing == [] and ([.nodes[] | select(.leader)] | length) == 1
         and all(.nodes[]; .member == "voter" and .caught_up)' wmfs node list --json
    check_json "wmfs cluster plugins show：每个节点都必须启用的插件清单（钩子:名字）" \
        '.plugins | index("authn:shared-key") and index("authz:posix-mode")' wmfs cluster plugins show --json
    check "wmfs dev mount 一个挂载点" wmfs dev mount "$M"
    check "3000 次 mkdir 的循环中 kill -9 leader：只看到 DONE、没有 FAIL，最长停顿不超过 5 s（规格 1–3 s）；换了新 leader、term 变大；ls 数出 3000" \
        m8_mkdir_kill
    check "被杀的节点 wmfs dev start 回来：当 follower 并追平（wmfs debug meta-hash：3 个节点 applied 相同、哈希相同）" m8_restart
    check "CLI 多地址：node1 不在时 wmfs node list / cluster plugins show 换到活着的节点" m8_cli_failover
    check "连续 10 轮 kill -9 leader：每轮等到新 leader、经挂载点写入并读回（元数据操作，客户端跟随），最后 3 个节点一致" m8_ten_rounds
    check "强制小压缩阈值（fail point raft.compact.small=return(50)）：停掉的节点回来时 leader 的日志已压缩过它，它装快照追上；它停着时删掉的 200 个目录装完之后也不在了（规则 M5 先删 kv 表）；第一次下载中途失败（snapshot.fetch.midway）→ 报告、重发、装上（指标、日志、meta-hash）" \
        m8_snapshot_install
    check "插件配置不同的节点 wmfs join 被拒绝：退出码 3，列出差异（本节点缺少 authz:posix-mode），没有建出节点" m8_join_rejected
    check_json "wmfs debug meta-hash：3 个节点一致" '.consistent == true and (.nodes | length) == 3 and .unreachable == []' \
        wmfs debug meta-hash --json
    check "cluster plugins add 之后 3 个节点（leader 与 follower）都以退出码 3 退出，日志列出差异（先卸载挂载点）" m8_plugins_exit_all
    check "wmfs dev down（先卸载挂载点，再停 3 个节点）" wmfs dev down
    check "结束：本次验收自己的挂载都已卸载（$LOGDIR 下没有 fuse.wmfs 挂载）" m6_no_own_mounts
}

# 不论成败：结束 mkdir 循环、停开发集群（它先卸载 dev mount 的挂载点）。
m8_cleanup() {
    [ -n "${LOOP_PID:-}" ] && kill "$LOOP_PID" 2>/dev/null
    wmfs dev down >/dev/null 2>&1
}

m8_ports_free() {
    local p
    for p in 19100 19180 19200 19280 19300 19380; do
        if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
            echo "127.0.0.1:$p 已被占用：演示用的开发集群还在运行？请先 wmfs dev down，再跑 scripts/accept.sh M8"
            return 1
        fi
    done
    echo "19100/19180、19200/19280、19300/19380 都空闲"
}

m8_unit() {
    tests_at_least 7 --release -p wmfs-core --lib meta::raft::membership || return 1
    tests_at_least 1 --release -p wmfs-core --lib meta::cluster::plugins || return 1
    one_test --release -p wmfs-core --lib meta::raft::storage::tests::snapshot_exports_the_applied_state_as_a_desc
}

m8_golden() {
    cargo test --release -p wmfs-core --test golden_formats || return 1
    cargo test --release -p wmfs-core --test golden_proto || return 1
    tests_at_least 1 --release -p wmfs-api types::cluster
}

m8_sim() {
    local out rc
    out="$(WMFS_SIM_SEEDS=1000 cargo test --release -p wmfs-testkit --test raft_sim_cluster -- --nocapture 2>&1)"
    rc=$?
    printf '%s\n' "$out"
    [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -q '^1000 个种子' && printf '%s' "$out" | grep -q 'test result: ok. 1 passed'
}

# 某个开发节点 /metrics 里一个不带标签的指标的值（没有就是空）。
m8_metric() {
    curl --noproxy '*' -s "http://127.0.0.1:19${1}80/metrics" | awk -v n="$2" '$1 == n { print $2 }'
}

m8_term() {
    wmfs node list --json | jq -r '.term'
}

# 某个开发节点上一个 fail point 的命中次数（/metrics 的 wmfs_failpoint_hits_total{name="…"}；没有就是空）。
m8_fp_hits() {
    curl --noproxy '*' -s "http://127.0.0.1:19${1}80/metrics" |
        awk -v n="wmfs_failpoint_hits_total{name=\"$2\"}" '$1 == n { print $2 }'
}

m8_mkdir_kill() {
    local out="$LOGDIR/mkdir.out" i L L2 t1 t2 n gap
    t1="$(m8_term)" || return 1
    (
        # 顺带量相邻两次 mkdir 之间最长的停顿（微秒；EPOCHREALTIME 去掉小数点，不 fork）：kill -9 leader 时只停顿 1–3 s
        prev="${EPOCHREALTIME//[!0-9]/}" max=0
        for i in $(seq 1 3000); do
            mkdir -p "$M/m/d$i" || echo "FAIL $i"
            now="${EPOCHREALTIME//[!0-9]/}"
            [ $((now - prev)) -gt "$max" ] && max=$((now - prev))
            prev="$now"
        done
        echo "MAXGAP_US $max"
        echo DONE
    ) >"$out" 2>&1 &
    LOOP_PID=$!
    for i in $(seq 1 300); do # 等循环先跑起来（演示里的 sleep 2），最多 30 s
        [ -d "$M/m/d100" ] && break
        sleep 0.1
    done
    L="$(wmfs dev leader)" || return 1
    echo "$L" >"$LOGDIR/killed"
    echo "kill -9 leader node$L（这时循环已经建出 $(find "$M/m" -mindepth 1 -maxdepth 1 | wc -l) 个目录）"
    wmfs dev kill "$L" || return 1
    wait "$LOOP_PID"
    LOOP_PID=""
    cat "$out"
    grep -q FAIL "$out" && return 1
    grep -qx DONE "$out" || return 1
    gap="$(awk '$1 == "MAXGAP_US" { print $2 }' "$out")"
    echo "相邻两次 mkdir 之间最长停顿 $((gap / 1000)) ms（规格 1–3 s；超过 5 s 判失败，留一点余量）"
    [ "$gap" -le 5000000 ] || return 1
    wmfs dev wait leader --timeout 30 || return 1
    L2="$(wmfs dev leader)" || return 1
    t2="$(m8_term)" || return 1
    echo "新 leader node$L2；term $t1 → $t2"
    [ "$L2" != "$L" ] && [ "$t2" -gt "$t1" ] || return 1
    n="$(find "$M/m" -mindepth 1 -maxdepth 1 -type d | wc -l)"
    echo "ls 数出 $n 个目录"
    [ "$n" -eq 3000 ]
}

m8_restart() {
    local L
    L="$(cat "$LOGDIR/killed")" || return 1
    wmfs dev start "$L" || return 1
    wmfs dev wait leader --timeout 30 || return 1
    json_expect '.consistent == true and (.nodes | length) == 3' wmfs debug meta-hash --json || return 1
    json_expect ".nodes[] | select(.node_id == $L) | .member == \"voter\" and .caught_up and (.leader | not)" wmfs node list --json
}

m8_cli_failover() {
    wmfs dev kill 1 || return 1
    wmfs dev wait leader --timeout 30 || return 1
    echo "WMFS_CLUSTER=$WMFS_CLUSTER（第一个地址的 node1 已经停了）"
    json_expect '(.voters | length) == 3 and .leader != 1' wmfs node list --json || return 1
    json_expect '.plugins | length >= 3' wmfs cluster plugins show --json || return 1
    wmfs dev start 1 && wmfs dev wait leader --timeout 30
}

m8_ten_rounds() {
    local r L
    for r in $(seq 1 10); do
        L="$(wmfs dev leader)" || return 1
        echo "== 第 $r 轮：kill -9 leader node$L（term $(m8_term)）"
        wmfs dev kill "$L" || return 1
        wmfs dev wait leader --timeout 30 || return 1
        # 只做元数据操作（数据的多副本链在 M9）：小文件是 inline 的，内容在元数据里
        mkdir "$M/round$r" && echo "round $r" >"$M/round$r/f" || return 1
        [ "$(cat "$M/round$r/f")" = "round $r" ] || return 1
        echo "新 leader node$(wmfs dev leader)，term $(m8_term)；写入、读回都对"
        wmfs dev start "$L" || return 1 # 回来当 follower（下一轮可能又杀它）
    done
    json_expect '.consistent == true and (.nodes | length) == 3' wmfs debug meta-hash --json || return 1
    [ "$(find "$M" -mindepth 1 -maxdepth 1 -name 'round*' | wc -l)" -eq 10 ]
}

m8_snapshot_install() {
    local L F n i before hits
    for n in 1 2 3; do
        wmfs failpoint set "$n" raft.compact.small 'return(50)' || return 1
    done
    L="$(wmfs dev leader)" || return 1
    F=$((L % 3 + 1))
    # 先建 200 个目录、等 3 个节点都应用了，停掉 F 之后再删掉：日志里有删除，F 装快照时要先删掉整个 kv 表（规则 M5），否则这些目录的
    # 记录会留在它那里（meta-hash 不一致）
    mkdir "$M"/gone{1..200} || return 1
    json_expect '.consistent == true and (.nodes | length) == 3' wmfs debug meta-hash --json || return 1
    echo "leader node$L；停掉 follower node$F，删掉那 200 个目录，再写 20000 个键（远超过 50 条的压缩阈值；kv 表超过 1 MiB，快照流至少两个中间帧）"
    wmfs dev kill "$F" || return 1
    rmdir "$M"/gone{1..200} || return 1
    wmfs debug kv bench --n 20000 --json || return 1
    before="$(grep -c '日志压缩到' "$WMFS_DEV_DIR/n$L/logs/server.log")"
    echo "leader 的日志里压缩过 $before 次"
    [ "$before" -ge 1 ] || return 1
    # 第一次下载在发完第一个中间帧之后失败（坑 P11）：follower 丢掉不完整的文件，leader 报告失败、重发，第二次装上
    wmfs failpoint set "$L" snapshot.fetch.midway 'times(1,return(EIO))' || return 1
    wmfs dev start "$F" || return 1 # 它等到 leader 看到 F 追上才返回
    for i in $(seq 1 300); do # 最多 30 s
        [ "$(m8_metric "$F" wmfs_raft_snapshot_installed_total)" = "1" ] && break
        sleep 0.1
    done
    hits="$(m8_fp_hits "$L" snapshot.fetch.midway)"
    echo "node$F 的 wmfs_raft_snapshot_installed_total = $(m8_metric "$F" wmfs_raft_snapshot_installed_total)"
    echo "leader 的 wmfs_raft_snapshot_sent_total = $(m8_metric "$L" wmfs_raft_snapshot_sent_total)；snapshot.fetch.midway 触发了 $hits 次"
    [ "$(m8_metric "$F" wmfs_raft_snapshot_installed_total)" = "1" ] && [ "$hits" = "1" ] || return 1
    grep "下载快照失败" "$WMFS_DEV_DIR/n$F/logs/server.log" || return 1
    grep "node$F 安装了快照" "$WMFS_DEV_DIR/n$F/logs/server.log" || return 1
    json_expect '.consistent == true and (.nodes | length) == 3' wmfs debug meta-hash --json || return 1
    [ "$(find "$M" -mindepth 1 -maxdepth 1 -name 'gone*' | wc -l)" -eq 0 ] || return 1
    wmfs failpoint clear "$L" snapshot.fetch.midway || return 1
    for n in 1 2 3; do # 重启过的 node$F 上没有设过（fail point 只在进程内存里）
        [ "$n" = "$F" ] || wmfs failpoint clear "$n" raft.compact.small || return 1
    done
}

m8_join_rejected() {
    local out rc
    out="$(wmfs join 127.0.0.1:19100 --cluster-key "$WMFS_DEV_DIR/n1/cluster.key" --data-dir "$LOGDIR/n4" \
        --rpc-port 19400 --mgmt-port 19480 --disable posix-mode --no-start 2>&1)"
    rc=$?
    printf '%s\n退出码 %s\n' "$out" "$rc"
    [ "$rc" -eq 3 ] && printf '%s' "$out" | grep -q '本节点缺少 authz:posix-mode' && [ ! -e "$LOGDIR/n4/NODE" ] || return 1
    json_expect '(.nodes | length) == 3' wmfs node list --json # 集群里没有多出节点
}

# 3.7.2 第 3 条（M8 审查后）：改了集群插件清单之后，每个节点——不只是 leader——应用了这个批次就发现不一致、以退出码 3 退出（follower 不必
# 等拿到新集群图）。节点都要退出，所以先卸载挂载点；之后的 wmfs dev down 什么也不用停。
m8_plugins_exit_all() {
    local n i
    wmfs dev umount "$M" || return 1
    wmfs cluster plugins add authz:deny-uid --json || return 1
    for i in $(seq 1 150); do # 最多 15 s
        wmfs dev status --json | jq -e 'all(.nodes[]; .running | not)' >/dev/null && break
        sleep 0.1
    done
    wmfs dev status --json
    wmfs dev status --json | jq -e 'all(.nodes[]; .running | not)' >/dev/null || return 1
    for n in 1 2 3; do
        grep '本节点缺少 authz:deny-uid' "$WMFS_DEV_DIR/n$n/logs/server.log" | grep '进程退出（退出码 3）' || return 1
    done
}
