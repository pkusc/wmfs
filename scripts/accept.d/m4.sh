# M4 命名空间与会话：逐条对应 roadmap §3 M4 的"验收标准"与"演示命令"。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：POSIX 元数据语义单元测试；服务端"弄丢"一次回复时，客户端重试得到原结果而不是 EEXIST；rename 原子性测试；
#           重启后命名空间完好；非属主写入被拒绝；新文件的 layout 指向一条刚生成的链；server 日志里出现"有链少于声明副本数"
#           的警告（单节点 + default 类 replica=3）。另有 placement / 链生成、会话与去重（会话绑定 key、在途窗口按 seq 计）、
#           uid 范围与 root_squash、跟随 leader、集群图按角色过滤、init --target 与金样、插件（posix-mode、label-match）的
#           manifest 与 fail point，以及演示命令。
#
# 开发集群只用本次验收自己的目录（$LOGDIR/dev，端口照常是 19100/19180）：通过时随日志一起删除，失败时保留现场；
# 无论成败，退出前都停掉本次起的节点（不留后台进程）。只解析 --json 输出；例外是演示要求"看得见"的给人看的文字：
# `wmfs dev up` 输出里的集群警告、server 日志里的集群警告，与 `--verbose` 打印的"超时重试 seq=… → 去重命中"
# （另有 /metrics 的去重计数作为确凿证据）。等待一律用 wmfs dev wait leader，不用 sleep。
# 公共函数 dev_ports_free、expect_error、dev_metric 在 scripts/accept.sh 里。

accept_m4() {
    check "开发集群的端口 19100、19180 空闲（演示用的开发集群要先 wmfs dev down）" dev_ports_free M4
    check "cargo build --release 成功且零警告" build_no_warnings
    check "POSIX 元数据语义：create/mkdir/mknod/symlink/link/unlink/rmdir、readdir 分页、lookup 与按路径解析、setgid 目录、名字规则、权限位与粘滞位、没有执行权限看不出目录里的名字、孤儿、每个修改都写 changelog" \
        tests_at_least 12 --release -p wmfs-testkit --test meta_ns
    check "rename 的全部规则：覆盖、类型不匹配、非空目标、NOREPLACE/EXCHANGE/WHITEOUT、不得移进自己的子树、硬链接、粘滞位、没有执行权限看不出名字" \
        tests_at_least 8 --release -p wmfs-testkit --test meta_ns_rename
    check "setattr 与 xattr：chmod/chown/时间的属主规则、截断到 s>0、截断到 0 按当前属性重建 layout（新 epoch、重新选链、GC 记录）、xattr 名字空间与限制、ACL 与默认 ACL（umask 规则）、M10 之前带扩展 ACL 的只有 root 能访问、inline 内容只给有读权限的人" \
        tests_at_least 8 --release -p wmfs-testkit --test meta_ns_attr
    check "带种子的随机命名空间操作（1000 个种子）：每一步之后目录项、链接、nlink、size、孤儿标记、目录树的不变式都成立" \
        env WMFS_NS_SEEDS=1000 cargo test --release -p wmfs-testkit --test meta_ns_random
    check "ACL 的格式、校验、继承与 chmod 同步（core 单元测试）" \
        tests_at_least 5 --release -p wmfs-core --lib meta::ns::acl
    check "placement / 链生成：同样的 target 同样的链、故障域约束（随机种子）、dedicated 独占、只用 Up 的 target、集群警告" \
        tests_at_least 5 --release -p wmfs-core --lib meta::cluster
    check "label-match：manifest、按 class/replicas/Serving/exclude 过滤、排序、同 seed 同结果、不同 seed 分散" \
        tests_at_least 4 --release -p wmfs-plugins --lib label_match
    check "posix-mode：manifest、三组权限位与附加组、root（带扩展 ACL 的也放行）、扩展 ACL 弃权、粘滞位、chmod/chown/时间的属主规则、判定计数" \
        tests_at_least 8 --release -p wmfs-plugins --lib posix_mode
    check "重启后命名空间完好（模拟掉电的内存后端：只留下 fsync 过的字节）" \
        one_test --release -p wmfs-testkit --test meta_ns_engine namespace_survives_power_loss
    check "rename 原子性：300 次来回改名期间，并发读者在同一个读事务里总是恰好看到一个名字" \
        one_test --release -p wmfs-testkit --test meta_ns_engine rename_is_atomic_for_concurrent_readers
    check "会话 open/heartbeat/close 与去重（经 tcp）：同一个 seq 重发得到原结果、新 seq 照常 EEXIST、关闭后 SESSION_EVICTED、changelog" \
        one_test --release -p wmfs-testkit --test meta_ns_engine sessions_heartbeats_and_dedup_over_rpc
    check "会话绑定打开它的 key：别的 key 拿着 session id 发修改、心跳、关闭、重新打开都是 EACCES，也污染不了去重表" \
        one_test --release -p wmfs-testkit --test meta_ns_engine a_session_belongs_to_the_key_that_opened_it
    check "client key 的 uid 范围与 root_squash；admin 可以声明任意 uid（--as-uid）；uid 范围不含本进程 uid 的 key 也开得了会话（会话操作记为 nobody）" \
        one_test --release -p wmfs-testkit --test meta_ns_engine client_keys_are_held_to_their_uid_range_and_root_squash
    check "MetaClient 跟随 NOT_LEADER 的 leader_hint，没有 hint 时换下一个认识的节点" \
        one_test --release -p wmfs-testkit --test meta_ns_engine client_follows_the_leader_hint_and_rotates_without_one
    check "leader_hint = leader 在 control_scheme 上的地址（leader 是自己、是别人、未知；与 health 共用一套逻辑）" \
        one_test --release -p wmfs-core --lib server::tests::leader_hint_is_the_leaders_control_scheme_address
    check "集群图按角色过滤：client 与 admin 拿到的不含 keys、datakeys，server 角色的有" \
        one_test --release -p wmfs-testkit --test meta_ns_engine only_servers_get_the_keys_in_the_cluster_map
    check "fail point meta.reply.drop：回复被吞 → 一个 RPC 超时后用同一个 seq 重试 → 去重命中、结果成功（不是 EEXIST）；它开着时关闭与打开会话照常；一个请求卡在重试里时同一会话的 300 个新修改挤不掉它的去重记录（在途窗口按 seq 计）" \
        cargo test --release -p wmfs-testkit --test failpoint_reply_drop
    check "fail point plugin.posix-mode.authz 与 plugin.label-match.placement 生效、清除后恢复" \
        cargo test --release -p wmfs-testkit --test failpoint_ns_plugins
    check "init --target 写 TargetRecV1 与 node.toml 的 [[target]]、重复或嵌套的 target 被拒、init 的集群警告；初始 KV 与常量的金样逐字节一致" m4_init
    check "MetaCall 服务的老用例（调试键空间、角色、撤销的 key）仍然通过" \
        cargo test --release -p wmfs-testkit --test meta_service

    export WMFS_DEV_DIR="$LOGDIR/dev"
    trap 'wmfs dev down >/dev/null 2>&1' EXIT
    check "wmfs dev up --nodes 1：输出里有集群警告「default 类声明 3 副本，只有 1 个故障域」" m4_up
    eval "$(wmfs dev env)"
    check "wmfs dev wait leader" wmfs dev wait leader --timeout 30
    check_json "wmfs fs mkdir /a" '.ok == true' wmfs fs mkdir /a --json
    check_json "wmfs fs touch /a/f1" '.ok == true' wmfs fs touch /a/f1 --json
    check_json "wmfs fs ln /a/f1 /a/f2" '.ok == true' wmfs fs ln /a/f1 /a/f2 --json
    check_json "wmfs fs stat /a/f1：nlink=2；layout 指向一条刚生成的链（chain 1：声明 3 副本、实际 1 个成员 target 1）" \
        '.nlink == 2 and .kind == "file" and .layout.chains == [1] and .layout.epoch == 1 and .chains == [{"id": 1, "replicas": 3, "members": [1]}]' \
        wmfs fs stat /a/f1 --json
    check "server 日志里有集群警告「有 1 条链少于声明副本数」（生成了短链之后）" m4_log_short_chain
    check_json "wmfs fs mv /a/f1 /a/g" '.ok == true' wmfs fs mv /a/f1 /a/g --json
    check_json "wmfs fs ls -l /a：f2、g（同一个文件的两个链接）" \
        '[.entries[].name] == ["f2", "g"] and ([.entries[].nlink] | unique) == [2] and (.entries | map(.ino) | unique | length) == 1' \
        wmfs fs ls -l /a --json
    check "wmfs fs rmdir /a：ENOTEMPTY（Directory not empty）" expect_error ENOTEMPTY wmfs fs rmdir /a --json
    check "wmfs fs --as-uid 2000 mkdir /a/x：EACCES（非属主写入被拒绝）" \
        expect_error EACCES wmfs fs --as-uid 2000 mkdir /a/x --json
    check "--as-uid 只允许 admin key：用 client key 时 CLI 直接拒绝（--json 输出 EACCES）" \
        expect_error EACCES wmfs fs --as-uid 2000 --key-file "$WMFS_DEV_DIR/n1/keys/client.key" ls / --json
    check "wmfs debug gen-key 生成一把集群不认识的 key" wmfs debug gen-key --out "$LOGDIR/wrong.key"
    check "集群不认识的密钥：认证失败，--json 输出 AUTH_FAILED（发请求之前的失败也是 JSON）" \
        expect_error AUTH_FAILED wmfs fs --key-file "$LOGDIR/wrong.key" ls / --json
    check "meta.reply.drop：mkdir /b 约 10 s 后（一次 RPC 超时）重试、去重命中、结果成功；--verbose 打印「超时重试 seq=… → 去重命中」" \
        m4_reply_drop
    check "kill -9 → 重启 → wmfs dev wait leader → 命名空间完好（/a 里仍是 f2、g；/b 在）" m4_kill9
    check "wmfs_meta_batch_stale_total 恒为 0；server.log 里没有 panic / fatal" m4_after_restarts
    check "wmfs dev down 停掉全部开发节点" wmfs dev down
}

m4_after_restarts() {
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

m4_init() {
    tests_at_least 3 --release -p wmfs-core --lib init::tests || return 1
    cargo test --release -p wmfs-core --test golden_init --test golden_constants 2>&1 | tee /dev/stderr |
        grep -c 'test result: ok. 1 passed' | grep -qx 2
}

m4_up() {
    local out
    out="$(wmfs dev up --nodes 1)" || { printf '%s\n' "$out"; return 1; }
    printf '%s\n' "$out"
    printf '%s' "$out" | grep -q '警告：default 类声明 3 副本，只有 1 个故障域'
}

m4_log_short_chain() {
    local log="$WMFS_DEV_DIR/n1/logs/server.log"
    grep -E '集群警告：(default 类声明 3 副本|有 [0-9]+ 条链少于声明副本数)' "$log" || return 1
    grep -qE '集群警告：有 [0-9]+ 条链少于声明副本数' "$log"
}

m4_reply_drop() {
    local before after start took err="$LOGDIR/reply_drop.stderr"
    before="$(dev_metric wmfs_meta_dedup_hits_total)"
    [ -n "$before" ] || { echo "/metrics 里没有 wmfs_meta_dedup_hits_total（新起的节点上也应该有，值是 0）"; return 1; }
    wmfs failpoint set 1 meta.reply.drop 'times(1,return(EIO))' || return 1
    start=$(date +%s)
    json_expect '.ok == true' wmfs fs mkdir /b --verbose --json 2>"$err" || { cat "$err"; return 1; }
    took=$(($(date +%s) - start))
    cat "$err"
    after="$(dev_metric wmfs_meta_dedup_hits_total)"
    echo "用时 ${took} s；wmfs_meta_dedup_hits_total：$before → $after"
    [ "$took" -ge 9 ] || { echo "应该等了一个 RPC 超时（10 s）才重试"; return 1; }
    [ "$((after - before))" -eq 1 ] || { echo "服务端的去重计数应该正好 +1"; return 1; }
    grep -qE '超时重试 seq=[0-9]+ → 去重命中' "$err" || { echo "--verbose 没有打印「超时重试 seq=… → 去重命中」"; return 1; }
    grep -q '去重命中' "$WMFS_DEV_DIR/n1/logs/server.log" || { echo "server.log 里没有「去重命中」"; return 1; }
    json_expect '.kind == "dir"' wmfs fs stat /b --json
}

m4_kill9() {
    wmfs dev kill 1 || return 1
    wmfs dev start 1 || return 1
    wmfs dev wait leader --timeout 30 || return 1
    json_expect '[.entries[].name] == ["f2", "g"] and ([.entries[].nlink] | unique) == [2]' wmfs fs ls -l /a --json || return 1
    json_expect '.kind == "dir"' wmfs fs stat /b --json || return 1
    json_expect '.layout.chains == [1]' wmfs fs stat /a/g --json
}
