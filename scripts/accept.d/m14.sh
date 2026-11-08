# M14 管理面插件 + trace + 动态库 ABI：逐条对应 roadmap §3 M14 的"验收标准"与"演示命令"。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：dashboard 各面板有数据；/api/v1/status 对 design.md 3.10 表里的每个属性都返回 {key, declared, source, effect, actual,
#           mismatched}，验收脚本逐个键检查，dashboard 的"声明 vs 实际"面板逐个属性显示；杀掉一个节点约 60 秒后告警触发、恢复后 resolved；
#           审计日志包含预期事件，mgr 重启后无缺失（按 seq 可去重）；wmfs trace 显示链头 → 中间 → 链尾的时间线；deny-uid 插件生效；
#           坏 ABI 插件导致 server 拒绝启动并写明原因；集群插件清单加入 deny-uid 后，不带这个 .so 单独重启的节点被拒绝（退出并列出差异）；
#           完整 nas-safe profile 通过。
#
# 开发集群、挂载点、mgr 的目录与编出来的 .so 都在本次验收自己的日志目录里（$LOGDIR/dev、$LOGDIR/mnt、$LOGDIR/so；端口照常是 19n00/19n80，
# mgr 是 19090）；无论成败，退出前都 wmfs dev down（先卸载挂载点、停 mgr，再停节点）并删掉生成的大文件与 .so（m14_cleanup）。只解析 --json
# 输出、指标、mgr 的 JSON 与退出码；例外是要检查的东西本身就是给人看的文字：节点日志里"拒绝启动"与"缺少 authz:deny-uid"的原因、
# wmfs trace 的时间线（各跳一行）。等待一律按条件（wmfs dev wait、poll）。告警规则用内置的默认值（target 离线 > 60 s），不为测试改短。

# shellcheck source=/dev/null
source "$ROOT/scripts/accept.d/m6.sh"

# design.md 3.10 表的全部键（`quota.<类>` 在 status 里是 `quota.*` 一项）：缺一个就失败
M14_STATUS_KEYS='["class","replicas","compress","encrypt","chunk","stripe","inline","checksum","quota","quota.*","qos","consistency","acl.default","locality","inherit","trash","tier","ttl","audit"]'

accept_m14() {
    check "开发集群的端口 19100–19380 与 mgr 的 19090 都空闲（演示用的开发集群要先 wmfs dev down）" m14_ports_free
    check "没有残留的 wmfs FUSE 挂载（有的话先 wmfs dev down --purge）" m6_no_mounts
    check "cargo build --release 成功且零警告" build_no_warnings
    check "编码约定：unsafe 只在白名单文件里（cabi.rs）、每个文件 ≤ 400 行、不写宏" scripts/ci.sh rules
    check "单元（wmfs-mgr）：路由、manifest 校验、每个插件的 fail point、告警规则与状态机（虚拟时钟：for 防抖、resolved、sum、按序列的 rate——节点缺一轮不误报、grows——协调器积压 1 h 内持续增长才告警、一直在减的大迁移与持平不告警（ADR-0045 第 10 条）、事件型、操作名核对）、changelog 循环（投递失败不提交游标、410 按 oldest 跳过并计数、stalled、audit-log 没有游标不建、webhook 攒批）、exporter 的配额 gauge（删掉的归 0）、audit-log（不留半行）、webhook（每批 100 条、非 2xx 报错）、/metrics 合并、冻住的节点不拖住抓取" \
        tests_at_least 23 --release -p wmfs-mgr --lib
    check "单元（core）：span 环形缓冲（按节点、按跳、满了覆盖最旧的）、慢日志的各阶段耗时（只列走过的、同一阶段累加）、线程数、C ABI 的编号表与结构大小、加载不存在的库报错（带 dlerror 原文）、node.toml 的 [obs]、移除的节点上的 target 不算离线、失去 leader 时任务积压归零、changelog 被消费过的至少留最近 1 万条（ADR-0045 第 12 条）" m14_units
    check "金样：trace、node（只增的 target、boot_id、内存池、线程数）、cluster（只增的 target、链数、会话数）、features（ADR-0045 第 11 条）的 JSON；任务端点的 JSON 照旧" m14_golden
    check "动态库（gcc 编 examples/dylib-deny-uid 与测试插件）：deny-uid 加载、拒绝 uid 4242 的写、配置 uid、进集群插件清单、配置键按 schema 校验、管线里任一拒绝即拒绝、fail point plugin.deny-uid.authz；abi_version = 2 被拒绝并写明原因；失败即关闭（返回值、errno）、manifest 与名字不合规、init 返回 NULL、缺符号（RTLD_NOW）都拒绝；两个 v1 结构在 C 与 Rust 里逐字段同布局" \
        tests_at_least 3 --release -p wmfs-testkit --test dylib
    check "trace（进程内 3 节点）：一次 WriteChunk 在链头、中间、链尾各一个 span（同一个 trace_id、第 0/1/2 跳、三个节点、耗时递减）；recent 只列客户端请求（中间节点上没有它）；GET /api/v1/node 的新字段；节点 /metrics 的 target 用量与容量、时钟偏差、线程数、离线 target 数；慢请求阈值（node.toml [obs]）；慢日志列出各阶段耗时（元数据写：decode、排队、authz、quota、执行、raft 提交；链头上的数据写：decode、排队、authz、磁盘、转发；ADR-0045 第 9 条）" \
        one_test --release -p wmfs-testkit --test trace a_chain_write_leaves_one_span_per_hop
    check "告警 quota_denied 的计数：写到没有余量、来要额度的 CommitWrite 也计一次拒绝" \
        one_test --release -p wmfs-testkit --test quota_denied asking_for_quota_with_none_left_counts_a_denial
    check "回归：管理 API（含 features 的 list 与 enable 的两种错误，ADR-0045 第 11 条）、存储类（M13）、后台任务（M11）、gc 的 changelog 修剪（至少留最近 1 万条，ADR-0045 第 12 条）" m14_regress

    export WMFS_DEV_DIR="$LOGDIR/dev"
    M="$LOGDIR/mnt"
    SO="$LOGDIR/so"
    mkdir -p "$M" "$SO"
    trap m14_cleanup EXIT
    check "演示：wmfs dev up --nodes 3 --profile nas-safe → 集群起好之后自动起 mgr，最后一行是带 token 的 dashboard 网址" m14_up
    eval "$(wmfs dev env)"
    check "演示：wmfs dev mount" wmfs dev mount "$M"
    check "演示：touch、rm 之后 wmfs dev wait audit-caught-up；审计日志里有 Create 与（进回收站的）Unlink" m14_audit
    check_json "演示：wmfs status /home --json 对 3.10 表的每个键都返回 {key, declared, source, effect, actual, mismatched}（逐个键，缺一个就失败）" \
        "($M14_STATUS_KEYS | sort) == ([.attrs[].key] | sort) and all(.attrs[]; (keys | sort) == [\"actual\",\"declared\",\"effect\",\"key\",\"mismatched\",\"source\"] and (.actual | type) == \"object\" and (.actual.new_files | type) == \"string\" and (.mismatched | type) == \"boolean\" and ([.declared, .source, .effect] | all(type == \"string\")))" \
        wmfs status "$M/home" --json
    check "dashboard 各面板有数据（带 token 拉页面用的代理端点）：集群、容量与配额、声明 vs 实际（逐个属性）、QoS、后台任务、事件与告警" m14_dashboard
    check "mgr：不带 token → 401；/metrics 免 token，合并了 3 个节点与 mgr 自己的指标（node 标签），有 7.3 推迟到 M14 的几个指标" m14_mgr_http
    check "wmfs feature list --json 能解析（集群已启用的为空、3 个节点各一项）；wmfs feature enable chunk.snapshots 报\"二期功能\"、不认识的名字 einval（ADR-0045 第 11 条）" m14_feature
    check "演示：wmfs dev kill 3 → 约 60 s 后告警 target_offline firing（wmfs dev wait alert … --timeout 120；不早于 55 s：for 60s 的防抖）" m14_offline
    check "演示：wmfs dev start 3 → target_offline resolved，链全部 Serving" m14_recover
    check "演示：dd 写 16 MiB 之后 wmfs trace --recent 列出请求，wmfs trace <WriteChunk 的 trace_id> 显示链头 → 中间 → 链尾的时间线" m14_trace
    check "审计日志在 mgr 重启后无缺失：kill -9 mgr、它停着的时候建文件、wmfs dev mgr 重启，按 seq 去重之后每个文件的 Create 恰好一条" m14_mgr_restart
    check "演示：gcc 编 deny_uid.so 与 bad_abi.so（编到本次验收的目录里）" m14_gcc
    check "演示：wmfs cluster plugins add authz:deny-uid → 各节点退出；wmfs dev restart-all --dylib deny_uid.so → 有就绪的 leader" m14_add_plugin
    check "演示：deny-uid 生效：wmfs fs --as-uid 4242 touch /home/zz → EACCES；uid 4243 照常（/home 是 1777，posix-mode 放行）" m14_deny
    check "演示：不带 .so 重启 node1 → 它退出（退出码 3），日志列出缺少 authz:deny-uid" m14_missing
    check "演示：带 bad_abi.so 重启 node1 → 拒绝启动，日志写明\"动态库 ABI 版本 2 不受支持（支持 [1]），拒绝启动\"" m14_bad_abi
    check "演示：带 deny_uid.so 重启 node1 → 恢复正常，链全部 Serving" m14_restore
    check "wmfs dev down（先卸载挂载点、停 mgr，再停节点）" wmfs dev down
    check "结束：本次验收自己的挂载都已卸载、mgr 已停" m14_clean_end
    check "profile minimal（scripts/profiles.sh minimal）" scripts/profiles.sh minimal
    check "profile nas-safe 完整版（scripts/profiles.sh nas-safe；ADR-0015：M14 起含 mgr 与审计日志：mgr 重启后审计日志完整）" \
        scripts/profiles.sh nas-safe
}

m14_cleanup() {
    wmfs dev down >/dev/null 2>&1
    if [ -n "$LOGDIR" ] && [ "${WMFS_DEV_DIR:-}" = "$LOGDIR/dev" ]; then
        rm -rf "$WMFS_DEV_DIR"/n*/targets "$SO"
    fi
}

m14_ports_free() {
    dev_ports_free M14 3 || return 1
    if (exec 3<>/dev/tcp/127.0.0.1/19090) 2>/dev/null; then
        echo "127.0.0.1:19090（wmfs dev mgr）已被占用：先 wmfs dev down"
        return 1
    fi
}

m14_units() {
    tests_at_least 3 --release -p wmfs-core --lib obs:: || return 1
    one_test --release -p wmfs-core --lib meta::gc_meta::tests::consumed_records_keep_the_newest_ten_thousand || return 1
    tests_at_least 2 --release -p wmfs-core --lib hooks::cabi || return 1
    tests_at_least 3 --release -p wmfs-core --lib base::config || return 1
    one_test --release -p wmfs-core --lib meta::cluster::chain_mgr::tests::targets_of_removed_nodes_are_not_counted_offline ||
        return 1
    one_test --release -p wmfs-core --lib bg::scheduler::tests::losing_leadership_zeroes_the_backlog_and_progress_gauges ||
        return 1
    one_test --release -p wmfs --bin wmfs trace::tests::the_timeline_shows_each_hop_and_its_own_time
}

m14_golden() {
    one_test --release -p wmfs-api types::obs::tests::trace_and_node_json_golden || return 1
    one_test --release -p wmfs-api types::cluster::tests::cluster_json_golden || return 1
    one_test --release -p wmfs-api types::cluster::tests::features_json_golden || return 1
    one_test --release -p wmfs-api types::tasks::tests::tasks_and_datakeys_json_golden
}

m14_regress() {
    tests_at_least 3 --release -p wmfs-testkit --test mgmt_api || return 1
    one_test --release -p wmfs-testkit --test gc_full dedup_records_of_closed_sessions_and_consumed_changelog_are_cleaned_up ||
        return 1
    tests_at_least 5 --release -p wmfs-testkit --test classes || return 1
    tests_at_least 4 --release -p wmfs-testkit --test bg_cluster
}

m14_up() {
    local out
    out="$(wmfs dev up --nodes 3 --profile nas-safe)" || {
        printf '%s\n' "$out"
        return 1
    }
    printf '%s\n' "$out"
    printf '%s' "$out" | tail -1 | grep -qE '^dashboard：http://127\.0\.0\.1:19090/\?token=[0-9a-f]{32}$'
}

m14_audit() {
    touch "$M/home/a" && rm "$M/home/a" && wmfs dev wait audit-caught-up --timeout 60 || return 1
    grep -hE '"op":"(Create|Unlink)"' "$WMFS_MGR_DIR"/audit/audit-*.log | tail -2
    cat "$WMFS_MGR_DIR"/audit/audit-*.log | jq -se '
        ([.[] | select(.op == "Create" and .name == "a" and .audited)] | length) >= 1 and
        ([.[] | select(.op == "Unlink" and .name == "a" and (.extra | any(.[0] == "trash")))] | length) >= 1'
}

# m14_get <路径（含查询串）>：带 token 问 mgr
m14_get() {
    local sep="?"
    case "$1" in *\?*) sep="&" ;; esac
    curl --noproxy '*' -sf "http://127.0.0.1:19090$1${sep}token=$(cat "$WMFS_MGR_DIR/token")"
}
# m14_api <管理 API 路径> / m14_each <节点本地的管理 API 路径>：dashboard 的两个代理
m14_api() { m14_get "/dash/api?path=$(jq -rn --arg p "$1" '$p | @uri')"; }
m14_each() { m14_get "/dash/each?path=$(jq -rn --arg p "$1" '$p | @uri')"; }
# m14_has <jq 表达式> 命令…：命令的 JSON 输出满足表达式（输出留在日志里）
m14_has() {
    local expr="$1" out
    shift
    out="$("$@")" || return 1
    printf '%s\n' "$out" | head -c 2000
    echo
    printf '%s' "$out" | jq -e "$expr" >/dev/null
}

m14_dashboard() {
    m14_get / | grep -q '声明 vs 实际' || return 1
    echo "== 集群"
    m14_has '(.nodes | length) == 3 and .leader > 0 and (.targets | length) == 3 and .chains > 0 and .sessions >= 1' \
        m14_api /api/v1/cluster || return 1
    m14_has '(.chains | length) > 0' m14_api /api/v1/chains || return 1
    echo "== 容量与配额"
    wmfs set "$M/home" quota=10G --json >/dev/null || return 1
    m14_has 'length == 3 and all(.[]; (.reply.targets | length) == 1 and .reply.targets[0].capacity > 0 and (.reply.pool | length) == 3)' \
        m14_each /api/v1/node || return 1
    m14_has '[.subtree[] | select(.attrs | has("quota"))] | length >= 1' m14_api "/api/v1/attr?path=%2F&recursive=1" || return 1
    m14_has '[.quotas[] | select(.dir == "/home" and .hard == 10737418240)] | length == 1' m14_api "/api/v1/usage?path=%2Fhome" ||
        return 1
    echo "== 声明 vs 实际"
    m14_has "($M14_STATUS_KEYS | sort) == ([.attrs[].key] | sort)" m14_api "/api/v1/status?path=%2Fhome" || return 1
    echo "== QoS（先写一点数据：mclock 的队列在有请求时才有）"
    head -c 3000000 /dev/urandom >"$M/home/qos-probe" || return 1
    poll 20 0.5 m14_has 'length == 3 and all(.[]; .reply.plugin == "mclock") and ([.[].reply.queues[]] | length) > 0' \
        m14_each /api/v1/qos || return 1
    curl --noproxy '*' -s http://127.0.0.1:19090/metrics | grep -q '^wmfs_qos_wait_seconds_bucket{node=' || return 1
    m14_has '(.classes | length) > 0 and all(.classes[]; (.promise | length) > 0 and (.isolation | length) > 0)' \
        m14_api /api/v1/classes || return 1
    echo "== 后台任务（leader 上的任务；节点任务的进度在各节点的 /api/v1/node）"
    m14_has '(.tasks | length) > 0 and any(.tasks[]; .kind == "reconciler")' m14_api /api/v1/tasks || return 1
    m14_has 'all(.[]; any(.reply.tasks[]; .kind == "scrub"))' m14_each /api/v1/node || return 1
    echo "== 事件与告警"
    m14_has '(.alerts | length) >= 14 and any(.alerts[]; .name == "target_offline") and (.recent | length) > 0' m14_get /alerts
}

m14_mgr_http() {
    local code text
    code="$(curl --noproxy '*' -s -o /dev/null -w '%{http_code}' http://127.0.0.1:19090/alerts)"
    echo "不带 token：HTTP $code"
    [ "$code" = 401 ] || return 1
    text="$(curl --noproxy '*' -sf http://127.0.0.1:19090/metrics)" || return 1
    printf '%s\n' "$text" | grep -E '^wmfs_(quota_used_bytes|quota_limit_bytes|declared_mismatch_files|target_used_bytes|threads)' | head -12
    local n
    for n in 1 2 3; do
        printf '%s' "$text" | grep -q "^wmfs_threads{node=\"127.0.0.1:19${n}80\"}" || return 1
    done
    printf '%s' "$text" | grep -q '^wmfs_hook_calls_total{node="mgr",hook="event_sink",plugin="audit-log"}' || return 1
    printf '%s' "$text" | grep -q '^wmfs_target_capacity_bytes{node=' || return 1
    [ "$(printf '%s' "$text" | grep -c '^# TYPE wmfs_threads ')" = 1 ] || return 1 # 同名指标的 TYPE 只写一次
    # exporter 每 30 s 算一次配额：刚设的 quota=10G 按条件等
    poll 60 1 m14_metric_has 'wmfs_quota_limit_bytes{node="mgr",dir="/home",kind="hard"} 10737418240' || return 1
    poll 60 1 m14_metric_has 'wmfs_declared_mismatch_files{node="mgr",attr="class"}'
}
m14_metric_has() { curl --noproxy '*' -s http://127.0.0.1:19090/metrics | grep -qF "$1"; }

m14_feature() {
    local out
    wmfs feature list || return 1
    wmfs feature list --json | jq -e '.enabled == [] and (.nodes | length) == 3 and all(.nodes[]; .features == [])' || return 1
    if out="$(wmfs feature enable chunk.snapshots 2>&1)"; then
        echo "预留名不应启用成功：$out"
        return 1
    fi
    printf '%s\n' "$out"
    printf '%s' "$out" | grep -q '二期功能' || return 1
    expect_error einval wmfs feature enable no.such --json
}

m14_offline() {
    local t0 t1
    t0="$(date +%s)"
    wmfs dev kill 3 && wmfs dev wait alert target_offline firing --timeout 120 || return 1
    t1="$(date +%s)"
    echo "kill 之后 $((t1 - t0)) s 告警触发（target 租约 8 s 才判离线，再持续 60 s）"
    [ "$((t1 - t0))" -ge 55 ]
}

m14_recover() {
    wmfs dev start 3 && wmfs dev wait alert target_offline resolved --timeout 60 && wmfs dev wait chains-serving --timeout 120
}

# 一个 WriteChunk 的 trace 走满了三跳（没有重试、没碰上换图）：第 0/1/2 跳、三个不同的节点、都 OK
m14_three_hops() {
    wmfs trace "$1" --json | jq -e '[.[].span_id] == [0, 1, 2] and ([.[].node] | unique | length) == 3
        and ([.[].op] == ["WriteChunk", "ForwardWrite", "ForwardWrite"]) and all(.[]; .code == "OK")' >/dev/null
}

m14_trace_once() {
    local id
    dd if=/dev/zero of="$M/home/t" bs=4M count=4 conv=fsync status=none || return 1
    wmfs trace --recent 5 || return 1
    for id in $(wmfs trace --recent 50 --json | jq -r '.[] | select(.op == "WriteChunk") | .trace_id'); do
        if m14_three_hops "$id"; then
            wmfs trace "$id"
            return 0
        fi
    done
    return 1
}

m14_trace() { poll 5 1 m14_trace_once; }

m14_dead() { ! kill -0 "$1" 2>/dev/null; }

m14_mgr_restart() {
    local pid i
    wmfs dev wait audit-caught-up --timeout 60 || return 1
    pid="$(cat "$WMFS_MGR_DIR/mgr.pid")" || return 1
    tr '\0' ' ' <"/proc/$pid/cmdline" | grep -q "mgr .*--dir $WMFS_MGR_DIR" || return 1 # 只对本集群的 mgr 发信号
    kill -9 "$pid" && poll 50 0.1 m14_dead "$pid" || return 1
    for i in 1 2 3 4 5; do echo "x$i" >"$M/home/ma$i" || return 1; done
    rm "$M/home/ma1" || return 1
    wmfs dev mgr && wmfs dev wait audit-caught-up --timeout 60 || return 1
    cat "$WMFS_MGR_DIR"/audit/audit-*.log | jq -se '
        ([.[] | select(.op == "Create" and (.name | startswith("ma")))] | unique_by(.seq) | map(.name) | sort) == ["ma1","ma2","ma3","ma4","ma5"]
        and ([.[] | select(.op == "Unlink" and .name == "ma1")] | unique_by(.seq) | length) == 1'
}

m14_gcc() {
    gcc -shared -fPIC -I core/include -o "$SO/deny_uid.so" examples/dylib-deny-uid/deny_uid.c &&
        gcc -shared -fPIC -I core/include -o "$SO/bad_abi.so" examples/dylib-deny-uid/bad_abi.c
}

m14_add_plugin() {
    wmfs fs --as-uid 4242 touch /home/yy --json | jq -e '.ok' || return 1 # 加插件之前：4242 照常能建
    wmfs cluster plugins add authz:deny-uid --json | jq -e '.plugins | index("authz:deny-uid") != null' || return 1
    wmfs dev restart-all --dylib "$SO/deny_uid.so" && wmfs dev wait leader --timeout 60
}

m14_deny() {
    expect_error EACCES wmfs fs --as-uid 4242 touch /home/zz --json || return 1
    wmfs fs --as-uid 4243 touch /home/zz2 --json | jq -e '.ok'
}

m14_missing() {
    if wmfs dev restart 1; then
        echo "不带 .so 的 node1 应该起不来"
        return 1
    fi
    wmfs dev logs 1 | tail -3
    wmfs dev logs 1 | tail -20 | grep -q '插件清单与集群插件清单不一致：本节点缺少 authz:deny-uid' || return 1
    wmfs dev logs 1 | tail -20 | grep -q '退出码 3'
}

m14_bad_abi() {
    if wmfs dev restart 1 --dylib "$SO/deny_uid.so" --dylib "$SO/bad_abi.so"; then
        echo "带 bad_abi.so 的 node1 应该拒绝启动"
        return 1
    fi
    wmfs dev logs 1 | tail -3
    wmfs dev logs 1 | tail -1 | grep -qF '动态库 ABI 版本 2 不受支持（支持 [1]），拒绝启动'
}

m14_restore() {
    wmfs dev restart 1 --dylib "$SO/deny_uid.so" && wmfs dev wait chains-serving --timeout 120 && wmfs dev wait leader
}

m14_clean_end() {
    m6_no_own_mounts || return 1
    if (exec 3<>/dev/tcp/127.0.0.1/19090) 2>/dev/null; then
        echo "mgr 还在听 19090"
        return 1
    fi
}
