# M1 传输与认证：逐条对应 roadmap §3 M1 的"验收标准"。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：ping 1000 次全成功；4 MiB bulk 往返 200 次 CRC 全对；错误密钥被拒绝；client key 申请 server 角色被拒；
#           客户端注入 30% 发送失败时靠重连重试全部成功；一个故意不读 socket 的假客户端不会卡住服务端（5 s 写超时后它被断开，
#           别的客户端不受影响）；mem 传输插件的测试通过；帧与握手的金样测试通过；wmfs failpoint 经签名的管理 API 生效。
#
# 开发集群只用本次验收自己的目录（$LOGDIR/dev，端口照常是 19100/19180）：通过时随日志一起删除，失败时保留现场；
# 无论成败，退出前都停掉本次起的节点（不留后台进程）。只解析 --json 输出（/metrics 是 Prometheus 文本格式）。

M1_RPC=tcp://127.0.0.1:19100
M1_HTTP=http://127.0.0.1:19180

accept_m1() {
    check "cargo build --release 成功且零警告" m1_build
    check "金样逐字节一致（帧头、握手消息、密钥派生与证明、管理 API 签名、changelog 操作码表）" \
        cargo test --release -p wmfs-core golden
    check "mem 传输插件（独立 crate，只依赖 wmfs-core）的测试通过" cargo test --release -p plugin-mem-transport
    check "插件与本体的单元测试（tcp、shared-key、内存池、PluginSet 校验、握手证明、管理 API 签名校验）" \
        cargo test --release -p wmfs-plugins -p wmfs-core -p wmfs-api -p wmfs-mgr --lib
    check "RPC 组件测试（tcp 与 mem）：ping 1000、4 MiB bulk ×200、认证与角色规则、角色权限表、start/call_cb/stream、超时、池满回 BUSY" \
        cargo test --release -p wmfs-testkit --test net_rpc
    check "慢客户端：握手后故意不读 socket 的假客户端 5 s 写超时后被断开，期间别的客户端照常 ping" \
        cargo test --release -p wmfs-testkit --test slow_client
    check "fail point 用例：客户端注入 30% 发送失败，日志里有「连接断开 → 重连重试」，100/100 成功" \
        cargo test --release -p wmfs-testkit --test failpoint_retry
    check "fail point 用例：plugin.shared-key.authn 触发 = 认证被拒，清除后恢复" \
        cargo test --release -p wmfs-testkit --test failpoint_authn
    check "连接故障：发送队列满与 CRC 不对就断开、冒充的服务端被拒、OneWay 也过角色表、回复缺 bulk 按 BUSY、超限只失败一个请求、流被丢弃回 EIO" \
        cargo test --release -p wmfs-testkit --test conn_faults
    check "保活与读超时（约 20 s）：认证后空闲 20 s 不断开、裸连接 15 s 被断开、握手完成前不发保活 Ping" \
        cargo test --release -p wmfs-testkit --test conn_idle

    export WMFS_DEV_DIR="$LOGDIR/dev"
    trap 'wmfs dev down >/dev/null 2>&1' EXIT
    check "wmfs dev up --nodes 1：起节点并等到 /api/v1/health 有响应" wmfs dev up --nodes 1
    eval "$(wmfs dev env)"
    check_json "wmfs dev status --json：node1 在运行且健康" '.nodes | length == 1 and .[0].running and .[0].healthy' \
        wmfs dev status --json
    check_json "GET /api/v1/health（免认证）：含本节点的 rpc 与管理地址" \
        '.ok and .node.node_id == 1 and .node.addrs == [["tcp","127.0.0.1:19100"]] and .node.mgmt_addr == "127.0.0.1:19180"' \
        curl --noproxy '*' -s "$M1_HTTP/api/v1/health"
    check_json "ping 1000 次全成功" '.ok == 1000 and .failed == 0' wmfs ping "$M1_RPC" --count 1000 --json
    check_json "4 MiB bulk 往返 200 次 CRC 全对" '.ok == 200 and .failed == 0 and .bulk_bytes == 4194304' \
        wmfs ping "$M1_RPC" --bulk 4M --count 200 --json
    check "wmfs debug gen-key 生成一把集群不认识的 key" wmfs debug gen-key --out "$LOGDIR/wrong.key"
    check "错误密钥被拒绝（AUTH_FAILED）" m1_expect_error AUTH_FAILED wmfs ping "$M1_RPC" --key-file "$LOGDIR/wrong.key" --json
    check "client key 申请 server 角色被拒（AUTH_FAILED）" \
        m1_expect_error AUTH_FAILED wmfs ping "$M1_RPC" --key-file "$WMFS_DEV_DIR/n1/keys/client.key" --role server --json
    check_json "client key 以 client 角色正常连接" '.ok == 3' \
        wmfs ping "$M1_RPC" --key-file "$WMFS_DEV_DIR/n1/keys/client.key" --count 3 --json
    check "演示：WMFS_FAILPOINTS='plugin.tcp.transport=pct(30,return(EIO))' 的 ping 靠重连重试 100/100 ok" m1_fp_demo
    check_json "wmfs failpoint list 1：空列表（走带签名的管理 API）" '.failpoints == []' wmfs failpoint list 1 --json
    check "不带签名的写请求被拒（HTTP 401，auth_failed）" m1_unsigned_rejected
    check "wmfs failpoint set 经签名的管理 API 生效：服务端下一次发送失败 → 客户端重连重试成功、命中计数 +1，clear 后清空" \
        m1_fp_api
    check "/metrics（免认证）有 wmfs_rpc_requests_total；连接层计数器一启动就在（值可以是 0）" m1_metrics
    check "wmfs dev down 停掉全部开发节点" wmfs dev down
    check_json "dev status --json：node1 已停止" '.nodes[0].running == false' wmfs dev status --json
}

# cargo build；cargo 与 rustc 的警告都打印在 stderr 上，有一行以 warning 开头就算失败。
m1_build() {
    local err
    err="$(mktemp)"
    cargo build --release --workspace 2>"$err"
    local rc=$?
    cat "$err"
    if grep -q '^warning' "$err"; then
        echo "有编译警告（见上面）"
        rc=1
    fi
    rm -f "$err"
    return "$rc"
}

# m1_expect_error 期望的错误码 命令…：命令必须失败（退出码非 0），且 --json 输出里 .error 是期望的错误码、一次都没成功
m1_expect_error() {
    local want="$1"
    shift
    local out rc
    out="$("$@")"
    rc=$?
    printf '%s\n' "$out"
    if [ "$rc" -eq 0 ]; then
        echo "命令应该失败，却成功了"
        return 1
    fi
    printf '%s' "$out" | jq -e --arg w "$want" '.error == $w and .ok == 0'
}

# 演示命令原样：fail point 只在 ping 这个客户端进程里生效（环境变量），服务端是另一个进程
m1_fp_demo() {
    local out
    out="$(WMFS_FAILPOINTS='plugin.tcp.transport=pct(30,return(EIO))' wmfs ping "$M1_RPC" --count 100 --json)" || {
        printf '%s\n' "$out"
        return 1
    }
    printf '%s\n' "$out"
    printf '%s' "$out" | jq -e '.ok == 100 and .failed == 0 and .retries > 0'
}

m1_unsigned_rejected() {
    local out code
    out="$(curl --noproxy '*' -s -w '\n%{http_code}' -X POST -d '{"name":"x","action":"off"}' "$M1_HTTP/api/v1/failpoints")"
    printf '%s\n' "$out"
    code="$(printf '%s\n' "$out" | tail -1)"
    [ "$code" = "401" ] && printf '%s\n' "$out" | head -1 | jq -e '.error == "auth_failed"'
}

m1_fp_api() {
    wmfs failpoint set 1 plugin.tcp.transport 'times(1,return(EIO))' || return 1
    wmfs failpoint list 1 --json |
        jq -e '.failpoints == [{"name": "plugin.tcp.transport", "action": "times(1,return(EIO))"}]' || return 1
    json_expect '.ok == 3 and .failed == 0 and .retries >= 1' wmfs ping "$M1_RPC" --count 3 --json || return 1
    curl --noproxy '*' -s "$M1_HTTP/metrics" | grep -E '^wmfs_failpoint_hits_total\{name="plugin\.tcp\.transport"\} [1-9]' || return 1
    wmfs failpoint clear 1 plugin.tcp.transport || return 1
    wmfs failpoint list 1 --json | jq -e '.failpoints == []'
}

m1_metrics() {
    local m c
    m="$(curl --noproxy '*' -s "$M1_HTTP/metrics")" || return 1
    printf '%s\n' "$m" | grep -E '^wmfs_rpc_requests_total\{family="Ping",op="Ping",code="OK"\} [1-9]' || return 1
    for c in wmfs_conn_write_timeout_total wmfs_conn_send_queue_full_total wmfs_conn_proto_errors_total wmfs_req_dropped_total; do
        printf '%s\n' "$m" | grep -E "^$c [0-9]" || { echo "缺少计数器 $c"; return 1; }
    done
}
