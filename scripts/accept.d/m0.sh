# M0 骨架、体检与接口骨架：逐条对应 roadmap §3 M0 的"验收标准"。
# 由 scripts/accept.sh 加载（用法见那里的说明）；这里只定义函数，不直接运行。
#
# 验收标准：cargo build --release 零警告；cargo test 通过（含全部金样）；wmfs doctor 输出检查表并以 0 退出；
#           scripts/accept.sh M0 PASS。另外核对演示命令里的 wmfs --version 与 scripts/loc.sh。

# 全部 13 项体检的 id（wmfs doctor --json 的 .checks[].id）
M0_DOCTOR_IDS='["kernel","fuse_dev","fusermount3","memlock","rootfs","rootdisk","proxy","rdma","cgroup_io","fdp","docker","allow_other","stale_mounts"]'
# 金样文件（tests/golden/<名字>.golden）：帧头、握手、元数据/数据/服务端消息、记录、写批次、key、chunk 头、TARGET、常量
M0_GOLDEN_FILES="frame_header handshake meta data peer records write_batch keys chunk target_superblock constants"

accept_m0() {
    check_json "cargo build --release 成功且零警告（含测试代码；解析 cargo 的 JSON 消息，并检查 cargo 自己在 stderr 上的警告）" \
        '(map(select(.reason == "build-finished")) | .[-1].success) == true
         and ([.[] | select(.reason == "compiler-message" and .message.level == "warning")] | length) == 0' \
        m0_build_json
    check "编码约定（每个文件 ≤ 400 行、unsafe 白名单、不写 macro_rules!、不引入信号/随机数 crate 与过程宏）" \
        scripts/ci.sh rules
    check "cargo clippy 零警告（含测试代码；检查每处 unsafe 都有 // SAFETY: 说明）" \
        cargo clippy --workspace --all-targets -- -D warnings
    check "cargo test 全部通过（全部 crate，含金样与 fail point 用例）" cargo test --release --workspace
    check "金样齐全：tests/golden/ 有 11 份金样文件，wmfs-core 里至少 11 个 golden 测试" m0_golden_present
    check "金样逐字节一致：cargo test --release -p wmfs-core golden" cargo test --release -p wmfs-core golden
    check "wmfs --version 输出 wmfs 0.1.0 (proto 1, core-abi 1, c-abi 1)" \
        m0_output_is "wmfs 0.1.0 (proto 1, core-abi 1, c-abi 1)" wmfs --version
    check "wmfs doctor 输出检查表并以 0 退出" wmfs doctor
    check_json "wmfs doctor --json：13 项体检齐全、没有 ✗、版本号正确" \
        ".ok == true and .version == \"wmfs 0.1.0 (proto 1, core-abi 1, c-abi 1)\"
         and ([.checks[].id] | sort) == ($M0_DOCTOR_IDS | sort)
         and all(.checks[]; .status == \"ok\" or .status == \"warn\")" \
        wmfs doctor --json
    check_json "doctor 不提示任何 sudo 操作；RDMA 不可用时写明「本机不做 RDMA 实机测试」" \
        '(tostring | contains("sudo") | not)
         and (.checks[] | select(.id == "rdma") | .status == "ok" or (.title | contains("本机不做 RDMA 实机测试")))' \
        wmfs doctor --json
    check_json "scripts/loc.sh 统计非测试代码行数、与 M0 预算（2,900 行）对照，且按模块重估的首发总量不超过上限（36,000 行）" \
        '.milestone == "M0" and .budget == 2900 and .total > 0 and (.modules | length) > 0
         and .projected <= .limit' \
        scripts/loc.sh M0 --json
}

# cargo 的 JSON 消息是一行一个对象；用 jq -s 收成一个数组交给 check_json。
# --all-targets 连测试代码一起编译（cargo test 只打印测试代码的警告、不失败）；cargo 自己的警告
# （例如 Cargo.toml 里拼错的键）不在 JSON 里，只打印在 stderr 上，所以另外检查以 warning 开头的行。
m0_build_json() {
    local out rc err
    err="$(mktemp)"
    out="$(cargo build --release --workspace --all-targets --message-format=json 2>"$err")"
    rc=$?
    printf '%s\n' "$out" | jq -s '.'
    cat "$err" >&2
    if grep -q '^warning' "$err"; then
        echo "cargo 在 stderr 上打印了警告（见上面）" >&2
        rc=1
    fi
    rm -f "$err"
    return "$rc"
}

m0_golden_present() {
    local f
    for f in $M0_GOLDEN_FILES; do
        if [ ! -s "tests/golden/$f.golden" ]; then
            echo "缺少金样文件 tests/golden/$f.golden"
            return 1
        fi
    done
    # `--list` 每行 "<测试名>: test"，是给工具读的固定格式
    local n
    n=$(cargo test --release -p wmfs-core golden -- --list 2>/dev/null | grep -c ': test$')
    echo "wmfs-core 里名字含 golden 的测试：$n 个"
    [ "$n" -ge 11 ]
}

# m0_output_is "期望的整行输出" 命令…
m0_output_is() {
    local want="$1"
    shift
    local got
    got="$("$@")" || return 1
    echo "期望：$want"
    echo "得到：$got"
    [ "$got" = "$want" ]
}
