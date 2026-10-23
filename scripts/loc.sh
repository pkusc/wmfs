#!/usr/bin/env bash
# 统计非测试代码行数，与 roadmap §1 的预算对照，并按模块重新估算首发总量（roadmap §0 第 7 条、design.md 2.2）。
#
# 用法：scripts/loc.sh [Mx] [--json]
#   Mx 省略时取 scripts/accept.d/ 里编号最大的里程碑（也就是当前做到的里程碑）；--json 输出机器可读的结果。
#
# 计数规则（简单、可复现）：
#   - 只数 .rs 文件；不数 target/、测试 crate tests/（wmfs-testkit）、各 crate 的 tests/ 目录、名为 tests.rs 的文件；
#   - 每个文件从第一行数到第一个 `#[cfg(test)]` 为止（约定：单元测试模块一律放在文件末尾）；
#   - 不数空行与纯注释行（以 // 开头，含 /// 与 //!）。
# 预计首发总量 = Σ max(模块当前行数, 模块估算)。上限 36,000（人类于 2026-10-21 从 30,000 放宽，功能不砍）；超过上限就停下来和人类商量。
# 提醒（只打印，不改退出码）：累计行数超过本里程碑预算；某个模块超过估算（"超出"）或已达估算的 90%（"≥90%"——
# 后续里程碑还要往里加代码时，请按剩余工作在下面的 MODULES 里上调估算，并在 design.md 2.2 写实现注记）。
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1

MILESTONE=""
JSON=0
for a in "$@"; do
    case "$a" in
        --json) JSON=1 ;;
        M[0-9]*) MILESTONE="$a" ;;
        *) echo "用法：scripts/loc.sh [Mx] [--json]"; exit 2 ;;
    esac
done
if [ -z "$MILESTONE" ]; then
    n=$(find scripts/accept.d -name 'm*.sh' 2>/dev/null | sed -E 's|.*/m([0-9]+)\.sh$|\1|' | sort -n | tail -1)
    MILESTONE="M${n:-0}"
fi

# roadmap §1：每个里程碑结束时的累计行数预算
declare -A BUDGET=([M0]=2900 [M1]=5440 [M2]=6440 [M3]=8740 [M4]=11390 [M5]=14190 [M6]=17090 [M7]=19190
    [M8]=20500 [M9]=22650 [M10]=23800 [M11]=25900 [M12]=26750 [M13]=28100 [M14]=29290 [M15]=29600)
LIMIT=36000

# 首发时各模块的行数估算（顺序即输出顺序）。来自 design.md 2.2，M0 结束时按剩余工作重估了三个模块（见 design.md 2.2
# 表下的实现注记）：wmfs-api 550 → 900（7.1 约 30 个管理端点的请求/响应结构都还没写）、core/proto 1300 → 900（全部消息
# 类型已在 M0 写完，只剩握手证明与密钥派生、cap 签名与校验）、cli 2810 → 2950（doctor 已约 300 行，原估 180）。
# M1 结束时又重估了四个（design.md 2.2 的 M1 注记）：core/net 800 → 1000（M1 已写完几乎全部，余下 leader_hint、
# OneWay 服务、call_cb 后台建连）、examples/plugin-mem-transport 150 → 180（已写完）、plugins 4980 → 4810（tcp 300 → 200、
# shared-key 150 → 80，都已写完）、dashboard 920 → 880（ApiClient 160 → 120，已写完）。
# M1 审查修复后又重估了两个（design.md 2.2 的 M1 注记）：core/net 1000 → 1120（修复后约 1,060，余下约 60）、
# examples/plugin-mem-transport 180 → 195（已写完：加了帧上限与收帧计时）。
# M2：rdma 实际约 1,490 行（估算 920），plugins 4810 → 5380。人类于 2026-10-21 把上限从 30,000 放宽到 36,000、功能不砍，
# 所以不动用"简化储备"（design.md 2.2 的 M2 注记）。
# M3（design.md 2.2 的 M3 注记）：core/net 1120 → 1180（单飞建连约 +65：审查后补了"拿着登记表的锁再查一次连接表"与
# 建连线程 panic 时的收尾；余下 leader_hint、OneWay 服务、call_cb 后台建连约 60）、
# core/meta/engine 720 → 760（已写约 660：执行器、覆盖层、读闸门、引擎、去重；余下 M4 的 changelog 接入、M8 的 leader 提示约 100）。
# M4（design.md 2.2 的 M4 注记）：新增约 2,680 行（预算 2,650），没有模块超过估算，估算都不变。
# M5（design.md 2.2 的 M5 注记）：新增约 3,150 行（预算约 2,950）。重估两个：core 540 → 720（server.rs + init.rs 已约 580，
# 余下 M7 管理面、M8 join 的本地侧约 150）、cli 2950 → 3700（已约 2,300，`wmfs fs` 实际约 560 行而原估 180；余下 fuse/ 约 1,150、
# 挂载路径映射约 70、其余子命令约 180）。
# M6（design.md 2.2 的 M6 注记）：连同审查修复新增约 2,620 行（预算约 2,955）。重估两个：core/client 1800 → 2200（已约 1,860：
# inode、files、writeback、flusher、read、strong、session；余下 M10 的配额额度、M12 的 locks.rs 与 strong 的读和 O_APPEND、驱逐后的
# 处理约 330）、cli 3700 → 3800（已约 3,330：fuse/ 约 830、dev 的 mount/proc 约 200；余下挂载路径映射约 70、M12 的 getlk/setlk 与
# lock-wait 约 120、M7–M15 的其余子命令约 180）。
MODULES="wmfs-api:900 core/base:950 core/obs:250 core/proto:900 core/buf:420 core/net:1180 core/hooks:1060
core/meta/raft:1570 core/meta/engine:760 core/meta/schema:620 core/meta/ns:2000 core/meta:2220 core/meta/cluster:1260
core/data:3000 core/client:2200 core/bg:850 core/mgmt:790 core:720 plugins:5380 cli:3800 dashboard:880
examples/plugin-mem-transport:195"

# 文件路径 → 模块名；测试文件返回空。
module_of() {
    case "$1" in
        tests/* | */tests/* | */tests.rs) echo "" ;;
        api/*) echo "wmfs-api" ;;
        core/src/meta/raft/* | core/src/meta/engine/* | core/src/meta/schema/* | core/src/meta/ns/* | core/src/meta/cluster/*)
            local r="${1#core/src/meta/}"
            echo "core/meta/${r%%/*}"
            ;;
        core/src/meta/*) echo "core/meta" ;;
        core/src/init/*) echo "core" ;; # init.rs 的子模块（init/initial_kv.rs）与 init.rs、server.rs 同属"core"（design.md 2.2）
        core/src/*/*)
            local r="${1#core/src/}"
            echo "core/${r%%/*}"
            ;;
        core/src/*) echo "core" ;;
        plugins/*) echo "plugins" ;;
        cli/*) echo "cli" ;;
        dashboard/*) echo "dashboard" ;;
        examples/plugin-mem-transport/*) echo "examples/plugin-mem-transport" ;;
        *) echo "" ;;
    esac
}

count_file() {
    awk '/^[[:space:]]*#\[cfg\(test\)\]/ { exit }
         /^[[:space:]]*$/ { next }
         /^[[:space:]]*\/\// { next }
         { n++ }
         END { print n + 0 }' "$1"
}

declare -A LINES=()
while IFS= read -r f; do
    m=$(module_of "$f")
    [ -n "$m" ] || continue
    LINES[$m]=$((${LINES[$m]:-0} + $(count_file "$f")))
done < <(find . -name '*.rs' -not -path './target/*' -not -path '*/target/*' | sed 's|^\./||' | sort)

total=0
est_total=0
projected=0
rows=""
for spec in $MODULES; do
    name="${spec%%:*}"
    est="${spec##*:}"
    cur="${LINES[$name]:-0}"
    total=$((total + cur))
    est_total=$((est_total + est))
    projected=$((projected + (cur > est ? cur : est)))
    note="-"
    if [ "$cur" -gt "$est" ]; then
        note="超出"
    elif [ $((cur * 100)) -ge $((est * 90)) ]; then
        note="≥90%"
    fi
    rows="$rows$name $cur $est $note"$'\n'
done
budget="${BUDGET[$MILESTONE]:-0}"
over_budget=false
if [ "$budget" -gt 0 ] && [ "$total" -gt "$budget" ]; then
    over_budget=true
fi

if [ "$JSON" -eq 1 ]; then
    printf '%s' "$rows" | jq -R -s --arg m "$MILESTONE" --argjson budget "$budget" --argjson total "$total" \
        --argjson projected "$projected" --argjson limit "$LIMIT" --argjson est_total "$est_total" \
        --argjson over_budget "$over_budget" '
        [split("\n")[] | select(length > 0) | split(" ")
         | {name: .[0], lines: (.[1] | tonumber), estimate: (.[2] | tonumber), note: .[3]}] as $mods
        | {milestone: $m, budget: $budget, total: $total, over_budget: $over_budget,
           estimate_total: $est_total, projected: $projected, limit: $limit,
           over_estimate_modules: [$mods[] | select(.note == "超出") | .name],
           near_estimate_modules: [$mods[] | select(.note == "≥90%") | .name],
           modules: [$mods[] | del(.note)]}'
    exit 0
fi

echo "wmfs 非测试代码行数（不含空行、纯注释行与测试代码）"
# printf 按字节补齐，一个汉字占 3 字节、显示 2 列，所以中文表头的宽度要多给一些
printf '%-34s %10s %14s %14s  %s\n' "模块" "当前" "首发估算" "剩余估算" "说明"
printf '%s' "$rows" | while read -r name cur est note; do
    left=$((est > cur ? est - cur : 0))
    [ "$note" = "-" ] && note=""
    printf '%-32s %8d %10d %10d  %s\n' "$name" "$cur" "$est" "$left" "$note"
done
printf '%-34s %8d %10d\n' "合计" "$total" "$est_total"
if [ "$budget" -gt 0 ]; then
    echo "$MILESTONE 结束时的累计预算（roadmap §1）：$budget 行；当前 $total 行（$((total * 100 / budget))%）"
    if [ "$over_budget" = true ]; then
        echo "注意：$MILESTONE 已超出累计预算 $(((total - budget) * 100 / budget))%（+$((total - budget)) 行）：请按模块重估剩余工作（roadmap §0 第 7 条）"
    fi
fi
echo "说明列：超出 = 当前行数已超过估算；≥90% = 已达估算的 90%（后续还要往里加代码就上调估算）"
echo "按模块重估的首发总量 Σ max(当前, 估算) = $projected 行（上限 $LIMIT）"
if [ "$projected" -gt "$LIMIT" ]; then
    echo "注意：预计超过 $LIMIT 行，请先按 roadmap §4 简化储备删减；删完仍超就停下来和人类商量"
fi
