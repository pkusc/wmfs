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
# 预计首发总量 = Σ max(模块当前行数, 模块估算)。上限 38,000（人类于 2026-10-21 从 30,000 放宽到 36,000、2026-11-05 再放宽到 38,000，
# 功能不砍，ADR-0041 第 2 条）；超过上限就停下来和人类商量；超过 37,000 时上报主管。
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
LIMIT=38000

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
# M7（design.md 2.2 的 M7 注记）：连同审查修复新增约 2,135 行（预算约 2,140）。重估两个：wmfs-api 900 → 1100（已约 815：M7 的 12 个端点的结构
# 约 145 行；余下集群、链、会话、任务、存储类、key、feature、trace、qos、插件清单等约 15 组端点约 290）、cli 3800 → 4850（已约 3,800：M7 的策略命令、
# 路径换算、changelog/cluster 约 455；余下 M8 的 join/node/cluster plugins/dev 多节点约 300、M9 chain 约 80、M10 key/session 约 130、M11 task/
# datakey/feature 约 135、M12 锁约 150、M13 class 约 115、M14 mgr/trace/dev wait 约 165）。
# M8（design.md 2.2 的 M8 注记）：新增约 1,950 行（预算约 1,310：留给 M8 的几件——uninline 的 fencing、OneWay 服务、插件清单的退出与逃生口、
# 集群与节点的管理端点、debug meta-hash——都在这里做了）。重估五个：core/net 1180 → 1220（已约 1,180：OneWay 分发、send_oneway、wait_room；
# 余下 M9 的 call_cb 后台建连约 40）、core/meta/raft 1570 → 1650（已约 1,630：快照、raft-send、成员变更都写完了，余下小改）、
# core/meta/engine 760 → 800（已约 760，余下 M10 的配额与锁的对接约 40）、core 720 → 900（已约 840：join 的本地侧、server 的装配；
# 余下 M9 的 nas-safe/hpc-rdma profile、M14 的 mgr 装配约 60）、cli 4850 → 4900（已约 4,140：M8 的命令实际约 345，原估 300；余下 M9–M14
# 的子命令约 745）。
# M8 审查修复（design.md 2.2 的 M8 审查注记）：新增约 275 行。重估三个：core/meta/raft 1650 → 1750（已约 1,725：raft-send 的退避与
# 不认识的对端地址的查找、leader 发布的 active；余下小改约 25）、core/meta/engine 800 → 830（已约 790，余下 M10 的配额与锁的对接约 40）、
# core/net 1220 → 1230（已约 1,190：流的有界通道；余下 M9 的 call_cb 后台建连约 40）。
# M9（design.md 2.2 的 M9 注记）：新增约 2,000 行（预算约 2,220）。重估两个已达 90% 的：core/mgmt 790 → 950（已约 745：M9 的链端点约 65；
# 余下 M10–M14 的约 12 组端点约 200）、core 900 → 920（已约 855；余下 M14 的 mgr 装配约 60）。
# M9 审查修复（design.md 2.2 的 M9 审查修复注记）：再新增约 150 行（M9 合计约 2,140，预算约 2,220），没有模块超过估算，估算都不变。
# M9 验证修复（design.md 2.2 的 M9 验证修复注记）：再新增约 23 行。重估一个：core/meta/raft 1750 → 1780（已约 1,757：snap-recv 的排队；余下小改约 25）。
# M10（design.md 2.2 的 M10 注记）：新增约 1,800 行（预算约 1,170）。重估八个：wmfs-api 1100 → 1150（余下 M11–M14 约 10 组端点的结构约 175）、
# core/net 1230 → 1270（已约 1,250：撤销 key 时关闭连接；余下小改约 20）、core/hooks 1060 → 1120（已约 960；余下 M14 的 cabi.rs 约 150）、
# core/meta 2220 → 2550（已约 2,200：quota.rs、evict.rs；余下 M12 的 locks.rs、回收站与审计、M11 gc 的去重记录清理约 350）、
# core/client 2200 → 2350（已约 2,130：lease.rs、配额额度、驱逐之后的处理；余下 M12 的 locks.rs、strong 的读与 O_APPEND 约 220）、
# core/mgmt 950 → 1250（已约 1,000：keys.rs；余下 M11–M14 的任务、数据密钥、feature、存储类、qos、trace 约 250）、
# cli 4900 → 5200（已约 4,620：key/session、dev wait session-evicted；余下 M11–M14 的子命令约 565）、
# plugins 5380 → 5100（已约 3,080；余下 mclock、xxh3/blake3、lz4、zstd、aes-gcm、cache-local-nvme、reconciler、scrub、tiering、rebalance、
# gc 的其余部分、trash-expire、ttl，按 2.2 的表约 1,925）。Σ max ≈ 34,850（上限 36,000）。
# M10 审查修复（design.md 2.2 的 M10 审查修复注记）：再新增约 165 行。重估三个已达 90% 的：core/client 2350 → 2430（已约 2,205：
# 经句柄的请求只在句柄的会话里发；余下 M12 的 locks.rs、strong 的读与 O_APPEND 约 220）、core/meta 2550 → 2600（已约 2,255；余下约 350）、
# core 920 → 925（已约 865；余下 M14 的 mgr 装配约 60）。Σ max ≈ 34,980（上限 36,000；plugins 若不按 M10 下调到 5,100 则约 35,260）。
# M10 核对修复（ADR-0041，design.md 2.2 的注记）：再新增约 40 行。重估一个已达 90% 的：core 925 → 930（已约 867；余下 M14 的 mgr 装配
# 约 60）。上限放宽到 38,000（ADR-0041 第 2 条，plugins 下调到 5,100 主管已确认），Σ max ≈ 34,985。
# M10 链成员变更的修正（ADR-0041 第 13–17 条，design.md 2.2 的注记）：再新增约 85 行。重估一个已达 90% 的：core/meta/cluster
# 1260 → 1340（已约 1,247：两步移出约 +76；余下 M13 的存储类 CRUD 与 QoS 表随集群图下发、按用量放置约 90）。Σ max ≈ 35,065。
# M11（design.md 2.2 的 M11 注记）：连同审查修复新增约 3,200 行（预算约 2,100；审查修复约 +420，ADR-0042）。已超出或已达 90%、后面还要加
# 代码的都按剩余工作重估，共八个：wmfs-api 1150 → 1170（已约 1,061；余下存储类、qos、trace、feature、node 的结构约 110）、core/data 3000 → 3120
# （已约 3,033：repair.rs、datakey.rs；余下 M13 的 QoS 表与保底欠额上报、M14 的 trace span 与 target 用量约 90）、core/bg 850 → 1170（已约 1,106：
# 调度器的节点任务与本地游标、进度与控制、遍历器、迁移原语、status 的逐文件统计；余下 M13 的 tiering/rebalance 对迁移原语与 declared() 的
# 小改约 55、M14 的 declared_mismatch_files 约 10）、core/meta 2600 → 2850（已约 2,532：data_reports、在线自检、gc 的元数据清理、compress/encrypt/
# 数据密钥的属性；余下 M12 的 locks.rs、回收站与审计约 285，status 里 trash、qos、audit 的实际约 35）、core/mgmt 1250 → 1340（已约 1,143：tasks.rs；
# 余下存储类、qos、trace、feature、node 的端点约 200）、core 930 → 965（已约 914：server.rs 拆出 server/roles.rs——module_of 以前把
# core/src/server/ 算成了不存在的模块"core/server"，现在算进 core；余下 M14 的 mgr 装配约 50）、cli 5200 → 5500（已约 4,954；按 7.2 的命令
# 清单重估 M12–M14 余下约 540，含 roadmap 没排进里程碑的 feature list|enable 约 30）、plugins 5100 → 5270（已约 4,117；按 design 2.2 的表余下
# 约 1,150，含 M10 下调时漏算的 label-match 完整版约 140）。Σ max ≈ 36,370（上限 38,000；超过 37,000 要上报）。
MODULES="wmfs-api:1170 core/base:950 core/obs:250 core/proto:900 core/buf:420 core/net:1270 core/hooks:1120
core/meta/raft:1780 core/meta/engine:830 core/meta/schema:620 core/meta/ns:2000 core/meta:2850 core/meta/cluster:1340
core/data:3120 core/client:2430 core/bg:1170 core/mgmt:1340 core:965 plugins:5270 cli:5500 dashboard:880
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
        core/src/init/* | core/src/server/*) echo "core" ;; # init.rs、server.rs 的子模块（init/initial_kv.rs、server/roles.rs）同属"core"（design.md 2.2）
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
