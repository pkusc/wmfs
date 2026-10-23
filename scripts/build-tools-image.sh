#!/usr/bin/env bash
# 构建外部测试工具镜像 wmfs-tools（design.md 9.6、roadmap §5：人类于 2026-10-20 同意联网构建）。
# 用法：scripts/build-tools-image.sh          第一次需要几分钟并联网；之后有缓存，很快
# 代理：构建容器里的 apt、git 不会自动继承 shell 的 http_proxy，所以用 --build-arg 传进去（地址只来自环境变量，不写进仓库）；
# --network host 让构建容器能连到本机的代理。只建名字是 wmfs-tools 的镜像，不碰机器上别的镜像与容器。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
command -v docker >/dev/null 2>&1 || { echo "没有 docker：装好 docker 并把自己加进 docker 组再来"; exit 2; }
# 本机经代理联网时断时续：整个构建最多试 5 次（已经成功的层有缓存，重试很快）
for i in 1 2 3 4 5; do
    if docker build --network host \
        --build-arg "http_proxy=${http_proxy:-}" --build-arg "https_proxy=${https_proxy:-}" \
        --build-arg "HTTP_PROXY=${HTTP_PROXY:-${http_proxy:-}}" --build-arg "HTTPS_PROXY=${HTTPS_PROXY:-${https_proxy:-}}" \
        -t wmfs-tools -f "$ROOT/tests/docker/Dockerfile.tools" "$ROOT/tests/docker"; then
        echo "已构建镜像 wmfs-tools（docker images wmfs-tools 查看）"
        exit 0
    fi
    echo "第 $i 次构建失败（多半是联网断了一下），10 s 后重试"
    sleep 10
done
echo "构建 wmfs-tools 失败：请检查网络与代理（http_proxy / https_proxy）"
exit 1
