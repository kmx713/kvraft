#!/bin/bash
# bench.sh - 简易吞吐压测（P4）
# 用法： bash scripts/bench.sh [总操作数] [并发数] [服务器地址]
# 默认： 10000 条 / 8 并发 / 127.0.0.1:9101
# 注意：kvclient 每条命令新建连接，测的是"含建连开销"的保守 QPS。

TOTAL=${1:-10000}
CONC=${2:-8}
ADDR=${3:-127.0.0.1:9101}

cd "$(dirname "$0")/.."
CLI=build/kvclient

# 压测必须打 Leader（Follower 只会回重定向）
DATA=${DATA:-/tmp/kvraft-demo}
LEADER_PORT=$ADDR
for i in 0 1 2; do
    if [ -f $DATA/n$i/role ] && grep -q LEADER $DATA/n$i/role; then
        case $i in
            0) LEADER_PORT=127.0.0.1:9101 ;;
            1) LEADER_PORT=127.0.0.1:9102 ;;
            2) LEADER_PORT=127.0.0.1:9103 ;;
        esac
        break
    fi
done
echo "压测目标：$LEADER_PORT  总数=$TOTAL  并发=$CONC"

PER=$((TOTAL / CONC))
START=$(date +%s.%N)

# N 个并发进程各跑一段 key（分段避免互相覆盖），wait 等全部结束
for c in $(seq 0 $((CONC - 1))); do
    (
        for i in $(seq 0 $((PER - 1))); do
            KEY="k$((c * PER + i))"
            $CLI $LEADER_PORT set $KEY v$i >/dev/null
        done
    ) &
done
wait

END=$(date +%s.%N)
ELAPSED=$(echo "$END $START" | awk '{printf "%.2f", $1 - $2}')
QPS=$(echo "$TOTAL $ELAPSED" | awk '{printf "%.0f", $1 / $2}')

echo "----------------------------------------"
echo "总耗时   : ${ELAPSED}s"
echo "吞吐     : ${QPS} ops/s（含每条命令建连开销）"
echo "数据校验 : $( $CLI $LEADER_PORT info )"
