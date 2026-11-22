#!/bin/bash
# demo_cluster.sh - 三节点集群故障演练（P4）
# 用法： bash scripts/demo_cluster.sh

set -e
cd "$(dirname "$0")/.."

BUILD=build
BIN=$BUILD/kvserverd
CLI=$BUILD/kvclient
DATA=/tmp/kvraft-demo
A1=127.0.0.1:9101
A2=127.0.0.1:9102
A3=127.0.0.1:9103
CLUSTER="$A1 $A2 $A3"

echo "==== [1/6] 编译 ===="
( cd $BUILD && make -j4 >/dev/null )

echo "==== [2/6] 清理旧数据并启动 3 节点 ===="
rm -rf $DATA
mkdir -p $DATA
$BIN 0 $DATA/n0 $CLUSTER >$DATA/n0.log 2>&1 &
$BIN 1 $DATA/n1 $CLUSTER >$DATA/n1.log 2>&1 &
$BIN 2 $DATA/n2 $CLUSTER >$DATA/n2.log 2>&1 &
sleep 1

# 轮询各节点的 role 状态文件找 Leader（节点持续维护 data/role）
find_leader() {
    for i in 0 1 2; do
        if grep -q LEADER $DATA/n$i/role 2>/dev/null; then
            echo $i
            return
        fi
    done
    echo -1
}

LEADER=$(find_leader)
while [ "$LEADER" = "-1" ]; do
    sleep 0.2
    LEADER=$(find_leader)
done
LEADER_PORT=$([ "$LEADER" = 0 ] && echo $A1 || { [ "$LEADER" = 1 ] && echo $A2 || echo $A3; })
echo "Leader 当选：节点 $LEADER ($LEADER_PORT)"
grep -H . $DATA/n*/role

echo "==== [3/6] 写入 3 条数据 ===="
$CLI $LEADER_PORT set name kmx
$CLI $LEADER_PORT set school nepu
$CLI $LEADER_PORT set lang cpp
$CLI $LEADER_PORT get name

echo "==== [4/6] kill -9 强杀 Leader（模拟宕机）===="
LEADER_PID=$(cat $DATA/n$LEADER/pid)
kill -9 $LEADER_PID
echo "旧 Leader (节点 $LEADER, pid $LEADER_PID) 已被强杀，等待重新选主..."
sleep 3

NEW_LEADER=
for i in 0 1 2; do
    [ "$i" = "$LEADER" ] && continue
    if grep -q LEADER $DATA/n$i/role 2>/dev/null; then
        NEW_LEADER=$i
        break
    fi
done
if [ -z "$NEW_LEADER" ]; then
    echo "ERR: 3 秒内没有选出新 Leader！"; exit 1
fi
NEW_PORT=$([ "$NEW_LEADER" = 0 ] && echo $A1 || { [ "$NEW_LEADER" = 1 ] && echo $A2 || echo $A3; })
echo "新 Leader 当选：节点 $NEW_LEADER ($NEW_PORT)"

echo "==== [5/6] 向新 Leader 读数据（验证故障转移不丢数据）===="
$CLI $NEW_PORT get name
$CLI $NEW_PORT get school
$CLI $NEW_PORT set after_failover ok   # 新 Leader 继续可写

echo "==== [6/6] 重启旧 Leader（验证日志追平）===="
$BIN $LEADER $DATA/n$LEADER $CLUSTER >>$DATA/n$LEADER.log 2>&1 &
sleep 2
# 旧 Leader 回来后变 Follower，从新 Leader 补齐宕机期间漏掉的日志
grep -H . $DATA/n*/role
$CLI $NEW_PORT get after_failover

echo "==== 演练完成 ===="
echo "日志：$DATA/n*.log（tail -f 可看选主/复制过程）"
echo "数据：$DATA/n*/raft.wal"
