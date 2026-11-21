// ============================================================================
// test_skiplist.cpp - 跳表单元测试（用 std::map 作参考实现做差分测试）
// ============================================================================
//
// 【差分测试思路】
//   跳表是复杂结构，怎么证明它是对的？用一个"公认正确"的简单实现
//   （std::map，红黑树）作对照组：对同一串随机操作序列，
//   断言两者每一步的返回值、size、查询结果完全一致。
//   这也是工业界验证复杂数据结构的主流手段（如 RocksDB 用同样的方法测 MemTable）。
//
// 【为什么固定随机种子？】
//   mt19937(42) 保证每次测试跑的是同一条操作序列 —— 测试可复现，
//   一旦失败能稳定复现定位；若想扩大覆盖面可在 CI 里多换几个种子。
// ============================================================================

#include <cassert>
#include <cstdio>
#include <map>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "storage/SkipList.h"

// --------------------------------------------------------------------------
// 测试 1：顺序插入 + 精确查询 + 更新语义 + 删除（确定性场景，覆盖基本路径）
// --------------------------------------------------------------------------
static void test_sequential() {
    SkipList<int, std::string> list;

    // 1) 顺序插入 10000 个不重复 key，全部应返回"新插入"
    for (int i = 0; i < 10000; ++i) {
        bool updated = list.insert_or_update(i, "v" + std::to_string(i));
        assert(!updated);
    }
    assert(list.size() == 10000);

    // 2) 每个 key 都能精确查到，值正确
    for (int i = 0; i < 10000; ++i) {
        std::string* v = list.find(i);
        assert(v && *v == "v" + std::to_string(i));
    }
    // 3) 边界外的 key 必须查不到
    assert(list.find(-1) == nullptr);
    assert(list.find(10000) == nullptr);

    // 4) 更新语义：已存在的 key 返回 true，且值被覆盖
    bool updated = list.insert_or_update(500, "UPDATED");
    assert(updated);
    assert(*list.find(500) == "UPDATED");

    // 5) 删除前一半，验证 size 与查询结果
    for (int i = 0; i < 5000; ++i) {
        assert(list.erase(i));
    }
    assert(list.size() == 5000);
    assert(list.find(0) == nullptr);      // 已删，查不到
    assert(list.find(9999) != nullptr);   // 未删，能查到

    printf("[PASS] test_sequential\n");
}

// --------------------------------------------------------------------------
// 测试 2：10 万次随机操作（插入/更新/删除/查询）与 std::map 差分比对
//   这是核心测试：随机顺序能覆盖跳表各种层高分布、摘链、层数收缩等路径
// --------------------------------------------------------------------------
static void test_random_vs_map() {
    SkipList<int, int> list;
    std::map<int, int> ref; // 参考实现（对照组）

    std::mt19937 rng(42); // 固定种子，测试可复现
    std::uniform_int_distribution<int> key_dist(0, 20000);  // key 有碰撞 -> 覆盖更新
    std::uniform_int_distribution<int> val_dist(0, 1000000);
    std::uniform_int_distribution<int> op_dist(0, 2);       // 三种操作均匀分布

    const int OPS = 100000;
    for (int i = 0; i < OPS; ++i) {
        int k = key_dist(rng);
        int op = op_dist(rng);

        if (op == 0) {
            // ---- 插入/更新 ----
            // 断言点 1：返回值（是否发生更新）必须与 map 一致
            int v = val_dist(rng);
            bool updated = list.insert_or_update(k, v);
            auto it = ref.find(k);
            bool ref_updated = (it != ref.end());
            assert(updated == ref_updated);
            ref[k] = v;
        } else if (op == 1) {
            // ---- 删除 ----
            // 断言点 2：是否删除成功必须与 map 一致
            bool removed = list.erase(k);
            size_t n = ref.erase(k);
            assert(removed == (n == 1));
        } else {
            // ---- 查询 ----
            // 断言点 3：查询结果（不存在/值）必须与 map 一致
            int* v = list.find(k);
            auto it = ref.find(k);
            if (it == ref.end()) {
                assert(v == nullptr);
            } else {
                assert(v && *v == it->second);
            }
        }
        // 断言点 4：每步操作后 size 严格一致
        assert(list.size() == ref.size());
    }
    printf("[PASS] test_random_vs_map (%d ops)\n", OPS);
}

// --------------------------------------------------------------------------
// 测试 3：范围扫描与 std::map 区间结果比对（1000 次随机区间）
//   重点验证 scan 的两个易错点：
//   a. 起点：第一个 >= begin 的定位（lower_bound 语义）
//   b. 终点：包含 key == end 的节点（闭区间语义）
// --------------------------------------------------------------------------
static void test_scan() {
    SkipList<int, int> list;
    std::map<int, int> ref;
    std::mt19937 rng(7);
    std::uniform_int_distribution<int> dist(0, 1000);

    // 插入 500 个随机 key（有重复 -> insert_or_update 走更新路径）
    for (int i = 0; i < 500; ++i) {
        int k = dist(rng);
        list.insert_or_update(k, k * 10);
        ref[k] = k * 10;
    }

    for (int trial = 0; trial < 1000; ++trial) {
        int b = dist(rng), e = dist(rng);
        if (b > e) std::swap(b, e); // 区间无序时交换（边界健壮性）

        // 跳表扫描结果
        std::vector<std::pair<int, int>> out;
        list.scan(b, e, out);

        // 期望结果：map 的 [b, e] 闭区间（lower_bound 到 first > e 之前）
        std::vector<std::pair<int, int>> expect;
        for (auto it = ref.lower_bound(b); it != ref.end() && it->first <= e; ++it) {
            expect.emplace_back(it->first, it->second);
        }

        // 逐项比对：数量、key、value、顺序（升序）
        assert(out.size() == expect.size());
        for (size_t i = 0; i < out.size(); ++i) {
            assert(out[i] == expect[i]);
        }
    }
    printf("[PASS] test_scan\n");
}

int main() {
    test_sequential();     // 基本路径
    test_random_vs_map();  // 差分测试（10 万随机操作）
    test_scan();           // 范围扫描
    printf("ALL TESTS PASSED\n");
    return 0;
}
