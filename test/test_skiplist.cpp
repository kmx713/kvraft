// test_skiplist.cpp - 跳表单元测试
// 1. 顺序插入 + 精确查询
// 2. 随机操作（插入/更新/删除）与 std::map 参考实现比对
// 3. 范围扫描与 std::map 区间结果比对
#include <cassert>
#include <cstdio>
#include <map>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "storage/SkipList.h"

static void test_sequential() {
    SkipList<int, std::string> list;
    for (int i = 0; i < 10000; ++i) {
        bool updated = list.insert_or_update(i, "v" + std::to_string(i));
        assert(!updated);
    }
    assert(list.size() == 10000);
    for (int i = 0; i < 10000; ++i) {
        std::string* v = list.find(i);
        assert(v && *v == "v" + std::to_string(i));
    }
    assert(list.find(-1) == nullptr);
    assert(list.find(10000) == nullptr);

    // 更新语义
    bool updated = list.insert_or_update(500, "UPDATED");
    assert(updated);
    assert(*list.find(500) == "UPDATED");

    // 删除
    for (int i = 0; i < 5000; ++i) {
        assert(list.erase(i));
    }
    assert(list.size() == 5000);
    assert(list.find(0) == nullptr);
    assert(list.find(9999) != nullptr);
    printf("[PASS] test_sequential\n");
}

static void test_random_vs_map() {
    SkipList<int, int> list;
    std::map<int, int> ref;
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> key_dist(0, 20000);
    std::uniform_int_distribution<int> val_dist(0, 1000000);
    std::uniform_int_distribution<int> op_dist(0, 2);

    const int OPS = 100000;
    for (int i = 0; i < OPS; ++i) {
        int k = key_dist(rng);
        int op = op_dist(rng);
        if (op == 0) { // 插入/更新
            int v = val_dist(rng);
            bool updated = list.insert_or_update(k, v);
            auto it = ref.find(k);
            bool ref_updated = (it != ref.end());
            assert(updated == ref_updated);
            ref[k] = v;
        } else if (op == 1) { // 删除
            bool removed = list.erase(k);
            size_t n = ref.erase(k);
            assert(removed == (n == 1));
        } else { // 查询
            int* v = list.find(k);
            auto it = ref.find(k);
            if (it == ref.end()) {
                assert(v == nullptr);
            } else {
                assert(v && *v == it->second);
            }
        }
        assert(list.size() == ref.size());
    }
    printf("[PASS] test_random_vs_map (%d ops)\n", OPS);
}

static void test_scan() {
    SkipList<int, int> list;
    std::map<int, int> ref;
    std::mt19937 rng(7);
    std::uniform_int_distribution<int> dist(0, 1000);
    for (int i = 0; i < 500; ++i) {
        int k = dist(rng);
        list.insert_or_update(k, k * 10);
        ref[k] = k * 10;
    }

    for (int trial = 0; trial < 1000; ++trial) {
        int b = dist(rng), e = dist(rng);
        if (b > e) std::swap(b, e);

        std::vector<std::pair<int, int>> out;
        list.scan(b, e, out);

        std::vector<std::pair<int, int>> expect;
        for (auto it = ref.lower_bound(b); it != ref.end() && it->first <= e; ++it) {
            expect.emplace_back(it->first, it->second);
        }
        assert(out.size() == expect.size());
        for (size_t i = 0; i < out.size(); ++i) {
            assert(out[i] == expect[i]);
        }
    }
    printf("[PASS] test_scan\n");
}

int main() {
    test_sequential();
    test_random_vs_map();
    test_scan();
    printf("ALL TESTS PASSED\n");
    return 0;
}
