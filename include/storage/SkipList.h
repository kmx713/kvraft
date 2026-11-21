#pragma once
// SkipList.h - 手写跳表存储引擎
// 多级索引 + 随机层高，O(log n) 插入/查询/删除，支持范围扫描
// 自带互斥锁，可被多线程安全调用

#include <mutex>
#include <random>
#include <utility>
#include <vector>
#include <functional>

template <typename Key, typename Value, typename Compare = std::less<Key>>
class SkipList {
public:
    explicit SkipList(int max_level = 12, double p = 0.25)
        : max_level_(max_level), p_(p), level_(1), size_(0),
          rng_(20261121), dist_(0.0, 1.0) {
        head_ = new Node(Key(), Value(), max_level_);
    }

    ~SkipList() {
        Node* cur = head_->forward[0];
        while (cur) {
            Node* next = cur->forward[0];
            delete cur;
            cur = next;
        }
        delete head_;
    }

    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    // 插入或更新。返回 true 表示 key 已存在（发生了更新），false 表示新插入
    bool insert_or_update(const Key& k, const Value& v) {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<Node*> update(max_level_, nullptr);

        Node* cur = head_;
        for (int i = level_ - 1; i >= 0; --i) {
            while (cur->forward[i] && comp_(cur->forward[i]->key, k)) {
                cur = cur->forward[i];
            }
            update[i] = cur;
        }

        Node* next = cur->forward[0];
        if (next && !comp_(k, next->key) && !comp_(next->key, k)) {
            next->value = v; // 已存在，原地更新
            return true;
        }

        int lvl = random_level();
        if (lvl > level_) {
            for (int i = level_; i < lvl; ++i) update[i] = head_;
            level_ = lvl;
        }

        Node* node = new Node(k, v, lvl);
        for (int i = 0; i < lvl; ++i) {
            node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = node;
        }
        ++size_;
        return false;
    }

    // 查找，找到返回 value 指针（调用方需在锁保护期内使用或拷贝），否则返回 nullptr
    Value* find(const Key& k) {
        std::lock_guard<std::mutex> lk(mu_);
        Node* node = find_greater_or_equal(k);
        if (node && !comp_(k, node->key) && !comp_(node->key, k)) {
            return &node->value;
        }
        return nullptr;
    }

    // 删除。返回 true 表示 key 存在并已删除
    bool erase(const Key& k) {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<Node*> update(max_level_, nullptr);

        Node* cur = head_;
        for (int i = level_ - 1; i >= 0; --i) {
            while (cur->forward[i] && comp_(cur->forward[i]->key, k)) {
                cur = cur->forward[i];
            }
            update[i] = cur;
        }

        Node* target = cur->forward[0];
        if (!target || comp_(target->key, k) || comp_(k, target->key)) {
            return false;
        }

        for (int i = 0; i < level_; ++i) {
            if (update[i]->forward[i] == target) {
                update[i]->forward[i] = target->forward[i];
            }
        }
        delete target;
        --size_;

        while (level_ > 1 && head_->forward[level_ - 1] == nullptr) {
            --level_;
        }
        return true;
    }

    // 范围扫描 [begin, end]，结果按 key 升序
    void scan(const Key& begin, const Key& end,
              std::vector<std::pair<Key, Value>>& out) {
        std::lock_guard<std::mutex> lk(mu_);
        Node* node = find_greater_or_equal(begin);
        while (node && !comp_(end, node->key)) { // node->key <= end
            out.emplace_back(node->key, node->value);
            node = node->forward[0];
        }
    }

    // 全量按序导出（用于 WAL 压缩/快照）
    void dump_all(std::vector<std::pair<Key, Value>>& out) {
        std::lock_guard<std::mutex> lk(mu_);
        for (Node* node = head_->forward[0]; node; node = node->forward[0]) {
            out.emplace_back(node->key, node->value);
        }
    }

    size_t size() {
        std::lock_guard<std::mutex> lk(mu_);
        return size_;
    }

private:
    struct Node {
        Key key;
        Value value;
        std::vector<Node*> forward;
        Node(const Key& k, const Value& v, int level)
            : key(k), value(v), forward(level, nullptr) {}
    };

    // 找到第一个 key >= k 的节点（未加锁，内部使用）
    Node* find_greater_or_equal(const Key& k) {
        Node* cur = head_;
        for (int i = level_ - 1; i >= 0; --i) {
            while (cur->forward[i] && comp_(cur->forward[i]->key, k)) {
                cur = cur->forward[i];
            }
        }
        return cur->forward[0];
    }

    int random_level() {
        int lvl = 1;
        while (lvl < max_level_ && dist_(rng_) < p_) {
            ++lvl;
        }
        return lvl;
    }

    int max_level_;
    double p_;
    int level_;
    size_t size_;
    Node* head_;
    Compare comp_;
    std::mutex mu_;
    std::mt19937 rng_;
    std::uniform_real_distribution<double> dist_;
};
