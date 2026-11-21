#pragma once
// ============================================================================
// SkipList.h - 手写跳表存储引擎（本项目存储层的核心数据结构）
// ============================================================================
//
// 【为什么用跳表而不是红黑树 / B+ 树？】（高频面试题）
//   1. 范围查询友好：最底层是按 key 有序的链表，扫描 [begin, end] 只需
//      定位起点后沿 level-0 链表顺序走，天然支持 SCAN；
//   2. 实现简单：插入/删除只需修改指针，不像红黑树有复杂的旋转+变色；
//   3. 并发友好：LevelDB / Redis 有序集合（zset）底层都是跳表；
//   4. 复杂度：平均 O(log n)，最坏 O(n)（概率极低，可忽略）。
//
// 【跳表原理一句话】
//   在有序链表之上建多级稀疏索引（第 i 层节点以概率 p 晋升到第 i+1 层），
//   查找时从最高层往右走，走不动了就下一层 —— 二分查找的效果，链表的实现。
//
// 【线程安全】
//   内部一把粗粒度互斥锁保护全部操作。正确且简单；
//   更进一步可做"无锁读 + 局部锁写"或分段锁，留作优化方向。
// ============================================================================

#include <mutex>
#include <random>
#include <utility>
#include <vector>
#include <functional>

template <typename Key, typename Value, typename Compare = std::less<Key>>
class SkipList {
public:
    // ------------------------------------------------------------------
    // 构造函数
    //   max_level : 最大层数。12 层时 0.25^11 ≈ 6e-7，足以支撑千万级数据；
    //   p         : 层间晋升概率。LevelDB/Redis 常用 1/4，比 1/2 更省内存
    //               （层数期望 1/(1-p) = 1.33 个指针），查询略慢但缓存友好；
    //   rng_      : 随机数发生器。固定种子（20261121）保证每次运行行为可复现，
    //               方便调试和单元测试；生产环境可用 time(nullptr) 播种；
    //   head_     : 哨兵节点（key 为默认值，不存数据），层数拉满到 max_level_，
    //               所有查找/插入都从头节点出发。
    // ------------------------------------------------------------------
    explicit SkipList(int max_level = 12, double p = 0.25)
        : max_level_(max_level), p_(p), level_(1), size_(0),
          rng_(20261121), dist_(0.0, 1.0) {
        head_ = new Node(Key(), Value(), max_level_);
    }

    // 析构：沿最底层链表逐个释放（最底层包含全部节点，一条链走完即可）
    ~SkipList() {
        Node* cur = head_->forward[0];
        while (cur) {
            Node* next = cur->forward[0];
            delete cur;
            cur = next;
        }
        delete head_;
    }

    // 禁止拷贝：内部持有锁和裸指针资源，拷贝语义复杂，直接禁止最安全
    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    // ==================================================================
    // 插入或更新（upsert 语义，对应 KV 的 SET 命令）
    // 返回 true  -> key 已存在，发生了原地更新
    // 返回 false -> key 是新插入的
    //
    // 算法步骤：
    //   1. 从最高层出发，每层先向右走、走不动了就"记下当前位置并下一层"，
    //      记下的位置叫 update[] —— 即新节点在第 i 层的前驱；
    //   2. 到底后检查 level-0 的下一个节点，若 key 相等则原地改值返回；
    //   3. 否则随机抽层高，按 update[] 记录的前驱把新节点"缝合"进各层。
    // ==================================================================
    bool insert_or_update(const Key& k, const Value& v) {
        std::lock_guard<std::mutex> lk(mu_);
        // update[i] = 新节点在第 i 层的前驱节点
        // 注意不能用变长数组（VLA 是 GCC 扩展且 max_level_ 是运行期值），
        // 用 vector 预分配，O(1) 定位
        std::vector<Node*> update(max_level_, nullptr);

        // 步骤 1：自上而下找每层的前驱
        // cur 沿第 i 层向右走，条件是"下一个节点的 key 仍严格小于 k"
        // （严格小于保证停下位置就是插入点）
        Node* cur = head_;
        for (int i = level_ - 1; i >= 0; --i) {
            while (cur->forward[i] && comp_(cur->forward[i]->key, k)) {
                cur = cur->forward[i];
            }
            update[i] = cur;
        }

        // 步骤 2：检查 level-0 的下一个节点是否就是 k
        // 用两个方向比较都不成立来判断相等（Compare 只定义了 <），
        // 这样兼容任意仿函数，不依赖 operator==
        Node* next = cur->forward[0];
        if (next && !comp_(k, next->key) && !comp_(next->key, k)) {
            next->value = v; // 已存在，原地更新，不改变链表结构
            return true;
        }

        // 步骤 3：随机层高。若超过当前最高层 level_，补齐高层的前驱为 head_
        // （新层之前没有节点，所以前驱只能是哨兵）
        int lvl = random_level();
        if (lvl > level_) {
            for (int i = level_; i < lvl; ++i) update[i] = head_;
            level_ = lvl;
        }

        // 步骤 4：创建节点并逐层缝合 —— 经典链表插入两步：
        //   node->forward[i] = 前驱原来的下一个
        //   前驱的下一个 = node
        Node* node = new Node(k, v, lvl);
        for (int i = 0; i < lvl; ++i) {
            node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = node;
        }
        ++size_;
        return false;
    }

    // ==================================================================
    // 查找，返回 value 的指针（O(log n)）
    //   - 找到：返回内部节点 value 的地址（只读访问，不要长期持有指针，
    //           并发写可能使其失效）
    //   - 没找到：nullptr
    //   下沉策略与插入完全一致：能右移就右移，走不动就下一层
    // ==================================================================
    Value* find(const Key& k) {
        std::lock_guard<std::mutex> lk(mu_);
        Node* node = find_greater_or_equal(k);
        // 用两次比较判断 key 相等，原因同 insert_or_update 步骤 2
        if (node && !comp_(k, node->key) && !comp_(node->key, k)) {
            return &node->value;
        }
        return nullptr;
    }

    // ==================================================================
    // 删除，返回 true 表示 key 存在并已删除
    // 步骤：与插入相同的"记录前驱"过程 ->
    //   1. 确认 level-0 的下一个节点确实是要删的 key；
    //   2. 逐层摘链（只有该节点实际出现的那几层 forward 指向它）；
    //   3. 释放节点；
    //   4. 收缩 level_：如果最高层已经空了，层数下调（节省无效查找层数）
    // ==================================================================
    bool erase(const Key& k) {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<Node*> update(max_level_, nullptr);

        // 与插入相同：自上而下记录每层的前驱
        Node* cur = head_;
        for (int i = level_ - 1; i >= 0; --i) {
            while (cur->forward[i] && comp_(cur->forward[i]->key, k)) {
                cur = cur->forward[i];
            }
            update[i] = cur;
        }

        // 确认目标节点存在
        Node* target = cur->forward[0];
        if (!target || comp_(target->key, k) || comp_(k, target->key)) {
            return false;
        }

        // 逐层摘链：前驱 forward 越过 target 直指 target 的下一个
        // 循环范围是整个 level_，因为 target 可能出现在任意层
        for (int i = 0; i < level_; ++i) {
            if (update[i]->forward[i] == target) {
                update[i]->forward[i] = target->forward[i];
            }
        }
        delete target;
        --size_;

        // 层数收缩：最高层链表空了就降一层，保持跳表"紧凑"
        // （否则查找会白白经过很多空层级）
        while (level_ > 1 && head_->forward[level_ - 1] == nullptr) {
            --level_;
        }
        return true;
    }

    // ==================================================================
    // 范围扫描 [begin, end]，结果按 key 升序放入 out
    // 【这是跳表相对红黑树的核心优势】
    //   1. 用查找定位到第一个 key >= begin 的节点（O(log n)）；
    //   2. 之后沿 level-0 链表顺序走 —— level-0 是完整的有序链表，
    //      天然就是扫描的"数据轨道"，直到 key > end 停止。
    //   红黑树虽然也能中序遍历，但跳表的顺序遍历对缓存更友好，
    //   且与 LevelDB 迭代器的设计同构。
    // ==================================================================
    void scan(const Key& begin, const Key& end,
              std::vector<std::pair<Key, Value>>& out) {
        std::lock_guard<std::mutex> lk(mu_);
        Node* node = find_greater_or_equal(begin);
        // 循环条件：node->key <= end
        // 用 "!(end < node->key)" 表达，兼容任意 Compare
        while (node && !comp_(end, node->key)) {
            out.emplace_back(node->key, node->value);
            node = node->forward[0];
        }
    }

    // 全量按序导出（KVStore 压缩 WAL 时用它把内存快照刷成新 WAL）
    void dump_all(std::vector<std::pair<Key, Value>>& out) {
        std::lock_guard<std::mutex> lk(mu_);
        for (Node* node = head_->forward[0]; node; node = node->forward[0]) {
            out.emplace_back(node->key, node->value);
        }
    }

    // 当前键数量（加锁是为了多线程下读到一致的快照值）
    size_t size() {
        std::lock_guard<std::mutex> lk(mu_);
        return size_;
    }

private:
    // ------------------------------------------------------------------
    // 节点定义
    //   forward[i] : 第 i 层的"下一个节点"指针（单向往右）
    //   每个节点的层数在创建时随机决定，之后不再变化
    //   Key/Value 按值存储（string 会在堆上分配，键值很大时注意内存）
    // ------------------------------------------------------------------
    struct Node {
        Key key;
        Value value;
        std::vector<Node*> forward;
        Node(const Key& k, const Value& v, int level)
            : key(k), value(v), forward(level, nullptr) {}
    };

    // ------------------------------------------------------------------
    // 找到第一个 key >= k 的节点（不判断 key 是否精确相等）
    // 是 find / scan 共用的定位原语。调用方必须已持有 mu_（内部函数不加锁）
    // ------------------------------------------------------------------
    Node* find_greater_or_equal(const Key& k) {
        Node* cur = head_;
        for (int i = level_ - 1; i >= 0; --i) {
            while (cur->forward[i] && comp_(cur->forward[i]->key, k)) {
                cur = cur->forward[i];
            }
        }
        return cur->forward[0];
    }

    // ------------------------------------------------------------------
    // 随机层高：从第 1 层开始，每次以概率 p 晋升一层
    // 期望层数 = 1/(1-p) = 1.33（p=0.25），绝大多数节点只有 1~2 层
    // ------------------------------------------------------------------
    int random_level() {
        int lvl = 1;
        while (lvl < max_level_ && dist_(rng_) < p_) {
            ++lvl;
        }
        return lvl;
    }

    int max_level_;   // 最大层数上限
    double p_;        // 层间晋升概率
    int level_;       // 当前实际最高层（动态收缩/扩张，<= max_level_）
    size_t size_;     // 键值对数量
    Node* head_;      // 哨兵头节点，不存数据
    Compare comp_;    // key 比较器
    std::mutex mu_;   // 粗粒度锁：保护所有操作
    std::mt19937 rng_;                              // 随机数引擎
    std::uniform_real_distribution<double> dist_;   // [0,1) 均匀分布
};
