/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_BASE_SKIPLIST_H_
#define SRC_BASE_SKIPLIST_H_

#include <assert.h>
#include <stdint.h>

#include <atomic>

#include "base/random.h"
#include "base/time_series_pool.h"

namespace openmldb {
namespace base {

struct DefaultComparator {
    int operator()(const uint64_t a, const uint64_t b) const {
        if (a > b) {
            return -1;
        } else if (a == b) {
            return 0;
        }
        return 1;
    }
};

// Skiplist node , a thread safe structure
template <class K, class V>
class Node {
 public:
    // Set data reference and Node height
    Node(const K& key, V& value, uint8_t height, std::atomic<Node<K, V>*>* preAlloc)  // NOLINT
        : height_(height), key_(key), value_(value) {
        nexts_ = preAlloc;
    }

    Node(uint8_t height, std::atomic<Node<K, V>*>* preAlloc) : height_(height), key_(), value_() {  // NOLINT
        nexts_ = preAlloc;
    }

    // Set data reference and Node height
    Node(const K& key, V& value, uint8_t height)  // NOLINT
        : height_(height), key_(key), value_(value) {
        nexts_ = new std::atomic<Node<K, V>*>[height];
    }

    Node(uint8_t height) : height_(height), key_(), value_() {  // NOLINT
        nexts_ = new std::atomic<Node<K, V>*>[height];
    }

    // Set the next node with memory barrier
    void SetNext(uint8_t level, Node<K, V>* node) {
        assert(level < height_ && level >= 0);
        nexts_[level].store(node, std::memory_order_release);
    }

    // Set the next node without memory barrier
    void SetNextNoBarrier(uint8_t level, Node<K, V>* node) {
        assert(level < height_ && level >= 0);
        nexts_[level].store(node, std::memory_order_relaxed);
    }

    uint8_t Height() { return height_; }

    Node<K, V>* GetNext(uint8_t level) {
        assert(level < height_ && level >= 0);
        return nexts_[level].load(std::memory_order_acquire);
    }

    Node<K, V>* GetNextNoBarrier(uint8_t level) {
        assert(level < height_ && level >= 0);
        return nexts_[level].load(std::memory_order_relaxed);
    }

    V& GetValue() { return value_; }

    const K& GetKey() const { return key_; }

    ~Node() { delete[] nexts_; }

 private:
    uint8_t const height_;
    K const key_;
    V value_;
    std::atomic<Node<K, V>*>* nexts_;
};

template <class K, class V, class Comparator>
class Skiplist {
 public:
    Skiplist(uint8_t max_height, uint8_t branch, const Comparator& compare)
        : MaxHeight(max_height), Branch(branch), max_height_(0), compare_(compare), rand_(0xdeadbeef), tail_(NULL) {
        head_ = new Node<K, V>(MaxHeight);
        for (uint8_t i = 0; i < head_->Height(); i++) {
            head_->SetNext(i, NULL);
        }
        max_height_.store(1, std::memory_order_relaxed);
    }
    ~Skiplist() { delete head_; }

    // Insert need external synchronized
    uint8_t Insert(const K& key, V& value) {  // NOLINT
        uint8_t height = RandomHeight();
        Node<K, V>* pre[MaxHeight];
        FindLessOrEqual(key, pre);
        if (height > GetMaxHeight()) {
            for (uint8_t i = GetMaxHeight(); i < height; i++) {
                pre[i] = head_;
            }
            max_height_.store(height, std::memory_order_relaxed);
        }
        Node<K, V>* node = NewNode(key, value, height);
        if (pre[0]->GetNext(0) == NULL) {
            tail_.store(node, std::memory_order_release);
        }
        for (uint8_t i = 0; i < height; i++) {
            node->SetNextNoBarrier(i, pre[i]->GetNextNoBarrier(i));
            pre[i]->SetNext(i, node);
        }
        return height;
    }

    // Insert need external synchronized
    // use if skiplist is using a pool
    uint8_t Insert(const K& key, V& value, uint64_t time, TimeSeriesPool& pool) {  // NOLINT
        uint8_t height = RandomHeight();
        Node<K, V>* pre[MaxHeight];
        FindLessOrEqual(key, pre);
        if (height > GetMaxHeight()) {
            for (uint8_t i = GetMaxHeight(); i < height; i++) {
                pre[i] = head_;
            }
            max_height_.store(height, std::memory_order_relaxed);
        }
        Node<K, V>* node = NewNode(key, value, height, time, pool);
        if (pre[0]->GetNext(0) == NULL) {
            tail_.store(node, std::memory_order_release);
        }
        for (uint8_t i = 0; i < height; i++) {
            node->SetNextNoBarrier(i, pre[i]->GetNextNoBarrier(i));
            pre[i]->SetNext(i, node);
        }
        return height;
    }

    bool IsEmpty() {
        if (head_->GetNextNoBarrier(0) == NULL) {
            return true;
        }
        return false;
    }

    // Remove need external synchronized
    Node<K, V>* Remove(const K& key) {
        Node<K, V>* pre[MaxHeight];
        for (uint8_t i = 0; i < MaxHeight; i++) {
            pre[i] = head_;
        }
        Node<K, V>* target = FindLessOrEqual(key, pre);
        if (target == NULL) {
            return NULL;
        }
        Node<K, V>* result = target->GetNextNoBarrier(0);
        if (result == NULL || compare_(result->GetKey(), key) != 0) {
            return NULL;
        }
        for (uint8_t i = 0; i < result->Height(); i++) {
            pre[i]->SetNextNoBarrier(i, result->GetNextNoBarrier(i));
            result->SetNextNoBarrier(i, NULL);
        }
        if (result == tail_) {
            pre[0] == head_ ? tail_.store(NULL, std::memory_order_relaxed)
                            : tail_.store(pre[0], std::memory_order_relaxed);
        }
        return result;
    }

    // Split list two parts, the return part is just a linkedlist
    Node<K, V>* Split(const K& key) {
        Node<K, V>* pre[MaxHeight];
        for (uint8_t i = 0; i < MaxHeight; i++) {
            pre[i] = NULL;
        }
        Node<K, V>* target = FindLessOrEqual(key, pre);
        if (target == NULL) {
            return NULL;
        }
        tail_.store(target, std::memory_order_release);
        Node<K, V>* result = target->GetNextNoBarrier(0);
        for (uint8_t i = 0; i < MaxHeight; i++) {
            if (pre[i] == NULL) {
                continue;
            }
            pre[i]->SetNext(i, NULL);
        }
        return result;
    }

    Node<K, V>* SplitByPos(uint64_t pos) {
        Node<K, V>* pos_node = head_->GetNext(0);
        for (uint64_t idx = 0; idx < pos; idx++) {
            if (pos_node == NULL) {
                return NULL;
            }
            pos_node = pos_node->GetNext(0);
        }
        if (pos_node == NULL) {
            return NULL;
        }
        return SplitOnPosNode(pos, pos_node);
    }

    Node<K, V>* SplitByKeyOrPos(const K& key, uint64_t pos) {
        Node<K, V>* pos_node = head_->GetNext(0);
        for (uint64_t idx = 0; idx < pos; idx++) {
            if (pos_node == NULL) {  // doesnt find key or pos, just return
                return NULL;
            }
            if (compare_(pos_node->GetKey(), key) >= 0) {  // find key before pos, use key
                return Split(pos_node->GetKey());
            }
            pos_node = pos_node->GetNext(0);
        }
        if (pos_node == NULL) {
            return NULL;
        }
        return SplitOnPosNode(pos, pos_node);
    }

    Node<K, V>* SplitByKeyAndPos(const K& key, uint64_t pos) {
        Node<K, V>* pos_node = head_->GetNext(0);
        bool find_key = false;
        for (uint64_t idx = 0; idx < pos; idx++) {
            if (pos_node == NULL) {  // doesnt find pos, just return
                return NULL;
            }
            if (compare_(pos_node->GetKey(), key) >= 0) {  // find key before pos, mark it
                find_key = true;
            }
            pos_node = pos_node->GetNext(0);
        }
        if (pos_node == NULL) {
            return NULL;
        }
        if (find_key) {  // find key before pos, split by pos
            return SplitOnPosNode(pos, pos_node);
        } else {  // find pos without key, split by key
            return Split(key);
        }
    }

    const V& Get(const K& key) {
        Node<K, V>* node = FindEqual(key);
        return node->GetValue();
    }

    int Get(const K& key, V& v) {  // NOLINT
        Node<K, V>* node = FindEqual(key);
        if (node != NULL && compare_(node->GetKey(), key) == 0) {
            v = node->GetValue();
            return 0;
        }
        return -1;
    }

    Node<K, V>* GetLast() { return tail_.load(std::memory_order_acquire); }

    uint32_t GetSize() {
        uint32_t cnt = 0;
        Node<K, V>* node = head_->GetNext(0);
        while (node != NULL) {
            cnt++;
            Node<K, V>* tmp = node->GetNext(0);
            // find the end
            if (tmp == NULL) {
                break;
            }
            node = tmp;
        }
        return cnt;
    }

    // Need external synchronized
    // called if skiplist is using tcalloc
    uint64_t Clear() {
        uint64_t cnt = 0;
        Node<K, V>* node = head_->GetNext(0);
        // Unlink all next node
        for (uint8_t i = 0; i < head_->Height(); i++) {
            head_->SetNextNoBarrier(i, NULL);
        }
        tail_.store(NULL, std::memory_order_relaxed);

        while (node != NULL) {
            cnt++;
            Node<K, V>* tmp = node;
            node = node->GetNext(0);
            // Unlink all next node
            for (uint8_t i = 0; i < tmp->Height(); i++) {
                tmp->SetNextNoBarrier(i, NULL);
            }
            delete tmp;
        }
        return cnt;
    }

    // Need external synchronized
    // use if skiplist is using a pool
    bool AddToFirst(const K& key, V& value, uint64_t time, TimeSeriesPool& pool) {  // NOLINT
        {
            Node<K, V>* node = head_->GetNext(0);
            if (node != NULL && compare_(key, node->GetKey()) > 0) {
                return false;
            }
        }
        uint8_t height = RandomHeight();
        Node<K, V>* pre[MaxHeight];
        for (uint8_t i = 0; i < height; i++) {
            pre[i] = head_;
        }
        if (height > GetMaxHeight()) {
            max_height_.store(height, std::memory_order_relaxed);
        }
        Node<K, V>* node = NewNode(key, value, height, time, pool);
        if (pre[0]->GetNext(0) == NULL) {
            tail_.store(node, std::memory_order_release);
        }
        for (uint8_t i = 0; i < height; i++) {
            node->SetNextNoBarrier(i, pre[i]->GetNextNoBarrier(i));
            pre[i]->SetNext(i, node);
        }
        return true;
    }

    // Need external synchronized
    bool AddToFirst(const K& key, V& value) {  // NOLINT
        {
            Node<K, V>* node = head_->GetNext(0);
            if (node != NULL && compare_(key, node->GetKey()) > 0) {
                return false;
            }
        }
        uint8_t height = RandomHeight();
        Node<K, V>* pre[MaxHeight];
        for (uint8_t i = 0; i < height; i++) {
            pre[i] = head_;
        }
        if (height > GetMaxHeight()) {
            max_height_.store(height, std::memory_order_relaxed);
        }
        Node<K, V>* node = NewNode(key, value, height);
        if (pre[0]->GetNext(0) == NULL) {
            tail_.store(node, std::memory_order_release);
        }
        for (uint8_t i = 0; i < height; i++) {
            node->SetNextNoBarrier(i, pre[i]->GetNextNoBarrier(i));
            pre[i]->SetNext(i, node);
        }
        return true;
    }

    class Iterator {
     public:
        Iterator(Skiplist<K, V, Comparator>* list) : node_(NULL), list_(list) {}  // NOLINT
        ~Iterator() {}

        bool Valid() const { return node_ != NULL; }

        void Next() {
            assert(Valid());
            node_ = node_->GetNext(0);
        }

        const K& GetKey() const {
            assert(Valid());
            return node_->GetKey();
        }

        V& GetValue() {
            assert(Valid());
            return node_->GetValue();
        }

        void Seek(const K& k) {
            node_ = list_->FindLessThan(k);
            Next();
        }

        void SeekToFirst() {
            node_ = list_->head_;
            Next();
        }

        void SeekToLast() { node_ = list_->GetLast(); }

        uint32_t GetSize() { return list_->GetSize(); }

     private:
        Node<K, V>* node_;
        Skiplist<K, V, Comparator>* const list_;
    };

    // delete the iterator after it's used
    Iterator* NewIterator() { return new Iterator(this); }

 private:
    Node<K, V>* NewNode(const K& key, V& value, uint8_t height, uint64_t time, TimeSeriesPool& pool) {  // NOLINT
        auto arrmemvptr = pool.Alloc(sizeof(std::atomic<Node<K, V>*>) * height, time);
        auto arrmem = reinterpret_cast<std::atomic<Node<K, V>*>*>(arrmemvptr);
        auto nodemem = pool.Alloc(sizeof(Node<K, V>), time);
        Node<K, V>* node = new (nodemem) Node<K, V>(key, value, height, arrmem);
        return node;
    }

    Node<K, V>* NewNode(const K& key, V& value, uint8_t height) {  // NOLINT
        Node<K, V>* node = new Node<K, V>(key, value, height);
        return node;
    }

    uint8_t RandomHeight() {
        uint8_t height = 1;
        while (height < MaxHeight && (rand_.Next() % Branch) == 0) {
            height++;
        }
        return height;
    }

    Node<K, V>* FindLessOrEqual(const K& key, Node<K, V>** nodes) {
        assert(nodes != NULL);
        Node<K, V>* node = head_;
        uint8_t level = GetMaxHeight() - 1;
        while (true) {
            Node<K, V>* next = node->GetNext(level);
            if (IsAfterNode(key, next)) {
                node = next;
            } else {
                nodes[level] = node;
                if (level <= 0) {
                    return node;
                }
                level--;
            }
        }
    }

    Node<K, V>* FindEqual(const K& key) {
        Node<K, V>* node = head_;
        uint8_t level = GetMaxHeight() - 1;
        while (true) {
            Node<K, V>* next = node->GetNext(level);
            if (next == NULL || compare_(next->GetKey(), key) > 0) {
                if (level <= 0) {
                    return node;
                }
                level--;
            } else {
                node = next;
            }
        }
    }

    Node<K, V>* FindLessThan(const K& key) {
        Node<K, V>* node = head_;
        uint8_t level = GetMaxHeight() - 1;
        while (true) {
            assert(node == head_ || compare_(node->GetKey(), key) < 0);
            Node<K, V>* next = node->GetNext(level);
            if (next == NULL || compare_(next->GetKey(), key) >= 0) {
                if (level <= 0) {
                    return node;
                }
                level--;
            } else {
                node = next;
            }
        }
    }

    bool IsAfterNode(const K& key, const Node<K, V>* node) const {
        return (node != NULL) && (compare_(key, node->GetKey()) > 0);
    }

    uint8_t GetMaxHeight() const { return max_height_.load(std::memory_order_relaxed); }

    Node<K, V>* SplitOnPosNode(uint64_t pos, Node<K, V>* pos_node) {
        Node<K, V>* node = head_;
        Node<K, V>* pre = head_;
        pos++;
        uint64_t cnt = 0;
        while (node != NULL) {
            if (cnt == pos) {
                tail_.store(pre, std::memory_order_release);
                for (uint8_t i = 0; i < pre->Height(); i++) {
                    pre->SetNext(i, NULL);
                }
                return node;
            }
            for (uint8_t i = 1; i < node->Height(); i++) {
                Node<K, V>* next = node->GetNext(i);
                if (next != NULL && compare_(pos_node->GetKey(), next->GetKey()) <= 0) {
                    node->SetNext(i, NULL);
                }
            }
            pre = node;
            node = node->GetNext(0);
            cnt++;
        }
        return NULL;
    }

 private:
    uint8_t const MaxHeight;
    uint8_t const Branch;
    std::atomic<uint8_t> max_height_;
    Comparator const compare_;
    Random rand_;
    Node<K, V>* head_;
    std::atomic<Node<K, V>*> tail_;
    friend Iterator;
};

}  // namespace base
}  // namespace openmldb

#endif  // SRC_BASE_SKIPLIST_H_
