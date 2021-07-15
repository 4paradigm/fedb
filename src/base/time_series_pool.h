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

#ifndef SRC_BASE_TIME_Series_POOL_H_
#define SRC_BASE_TIME_Series_POOL_H_

#include <malloc.h>

#include <map>
#include <memory>
#include <utility>

namespace openmldb {
namespace base {

class TimeBucket {
 public:
    explicit TimeBucket(uint32_t block_size)
        : block_size_(block_size), current_offset_(block_size + 1), object_num_(0) {
        head_ = reinterpret_cast<Block*>(malloc(sizeof(Block*)));  // empty block at end
        head_->next = NULL;
    }
    ~TimeBucket() {
        auto p = head_;
        while (p) {
            auto q = p->next;
            free(p);
            p = q;
        }
    }
    void* Alloc(uint32_t size) {
        // return new char[size];
        object_num_++;
        if (current_offset_ + size <= block_size_ - sizeof(Block)) {
            void* addr = head_->data + current_offset_;
            current_offset_ += size;
            return addr;
        } else {
            auto block = reinterpret_cast<Block*>(malloc(block_size_));
            current_offset_ = size;
            block->next = head_->next;
            head_ = block;
            return head_->data;
        }
    }
    bool Free() {  // ret if fully freed
        return !--object_num_;
    }

 private:
    uint32_t block_size_;
    uint32_t current_offset_;
    uint32_t object_num_;
    struct Block {
        Block* next;
        char data[];
    } * head_;
};

class TimeSeriesPool {
 public:
    explicit TimeSeriesPool(uint32_t block_size) : block_size_(block_size) {}
    void* Alloc(uint32_t size, uint64_t time) {
        auto key = ComputeTimeSlot(time);
        auto pair = pool_.find(key);
        if (pair == pool_.end()) {
            auto bucket = new TimeBucket(block_size_);
            pool_.insert(std::pair<uint32_t, std::unique_ptr<TimeBucket>>(key, std::unique_ptr<TimeBucket>(bucket)));
            return bucket->Alloc(size);
        }

        return pair->second->Alloc(size);
    }
    void Free(uint64_t time) {
        auto pair = pool_.find(ComputeTimeSlot(time));
        if (pair != pool_.end() && pair->second->Free()) {
            pool_.erase(pair);
        }
    }
    bool Empty() { return pool_.empty(); }

 private:
    // key is the time / (60 * 60 * 1000)
    uint32_t block_size_;
    std::map<uint32_t, std::unique_ptr<TimeBucket>> pool_;
    inline static uint32_t ComputeTimeSlot(uint64_t time) { return time / (60 * 60 * 1000); }
};

}  // namespace base
}  // namespace openmldb

#endif  // SRC_BASE_TIME_Series_POOL_H_
