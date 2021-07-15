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

#ifndef SRC_STORAGE_SEGMENT_H_
#define SRC_STORAGE_SEGMENT_H_

#include <atomic>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <vector>

#include "boost/pool/pool.hpp"

#include "base/skiplist.h"
#include "base/slice.h"
#include "base/time_series_pool.h"
#include "proto/tablet.pb.h"
#include "storage/iterator.h"
#include "storage/schema.h"
#include "storage/ticket.h"

namespace openmldb {
namespace storage {

typedef google::protobuf::RepeatedPtrField<::openmldb::api::TSDimension> TSDimensions;

using ::openmldb::base::Slice;

class Segment;
class Ticket;

struct DataBlock {
    // dimension count down
    uint8_t dim_cnt_down;
    uint32_t size;
    char* data;

    DataBlock(uint8_t dim_cnt, const char* input, uint32_t len) : dim_cnt_down(dim_cnt), size(len), data(NULL) {
        data = new char[len];
        memcpy(data, input, len);
    }

    ~DataBlock() {
        delete[] data;
        data = NULL;
    }
};

// the desc time comparator
struct TimeComparator {
    int operator()(const uint64_t& a, const uint64_t& b) const {
        if (a > b) {
            return -1;
        } else if (a == b) {
            return 0;
        }
        return 1;
    }
};

static const TimeComparator tcmp;
typedef ::openmldb::base::Skiplist<uint64_t, DataBlock*, TimeComparator> TimeEntries;

class MemTableIterator : public TableIterator {
 public:
    explicit MemTableIterator(TimeEntries::Iterator* it);
    virtual ~MemTableIterator();
    void Seek(const uint64_t time) override;
    bool Valid() override;
    void Next() override;
    openmldb::base::Slice GetValue() const override;
    uint64_t GetKey() const override;
    void SeekToFirst() override;
    void SeekToLast() override;

 private:
    TimeEntries::Iterator* it_;
};

class KeyEntry {
 public:
    KeyEntry() : entries(12, 4, tcmp), refs_(0), count_(0) {}
    explicit KeyEntry(uint8_t height) : entries(height, 4, tcmp), refs_(0), count_(0) {}
    ~KeyEntry() {}

    // just return the count of datablock
    uint64_t Release(::openmldb::base::TimeSeriesPool& pool) {
        uint64_t cnt = 0;
        TimeEntries::Iterator* it = entries.NewIterator();
        it->SeekToFirst();
        while (it->Valid()) {
            cnt += 1;
            DataBlock* block = it->GetValue();
            // Avoid double free
            if (block->dim_cnt_down > 1) {
                block->dim_cnt_down--;
            } else {
                delete block;
            }
            pool.Free(it->GetKey());
            it->Next();
        }
        // not clearing for using pool for time entry
        // entries.Clear();
        delete it;
        return cnt;
    }

    void Ref() { refs_.fetch_add(1, std::memory_order_relaxed); }

    void UnRef() { refs_.fetch_sub(1, std::memory_order_relaxed); }

    uint64_t GetCount() { return count_.load(std::memory_order_relaxed); }

 public:
    TimeEntries entries;
    std::atomic<uint64_t> refs_;
    std::atomic<uint64_t> count_;
    friend Segment;
};

struct SliceComparator {
    int operator()(const ::openmldb::base::Slice& a, const ::openmldb::base::Slice& b) const { return a.compare(b); }
};

typedef ::openmldb::base::Skiplist<::openmldb::base::Slice, void*, SliceComparator> KeyEntries;
typedef ::openmldb::base::Skiplist<uint64_t, ::openmldb::base::Node<Slice, void*>*, TimeComparator> KeyEntryNodeList;

class Segment {
 public:
    Segment();
    explicit Segment(uint8_t height);
    Segment(uint8_t height, const std::vector<uint32_t>& ts_idx_vec);
    ~Segment();

    // Put time data
    void Put(const Slice& key, uint64_t time, const char* data, uint32_t size);

    void Put(const Slice& key, uint64_t time, DataBlock* row);

    void Put(const Slice& key, const TSDimensions& ts_dimension, DataBlock* row);

    // Get time data
    bool Get(const Slice& key, uint64_t time, DataBlock** block);

    bool Get(const Slice& key, uint32_t idx, uint64_t time, DataBlock** block);

    bool Delete(const Slice& key);

    uint64_t Release();

    void ExecuteGc(const TTLSt& ttl_st, uint64_t& gc_idx_cnt,                          // NOLINT
                   uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size);            // NOLINT
    void ExecuteGc(const std::map<uint32_t, TTLSt>& ttl_st_map, uint64_t& gc_idx_cnt,  // NOLINT
                   uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size);            // NOLINT

    void Gc4TTL(const uint64_t time, uint64_t& gc_idx_cnt,  // NOLINT
                uint64_t& gc_record_cnt,                    // NOLINT
                uint64_t& gc_record_byte_size);             // NOLINT
    void Gc4Head(uint64_t keep_cnt, uint64_t& gc_idx_cnt,   // NOLINT
                 uint64_t& gc_record_cnt,                   // NOLINT
                 uint64_t& gc_record_byte_size);            // NOLINT
    void Gc4TTLAndHead(const uint64_t time, const uint64_t keep_cnt,
                       uint64_t& gc_idx_cnt,            // NOLINT
                       uint64_t& gc_record_cnt,         // NOLINT
                       uint64_t& gc_record_byte_size);  // NOLINT
    void Gc4TTLOrHead(const uint64_t time, const uint64_t keep_cnt,
                      uint64_t& gc_idx_cnt,                                            // NOLINT
                      uint64_t& gc_record_cnt,                                         // NOLINT
                      uint64_t& gc_record_byte_size);                                  // NOLINT
    void GcAllType(const std::map<uint32_t, TTLSt>& ttl_st_map, uint64_t& gc_idx_cnt,  // NOLINT
                   uint64_t& gc_record_cnt,                                            // NOLINT
                   uint64_t& gc_record_byte_size);                                     // NOLINT
    MemTableIterator* NewIterator(const Slice& key, Ticket& ticket);                   // NOLINT
    MemTableIterator* NewIterator(const Slice& key, uint32_t idx,
                                  Ticket& ticket);  // NOLINT

    inline uint64_t GetIdxCnt() {
        return ts_cnt_ > 1 ? idx_cnt_vec_[0]->load(std::memory_order_relaxed)
                           : idx_cnt_.load(std::memory_order_relaxed);
    }

    int GetIdxCnt(uint32_t ts_idx, uint64_t& ts_cnt) {  // NOLINT
        uint32_t real_idx = 0;
        if (GetTsIdx(ts_idx, real_idx) < 0) {
            return -1;
        }
        ts_cnt = idx_cnt_vec_[real_idx]->load(std::memory_order_relaxed);
        return 0;
    }

    inline uint64_t GetTsCnt() { return ts_cnt_; }

    int GetTsIdx(uint32_t raw_idx, uint32_t& real_idx) {  // NOLINT
        auto iter = ts_idx_map_.find(raw_idx);
        if (iter == ts_idx_map_.end()) {
            return -1;
        } else {
            real_idx = iter->second;
        }
        return 0;
    }

    inline uint64_t GetIdxByteSize() { return idx_byte_size_.load(std::memory_order_relaxed); }

    inline uint64_t GetPkCnt() { return pk_cnt_.load(std::memory_order_relaxed); }

    void GcFreeList(uint64_t& entry_gc_idx_cnt,      // NOLINT
                    uint64_t& gc_record_cnt,         // NOLINT
                    uint64_t& gc_record_byte_size);  // NOLINT

    KeyEntries* GetKeyEntries() { return entries_; }

    int GetCount(const Slice& key, uint64_t& count);                // NOLINT
    int GetCount(const Slice& key, uint32_t idx, uint64_t& count);  // NOLINT

    void IncrGcVersion() { gc_version_.fetch_add(1, std::memory_order_relaxed); }

    void ReleaseAndCount(uint64_t& gc_idx_cnt,            // NOLINT
                         uint64_t& gc_record_cnt,         // NOLINT
                         uint64_t& gc_record_byte_size);  // NOLINT

 private:
    void FreeList(::openmldb::base::Node<uint64_t, DataBlock*>* node, uint64_t& gc_idx_cnt,  // NOLINT
                  uint64_t& gc_record_cnt,         // NOLINT
                  uint64_t& gc_record_byte_size);  // NOLINT
    void SplitList(KeyEntry* entry, uint64_t ts, ::openmldb::base::Node<uint64_t, DataBlock*>** node);

    void GcEntryFreeList(uint64_t version, uint64_t& gc_idx_cnt,  // NOLINT
                         uint64_t& gc_record_cnt,                 // NOLINT
                         uint64_t& gc_record_byte_size);          // NOLINT
    void FreeEntry(::openmldb::base::Node<Slice, void*>* entry_node, uint64_t& gc_idx_cnt,  // NOLINT
                   uint64_t& gc_record_cnt,         // NOLINT
                   uint64_t& gc_record_byte_size);  // NOLINT

 private:
    KeyEntries* entries_;
    // only Put need mutex
    std::mutex mu_;
    std::mutex gc_mu_;
    std::atomic<uint64_t> idx_cnt_;
    std::atomic<uint64_t> idx_byte_size_;
    std::atomic<uint64_t> pk_cnt_;
    uint8_t key_entry_max_height_;
    KeyEntryNodeList* entry_free_list_;  // NOTE: entry free list DO NOT use pool
    uint32_t ts_cnt_;
    std::atomic<uint64_t> gc_version_;
    std::map<uint32_t, uint32_t> ts_idx_map_;
    std::vector<std::shared_ptr<std::atomic<uint64_t>>> idx_cnt_vec_;
    ::openmldb::base::TimeSeriesPool pool_;
    boost::pool<> boost_pool_;
    uint64_t ttl_offset_;
};

}  // namespace storage
}  // namespace openmldb
#endif  // SRC_STORAGE_SEGMENT_H_
