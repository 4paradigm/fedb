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

#include "storage/segment.h"

#include <gflags/gflags.h>

#include "base/glog_wapper.h"
#include "base/strings.h"
#include "common/timer.h"
#include "storage/record.h"

DECLARE_int32(gc_safe_offset);
DECLARE_uint32(time_series_pool_block_size);
DECLARE_uint32(skiplist_max_height);
DECLARE_uint32(gc_deleted_pk_version_delta);

namespace openmldb {
namespace storage {

static const SliceComparator scmp;
Segment::Segment()
    : entries_(NULL),
      mu_(),
      idx_cnt_(0),
      idx_byte_size_(0),
      pk_cnt_(0),
      ts_cnt_(1),
      gc_version_(0),
      pool_(FLAGS_time_series_pool_block_size),
      boost_pool_(sizeof(KeyEntry)),
      ttl_offset_(FLAGS_gc_safe_offset * 60 * 1000) {
    entries_ = new KeyEntries((uint8_t)FLAGS_skiplist_max_height, 4, scmp);
    key_entry_max_height_ = (uint8_t)FLAGS_skiplist_max_height;
    entry_free_list_ = new KeyEntryNodeList(4, 4, tcmp);
}

Segment::Segment(uint8_t height)
    : entries_(NULL),
      mu_(),
      idx_cnt_(0),
      idx_byte_size_(0),
      pk_cnt_(0),
      key_entry_max_height_(height),
      ts_cnt_(1),
      gc_version_(0),
      pool_(FLAGS_time_series_pool_block_size),
      boost_pool_(sizeof(KeyEntry)),
      ttl_offset_(FLAGS_gc_safe_offset * 60 * 1000) {
    entries_ = new KeyEntries((uint8_t)FLAGS_skiplist_max_height, 4, scmp);
    entry_free_list_ = new KeyEntryNodeList(4, 4, tcmp);
}

Segment::Segment(uint8_t height, const std::vector<uint32_t>& ts_idx_vec)
    : entries_(NULL),
      mu_(),
      idx_cnt_(0),
      idx_byte_size_(0),
      pk_cnt_(0),
      key_entry_max_height_(height),
      ts_cnt_(ts_idx_vec.size()),
      gc_version_(0),
      pool_(FLAGS_time_series_pool_block_size),
      boost_pool_(sizeof(KeyEntry)),
      ttl_offset_(FLAGS_gc_safe_offset * 60 * 1000) {
    entries_ = new KeyEntries((uint8_t)FLAGS_skiplist_max_height, 4, scmp);
    entry_free_list_ = new KeyEntryNodeList(4, 4, tcmp);
    for (uint32_t i = 0; i < ts_idx_vec.size(); i++) {
        ts_idx_map_[ts_idx_vec[i]] = i;
        idx_cnt_vec_.push_back(std::make_shared<std::atomic<uint64_t>>(0));
    }
}

Segment::~Segment() {
    delete entries_;
    delete entry_free_list_;
}

uint64_t Segment::Release() {
    uint64_t cnt = 0;
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        delete[] it->GetKey().data();
        if (it->GetValue() != NULL) {
            if (ts_cnt_ > 1) {
                KeyEntry** entry_arr = (KeyEntry**)it->GetValue();  // NOLINT
                for (uint32_t i = 0; i < ts_cnt_; i++) {
                    cnt += entry_arr[i]->Release(pool_);
                    boost_pool_.free(entry_arr[i]);
                }
                delete[] entry_arr;
            } else {
                KeyEntry* entry = (KeyEntry*)it->GetValue();  // NOLINT
                cnt += entry->Release(pool_);
                boost_pool_.free(entry);
            }
        }
        it->Next();
    }
    entries_->Clear();
    delete it;

    KeyEntryNodeList::Iterator* f_it = entry_free_list_->NewIterator();
    f_it->SeekToFirst();
    while (f_it->Valid()) {
        ::openmldb::base::Node<Slice, void*>* node = f_it->GetValue();
        delete[] node->GetKey().data();
        if (ts_cnt_ > 1) {
            KeyEntry** entry_arr = (KeyEntry**)node->GetValue();  // NOLINT
            for (uint32_t i = 0; i < ts_cnt_; i++) {
                entry_arr[i]->Release(pool_);
                boost_pool_.free(entry_arr[i]);
            }
            delete[] entry_arr;
        } else {
            KeyEntry* entry = (KeyEntry*)node->GetValue();  // NOLINT
            entry->Release(pool_);
            boost_pool_.free(entry);
        }
        delete node;
        f_it->Next();
    }
    delete f_it;
    entry_free_list_->Clear();
    idx_cnt_vec_.clear();
    return cnt;
}

void Segment::ReleaseAndCount(uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size) {
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        Slice key = it->GetKey();
        ::openmldb::base::Node<Slice, void*>* entry_node = NULL;
        {
            std::lock_guard<std::mutex> lock(mu_);
            entry_node = entries_->Remove(key);
        }
        if (entry_node != NULL) {
            FreeEntry(entry_node, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        }
        it->Next();
        pk_cnt_.fetch_sub(1, std::memory_order_relaxed);
    }
    delete it;
    uint64_t cur_version = gc_version_.load(std::memory_order_relaxed);
    GcEntryFreeList(cur_version, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    Release();
}

void Segment::Put(const Slice& key, uint64_t time, const char* data, uint32_t size) {
    if (ts_cnt_ > 1) {
        return;
    }
    DataBlock* db = new DataBlock(1, data, size);
    Put(key, time, db);
}

void Segment::Put(const Slice& key, uint64_t time, DataBlock* row) {
    if (ts_cnt_ > 1) {
        return;
    }
    void* entry = NULL;
    uint32_t byte_size = 0;
    std::lock_guard<std::mutex> lock(mu_);
    int ret = entries_->Get(key, entry);
    if (ret < 0 || entry == NULL) {
        char* pk = new char[key.size()];
        memcpy(pk, key.data(), key.size());
        // need to delete memory when free node
        Slice skey(pk, key.size());
        entry = new (boost_pool_.malloc())KeyEntry(key_entry_max_height_);
        // Key entry do not use pool
        uint8_t height = entries_->Insert(skey, entry);
        byte_size += GetRecordPkIdxSize(height, key.size(), key_entry_max_height_);
        pk_cnt_.fetch_add(1, std::memory_order_relaxed);
    }
    idx_cnt_.fetch_add(1, std::memory_order_relaxed);
    uint8_t height = ((KeyEntry*)entry)->entries.Insert(time, row, time, pool_);  // NOLINT
    ((KeyEntry*)entry)                                                            // NOLINT
        ->count_.fetch_add(1, std::memory_order_relaxed);
    byte_size += GetRecordTsIdxSize(height);
    idx_byte_size_.fetch_add(byte_size, std::memory_order_relaxed);
}

void Segment::Put(const Slice& key, const TSDimensions& ts_dimension, DataBlock* row) {
    uint32_t ts_size = ts_dimension.size();
    if (ts_size == 0) {
        return;
    }
    if (ts_cnt_ == 1) {
        if (ts_size == 1) {
            Put(key, ts_dimension.begin()->ts(), row);
        } else if (!ts_idx_map_.empty()) {
            for (const auto& cur_ts : ts_dimension) {
                auto pos = ts_idx_map_.find(cur_ts.idx());
                if (pos != ts_idx_map_.end()) {
                    Put(key, cur_ts.ts(), row);
                    break;
                }
            }
        }
        return;
    }
    void* entry_arr = NULL;
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& cur_ts : ts_dimension) {
        uint32_t byte_size = 0;
        auto pos = ts_idx_map_.find(cur_ts.idx());
        if (pos == ts_idx_map_.end()) {
            continue;
        }
        if (entry_arr == NULL) {
            int ret = entries_->Get(key, entry_arr);
            if (ret < 0 || entry_arr == NULL) {
                char* pk = new char[key.size()];
                memcpy(pk, key.data(), key.size());
                Slice skey(pk, key.size());
                KeyEntry** entry_arr_tmp = new KeyEntry*[ts_cnt_];
                for (uint32_t i = 0; i < ts_cnt_; i++) {
                    entry_arr_tmp[i] = new (boost_pool_.malloc())KeyEntry(key_entry_max_height_);
                }
                entry_arr = (void*)entry_arr_tmp;  // NOLINT
                // key entry do not use pool
                uint8_t height = entries_->Insert(skey, entry_arr);
                byte_size += GetRecordPkMultiIdxSize(height, key.size(), key_entry_max_height_, ts_cnt_);
                pk_cnt_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        uint8_t height = ((KeyEntry**)entry_arr)[pos->second]->entries.Insert(  // NOLINT
            cur_ts.ts(), row, cur_ts.ts(), pool_);
        ((KeyEntry**)entry_arr)[pos->second]->count_.fetch_add(  // NOLINT
            1, std::memory_order_relaxed);
        byte_size += GetRecordTsIdxSize(height);
        idx_byte_size_.fetch_add(byte_size, std::memory_order_relaxed);
        idx_cnt_vec_[pos->second]->fetch_add(1, std::memory_order_relaxed);
    }
}

bool Segment::Get(const Slice& key, const uint64_t time, DataBlock** block) {
    if (block == NULL || ts_cnt_ > 1) {
        return false;
    }
    void* entry = NULL;
    if (entries_->Get(key, entry) < 0 || entry == NULL) {
        return false;
    }
    *block = ((KeyEntry*)entry)->entries.Get(time);  // NOLINT
    return true;
}

bool Segment::Get(const Slice& key, uint32_t idx, const uint64_t time, DataBlock** block) {
    if (block == NULL) {
        return false;
    }
    auto pos = ts_idx_map_.find(idx);
    if (pos == ts_idx_map_.end()) {
        return false;
    }
    if (ts_cnt_ == 1) {
        return Get(key, time, block);
    }
    void* entry = NULL;
    if (entries_->Get(key, entry) < 0 || entry == NULL) {
        return false;
    }
    *block = ((KeyEntry**)entry)[pos->second]->entries.Get(time);  // NOLINT
    return true;
}

bool Segment::Delete(const Slice& key) {
    ::openmldb::base::Node<Slice, void*>* entry_node = NULL;
    {
        std::lock_guard<std::mutex> lock(mu_);
        entry_node = entries_->Remove(key);
        if (entry_node == NULL) {
            return false;
        }
    }
    {
        std::lock_guard<std::mutex> lock(gc_mu_);
        // gc insert should not use timepool
        entry_free_list_->Insert(gc_version_.load(std::memory_order_relaxed), entry_node);
    }
    return true;
}

void Segment::FreeList(::openmldb::base::Node<uint64_t, DataBlock*>* node, uint64_t& gc_idx_cnt,
                       uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size) {
    while (node != NULL) {
        gc_idx_cnt++;
        ::openmldb::base::Node<uint64_t, DataBlock*>* tmp = node;
        idx_byte_size_.fetch_sub(GetRecordTsIdxSize(tmp->Height()));
        node = node->GetNextNoBarrier(0);
        DEBUGLOG("delete key %lu with height %u", tmp->GetKey(), tmp->Height());
        if (tmp->GetValue()->dim_cnt_down > 1) {
            tmp->GetValue()->dim_cnt_down--;
        } else {
            DEBUGLOG("delele data block for key %lu", tmp->GetKey());
            gc_record_byte_size += GetRecordSize(tmp->GetValue()->size);
            delete tmp->GetValue();
            gc_record_cnt++;
        }
        // changed to pool free (time entry)
        pool_.Free(tmp->GetKey());
        // delete tmp;
    }
}

void Segment::FreeEntry(::openmldb::base::Node<Slice, void*>* entry_node, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt,
                        uint64_t& gc_record_byte_size) {
    if (entry_node == NULL) {
        return;
    }
    // free pk memory
    delete[] entry_node->GetKey().data();
    if (ts_cnt_ > 1) {
        KeyEntry** entry_arr = (KeyEntry**)entry_node->GetValue();  // NOLINT
        for (uint32_t i = 0; i < ts_cnt_; i++) {
            uint64_t old = gc_idx_cnt;
            KeyEntry* entry = entry_arr[i];
            TimeEntries::Iterator* it = entry->entries.NewIterator();
            it->SeekToFirst();
            if (it->Valid()) {
                uint64_t ts = it->GetKey();
                ::openmldb::base::Node<uint64_t, DataBlock*>* data_node = entry->entries.Split(ts);
                FreeList(data_node, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
            }
            delete it;
            boost_pool_.free(entry);
            idx_cnt_vec_[i]->fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
        }
        delete[] entry_arr;
        uint64_t byte_size =
            GetRecordPkMultiIdxSize(entry_node->Height(), entry_node->GetKey().size(), key_entry_max_height_, ts_cnt_);
        idx_byte_size_.fetch_sub(byte_size, std::memory_order_relaxed);
    } else {
        uint64_t old = gc_idx_cnt;
        KeyEntry* entry = (KeyEntry*)entry_node->GetValue();  // NOLINT
        TimeEntries::Iterator* it = entry->entries.NewIterator();
        it->SeekToFirst();
        if (it->Valid()) {
            uint64_t ts = it->GetKey();
            ::openmldb::base::Node<uint64_t, DataBlock*>* data_node = entry->entries.Split(ts);
            FreeList(data_node, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        }
        delete it;
        boost_pool_.free(entry);
        uint64_t byte_size =
            GetRecordPkIdxSize(entry_node->Height(), entry_node->GetKey().size(), key_entry_max_height_);
        idx_byte_size_.fetch_sub(byte_size, std::memory_order_relaxed);
        idx_cnt_.fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
    }
}

void Segment::GcEntryFreeList(uint64_t version, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt,
                              uint64_t& gc_record_byte_size) {
    ::openmldb::base::Node<uint64_t, ::openmldb::base::Node<Slice, void*>*>* node = NULL;
    {
        std::lock_guard<std::mutex> lock(gc_mu_);
        node = entry_free_list_->Split(version);
    }
    while (node != NULL) {
        ::openmldb::base::Node<Slice, void*>* entry_node = node->GetValue();
        FreeEntry(entry_node, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        // no change for key entry not using pool
        delete entry_node;
        ::openmldb::base::Node<uint64_t, ::openmldb::base::Node<Slice, void*>*>* tmp = node;
        node = node->GetNextNoBarrier(0);
        // no change for gc not using pool
        delete tmp;
        pk_cnt_.fetch_sub(1, std::memory_order_relaxed);
    }
}

void Segment::GcFreeList(uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size) {
    uint64_t cur_version = gc_version_.load(std::memory_order_relaxed);
    if (cur_version < FLAGS_gc_deleted_pk_version_delta) {
        return;
    }
    uint64_t free_list_version = cur_version - FLAGS_gc_deleted_pk_version_delta;
    GcEntryFreeList(free_list_version, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
}

void Segment::ExecuteGc(const TTLSt& ttl_st, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt,
                        uint64_t& gc_record_byte_size) {
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    switch (ttl_st.ttl_type) {
        case ::openmldb::storage::TTLType::kAbsoluteTime: {
            if (ttl_st.abs_ttl == 0) {
                return;
            }
            uint64_t expire_time = cur_time - ttl_offset_ - ttl_st.abs_ttl;
            Gc4TTL(expire_time, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
            break;
        }
        case ::openmldb::storage::TTLType::kLatestTime: {
            if (ttl_st.lat_ttl == 0) {
                return;
            }
            Gc4Head(ttl_st.lat_ttl, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
            break;
        }
        case ::openmldb::storage::TTLType::kAbsAndLat: {
            if (ttl_st.abs_ttl == 0 || ttl_st.lat_ttl == 0) {
                return;
            }
            uint64_t expire_time = cur_time - ttl_offset_ - ttl_st.abs_ttl;
            Gc4TTLAndHead(expire_time, ttl_st.lat_ttl, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
            break;
        }
        case ::openmldb::storage::TTLType::kAbsOrLat: {
            if (ttl_st.abs_ttl == 0 && ttl_st.lat_ttl == 0) {
                return;
            }
            uint64_t expire_time = ttl_st.abs_ttl == 0 ? 0 : cur_time - ttl_offset_ - ttl_st.abs_ttl;
            Gc4TTLOrHead(expire_time, ttl_st.lat_ttl, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
            break;
        }
        default:
            PDLOG(WARNING, "ttl type %d is unsupported", ttl_st.ttl_type);
    }
}

void Segment::ExecuteGc(const std::map<uint32_t, TTLSt>& ttl_st_map, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt,
                        uint64_t& gc_record_byte_size) {
    if (ttl_st_map.empty()) {
        return;
    }
    if (ts_cnt_ <= 1) {
        ExecuteGc(ttl_st_map.begin()->second, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        return;
    }
    bool need_gc = false;
    for (const auto& kv : ttl_st_map) {
        if (ts_idx_map_.find(kv.first) == ts_idx_map_.end()) {
            return;
        }
        if (kv.second.NeedGc()) {
            need_gc = true;
        }
    }
    if (!need_gc) {
        return;
    }
    GcAllType(ttl_st_map, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
}

void Segment::Gc4Head(uint64_t keep_cnt, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size) {
    if (keep_cnt == 0) {
        PDLOG(WARNING, "[Gc4Head] segment gc4head is disabled");
        return;
    }
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t old = gc_idx_cnt;
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry* entry = (KeyEntry*)it->GetValue();  // NOLINT
        ::openmldb::base::Node<uint64_t, DataBlock*>* node = NULL;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (entry->refs_.load(std::memory_order_acquire) <= 0) {
                node = entry->entries.SplitByPos(keep_cnt);
            }
        }
        uint64_t entry_gc_idx_cnt = 0;
        FreeList(node, entry_gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        entry->count_.fetch_sub(entry_gc_idx_cnt, std::memory_order_relaxed);
        gc_idx_cnt += entry_gc_idx_cnt;
        it->Next();
    }
    DEBUGLOG("[Gc4Head] segment gc keep cnt %lu consumed %lu, count %lu", keep_cnt,
             (::baidu::common::timer::get_micros() - consumed) / 1000, gc_idx_cnt - old);
    idx_cnt_.fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
    delete it;
}

void Segment::GcAllType(const std::map<uint32_t, TTLSt>& ttl_st_map, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt,
                        uint64_t& gc_record_byte_size) {
    uint64_t old = gc_idx_cnt;
    uint64_t consumed = ::baidu::common::timer::get_micros();
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry** entry_arr = (KeyEntry**)it->GetValue();  // NOLINT
        Slice key = it->GetKey();
        it->Next();
        uint32_t empty_cnt = 0;
        for (const auto& kv : ttl_st_map) {
            if (!kv.second.NeedGc()) {
                continue;
            }
            auto pos = ts_idx_map_.find(kv.first);
            if (pos == ts_idx_map_.end() || pos->second >= ts_cnt_) {
                continue;
            }
            KeyEntry* entry = entry_arr[pos->second];
            ::openmldb::base::Node<uint64_t, DataBlock*>* node = NULL;
            bool continue_flag = false;
            switch (kv.second.ttl_type) {
                case ::openmldb::storage::TTLType::kAbsoluteTime: {
                    node = entry->entries.GetLast();
                    if (node == NULL || node->GetKey() > kv.second.abs_ttl) {
                        continue_flag = true;
                    } else {
                        node = NULL;
                        std::lock_guard<std::mutex> lock(mu_);
                        SplitList(entry, kv.second.abs_ttl, &node);
                        if (entry->entries.IsEmpty()) {
                            empty_cnt++;
                        }
                    }
                    break;
                }
                case ::openmldb::storage::TTLType::kLatestTime: {
                    std::lock_guard<std::mutex> lock(mu_);
                    if (entry->refs_.load(std::memory_order_acquire) <= 0) {
                        node = entry->entries.SplitByPos(kv.second.lat_ttl);
                    }
                    break;
                }
                case ::openmldb::storage::TTLType::kAbsAndLat: {
                    node = entry->entries.GetLast();
                    if (node == NULL || node->GetKey() > kv.second.abs_ttl) {
                        continue_flag = true;
                    } else {
                        node = NULL;
                        std::lock_guard<std::mutex> lock(mu_);
                        if (entry->refs_.load(std::memory_order_acquire) <= 0) {
                            node = entry->entries.SplitByKeyAndPos(kv.second.abs_ttl, kv.second.lat_ttl);
                        }
                    }
                    break;
                }
                case ::openmldb::storage::TTLType::kAbsOrLat: {
                    node = entry->entries.GetLast();
                    if (node == NULL) {
                        continue_flag = true;
                    } else {
                        node = NULL;
                        std::lock_guard<std::mutex> lock(mu_);
                        if (entry->refs_.load(std::memory_order_acquire) <= 0) {
                            if (kv.second.abs_ttl == 0) {
                                node = entry->entries.SplitByPos(kv.second.lat_ttl);
                            } else if (kv.second.lat_ttl == 0) {
                                node = entry->entries.Split(kv.second.abs_ttl);
                            } else {
                                node = entry->entries.SplitByKeyOrPos(kv.second.abs_ttl, kv.second.lat_ttl);
                            }
                        }
                        if (entry->entries.IsEmpty()) {
                            empty_cnt++;
                        }
                    }
                    break;
                }
                default:
                    return;
            }
            if (continue_flag) {
                continue;
            }
            uint64_t entry_gc_idx_cnt = 0;
            FreeList(node, entry_gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
            entry->count_.fetch_sub(entry_gc_idx_cnt, std::memory_order_relaxed);
            idx_cnt_vec_[pos->second]->fetch_sub(entry_gc_idx_cnt, std::memory_order_relaxed);
            gc_idx_cnt += entry_gc_idx_cnt;
        }
        if (empty_cnt == ts_cnt_) {
            bool is_empty = true;
            ::openmldb::base::Node<Slice, void*>* entry_node = NULL;
            {
                std::lock_guard<std::mutex> lock(mu_);
                for (uint32_t i = 0; i < ts_cnt_; i++) {
                    if (!entry_arr[i]->entries.IsEmpty()) {
                        is_empty = false;
                        break;
                    }
                }
                if (is_empty) {
                    entry_node = entries_->Remove(key);
                }
            }
            if (entry_node != NULL) {
                std::lock_guard<std::mutex> lock(gc_mu_);
                // gc insert should not use timepool
                entry_free_list_->Insert(gc_version_.load(std::memory_order_relaxed), entry_node);
            }
        }
    }
    DEBUGLOG("[GcAll] segment gc consumed %lu, count %lu", (::baidu::common::timer::get_micros() - consumed) / 1000,
             gc_idx_cnt - old);
    delete it;
}

void Segment::SplitList(KeyEntry* entry, uint64_t ts, ::openmldb::base::Node<uint64_t, DataBlock*>** node) {
    // skip entry that ocupied by reader
    if (entry->refs_.load(std::memory_order_acquire) <= 0) {
        *node = entry->entries.Split(ts);
    }
}

// fast gc with no global pause
void Segment::Gc4TTL(const uint64_t time, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt,
                     uint64_t& gc_record_byte_size) {
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t old = gc_idx_cnt;
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry* entry = (KeyEntry*)it->GetValue();  // NOLINT
        Slice key = it->GetKey();
        it->Next();
        ::openmldb::base::Node<uint64_t, DataBlock*>* node = entry->entries.GetLast();
        if (node == NULL) {
            continue;
        } else if (node->GetKey() > time) {
            DEBUGLOG(
                "[Gc4TTL] segment gc with key %lu need not ttl, last node "
                "key %lu",
                time, node->GetKey());
            continue;
        }
        node = NULL;
        ::openmldb::base::Node<Slice, void*>* entry_node = NULL;
        {
            std::lock_guard<std::mutex> lock(mu_);
            SplitList(entry, time, &node);
            if (entry->entries.IsEmpty()) {
                entry_node = entries_->Remove(key);
            }
        }
        if (entry_node != NULL) {
            std::lock_guard<std::mutex> lock(gc_mu_);
            // gc insert should not use timepool
            entry_free_list_->Insert(gc_version_.load(std::memory_order_relaxed), entry_node);
        }
        uint64_t entry_gc_idx_cnt = 0;
        FreeList(node, entry_gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        entry->count_.fetch_sub(entry_gc_idx_cnt, std::memory_order_relaxed);
        gc_idx_cnt += entry_gc_idx_cnt;
    }
    DEBUGLOG("[Gc4TTL] segment gc with key %lu ,consumed %lu, count %lu", time,
             (::baidu::common::timer::get_micros() - consumed) / 1000, gc_idx_cnt - old);
    idx_cnt_.fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
    delete it;
}

void Segment::Gc4TTLAndHead(const uint64_t time, const uint64_t keep_cnt, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt,
                            uint64_t& gc_record_byte_size) {
    if (time == 0 || keep_cnt == 0) {
        PDLOG(INFO, "[Gc4TTLAndHead] segment gc4ttlandhead is disabled");
        return;
    }
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t old = gc_idx_cnt;
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry* entry = (KeyEntry*)it->GetValue();  // NOLINT
        ::openmldb::base::Node<uint64_t, DataBlock*>* node = entry->entries.GetLast();
        it->Next();
        if (node == NULL) {
            continue;
        } else if (node->GetKey() > time) {
            DEBUGLOG(
                "[Gc4TTLAndHead] segment gc with key %lu need not ttl, last "
                "node key %lu",
                time, node->GetKey());
            continue;
        }
        node = NULL;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (entry->refs_.load(std::memory_order_acquire) <= 0) {
                node = entry->entries.SplitByKeyAndPos(time, keep_cnt);
            }
        }
        uint64_t entry_gc_idx_cnt = 0;
        FreeList(node, entry_gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        entry->count_.fetch_sub(entry_gc_idx_cnt, std::memory_order_relaxed);
        gc_idx_cnt += entry_gc_idx_cnt;
    }
    DEBUGLOG(
        "[Gc4TTLAndHead] segment gc time %lu and keep cnt %lu consumed %lu, "
        "count %lu",
        time, keep_cnt, (::baidu::common::timer::get_micros() - consumed) / 1000, gc_idx_cnt - old);
    idx_cnt_.fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
    delete it;
}

void Segment::Gc4TTLOrHead(const uint64_t time, const uint64_t keep_cnt, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt,
                           uint64_t& gc_record_byte_size) {
    if (time == 0 && keep_cnt == 0) {
        PDLOG(INFO, "[Gc4TTLOrHead] segment gc4ttlorhead is disabled");
        return;
    } else if (time == 0) {
        Gc4Head(keep_cnt, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        return;
    } else if (keep_cnt == 0) {
        Gc4TTL(time, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        return;
    }
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t old = gc_idx_cnt;
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry* entry = (KeyEntry*)it->GetValue();  // NOLINT
        Slice key = it->GetKey();
        it->Next();
        ::openmldb::base::Node<uint64_t, DataBlock*>* node = entry->entries.GetLast();
        if (node == NULL) {
            continue;
        }
        node = NULL;
        ::openmldb::base::Node<Slice, void*>* entry_node = NULL;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (entry->refs_.load(std::memory_order_acquire) <= 0) {
                node = entry->entries.SplitByKeyOrPos(time, keep_cnt);
            }
            if (entry->entries.IsEmpty()) {
                entry_node = entries_->Remove(key);
            }
        }
        if (entry_node != NULL) {
            std::lock_guard<std::mutex> lock(gc_mu_);
            // gc insert should not use timepool
            entry_free_list_->Insert(gc_version_.load(std::memory_order_relaxed), entry_node);
        }
        uint64_t entry_gc_idx_cnt = 0;
        FreeList(node, entry_gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        entry->count_.fetch_sub(entry_gc_idx_cnt, std::memory_order_relaxed);
        gc_idx_cnt += entry_gc_idx_cnt;
    }
    DEBUGLOG(
        "[Gc4TTLAndHead] segment gc time %lu and keep cnt %lu consumed %lu, "
        "count %lu",
        time, keep_cnt, (::baidu::common::timer::get_micros() - consumed) / 1000, gc_idx_cnt - old);
    idx_cnt_.fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
    delete it;
}

int Segment::GetCount(const Slice& key, uint64_t& count) {
    if (ts_cnt_ > 1) {
        return -1;
    }
    void* entry = NULL;
    if (entries_->Get(key, entry) < 0 || entry == NULL) {
        return -1;
    }
    count = ((KeyEntry*)entry)->count_.load(std::memory_order_relaxed);  // NOLINT
    return 0;
}

int Segment::GetCount(const Slice& key, uint32_t idx, uint64_t& count) {
    auto pos = ts_idx_map_.find(idx);
    if (pos == ts_idx_map_.end()) {
        return -1;
    }
    if (ts_cnt_ == 1) {
        return GetCount(key, count);
    }
    void* entry_arr = NULL;
    if (entries_->Get(key, entry_arr) < 0 || entry_arr == NULL) {
        return -1;
    }
    count = ((KeyEntry**)entry_arr)[pos->second]->count_.load(  // NOLINT
        std::memory_order_relaxed);
    return 0;
}

// Iterator
MemTableIterator* Segment::NewIterator(const Slice& key, Ticket& ticket) {
    if (entries_ == NULL || ts_cnt_ > 1) {
        return new MemTableIterator(NULL);
    }
    void* entry = NULL;
    if (entries_->Get(key, entry) < 0 || entry == NULL) {
        return new MemTableIterator(NULL);
    }
    ticket.Push((KeyEntry*)entry);                                           // NOLINT
    return new MemTableIterator(((KeyEntry*)entry)->entries.NewIterator());  // NOLINT
}

MemTableIterator* Segment::NewIterator(const Slice& key, uint32_t idx, Ticket& ticket) {
    auto pos = ts_idx_map_.find(idx);
    if (pos == ts_idx_map_.end()) {
        return new MemTableIterator(NULL);
    }
    if (ts_cnt_ == 1) {
        return NewIterator(key, ticket);
    }
    void* entry_arr = NULL;
    if (entries_->Get(key, entry_arr) < 0 || entry_arr == NULL) {
        return new MemTableIterator(NULL);
    }
    ticket.Push(((KeyEntry**)entry_arr)[pos->second]);                                         // NOLINT
    return new MemTableIterator(((KeyEntry**)entry_arr)[pos->second]->entries.NewIterator());  // NOLINT
}

MemTableIterator::MemTableIterator(TimeEntries::Iterator* it) : it_(it) {}

MemTableIterator::~MemTableIterator() {
    if (it_ != NULL) {
        delete it_;
    }
}

void MemTableIterator::Seek(const uint64_t time) {
    if (it_ == NULL) {
        return;
    }
    it_->Seek(time);
}

bool MemTableIterator::Valid() {
    if (it_ == NULL) {
        return false;
    }
    return it_->Valid();
}

void MemTableIterator::Next() {
    if (it_ == NULL) {
        return;
    }
    it_->Next();
}

::openmldb::base::Slice MemTableIterator::GetValue() const {
    return ::openmldb::base::Slice(it_->GetValue()->data, it_->GetValue()->size);
}

uint64_t MemTableIterator::GetKey() const { return it_->GetKey(); }

void MemTableIterator::SeekToFirst() {
    if (it_ == NULL) {
        return;
    }
    it_->SeekToFirst();
}

void MemTableIterator::SeekToLast() {
    if (it_ == NULL) {
        return;
    }
    it_->SeekToLast();
}

}  // namespace storage
}  // namespace openmldb
