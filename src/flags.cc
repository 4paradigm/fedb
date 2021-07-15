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

#include <gflags/gflags.h>
// cluster config
DEFINE_string(endpoint, "", "config the ip and port that openmldb serves for");
DEFINE_int32(port, 0, "config the port that openmldb serves for");
DEFINE_string(openmldb_log_dir, "./logs", "config the log dir");
DEFINE_int32(zk_session_timeout, 2000, "config the session timeout of tablet or nameserver");
DEFINE_uint32(tablet_heartbeat_timeout, 5 * 60 * 1000, "config the heartbeat of tablet offline");
DEFINE_uint32(tablet_offline_check_interval, 1000, "config the check interval of tablet offline");
DEFINE_string(zk_cluster, "", "config the zookeeper cluster eg ip:2181,ip2:2181,ip3:2181");
DEFINE_string(zk_root_path, "/openmldb", "config the root path of zookeeper");
DEFINE_int32(zk_keep_alive_check_interval, 15000, "config the interval of keep alive check");
DEFINE_int32(get_task_status_interval, 2000, "config the interval of get task status");
DEFINE_uint32(get_table_status_interval, 2000, "config the interval of get table status");
DEFINE_uint32(get_table_diskused_interval, 600000, "config the interval of get table diskused");
DEFINE_int32(name_server_task_pool_size, 8, "config the size of name server task pool");
DEFINE_uint32(name_server_task_concurrency, 2, "config the concurrency of name_server_task");
DEFINE_uint32(name_server_task_concurrency_for_replica_cluster, 2,
              "config the concurrency of name_server_task for replica cluster");
DEFINE_uint32(name_server_task_max_concurrency, 8, "config the max concurrency of name_server_task");
DEFINE_int32(name_server_task_wait_time, 1000, "config the time of task wait");
DEFINE_uint32(name_server_op_execute_timeout, 2 * 60 * 60 * 1000, "config the timeout of nameserver op");
DEFINE_bool(auto_failover, false, "enable or disable auto failover");
DEFINE_bool(enable_timeseries_table, true, "enable or disable timeseries table");
DEFINE_int32(max_op_num, 10000, "config the max op num");
DEFINE_uint32(partition_num, 8, "config the default partition_num");
DEFINE_uint32(replica_num, 3,
              "config the default replica_num. if set 3, there is one leader "
              "and two followers");

DEFINE_int32(gc_interval, 120, "the gc interval of tablet every two hour");
DEFINE_int32(gc_pool_size, 2, "the size of tablet gc thread pool");
DEFINE_int32(gc_safe_offset, 1, "the safe offset of tablet gc in minute");
DEFINE_uint64(gc_on_table_recover_count, 10000000, "make a gc on recover count");
DEFINE_uint32(gc_deleted_pk_version_delta, 2, "config the gc version delta");
DEFINE_double(mem_release_rate, 5, "specify memory release rate, which should be in 0 ~ 10");
DEFINE_int32(task_pool_size, 3, "the size of tablet task thread pool");
DEFINE_int32(io_pool_size, 2, "the size of tablet io task thread pool");
DEFINE_bool(use_name, false, "enable or disable use server name");
DEFINE_string(data_dir, "./data", "the path of data dir");
DEFINE_bool(enable_distsql, false, "enable or disable distribute sql");
DEFINE_bool(enable_localtablet, true, "enable or disable local tablet opt when distribute sql circumstance");

// scan configuration
DEFINE_uint32(scan_max_bytes_size, 2 * 1024 * 1024, "config the max size of scan bytes size");
DEFINE_uint32(scan_reserve_size, 1024, "config the size of vec reserve");
DEFINE_uint32(preview_limit_max_num, 1000, "config the max num of preview limit");
DEFINE_uint32(preview_default_limit, 100, "config the default limit of preview");
// binlog configuration
DEFINE_int32(binlog_single_file_max_size, 1024 * 4, "the max size of single binlog file");
DEFINE_int32(binlog_sync_batch_size, 32, "the batch size of sync binlog");
DEFINE_bool(binlog_notify_on_put, false, "config the sync log to follower strategy");
DEFINE_bool(binlog_enable_crc, false, "enable crc");
DEFINE_int32(binlog_coffee_time, 1000, "config the coffee time");
DEFINE_int32(binlog_sync_wait_time, 100, "config the sync log wait time");
DEFINE_int32(binlog_sync_to_disk_interval, 20000, "config the interval of sync binlog to disk time");
DEFINE_int32(binlog_delete_interval, 60000, "config the interval of delete binlog");
DEFINE_int32(binlog_match_logoffset_interval, 1000, "config the interval of match log offset ");
DEFINE_int32(binlog_name_length, 8, "binlog name length");
DEFINE_uint32(check_binlog_sync_progress_delta, 100000, "config the delta of check binlog sync progress");
DEFINE_uint32(go_back_max_try_cnt, 10, "config max try time of go back");

DEFINE_uint32(put_slow_log_threshold, 50000, "config the threshold of put slow log");
DEFINE_uint32(query_slow_log_threshold, 50000, "config the threshold of query slow log");

// local db config
DEFINE_string(db_root_path, "/tmp/", "the root path of db");

// thread pool config
DEFINE_int32(scan_concurrency_limit, 8, "the limit of scan concurrency");
DEFINE_int32(put_concurrency_limit, 8, "the limit of put concurrency");
DEFINE_int32(thread_pool_size, 16, "the size of thread pool for other api");
DEFINE_int32(get_concurrency_limit, 8, "the limit of get concurrency");
DEFINE_int32(request_max_retry, 3, "max retry time when request error");
DEFINE_int32(request_timeout_ms, 20000, "request timeout");
DEFINE_int32(request_sleep_time, 1000, "the sleep time when request error");

DEFINE_uint32(max_traverse_cnt, 50000, "max traverse iter loop cnt");

DEFINE_uint32(task_check_interval, 1000, "config the check interval of task");

DEFINE_int32(send_file_max_try, 3, "the max retry time when send file failed");
DEFINE_int32(retry_send_file_wait_time_ms, 3000, "conf the wait time when retry send file");
DEFINE_int32(stream_close_wait_time_ms, 1000, "the wait time before close stream");
DEFINE_uint32(stream_block_size, 1 * 1204 * 1024, "config the write/read block size in streaming");
DEFINE_int32(stream_bandwidth_limit, 10 * 1204 * 1024, "the limit bandwidth. Byte/Second");

// if set 23, the task will execute 23:00 every day
DEFINE_int32(make_snapshot_time, 23, "config the time to make snapshot");
DEFINE_int32(make_snapshot_check_interval, 1000 * 60 * 10, "config the interval to check making snapshot time");
DEFINE_int32(make_snapshot_threshold_offset, 100000, "config the offset to reach the threshold");
DEFINE_uint32(make_snapshot_max_deleted_keys, 1000000, "config the max deleted keys store when make snapshot");
DEFINE_uint32(make_snapshot_offline_interval, 60 * 60 * 24,
              "config tablet self makesnapshot when how long time do not "
              "makesnapshot from ns. unit is second");
DEFINE_string(snapshot_compression, "off", "Type of snapshot compression, can be off, snappy, zlib");
DEFINE_int32(snapshot_pool_size, 1, "the size of tablet thread pool for making snapshot");

DEFINE_uint32(load_index_max_wait_time, 120 * 60 * 1000, "config the max wait time of load index");

DEFINE_string(recycle_bin_root_path, "/tmp/recycle", "specify the root path of recycle bin");
DEFINE_bool(recycle_bin_enabled, true, "enable the recycle bin storage");
DEFINE_uint32(recycle_ttl, 0, "ttl of recycle in minute");

DEFINE_uint32(latest_ttl_max, 1000, "the max ttl of latest");
DEFINE_uint32(absolute_ttl_max, 60 * 24 * 365 * 30, "the max ttl of absolute time");
DEFINE_uint32(time_series_pool_block_size, 4 * 1024, "the block size of time series pool");
DEFINE_uint32(skiplist_max_height, 12, "the max height of skiplist");
DEFINE_uint32(key_entry_max_height, 8, "the max height of key entry");
DEFINE_uint32(latest_default_skiplist_height, 1, "the default height of skiplist for latest table");
DEFINE_uint32(absolute_default_skiplist_height, 4, "the default height of skiplist for absolute table");
DEFINE_bool(enable_show_tp, false, "enable show tp");
DEFINE_uint32(max_col_display_length, 256, "config the max length of column display");

// rocksdb
DEFINE_bool(disable_wal, true, "If true, do not write WAL for write.");
DEFINE_string(file_compression, "off", "Type of compression, can be off, pz, lz4, zlib");
DEFINE_uint32(block_cache_mb, 4096,
              "Memory allocated for caching uncompressed block (OS page cache "
              "handles the compressed ones)");
DEFINE_uint32(write_buffer_mb, 128, "Memtable size");
DEFINE_uint32(block_cache_shardbits, 8, "Divide block cache into 2^8 shards to avoid cache contention");
DEFINE_bool(verify_compression, false, "For debug");

// load table resouce control
DEFINE_uint32(load_table_batch, 30, "set laod table batch size");
DEFINE_uint32(load_table_thread_num, 3, "set load tabale thread pool size");
DEFINE_uint32(load_table_queue_size, 1000, "set load tabale queue size");

// multiple data center
DEFINE_uint32(get_replica_status_interval, 10000, "config the interval to sync replica cluster status time");
