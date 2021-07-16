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

#include <fcntl.h>
#include <gflags/gflags.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include <sched.h>
#include <snappy.h>
#include <unistd.h>

#include <csignal>
#include <iostream>
#include <memory>
#include <random>

#include "base/file_util.h"
#include "base/glog_wapper.h"
#include "base/hash.h"
#include "base/ip.h"
#include "base/kv_iterator.h"
#include "base/linenoise.h"
#include "base/server_name.h"
#include "base/strings.h"
#if defined(__linux__) || defined(__mac_tablet__)
#include "nameserver/name_server_impl.h"
#include "tablet/tablet_impl.h"
#endif
#include "apiserver/api_server_impl.h"
#include "boost/algorithm/string.hpp"
#include "boost/lexical_cast.hpp"
#include "brpc/server.h"
#include "client/ns_client.h"
#include "client/tablet_client.h"
#include "cmd/display.h"
#include "cmd/sdk_iterator.h"
#include "cmd/sql_cmd.h"
#include "codec/row_codec.h"
#include "codec/schema_codec.h"
#include "codec/sdk_codec.h"
#include "common/timer.h"
#include "common/tprinter.h"
#include "config.h"  // NOLINT
#include "proto/client.pb.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "version.h"  // NOLINT
#include "vm/engine.h"

using Schema = ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>;
using TabletClient = openmldb::client::TabletClient;

DECLARE_string(endpoint);
DECLARE_int32(port);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(thread_pool_size);
DECLARE_int32(put_concurrency_limit);
DECLARE_int32(scan_concurrency_limit);
DECLARE_int32(get_concurrency_limit);
DEFINE_string(role, "tablet | nameserver | client | ns_client | sql_client | apiserver",
        "Set the openmldb role for start");
DEFINE_string(cmd, "", "Set the command");
DEFINE_bool(interactive, true, "Set the interactive");

DECLARE_string(openmldb_log_dir);
DEFINE_int32(log_file_size, 1024, "Config the log size in MB");
DEFINE_int32(log_file_count, 24, "Config the log count");
DEFINE_string(log_level, "debug", "Set the log level, eg: debug or info");
DECLARE_uint32(latest_ttl_max);
DECLARE_uint32(absolute_ttl_max);
DECLARE_uint32(skiplist_max_height);
DECLARE_uint32(preview_limit_max_num);
DECLARE_uint32(preview_default_limit);
DECLARE_uint32(max_col_display_length);
DECLARE_bool(version);
DECLARE_bool(use_name);
DECLARE_string(data_dir);

const std::string OPENMLDB_VERSION = std::to_string(OPENMLDB_VERSION_MAJOR) + "." +  // NOLINT
                                 std::to_string(OPENMLDB_VERSION_MINOR) + "." +
                                 std::to_string(OPENMLDB_VERSION_BUG) + "." +
                                 OPENMLDB_COMMIT_ID;

static std::map<std::string, std::string> real_ep_map;

void shutdown_signal_handler(int signal) {
    std::cout << "catch signal: " << signal << std::endl;
    brpc::AskToQuit();
}

void SetupLog() {
    // Config log
    if (FLAGS_log_level == "debug") {
        ::openmldb::base::SetLogLevel(DEBUG);
    } else {
        ::openmldb::base::SetLogLevel(INFO);
    }
    if (!FLAGS_openmldb_log_dir.empty()) {
        ::openmldb::base::Mkdir(FLAGS_openmldb_log_dir);
        std::string file = FLAGS_openmldb_log_dir + "/" + FLAGS_role;
        openmldb::base::SetLogFile(file);
    }
}

void GetRealEndpoint(std::string* real_endpoint) {
    if (real_endpoint == nullptr) {
        return;
    }
    if (FLAGS_endpoint.empty() && FLAGS_port > 0) {
        std::string ip;
        if (::openmldb::base::GetLocalIp(&ip)) {
            PDLOG(INFO, "local ip is: %s", ip.c_str());
        } else {
            PDLOG(WARNING, "fail to get local ip: %s", ip.c_str());
            exit(1);
        }
        std::ostringstream oss;
        oss << ip << ":" << std::to_string(FLAGS_port);
        *real_endpoint = oss.str();
        if (FLAGS_use_name) {
            std::string server_name;
            if (!::openmldb::base::GetNameFromTxt(FLAGS_data_dir, &server_name)) {
                PDLOG(WARNING, "GetNameFromTxt failed");
                exit(1);
            }
            FLAGS_endpoint = server_name;
        } else {
            FLAGS_endpoint = *real_endpoint;
        }
    }
}

#if defined(__linux__) || defined(__mac_tablet__)
void StartNameServer() {
    SetupLog();
    std::string real_endpoint;
    GetRealEndpoint(&real_endpoint);
    ::openmldb::nameserver::NameServerImpl* name_server = new ::openmldb::nameserver::NameServerImpl();
    if (!name_server->Init(real_endpoint)) {
        PDLOG(WARNING, "Fail to init");
        exit(1);
    }
    if (!name_server->RegisterName()) {
        PDLOG(WARNING, "Fail to register name");
        exit(1);
    }
    brpc::ServerOptions options;
    options.num_threads = FLAGS_thread_pool_size;
    brpc::Server server;
    if (server.AddService(name_server, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (real_endpoint.empty()) {
        real_endpoint = FLAGS_endpoint;
    }
    if (server.Start(real_endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    PDLOG(INFO, "start nameserver on endpoint %s with version %s", real_endpoint.c_str(), OPENMLDB_VERSION.c_str());
    server.set_version(OPENMLDB_VERSION.c_str());
    server.RunUntilAskedToQuit();
}

int THPIsEnabled() {
#if defined(__linux__) || defined(__mac_tablet__)
    char buf[1024];
    FILE* fp = fopen("/sys/kernel/mm/transparent_hugepage/enabled", "r");
    if (!fp) {
        return 0;
    }
    if (fgets(buf, sizeof(buf), fp) == NULL) {
        fclose(fp);
        return 0;
    }
    fclose(fp);
    if (strstr(buf, "[never]") == NULL) {
        return 1;
    }
    fp = fopen("/sys/kernel/mm/transparent_hugepage/defrag", "r");
    if (!fp) return 0;
    if (fgets(buf, sizeof(buf), fp) == NULL) {
        fclose(fp);
        return 0;
    }
    fclose(fp);
    return (strstr(buf, "[never]") == NULL) ? 1 : 0;
#else
    return 0;
#endif
}

int SwapIsEnabled() {
#if defined(__linux__) || defined(__mac_tablet__)
    char buf[1024];
    FILE* fp = fopen("/proc/swaps", "r");
    if (!fp) {
        return 0;
    }
    if (fgets(buf, sizeof(buf), fp) == NULL) {
        fclose(fp);
        return 0;
    }
    // if the swap is disabled, there is only one line in /proc/swaps.
    // Filename     Type        Size    Used    Priority
    if (fgets(buf, sizeof(buf), fp) == NULL) {
        fclose(fp);
        return 0;
    }
    fclose(fp);
    return 1;
#else
    return 0;
#endif
}

void StartTablet() {
    if (THPIsEnabled()) {
        PDLOG(WARNING,
              "THP is enabled in your kernel. This will create latency and "
              "memory usage issues with OPENMLDB."
              "To fix this issue run the command 'echo never > "
              "/sys/kernel/mm/transparent_hugepage/enabled' and "
              "'echo never > /sys/kernel/mm/transparent_hugepage/defrag' as "
              "root");
    }
    if (SwapIsEnabled()) {
        PDLOG(WARNING,
              "Swap is enabled in your kernel. This will create latency and "
              "memory usage issues with OPENMLDB."
              "To fix this issue run the command 'swapoff -a' as root");
    }
    SetupLog();
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    std::string real_endpoint;
    GetRealEndpoint(&real_endpoint);
    ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
    bool ok = tablet->Init(real_endpoint);
    if (!ok) {
        PDLOG(WARNING, "fail to init tablet");
        exit(1);
    }
    brpc::ServerOptions options;
    options.num_threads = FLAGS_thread_pool_size;
    brpc::Server server;
    if (server.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    server.MaxConcurrencyOf(tablet, "Scan") = FLAGS_scan_concurrency_limit;
    server.MaxConcurrencyOf(tablet, "Put") = FLAGS_put_concurrency_limit;
    tablet->SetServer(&server);
    server.MaxConcurrencyOf(tablet, "Get") = FLAGS_get_concurrency_limit;
    if (real_endpoint.empty()) {
        real_endpoint = FLAGS_endpoint;
    }
    if (server.Start(real_endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
#ifdef PZFPGA_ENABLE
    PDLOG(INFO, "start tablet on endpoint %s with version %s with fpga",
            real_endpoint.c_str(), OPENMLDB_VERSION.c_str());
#else
    PDLOG(INFO, "start tablet on endpoint %s with version %s", real_endpoint.c_str(), OPENMLDB_VERSION.c_str());
#endif
    if (!tablet->RegisterZK()) {
        PDLOG(WARNING, "Fail to register zk");
        exit(1);
    }
    server.set_version(OPENMLDB_VERSION.c_str());
    server.RunUntilAskedToQuit();
}
#endif

int PutData(uint32_t tid, const std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>>& dimensions,
            const std::vector<uint64_t>& ts_dimensions, uint64_t ts, const std::string& value,
            const google::protobuf::RepeatedPtrField<::openmldb::nameserver::TablePartition>& table_partition,
            uint32_t format_version) {
    std::map<std::string, std::shared_ptr<::openmldb::client::TabletClient>> clients;
    for (auto iter = dimensions.begin(); iter != dimensions.end(); iter++) {
        uint32_t pid = iter->first;
        std::string endpoint;
        for (const auto& cur_table_partition : table_partition) {
            if (cur_table_partition.pid() != pid) {
                continue;
            }
            for (int inner_idx = 0; inner_idx < cur_table_partition.partition_meta_size(); inner_idx++) {
                if (cur_table_partition.partition_meta(inner_idx).is_leader() &&
                    cur_table_partition.partition_meta(inner_idx).is_alive()) {
                    endpoint = cur_table_partition.partition_meta(inner_idx).endpoint();
                    break;
                }
            }
            break;
        }
        if (endpoint.empty()) {
            printf("put error. cannot find healthy endpoint. pid is %u\n", pid);
            return -1;
        }
        if (clients.find(endpoint) == clients.end()) {
            std::string real_endpoint;
            if (!real_ep_map.empty()) {
                auto rit = real_ep_map.find(endpoint);
                if (rit != real_ep_map.end()) {
                    real_endpoint = rit->second;
                }
            }
            clients.insert(
                std::make_pair(endpoint, std::make_shared<::openmldb::client::TabletClient>(endpoint, real_endpoint)));
            if (clients[endpoint]->Init() < 0) {
                printf("tablet client init failed, endpoint is %s\n", endpoint.c_str());
                return -1;
            }
        }
        if (ts_dimensions.empty()) {
            if (!clients[endpoint]->Put(tid, pid, ts, value, iter->second, format_version)) {
                printf("put failed. tid %u pid %u endpoint %s ts %lu \n", tid, pid, endpoint.c_str(), ts);
                return -1;
            }
        } else {
            if (!clients[endpoint]->Put(tid, pid, iter->second, ts_dimensions, value, format_version)) {
                printf("put failed. tid %u pid %u endpoint %s ts_dimensions\n", tid, pid, endpoint.c_str());
                return -1;
            }
        }
    }
    std::cout << "Put ok" << std::endl;
    return 0;
}

::openmldb::base::ResultMsg PutSchemaData(const ::openmldb::nameserver::TableInfo& table_info, uint64_t ts,
                                          const std::vector<std::string>& input_value) {
    std::string value;
    ::openmldb::codec::SDKCodec codec(table_info);
    std::map<uint32_t, ::openmldb::codec::Dimension> dimensions;
    const int part_size = table_info.table_partition_size();
    if (table_info.partition_key_size() > 0) {
        if (codec.EncodeDimension(input_value, 0, &dimensions) < 0) {
            return ::openmldb::base::ResultMsg(-1, "Encode dimension error");
        }
        std::string key;
        if (codec.CombinePartitionKey(input_value, &key) < 0) {
            return ::openmldb::base::ResultMsg(-1, "combine partition key error");
        }
        uint32_t pid = (uint32_t)(::openmldb::base::hash64(key) % part_size);
        if (pid != 0) {
            auto pair = dimensions.emplace(pid, ::openmldb::codec::Dimension());
            dimensions[0].swap(pair.first->second);
            dimensions.erase(0);
        }
    } else {
        if (codec.EncodeDimension(input_value, part_size, &dimensions) < 0) {
            return ::openmldb::base::ResultMsg(-1, "Encode dimension error");
        }
    }
    if (codec.EncodeRow(input_value, &value) < 0) {
        return ::openmldb::base::ResultMsg(-1, "Encode data error");
    }
    std::vector<uint64_t> ts_dimensions;
    if (codec.EncodeTsDimension(input_value, &ts_dimensions) < 0) {
        return ::openmldb::base::ResultMsg(-1, "Encode ts dimension error");
    }
    if (table_info.compress_type() == ::openmldb::type::CompressType::kSnappy) {
        std::string compressed;
        ::snappy::Compress(value.c_str(), value.length(), &compressed);
        value = compressed;
    }
    const int tid = table_info.tid();
    const uint32_t fmt_ver = table_info.format_version();
    PutData(tid, dimensions, ts_dimensions, ts, value, table_info.table_partition(), fmt_ver);

    return ::openmldb::base::ResultMsg(0, "ok");
}

int SplitPidGroup(const std::string& pid_group,
                  std::set<uint32_t>& pid_set) {  // NOLINT
    try {
        if (::openmldb::base::IsNumber(pid_group)) {
            pid_set.insert(boost::lexical_cast<uint32_t>(pid_group));
        } else if (pid_group.find('-') != std::string::npos) {
            std::vector<std::string> vec;
            boost::split(vec, pid_group, boost::is_any_of("-"));
            if (vec.size() != 2 || !::openmldb::base::IsNumber(vec[0]) || !::openmldb::base::IsNumber(vec[1])) {
                return -1;
            }
            uint32_t start_index = boost::lexical_cast<uint32_t>(vec[0]);
            uint32_t end_index = boost::lexical_cast<uint32_t>(vec[1]);
            while (start_index <= end_index) {
                pid_set.insert(start_index);
                start_index++;
            }
        } else if (pid_group.find(',') != std::string::npos) {
            std::vector<std::string> vec;
            boost::split(vec, pid_group, boost::is_any_of(","));
            for (const auto& pid_str : vec) {
                if (!::openmldb::base::IsNumber(pid_str)) {
                    return -1;
                }
                pid_set.insert(boost::lexical_cast<uint32_t>(pid_str));
            }
        } else {
            return -1;
        }
    } catch (const std::exception& e) {
        std::cout << "Invalid args. pid should be uint32_t" << std::endl;
        return -1;
    }
    return 0;
}

bool GetParameterMap(const std::string& first, const std::vector<std::string>& parts, const std::string& delimiter,
                     std::map<std::string, std::string>& parameter_map) {  // NOLINT
    std::vector<std::string> temp_vec;
    ::openmldb::base::SplitString(parts[1], delimiter, temp_vec);
    if (temp_vec.size() == 2 && temp_vec[0] == first && !temp_vec[1].empty()) {
        parameter_map.insert(std::make_pair(temp_vec[0], temp_vec[1]));
        for (uint32_t i = 2; i < parts.size(); i++) {
            ::openmldb::base::SplitString(parts[i], delimiter, temp_vec);
            if (temp_vec.size() < 2 || temp_vec[1].empty()) {
                return false;
            }
            parameter_map.insert(std::make_pair(temp_vec[0], temp_vec[1]));
        }
    }
    return true;
}

std::shared_ptr<::openmldb::client::TabletClient> GetTabletClient(const ::openmldb::nameserver::TableInfo& table_info,
                                                                  uint32_t pid,
                                                                  std::string& msg) {  // NOLINT
    std::string endpoint;
    for (int idx = 0; idx < table_info.table_partition_size(); idx++) {
        if (table_info.table_partition(idx).pid() != pid) {
            continue;
        }
        for (int inner_idx = 0; inner_idx < table_info.table_partition(idx).partition_meta_size(); inner_idx++) {
            if (table_info.table_partition(idx).partition_meta(inner_idx).is_leader() &&
                table_info.table_partition(idx).partition_meta(inner_idx).is_alive()) {
                endpoint = table_info.table_partition(idx).partition_meta(inner_idx).endpoint();
                break;
            }
        }
        break;
    }
    if (endpoint.empty()) {
        msg = "cannot find healthy endpoint. pid is " + std::to_string(pid);
        return std::shared_ptr<::openmldb::client::TabletClient>();
    }
    std::string real_endpoint;
    if (!real_ep_map.empty()) {
        auto rit = real_ep_map.find(endpoint);
        if (rit != real_ep_map.end()) {
            real_endpoint = rit->second;
        }
    }
    auto tablet_client = std::make_shared<::openmldb::client::TabletClient>(endpoint, real_endpoint);
    if (tablet_client->Init() < 0) {
        msg = "tablet client init failed, endpoint is " + endpoint;
        tablet_client.reset();
    }
    return tablet_client;
}

void HandleNSClientSetTTL(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 4) {
        std::cout << "bad setttl format, eg settl t1 absolute 10" << std::endl;
        return;
    }
    std::string index_name;
    try {
        std::string err;
        uint64_t abs_ttl = 0;
        uint64_t lat_ttl = 0;
        ::openmldb::type::TTLType type = ::openmldb::type::kLatestTime;
        if (parts[2] == "absolute") {
            type = ::openmldb::type::TTLType::kAbsoluteTime;
            abs_ttl = boost::lexical_cast<uint64_t>(parts[3]);
            if (parts.size() == 5) {
                index_name = parts[4];
            }
        } else if (parts[2] == "absandlat") {
            type = ::openmldb::type::TTLType::kAbsAndLat;
            abs_ttl = boost::lexical_cast<uint64_t>(parts[3]);
            lat_ttl = boost::lexical_cast<uint64_t>(parts[4]);
            if (parts.size() == 6) {
                index_name = parts[5];
            }
        } else if (parts[2] == "absorlat") {
            type = ::openmldb::type::TTLType::kAbsOrLat;
            abs_ttl = boost::lexical_cast<uint64_t>(parts[3]);
            lat_ttl = boost::lexical_cast<uint64_t>(parts[4]);
            if (parts.size() == 6) {
                index_name = parts[5];
            }
        } else {
            lat_ttl = boost::lexical_cast<uint64_t>(parts[3]);
            if (parts.size() == 5) {
                index_name = parts[4];
            }
        }
        bool ok = client->UpdateTTL(parts[1], type, abs_ttl, lat_ttl, index_name, err);
        if (ok) {
            std::cout << "Set ttl ok !" << std::endl;
        } else {
            std::cout << "Set ttl failed! " << err << std::endl;
        }
    } catch (std::exception const& e) {
        std::cout << "Invalid args ttl which should be uint64_t" << std::endl;
    }
}

void HandleNSClientCancelOP(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "bad cancelop format, eg cancelop 1002" << std::endl;
        return;
    }
    try {
        std::string err;
        if (boost::lexical_cast<int64_t>(parts[1]) <= 0) {
            std::cout << "Invalid args. op_id should be large than zero" << std::endl;
            return;
        }
        uint64_t op_id = boost::lexical_cast<uint64_t>(parts[1]);
        bool ok = client->CancelOP(op_id, err);
        if (ok) {
            std::cout << "Cancel op ok!" << std::endl;
        } else {
            std::cout << "Cancel op failed! " << err << std::endl;
        }
    } catch (std::exception const& e) {
        std::cout << "Invalid args. op_id should be uint64_t" << std::endl;
    }
}

void HandleNSShowTablet(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    std::vector<std::string> row;
    row.push_back("endpoint");
    row.push_back("real_endpoint");
    row.push_back("state");
    row.push_back("age");
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    std::vector<::openmldb::client::TabletInfo> tablets;
    std::string msg;
    bool ok = client->ShowTablet(tablets, msg);
    if (!ok) {
        std::cout << "Fail to show tablets. error msg: " << msg << std::endl;
        return;
    }
    for (size_t i = 0; i < tablets.size(); i++) {
        std::vector<std::string> row;
        row.push_back(tablets[i].endpoint);
        row.push_back(tablets[i].real_endpoint);
        row.push_back(tablets[i].state);
        row.push_back(::openmldb::base::HumanReadableTime(tablets[i].age));
        tp.AddRow(row);
    }
    tp.Print(true);
}

void HandleNSShowSdkEndpoint(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    std::vector<std::string> row;
    row.push_back("endpoint");
    row.push_back("sdk_endpoint");
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    std::vector<::openmldb::client::TabletInfo> tablets;
    std::string msg;
    bool ok = client->ShowSdkEndpoint(tablets, msg);
    if (!ok) {
        std::cout << "Fail to show sdkendpoint. error msg: " << msg << std::endl;
        return;
    }
    for (size_t i = 0; i < tablets.size(); i++) {
        std::vector<std::string> row;
        row.push_back(tablets[i].endpoint);
        row.push_back(tablets[i].real_endpoint);
        tp.AddRow(row);
    }
    tp.Print(true);
}

void HandleNSRemoveReplicaCluster(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "Bad format. eg removerepcluster dc2" << std::endl;
        return;
    }

    if (FLAGS_interactive) {
        printf("Drop replica %s? yes/no\n", parts[1].c_str());
        std::string input;
        std::cin >> input;
        std::transform(input.begin(), input.end(), input.begin(), ::tolower);
        if (input != "yes") {
            printf("'drop %s' cmd is canceled!\n", parts[1].c_str());
            return;
        }
    }
    std::string msg;
    bool ret = client->RemoveReplicaCluster(parts[1], msg);
    if (!ret) {
        std::cout << "remove failed. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "remove replica cluster ok" << std::endl;
}

void HandleNSSwitchMode(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    if ((parts[1] == "normal") || (parts[1] == "leader")) {
    } else {
        std::cout << "invalid mode type" << std::endl;
        return;
    }
    std::string msg;
    bool ok = false;
    if (parts[1] == "normal") {
        ok = client->SwitchMode(::openmldb::nameserver::kNORMAL, msg);
    } else if (parts[1] == "leader") {
        ok = client->SwitchMode(::openmldb::nameserver::kLEADER, msg);
    }
    if (!ok) {
        std::cout << "Fail to swith mode. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "switchmode ok" << std::endl;
}

void HandleNSShowNameServer(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client,
                            std::shared_ptr<::openmldb::zk::ZkClient> zk_client) {
    if (FLAGS_zk_cluster.empty() || !zk_client) {
        std::cout << "Show nameserver failed. zk_cluster is empty" << std::endl;
        return;
    }
    std::string node_path = FLAGS_zk_root_path + "/leader";
    std::vector<std::string> children;
    if (!zk_client->GetChildren(node_path, children) || children.empty()) {
        std::cout << "get children failed" << std::endl;
        return;
    }
    std::vector<std::string> endpoint_vec;
    for (auto path : children) {
        std::string endpoint;
        std::string real_path = node_path + "/" + path;
        if (!zk_client->GetNodeValue(real_path, endpoint)) {
            std::cout << "get endpoint failed. path " << real_path << std::endl;
            return;
        }
        if (std::find(endpoint_vec.begin(), endpoint_vec.end(), endpoint) == endpoint_vec.end()) {
            endpoint_vec.push_back(endpoint);
        }
    }
    std::vector<std::string> row;
    row.push_back("endpoint");
    row.push_back("real_endpoint");
    row.push_back("role");
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    for (size_t i = 0; i < endpoint_vec.size(); i++) {
        std::vector<std::string> row;
        row.push_back(endpoint_vec[i]);
        std::string real_endpoint;
        if (!real_ep_map.empty()) {
            auto rit = real_ep_map.find(endpoint_vec[i]);
            if (rit != real_ep_map.end()) {
                real_endpoint = rit->second;
            }
        }
        if (real_endpoint.empty()) {
            row.push_back("-");
        } else {
            row.push_back(real_endpoint);
        }
        if (i == 0) {
            row.push_back("leader");
        } else {
            row.push_back("standby");
        }
        tp.AddRow(row);
    }
    tp.Print(true);
}

void HandleNSMakeSnapshot(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    try {
        uint32_t pid = boost::lexical_cast<uint32_t>(parts[2]);
        std::string msg;
        bool ok = client->MakeSnapshot(parts[1], pid, 0, msg);
        if (!ok) {
            std::cout << "Fail to makesnapshot. error msg:" << msg << std::endl;
            return;
        }
        std::cout << "MakeSnapshot ok" << std::endl;
    } catch (std::exception const& e) {
        std::cout << "Invalid args. pid should be uint32_t" << std::endl;
    }
}

void HandleNSAddReplica(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::set<uint32_t> pid_set;
    if (SplitPidGroup(parts[2], pid_set) < 0) {
        printf("pid group[%s] format error\n", parts[2].c_str());
        return;
    }
    if (pid_set.empty()) {
        std::cout << "has not valid pid" << std::endl;
        return;
    }
    std::string msg;
    bool ok = client->AddReplica(parts[1], pid_set, parts[3], msg);
    if (!ok) {
        std::cout << "Fail to addreplica. error msg:" << msg << std::endl;
        return;
    }
    std::cout << "AddReplica ok" << std::endl;
}

void HandleNSDelReplica(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::set<uint32_t> pid_set;
    if (SplitPidGroup(parts[2], pid_set) < 0) {
        printf("pid group[%s] format error\n", parts[2].c_str());
        return;
    }
    if (pid_set.empty()) {
        std::cout << "has not valid pid" << std::endl;
        return;
    }
    std::string msg;
    bool ok = client->DelReplica(parts[1], pid_set, parts[3], msg);
    if (!ok) {
        std::cout << "Fail to delreplica. error msg:" << msg << std::endl;
        return;
    }
    std::cout << "DelReplica ok" << std::endl;
}

void HandleNSClientDropTable(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    if (FLAGS_interactive) {
        printf("Drop table %s? yes/no\n", parts[1].c_str());
        std::string input;
        std::cin >> input;
        std::transform(input.begin(), input.end(), input.begin(), ::tolower);
        if (input != "yes") {
            printf("'drop %s' cmd is canceled!\n", parts[1].c_str());
            return;
        }
    }
    std::string msg;
    bool ret = client->DropTable(parts[1], msg);
    if (!ret) {
        std::cout << "failed to drop. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "drop ok" << std::endl;
}

void HandleNSClientSyncTable(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() != 3 && parts.size() != 4) {
        std::cout << "Bad format for synctable! eg. synctable table_name "
                     "cluster_alias [pid]"
                  << std::endl;
        return;
    }
    uint32_t pid = UINT32_MAX;
    try {
        if (parts.size() == 4) {
            pid = boost::lexical_cast<uint32_t>(parts[3]);
        }
        std::string msg;
        bool ret = client->SyncTable(parts[1], parts[2], pid, msg);
        if (!ret) {
            std::cout << "failed to synctable. error msg: " << msg << std::endl;
            return;
        }
        std::cout << "synctable ok" << std::endl;
    } catch (std::exception const& e) {
        std::cout << "Invalid args. pid should be uint32_t" << std::endl;
    }
}

void HandleNSClientSetSdkEndpoint(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() != 3) {
        std::cout << "Bad format for setsdkendpoint!"
                     "eg. setsdkendpoint server_name sdkendpoint"
                  << std::endl;
        return;
    }
    std::string msg;
    bool ret = client->SetSdkEndpoint(parts[1], parts[2], &msg);
    if (!ret) {
        std::cout << "setsdkendpoint failed. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "setsdkendpoint ok" << std::endl;
}

void HandleNSClientAddIndex(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad format for addindex! eg. addindex table_name "
                     "index_name [col_name] [ts_name]"
                  << std::endl;
        return;
    }
    ::openmldb::common::ColumnKey column_key;
    column_key.set_index_name(parts[2]);
    std::vector<openmldb::common::ColumnDesc> cols;
    if (parts.size() > 3) {
        std::vector<std::string> col_vec;
        ::openmldb::base::SplitString(parts[3], ",", col_vec);
        for (const auto& col_name : col_vec) {
            std::vector<std::string> type_pair;
            openmldb::base::SplitString(col_name, ":", type_pair);
            if (type_pair.size() > 1) {
                column_key.add_col_name(type_pair[0]);
                openmldb::common::ColumnDesc col_desc;
                col_desc.set_name(type_pair[0]);
                auto it = ::openmldb::codec::DATA_TYPE_MAP.find(type_pair[1]);
                if (it == ::openmldb::codec::DATA_TYPE_MAP.end()) {
                    std::cerr << col_name << " type " << type_pair[0] << " invalid\n";
                    return;
                }
                col_desc.set_data_type(it->second);
                cols.push_back(std::move(col_desc));
            } else {
                column_key.add_col_name(col_name);
            }
        }
    } else {
        column_key.add_col_name(parts[2]);
    }
    if (parts.size() > 4) {
        column_key.set_ts_name(parts[4]);
    }
    std::string msg;
    bool ret = false;
    if (cols.empty()) {
        ret = client->AddIndex(parts[1], column_key, nullptr, msg);
    } else {
        ret = client->AddIndex(parts[1], column_key, &cols, msg);
    }
    if (!ret) {
        std::cout << "failed to addindex. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "addindex ok" << std::endl;
}

void HandleNSClientConfSet(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::string msg;
    bool ret = client->ConfSet(parts[1], parts[2], msg);
    if (!ret) {
        printf("failed to set %s. error msg: %s\n", parts[1].c_str(), msg.c_str());
        return;
    }
    printf("set %s ok\n", parts[1].c_str());
}

void HandleNSClientConfGet(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 1) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::string msg;
    std::map<std::string, std::string> conf_map;
    std::string key;
    if (parts.size() > 1) {
        key = parts[1];
    }
    bool ret = client->ConfGet(key, conf_map, msg);
    if (!ret) {
        printf("failed to set %s. error msg: %s\n", parts[1].c_str(), msg.c_str());
        return;
    }
    std::vector<std::string> row;
    row.push_back("key");
    row.push_back("value");
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    for (const auto& kv : conf_map) {
        row.clear();
        row.push_back(kv.first);
        row.push_back(kv.second);
        tp.AddRow(row);
    }
    tp.Print(true);
}

void HandleNSClientChangeLeader(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    try {
        uint32_t pid = boost::lexical_cast<uint32_t>(parts[2]);
        std::string msg;
        std::string candidate_leader;
        if (parts.size() > 3) {
            candidate_leader = parts[3];
        }
        bool ret = client->ChangeLeader(parts[1], pid, candidate_leader, msg);
        if (!ret) {
            std::cout << "failed to change leader. error msg: " << msg << std::endl;
            return;
        }
    } catch (const std::exception& e) {
        std::cout << "Invalid args. pid should be uint32_t" << std::endl;
        return;
    }
    std::cout << "change leader ok" << std::endl;
}

void HandleNSClientOfflineEndpoint(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::string msg;
    uint32_t concurrency = 0;
    if (parts.size() > 2) {
        try {
            if (boost::lexical_cast<int32_t>(parts[2]) <= 0) {
                std::cout << "Invalid args. concurrency should be greater than 0" << std::endl;
                return;
            }
            concurrency = boost::lexical_cast<uint32_t>(parts[2]);
        } catch (const std::exception& e) {
            std::cout << "Invalid args. concurrency should be uint32_t" << std::endl;
            return;
        }
    }
    bool ret = client->OfflineEndpoint(parts[1], concurrency, msg);
    if (!ret) {
        std::cout << "failed to offline endpoint. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "offline endpoint ok" << std::endl;
}

void HandleNSClientMigrate(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 5) {
        std::cout << "Bad format. eg, migrate 127.0.0.1:9991 table1 1-10 "
                     "127.0.0.1:9992"
                  << std::endl;
        return;
    }
    if (parts[1] == parts[4]) {
        std::cout << "migrate error. src_endpoint is same as des_endpoint" << std::endl;
        return;
    }
    std::string msg;
    std::set<uint32_t> pid_set;
    if (SplitPidGroup(parts[3], pid_set) < 0) {
        printf("pid group[%s] format error\n", parts[3].c_str());
        return;
    }
    if (pid_set.empty()) {
        std::cout << "has not valid pid" << std::endl;
        return;
    }
    bool ret = client->Migrate(parts[1], parts[2], pid_set, parts[4], msg);
    if (!ret) {
        std::cout << "failed to migrate partition. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "partition migrate ok" << std::endl;
}

void HandleNSClientRecoverEndpoint(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    bool need_restore = false;
    if (parts.size() > 2) {
        std::string value = parts[2];
        std::transform(value.begin(), value.end(), value.begin(), ::tolower);
        if (value == "true") {
            need_restore = true;
        } else if (value == "false") {
            need_restore = false;
        } else {
            std::cout << "Invalid args. need_restore should be true or false" << std::endl;
            return;
        }
    }
    uint32_t concurrency = 0;
    if (parts.size() > 3) {
        try {
            if (boost::lexical_cast<int32_t>(parts[3]) <= 0) {
                std::cout << "Invalid args. concurrency should be greater than 0" << std::endl;
                return;
            }
            concurrency = boost::lexical_cast<uint32_t>(parts[3]);
        } catch (const std::exception& e) {
            std::cout << "Invalid args. concurrency should be uint32_t" << std::endl;
            return;
        }
    }
    std::string msg;
    bool ret = client->RecoverEndpoint(parts[1], need_restore, concurrency, msg);
    if (!ret) {
        std::cout << "failed to recover endpoint. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "recover endpoint ok" << std::endl;
}

void HandleNSClientRecoverTable(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    try {
        uint32_t pid = boost::lexical_cast<uint32_t>(parts[2]);
        std::string msg;
        bool ok = client->RecoverTable(parts[1], pid, parts[3], msg);
        if (!ok) {
            std::cout << "Fail to recover table. error msg:" << msg << std::endl;
            return;
        }
        std::cout << "recover table ok" << std::endl;
    } catch (std::exception const& e) {
        std::cout << "Invalid args. pid should be uint32_t" << std::endl;
    }
}

void HandleNSClientConnectZK(const std::vector<std::string> parts, ::openmldb::client::NsClient* client) {
    std::string msg;
    bool ok = client->ConnectZK(msg);
    if (ok) {
        std::cout << "connect zk ok" << std::endl;
    } else {
        std::cout << "Fail to connect zk" << std::endl;
    }
}

void HandleNSClientDisConnectZK(const std::vector<std::string> parts, ::openmldb::client::NsClient* client) {
    std::string msg;
    bool ok = client->DisConnectZK(msg);
    if (ok) {
        std::cout << "disconnect zk ok" << std::endl;
    } else {
        std::cout << "Fail to disconnect zk" << std::endl;
    }
}

void HandleNSClientShowTable(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    std::string name;
    if (parts.size() >= 2) {
        name = parts[1];
    }
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(name, tables, msg);
    if (!ret) {
        std::cout << "failed to showtable. error msg: " << msg << std::endl;
        return;
    }
    ::openmldb::cmd::PrintTableInfo(tables);
}

void HandleNSClientShowSchema(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "showschema format error. eg: showschema tablename" << std::endl;
        return;
    }
    std::string name = parts[1];
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(name, tables, msg);
    if (!ret) {
        std::cout << "failed to showschema. error msg: " << msg << std::endl;
        return;
    }
    if (tables.empty()) {
        printf("table %s is not exist\n", name.c_str());
        return;
    }

    ::openmldb::cmd::PrintSchema(tables[0].column_desc(), tables[0].added_column_desc());
    printf("\n#ColumnKey\n");
    ::openmldb::cmd::PrintColumnKey(tables[0].column_key());
}

void HandleNSDelete(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    std::vector<std::string> vec;
    ::openmldb::base::SplitString(parts[1], "=", vec);
    if (vec.size() < 2 || vec.at(0) != "table_name") {
        if (parts.size() < 3) {
            std::cout << "delete format error. eg: delete table_name key | delete"
                         "table_name key idx_name"
                      << std::endl;
            return;
        }
        std::vector<::openmldb::nameserver::TableInfo> tables;
        std::string msg;
        bool ret = client->ShowTable(parts[1], tables, msg);
        if (!ret) {
            std::cout << "failed to get table info. error msg: " << msg << std::endl;
            return;
        }
        if (tables.empty()) {
            printf("delete failed! table %s is not exist\n", parts[1].c_str());
            return;
        }
        uint32_t tid = tables[0].tid();
        std::string key = parts[2];
        uint32_t pid = (uint32_t)(::openmldb::base::hash64(key) % tables[0].table_partition_size());
        std::shared_ptr<::openmldb::client::TabletClient> tablet_client = GetTabletClient(tables[0], pid, msg);
        if (!tablet_client) {
            std::cout << "failed to delete. error msg: " << msg << std::endl;
            return;
        }
        std::string idx_name;
        if (parts.size() > 3) {
            for (int idx = 0; idx < tables[0].column_key_size(); idx++) {
                if (tables[0].column_key(idx).index_name() == parts[3]) {
                    idx_name = parts[3];
                    break;
                }
            }
            if (idx_name.empty()) {
                printf("idx_name %s is not exist\n", parts[3].c_str());
                return;
            }
        }
        msg.clear();
        if (tables[0].partition_key_size() == 0) {
            if (tablet_client->Delete(tid, pid, key, idx_name, msg)) {
                std::cout << "delete ok" << std::endl;
            } else {
                std::cout << "delete failed. error msg: " << msg << std::endl;
            }
        } else {
            int failed_cnt = 0;
            for (uint32_t cur_pid = 0; cur_pid < (uint32_t)tables[0].table_partition_size(); cur_pid++) {
                tablet_client = GetTabletClient(tables[0], cur_pid, msg);
                if (!tablet_client) {
                    std::cout << "failed to delete. error msg: " << msg << std::endl;
                    return;
                }
                if (!tablet_client->Delete(tid, cur_pid, key, idx_name, msg)) {
                    failed_cnt++;
                }
            }
            if (failed_cnt == tables[0].table_partition_size()) {
                std::cout << "delete failed" << std::endl;
            } else {
                std::cout << "delete ok" << std::endl;
            }
        }
    }
}

bool GetColumnMap(const std::vector<std::string>& parts,
                  std::map<std::string, std::string>& condition_columns_map,  // NOLINT
                  std::map<std::string, std::string>& value_columns_map) {    // NOLINT
    std::string delimiter = "=";
    bool is_condition_columns_map = false;
    std::vector<std::string> temp_vec;
    for (uint32_t i = 2; i < parts.size(); i++) {
        if (parts[i] == "where") {
            is_condition_columns_map = true;
            continue;
        }
        ::openmldb::base::SplitString(parts[i], delimiter, temp_vec);
        if (temp_vec.size() < 2 || temp_vec[1].empty()) {
            return false;
        }
        if (is_condition_columns_map) {
            condition_columns_map.insert(std::make_pair(temp_vec[0], temp_vec[1]));
        } else {
            value_columns_map.insert(std::make_pair(temp_vec[0], temp_vec[1]));
        }
    }
    return true;
}

void HandleNsUseDb(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    std::string msg;
    if (parts.size() == 1) {
        client->ClearDb();
        std::cout << "Use default database" << std::endl;
        return;
    }
    if (client->Use(parts[1], msg)) {
        std::cout << "Use database: " << parts[1] << std::endl;
    } else {
        std::cout << "Use database failed. error msg: " << msg << std::endl;
    }
}

void HandleNsCreateDb(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "createdb format error. eg: createdb database_name" << std::endl;
        return;
    }
    std::string msg;
    if (client->CreateDatabase(parts[1], msg)) {
        std::cout << "Create database " << parts[1] << " ok" << std::endl;
    } else {
        std::cout << "Create database failed. error msg: " << msg << std::endl;
    }
}

void HandleNsDropDb(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "dropdb format error. eg: dropdb database_name" << std::endl;
        return;
    }
    if (FLAGS_interactive) {
        printf("Dropdb will drop all tables in the database %s? yes/no\n", parts[1].c_str());
        std::string input;
        std::cin >> input;
        std::transform(input.begin(), input.end(), input.begin(), ::tolower);
        if (input != "yes") {
            printf("'dropdb %s' cmd is canceled!\n", parts[1].c_str());
            return;
        }
    }
    std::string msg;
    if (client->DropDatabase(parts[1], msg)) {
        std::cout << "Drop database " << parts[1] << " ok" << std::endl;
    } else {
        std::cout << "Drop database failed. error msg: " << msg << std::endl;
    }
}

bool ParseCondAndOp(const std::string& source, uint64_t& first_end,  // NOLINT
                    uint64_t& value_begin, int32_t& get_type) {      // NOLINT
    for (uint64_t i = 0; i < source.length(); i++) {
        switch (source[i]) {
            case '=':
                first_end = i;
                value_begin = i + 1;
                get_type = openmldb::api::kSubKeyEq;
                return true;
            case '<':
                first_end = i;
                if (source[i + 1] == '=') {
                    value_begin = i + 2;
                    get_type = openmldb::api::kSubKeyLe;
                } else {
                    value_begin = i + 1;
                    get_type = openmldb::api::kSubKeyLt;
                }
                return true;
            case '>':
                first_end = i;
                if (source[i + 1] == '=') {
                    value_begin = i + 2;
                    get_type = openmldb::api::kSubKeyGe;
                } else {
                    value_begin = i + 1;
                    get_type = openmldb::api::kSubKeyGt;
                }
                return true;
            default:
                continue;
        }
    }
    return false;
}

bool GetCondAndPrintColumns(const std::vector<std::string>& parts,
                            std::map<std::string, std::string>& condition_columns_map,  // NOLINT
                            std::vector<std::string>& print_column,                     // NOLINT
                            openmldb::api::GetType& get_type) {                         // NOLINT
    uint64_t size = parts.size();
    uint64_t i = 2;
    if (parts[i] == "*") {
        print_column.clear();
        i += 1;
    } else {
        for (; i < size; i++) {
            if (parts[i] == "where") {
                break;
            }
            print_column.push_back(parts[i]);
        }
    }
    if (i + 1 >= size) {
        std::cerr << "not found where condition" << std::endl;
        return false;
    }
    int32_t first_type = 0;
    bool first_parse = true;
    for (i++; i < size; i++) {
        int32_t col_type;
        uint64_t col_end = 0, value_begin = 0;
        bool ok = ParseCondAndOp(parts[i], col_end, value_begin, col_type);
        if (!ok) {
            std::cerr << "parse " << parts[i] << " error" << std::endl;
            return false;
        }
        if (first_parse) {
            first_type = col_type;
            first_parse = false;
        }
        if (col_type != first_type) {
            std::cerr << "all relational operator must same" << std::endl;
            return false;
        }
        std::string col = parts[i].substr(0, col_end);
        std::string val = parts[i].substr(value_begin);
        condition_columns_map.insert(std::make_pair(col, val));
    }
    get_type = static_cast<openmldb::api::GetType>(first_type);
    return true;
}

void HandleNSShowCatalogVersion(::openmldb::client::NsClient* client) {
    std::map<std::string, uint64_t> catalog_version;
    std::string error;
    client->ShowCatalogVersion(&catalog_version, &error);
    std::unique_ptr<baidu::common::TPrinter> tp(new baidu::common::TPrinter(3, FLAGS_max_col_display_length));
    std::vector<std::string> row;
    row.push_back("#");
    row.push_back("endpoint");
    row.push_back("version");
    tp->AddRow(row);
    row.clear();
    int row_num = 0;
    for (const auto& kv : catalog_version) {
        row_num++;
        row.push_back(std::to_string(row_num));
        row.push_back(kv.first);
        row.push_back(std::to_string(kv.second));
        tp->AddRow(row);
        row.clear();
    }
    tp->Print(true);
}

void HandleNSShowDB(::openmldb::client::NsClient* client) {
    std::vector<std::string> dbs;
    std::string error;
    client->ShowDatabase(&dbs, error);
    auto tp = new baidu::common::TPrinter(2, FLAGS_max_col_display_length);
    std::vector<std::string> row;
    row.push_back("#");
    row.push_back("name");
    tp->AddRow(row);
    row.clear();
    for (uint64_t i = 0; i < dbs.size(); i++) {
        row.push_back(std::to_string(i + 1));
        row.push_back(dbs[i]);
        tp->AddRow(row);
        row.clear();
    }
    tp->Print(true);
    delete tp;
}

void HandleNSGet(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 4) {
        std::cout << "get format error. eg: get table_name key ts | get "
                     "table_name key idx_name ts | get table_name=xxx key=xxx "
                     "index_name=xxx ts=xxx ts_name=xxx "
                  << std::endl;
        return;
    }
    std::map<std::string, std::string> parameter_map;
    if (!GetParameterMap("table_name", parts, "=", parameter_map)) {
        std::cout << "get format error. eg: get table_name=xxx key=xxx "
                     "index_name=xxx ts=xxx ts_name=xxx "
                  << std::endl;
        return;
    }
    bool is_pair_format = parameter_map.empty() ? false : true;
    std::string table_name;
    std::string key;
    std::string index_name;
    uint64_t timestamp = 0;
    std::string ts_name;
    auto iter = parameter_map.begin();
    try {
        if (is_pair_format) {
            iter = parameter_map.find("table_name");
            if (iter != parameter_map.end()) {
                table_name = iter->second;
            } else {
                std::cout << "get format error: table_name does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("key");
            if (iter != parameter_map.end()) {
                key = iter->second;
            } else {
                std::cout << "get format error: key does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("index_name");
            if (iter != parameter_map.end()) {
                index_name = iter->second;
            }
            iter = parameter_map.find("ts");
            if (iter != parameter_map.end()) {
                timestamp = boost::lexical_cast<uint64_t>(iter->second);
            } else {
                std::cout << "get format error: ts does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("ts_name");
            if (iter != parameter_map.end()) {
                ts_name = iter->second;
            }
        } else {
            table_name = parts[1];
            key = parts[2];
            if (parts.size() == 5) {
                index_name = parts[3];
            }
            timestamp = boost::lexical_cast<uint64_t>(parts[parts.size() - 1]);
        }
    } catch (std::exception const& e) {
        printf("Invalid args. ts should be unsigned int\n");
        return;
    }
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(table_name, tables, msg);
    if (!ret) {
        std::cout << "failed to get table info. error msg: " << msg << std::endl;
        return;
    }
    if (tables.empty()) {
        printf("get failed! table %s is not exist\n", parts[1].c_str());
        return;
    }
    uint32_t tid = tables[0].tid();
    uint32_t pid = (uint32_t)(::openmldb::base::hash64(key) % tables[0].table_partition_size());
    std::shared_ptr<TabletClient> tb_client = GetTabletClient(tables[0], pid, msg);
    if (!tb_client) {
        std::cout << "failed to get. error msg: " << msg << std::endl;
        return;
    }
    ::openmldb::codec::SDKCodec codec(tables[0]);
    bool no_schema = tables[0].column_desc_size() == 0 && tables[0].column_desc_size() == 0;
    if (no_schema) {
        std::string value;
        uint64_t ts = 0;
        try {
            std::string msg;
            bool ok = tb_client->Get(tid, pid, key, timestamp, value, ts, msg);
            if (ok) {
                if (tables[0].compress_type() == ::openmldb::type::CompressType::kSnappy) {
                    std::string uncompressed;
                    ::snappy::Uncompress(value.c_str(), value.length(), &uncompressed);
                    value = uncompressed;
                }
                std::cout << "value :" << value << std::endl;
            } else {
                std::cout << "Get failed. error msg: " << msg << std::endl;
            }
        } catch (std::exception const& e) {
            printf("Invalid args. ts should be unsigned int\n");
            return;
        }
    } else {
        std::vector<std::string> row = codec.GetColNames();
        row.insert(row.begin(), "ts");
        row.insert(row.begin(), "#");
        uint64_t max_size = row.size();
        ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
        tp.AddRow(row);
        std::string value;
        uint64_t ts = 0;
        std::string msg;
        if (tables[0].partition_key_size() == 0) {
            if (!tb_client->Get(tid, pid, key, timestamp, index_name, ts_name, value, ts, msg)) {
                std::cout << "Fail to get value! error msg: " << msg << std::endl;
                return;
            }
        } else {
            int failed_cnt = 0;
            for (uint32_t cur_pid = 0; cur_pid < (size_t)tables[0].table_partition_size(); cur_pid++) {
                uint64_t cur_ts = 0;
                std::string cur_value;
                tb_client = GetTabletClient(tables[0], cur_pid, msg);
                if (!tb_client) {
                    std::cout << "failed to get. error msg: " << msg << std::endl;
                    return;
                }
                if (!tb_client->Get(tid, cur_pid, key, timestamp, index_name, ts_name, cur_value, cur_ts, msg)) {
                    failed_cnt++;
                    continue;
                }
                if (cur_ts > ts) {
                    value.swap(cur_value);
                }
            }
            if (failed_cnt == tables[0].table_partition_size()) {
                std::cout << "Fail to get value! error msg: " << msg << std::endl;
                return;
            }
        }
        if (tables[0].compress_type() == ::openmldb::type::CompressType::kSnappy) {
            std::string uncompressed;
            ::snappy::Uncompress(value.c_str(), value.length(), &uncompressed);
            value.swap(uncompressed);
        }
        row.clear();
        codec.DecodeRow(value, &row);
        ::openmldb::cmd::TransferString(&row);
        row.insert(row.begin(), std::to_string(ts));
        row.insert(row.begin(), "1");
        uint64_t row_size = row.size();
        for (uint64_t i = 0; i < max_size - row_size; i++) {
            row.push_back("");
        }
        tp.AddRow(row);
        tp.Print(true);
    }
}

void HandleNSScan(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 5) {
        std::cout << "scan format error. eg: scan table_name pk start_time end_time "
                     "[limit] | scan table_name key key_name start_time end_time "
                     "[limit] | scan table_name=xxx key=xxx index_name=xxx st=xxx "
                     "et=xxx ts_name=xxx [limit=xxx] [atleast=xxx]"
                  << std::endl;
        return;
    }
    std::map<std::string, std::string> parameter_map;
    if (!GetParameterMap("table_name", parts, "=", parameter_map)) {
        std::cout << "scan table_name=xxx key=xxx index_name=xxx st=xxx et=xxx "
                     "ts_name=xxx [limit=xxx] [atleast=xxx]"
                  << std::endl;
        return;
    }
    bool is_pair_format = parameter_map.empty() ? false : true;
    std::string table_name;
    std::string key;
    std::string index_name;
    uint64_t st = 0;
    uint64_t et = 0;
    std::string ts_name;
    uint32_t limit = 0;
    uint32_t atleast = 0;
    auto iter = parameter_map.begin();
    try {
        if (is_pair_format) {
            iter = parameter_map.find("table_name");
            if (iter != parameter_map.end()) {
                table_name = iter->second;
            } else {
                std::cout << "scan format error: table_name does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("key");
            if (iter != parameter_map.end()) {
                key = iter->second;
            } else {
                std::cout << "scan format error: key does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("index_name");
            if (iter != parameter_map.end()) {
                index_name = iter->second;
            }
            iter = parameter_map.find("st");
            if (iter != parameter_map.end()) {
                st = boost::lexical_cast<uint64_t>(iter->second);
            } else {
                std::cout << "scan format error: st does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("et");
            if (iter != parameter_map.end()) {
                et = boost::lexical_cast<uint64_t>(iter->second);
            } else {
                std::cout << "scan format error: et does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("ts_name");
            if (iter != parameter_map.end()) {
                ts_name = iter->second;
            }
            iter = parameter_map.find("limit");
            if (iter != parameter_map.end()) {
                limit = boost::lexical_cast<uint32_t>(iter->second);
            }
            iter = parameter_map.find("atleast");
            if (iter != parameter_map.end()) {
                atleast = boost::lexical_cast<uint32_t>(iter->second);
            }
        } else {
            table_name = parts[1];
            key = parts[2];
        }
    } catch (std::exception const& e) {
        printf(
            "Invalid args. st and et should be uint64_t, limit and atleast "
            "should be uint32_t\n");
        return;
    }

    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(table_name, tables, msg);
    if (!ret) {
        std::cout << "failed to get table info. error msg: " << msg << std::endl;
        return;
    }
    if (tables.empty()) {
        printf("scan failed! table %s is not exist\n", parts[1].c_str());
        return;
    }
    uint32_t tid = tables[0].tid();
    uint32_t pid = (uint32_t)(::openmldb::base::hash64(key) % tables[0].table_partition_size());
    std::shared_ptr<TabletClient> tb_client = GetTabletClient(tables[0], pid, msg);
    if (!tb_client) {
        std::cout << "failed to scan. error msg: " << msg << std::endl;
        return;
    }
    bool no_schema = tables[0].column_desc_size() == 0 && tables[0].column_desc_size() == 0;
    if (no_schema) {
        std::shared_ptr<::openmldb::base::KvIterator> it;
        if (is_pair_format) {
            it.reset(tb_client->Scan(tid, pid, key, st, et, limit, atleast, msg));
        } else {
            try {
                st = boost::lexical_cast<uint64_t>(parts[3]);
                et = boost::lexical_cast<uint64_t>(parts[4]);
                if (parts.size() > 5) {
                    limit = boost::lexical_cast<uint32_t>(parts[5]);
                }
                it.reset(tb_client->Scan(tid, pid, key, st, et, limit, atleast, msg));
            } catch (std::exception const& e) {
                std::cout << "Invalid args. st and et should be uint64_t, limit should"
                          << "be uint32_t" << std::endl;
                return;
            }
        }
        if (!it) {
            std::cout << "Fail to scan table. error msg: " << msg << std::endl;
            return;
        } else {
            std::vector<std::shared_ptr<::openmldb::base::KvIterator>> iter_vec;
            iter_vec.push_back(std::move(it));
            ::openmldb::cmd::SDKIterator sdk_it(iter_vec, limit);
            ::openmldb::cmd::ShowTableRows(key, &sdk_it, tables[0].compress_type());
        }
    } else {
        if (parts.size() < 6) {
            std::cout << "scan format error. eg: scan table_name key col_name "
                         "start_time end_time [limit]"
                      << std::endl;
            return;
        }
        if (!is_pair_format) {
            index_name = parts[3];
            try {
                st = boost::lexical_cast<uint64_t>(parts[4]);
                et = boost::lexical_cast<uint64_t>(parts[5]);
                if (parts.size() > 6) {
                    limit = boost::lexical_cast<uint32_t>(parts[6]);
                }
            } catch (std::exception const& e) {
                printf(
                    "Invalid args. st and et should be uint64_t, limit should "
                    "be uint32_t\n");
                return;
            }
        }
        std::vector<std::shared_ptr<::openmldb::base::KvIterator>> iter_vec;
        if (tables[0].partition_key_size() > 0) {
            for (uint32_t cur_pid = 0; cur_pid < (uint32_t)tables[0].table_partition_size(); cur_pid++) {
                tb_client = GetTabletClient(tables[0], cur_pid, msg);
                if (!tb_client) {
                    std::cout << "failed to scan. error msg: " << msg << std::endl;
                    return;
                }
                std::shared_ptr<::openmldb::base::KvIterator> it(
                    tb_client->Scan(tid, cur_pid, key, st, et, index_name, ts_name, limit, atleast, msg));
                if (!it) {
                    std::cout << "Fail to scan table. error msg: " << msg << std::endl;
                    return;
                }
                iter_vec.push_back(std::move(it));
            }
        } else {
            std::shared_ptr<::openmldb::base::KvIterator> it(
                tb_client->Scan(tid, pid, key, st, et, index_name, ts_name, limit, atleast, msg));
            if (!it) {
                std::cout << "Fail to scan table. error msg: " << msg << std::endl;
                return;
            }
            iter_vec.push_back(std::move(it));
        }
        ::openmldb::cmd::SDKIterator sdk_it(iter_vec, limit);
        ::openmldb::cmd::ShowTableRows(tables[0], &sdk_it);
    }
}

void HandleNSCount(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "count format error | count table_name key [col_name] "
                     "[filter_expired_data] | count table_name=xxx key=xxx "
                     "index_name=xxx ts_name [filter_expired_data]"
                  << std::endl;
        return;
    }
    std::map<std::string, std::string> parameter_map;
    if (!GetParameterMap("table_name", parts, "=", parameter_map)) {
        std::cout << "count format erro! eg. count tid=xxx pid=xxx key=xxx "
                     "index_name=xxx ts=xxx ts_name=xxx [filter_expired_data]"
                  << std::endl;
        return;
    }
    bool is_pair_format = parameter_map.empty() ? false : true;
    std::string table_name;
    std::string key;
    std::string index_name;
    std::string ts_name;
    bool filter_expired_data = false;
    auto iter = parameter_map.begin();
    if (is_pair_format) {
        iter = parameter_map.find("table_name");
        if (iter != parameter_map.end()) {
            table_name = iter->second;
        } else {
            std::cout << "count format error: table_name does not exist!" << std::endl;
            return;
        }
        iter = parameter_map.find("key");
        if (iter != parameter_map.end()) {
            key = iter->second;
        } else {
            std::cout << "count format error: key does not exist!" << std::endl;
            return;
        }
        iter = parameter_map.find("index_name");
        if (iter != parameter_map.end()) {
            index_name = iter->second;
        }
        iter = parameter_map.find("ts_name");
        if (iter != parameter_map.end()) {
            ts_name = iter->second;
        }
        iter = parameter_map.find("filter_expired_data");
        if (iter != parameter_map.end()) {
            std::string temp_str = iter->second;
            if (temp_str == "true") {
                filter_expired_data = true;
            } else if (temp_str == "false") {
                filter_expired_data = false;
            } else {
                printf("filter_expired_data parameter should be true or false\n");
                return;
            }
        }
    } else {
        table_name = parts[1];
        key = parts[2];
        if (parts.size() == 4) {
            if (parts[3] == "true") {
                filter_expired_data = true;
            } else if (parts[3] == "false") {
                filter_expired_data = false;
            } else {
                index_name = parts[3];
            }
        } else if (parts.size() == 5) {
            index_name = parts[3];
            if (parts[4] == "true") {
                filter_expired_data = true;
            } else if (parts[4] == "false") {
                filter_expired_data = false;
            } else {
                printf("filter_expired_data parameter should be true or false\n");
                return;
            }
        } else if (parts.size() != 3) {
            std::cout << "count format error" << std::endl;
            return;
        }
    }
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(table_name, tables, msg);
    if (!ret) {
        std::cout << "failed to get table info. error msg: " << msg << std::endl;
        return;
    }
    if (tables.empty()) {
        printf("get failed! table %s is not exist\n", parts[1].c_str());
        return;
    }
    uint32_t tid = tables[0].tid();
    uint32_t pid = (uint32_t)(::openmldb::base::hash64(key) % tables[0].table_partition_size());
    std::shared_ptr<::openmldb::client::TabletClient> tablet_client = GetTabletClient(tables[0], pid, msg);
    if (!tablet_client) {
        std::cout << "failed to count. cannot not found tablet client, pid is " << pid << std::endl;
        return;
    }
    uint64_t value = 0;
    if (tables[0].partition_key_size() == 0) {
        if (!tablet_client->Count(tid, pid, key, index_name, ts_name, filter_expired_data, value, msg)) {
            std::cout << "Count failed. error msg: " << msg << std::endl;
            return;
        }
    } else {
        for (uint32_t cur_pid = 0; cur_pid < (uint32_t)tables[0].table_partition_size(); cur_pid++) {
            uint64_t cur_value = 0;
            tablet_client = GetTabletClient(tables[0], cur_pid, msg);
            if (!tablet_client) {
                std::cout << "failed to count. cannot not found tablet client, "
                             "pid is "
                          << cur_pid << std::endl;
                return;
            }
            if (!tablet_client->Count(tid, cur_pid, key, index_name, ts_name, filter_expired_data, cur_value, msg)) {
                std::cout << "Count failed. error msg: " << msg << std::endl;
            }
            value += cur_value;
        }
    }
    std::cout << "count: " << value << std::endl;
}

void HandleNSPreview(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "preview format error. eg: preview table_name [limit]" << std::endl;
        return;
    }
    uint32_t limit = FLAGS_preview_default_limit;
    if (parts.size() > 2) {
        try {
            int64_t tmp = boost::lexical_cast<int64_t>(parts[2]);
            if (tmp < 0) {
                printf("preview error. limit should be unsigned int\n");
                return;
            }
            limit = boost::lexical_cast<uint32_t>(parts[2]);
        } catch (std::exception const& e) {
            printf("preview error. limit should be unsigned int\n");
            return;
        }
        if (limit > FLAGS_preview_limit_max_num) {
            printf("preview error. limit is greater than the max num %u\n", FLAGS_preview_limit_max_num);
            return;
        } else if (limit == 0) {
            printf("preview error. limit must be greater than zero\n");
            return;
        }
    }
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(parts[1], tables, msg);
    if (!ret) {
        std::cout << "failed to get table info. error msg: " << msg << std::endl;
        return;
    }
    if (tables.empty()) {
        printf("preview failed! table %s is not exist\n", parts[1].c_str());
        return;
    }
    uint32_t tid = tables[0].tid();
    ::openmldb::codec::SDKCodec codec(tables[0]);
    bool has_ts_col = codec.HasTSCol();
    bool no_schema = tables[0].column_desc_size() == 0 && tables[0].column_desc_size() == 0;
    std::vector<std::string> row;
    uint64_t max_size = 0;
    if (no_schema) {
        row.push_back("#");
        row.push_back("key");
        row.push_back("ts");
        row.push_back("data");
    } else {
        row = codec.GetColNames();
        if (!has_ts_col) {
            row.insert(row.begin(), "ts");
        }
        row.insert(row.begin(), "#");
        max_size = row.size();
    }
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    uint32_t index = 1;
    for (uint32_t pid = 0; pid < (uint32_t)tables[0].table_partition_size(); pid++) {
        if (limit == 0) {
            break;
        }
        std::shared_ptr<TabletClient> tb_client = GetTabletClient(tables[0], pid, msg);
        if (!tb_client) {
            std::cout << "failed to preview. error msg: " << msg << std::endl;
            return;
        }
        uint32_t count = 0;
        ::openmldb::base::KvIterator* it = tb_client->Traverse(tid, pid, "", "", 0, limit, count);
        if (it == NULL) {
            std::cout << "Fail to preview table" << std::endl;
            return;
        }
        limit -= count;
        while (it->Valid()) {
            row.clear();
            row.push_back(std::to_string(index));

            if (no_schema) {
                std::string value = it->GetValue().ToString();
                if (tables[0].compress_type() == ::openmldb::type::CompressType::kSnappy) {
                    std::string uncompressed;
                    ::snappy::Uncompress(value.c_str(), value.length(), &uncompressed);
                    value = uncompressed;
                }
                row.push_back(it->GetPK());
                row.push_back(std::to_string(it->GetKey()));
                row.push_back(value);
            } else {
                if (!has_ts_col) {
                    row.push_back(std::to_string(it->GetKey()));
                }
                std::string value;
                if (tables[0].compress_type() == ::openmldb::type::CompressType::kSnappy) {
                    ::snappy::Uncompress(it->GetValue().data(), it->GetValue().size(), &value);
                } else {
                    value.assign(it->GetValue().data(), it->GetValue().size());
                }
                codec.DecodeRow(value, &row);
                ::openmldb::cmd::TransferString(&row);
                uint64_t row_size = row.size();
                for (uint64_t i = 0; i < max_size - row_size; i++) {
                    row.push_back("");
                }
            }
            tp.AddRow(row);
            index++;
            it->Next();
        }
        delete it;
    }
    tp.Print(true);
}

void HandleNSAddTableField(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() != 4) {
        std::cout << "addtablefield format error. eg: addtablefield tablename "
                     "column_name column_type"
                  << std::endl;
        return;
    }
    auto iter = ::openmldb::codec::DATA_TYPE_MAP.find(parts[3]);
    if (iter == ::openmldb::codec::DATA_TYPE_MAP.end()) {
        printf("type %s is invalid\n", parts[3].c_str());
        return;
    }
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(parts[1], tables, msg);
    if (!ret) {
        std::cout << "failed to get table info. error msg: " << msg << std::endl;
        return;
    }
    if (tables.empty()) {
        printf("add table field failed! table %s doesn`t exist\n", parts[1].c_str());
        return;
    }
    ::openmldb::common::ColumnDesc column_desc;
    column_desc.set_name(parts[2]);
    column_desc.set_data_type(iter->second);
    if (!client->AddTableField(parts[1], column_desc, msg)) {
        std::cout << "Fail to add table field. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "add table field ok" << std::endl;
}

void HandleNSInfo(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "info format error. eg: info tablename" << std::endl;
        return;
    }
    std::string name = parts[1];
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(name, tables, msg);
    if (!ret) {
        std::cout << "failed to get table info. error msg: " << msg << std::endl;
        return;
    }
    ::openmldb::cmd::PrintTableInformation(tables);
}

void HandleNSAddReplicaCluster(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 4) {
        std::cout << "addrepcluster format error. eg: addrepcluster "
                     "zk_endpoints zk_path alias_name"
                  << std::endl;
        return;
    }
    std::string zk_endpoints, zk_path, alias, msg;
    zk_endpoints = parts[1];
    zk_path = parts[2];
    alias = parts[3];
    if (!client->AddReplicaCluster(zk_endpoints, zk_path, alias, msg)) {
        std::cout << "addrepcluster failed. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "adrepcluster ok" << std::endl;
}

void HandleShowReplicaCluster(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    std::vector<std::string> row = {"zk_endpoints", "zk_path", "alias", "state", "age"};
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);

    std::vector<::openmldb::nameserver::ClusterAddAge> cluster_info;
    std::string msg;
    bool ok = client->ShowReplicaCluster(cluster_info, msg);
    if (!ok) {
        std::cout << "Fail to show replica. error msg: " << msg << std::endl;
        return;
    }
    for (auto i : cluster_info) {
        std::vector<std::string> row;
        row.push_back(i.replica().zk_endpoints());
        row.push_back(i.replica().zk_path());
        row.push_back(i.replica().alias());
        row.push_back(i.state());
        row.push_back(::openmldb::base::HumanReadableTime(i.age()));
        tp.AddRow(row);
    }
    tp.Print(true);
}

bool HasIsTsCol(const google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey>& list) {
    if (list.empty()) {
        return false;
    }
    for (auto it = list.begin(); it != list.end(); it++) {
        if (it->has_ttl()) {
            return true;
        }
    }
    return false;
}

void HandleNSPut(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "put format error. eg: put table_name pk ts value | put table_name [ts] field1 field2 ...\n";
        return;
    }
    std::map<std::string, std::string> parameter_map;
    if (!GetParameterMap("table_name", parts, "=", parameter_map)) {
        std::cout << "put format error. eg: put table_name=xxx col1=xxx col2=xxx col3=xxx ...\n";
        return;
    }
    bool is_pair_format = parameter_map.empty() ? false : true;
    std::string table_name = "";
    if (is_pair_format) {
        auto iter = parameter_map.find("table_name");
        if (iter != parameter_map.end()) {
            table_name = iter->second;
        } else {
            std::cout << "get format error: table_name does not exist!\n";
            return;
        }
    } else {
        table_name = parts[1];
    }
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(table_name, tables, msg);
    if (!ret) {
        std::cout << "failed to get table info. error msg: " << msg << std::endl;
        return;
    }
    if (tables.empty()) {
        std::cout << "put failed! table " << parts[1] << " is not exist\n";
        return;
    }
    uint64_t ts = 0;
    uint32_t start_index = 0;
    if (!HasIsTsCol(tables[0].column_key())) {
        try {
            ts = boost::lexical_cast<uint64_t>(parts[2]);
        } catch (std::exception const& e) {
            std::cout << "Invalid args. ts " << parts[2] << " should be unsigned int\n";
            return;
        }
        start_index = 3;
    } else {
        start_index = 2;
    }
    int base_size = tables[0].column_desc().size();
    int add_size = tables[0].added_column_desc().size();
    int in_size = parts.size();
    int modify_index = in_size - start_index - base_size;
    if (modify_index - add_size > 0 || modify_index < 0) {
        printf("put format error! input value does not match the schema\n");
        return;
    }
    std::vector<std::string> input_value(parts.begin() + start_index, parts.end());
    auto result = PutSchemaData(tables[0], ts, input_value);
    if (result.code < 0) {
        std::cout << result.msg << "\n";
    }
}

int SetTablePartition(const ::openmldb::client::TableInfo& table_info,
                      ::openmldb::nameserver::TableInfo& ns_table_info) {  // NOLINT
    if (table_info.table_partition_size() > 0) {
        std::map<uint32_t, std::string> leader_map;
        std::map<uint32_t, std::set<std::string>> follower_map;
        for (int idx = 0; idx < table_info.table_partition_size(); idx++) {
            std::string pid_group = table_info.table_partition(idx).pid_group();
            uint32_t start_index = 0;
            uint32_t end_index = 0;
            if (::openmldb::base::IsNumber(pid_group)) {
                start_index = boost::lexical_cast<uint32_t>(pid_group);
                end_index = start_index;
            } else {
                std::vector<std::string> vec;
                boost::split(vec, pid_group, boost::is_any_of("-"));
                if (vec.size() != 2 || !::openmldb::base::IsNumber(vec[0]) || !::openmldb::base::IsNumber(vec[1])) {
                    printf("Fail to create table. pid_group[%s] format error.\n", pid_group.c_str());
                    return -1;
                }
                start_index = boost::lexical_cast<uint32_t>(vec[0]);
                end_index = boost::lexical_cast<uint32_t>(vec[1]);
            }
            for (uint32_t pid = start_index; pid <= end_index; pid++) {
                if (table_info.table_partition(idx).is_leader()) {
                    if (leader_map.find(pid) != leader_map.end()) {
                        printf("Fail to create table. pid %u has two leader\n", pid);
                        return -1;
                    }
                    leader_map.insert(std::make_pair(pid, table_info.table_partition(idx).endpoint()));
                } else {
                    if (follower_map.find(pid) == follower_map.end()) {
                        follower_map.insert(std::make_pair(pid, std::set<std::string>()));
                    }
                    if (follower_map[pid].find(table_info.table_partition(idx).endpoint()) != follower_map[pid].end()) {
                        printf(
                            "Fail to create table. pid %u has same follower on "
                            "%s\n",
                            pid, table_info.table_partition(idx).endpoint().c_str());
                        return -1;
                    }
                    follower_map[pid].insert(table_info.table_partition(idx).endpoint());
                }
            }
        }
        if (leader_map.empty()) {
            printf("Fail to create table. has not leader pid\n");
            return -1;
        }
        // check leader pid
        auto iter = leader_map.rbegin();
        if (iter->first != leader_map.size() - 1) {
            printf(
                "Fail to create table. pid is not start with zero and "
                "consecutive\n");
            return -1;
        }

        // check follower's leader
        for (const auto& kv : follower_map) {
            auto iter = leader_map.find(kv.first);
            if (iter == leader_map.end()) {
                printf("pid %u has not leader\n", kv.first);
                return -1;
            }
            if (kv.second.find(iter->second) != kv.second.end()) {
                printf("pid %u leader and follower at same endpoint %s\n", kv.first, iter->second.c_str());
                return -1;
            }
        }

        for (const auto& kv : leader_map) {
            ::openmldb::nameserver::TablePartition* table_partition = ns_table_info.add_table_partition();
            table_partition->set_pid(kv.first);
            ::openmldb::nameserver::PartitionMeta* partition_meta = table_partition->add_partition_meta();
            partition_meta->set_endpoint(kv.second);
            partition_meta->set_is_leader(true);
            auto iter = follower_map.find(kv.first);
            if (iter == follower_map.end()) {
                continue;
            }
            // add follower
            for (const auto& endpoint : iter->second) {
                ::openmldb::nameserver::PartitionMeta* partition_meta = table_partition->add_partition_meta();
                partition_meta->set_endpoint(endpoint);
                partition_meta->set_is_leader(false);
            }
        }
        ns_table_info.set_partition_num(ns_table_info.table_partition_size());
        ns_table_info.set_replica_num(ns_table_info.table_partition().Get(0).partition_meta_size());
    } else {
        if (table_info.has_partition_num()) {
            ns_table_info.set_partition_num(table_info.partition_num());
        }
        if (table_info.has_replica_num()) {
            ns_table_info.set_replica_num(table_info.replica_num());
        }
    }
    return 0;
}

int SetColumnDesc(const ::openmldb::client::TableInfo& table_info,
                  ::openmldb::nameserver::TableInfo& ns_table_info) {  // NOLINT
    std::map<std::string, ::openmldb::type::DataType> name_map;
    std::set<std::string> index_set;
    std::set<std::string> ts_col_set;
    for (int idx = 0; idx < table_info.column_desc_size(); idx++) {
        if (table_info.column_desc(idx).name() == "" ||
            name_map.find(table_info.column_desc(idx).name()) != name_map.end()) {
            printf("check column_desc name failed. name is %s\n", table_info.column_desc(idx).name().c_str());
            return -1;
        }
        name_map.insert(std::make_pair(table_info.column_desc(idx).name(), table_info.column_desc(idx).data_type()));
        ::openmldb::common::ColumnDesc* column_desc = ns_table_info.add_column_desc();
        column_desc->CopyFrom(table_info.column_desc(idx));
    }

    // set column_key
    index_set.clear();
    std::set<std::string> key_set;
    for (int idx = 0; idx < table_info.column_key_size(); idx++) {
        if (!table_info.column_key(idx).has_index_name() || table_info.column_key(idx).index_name().size() == 0) {
            printf("not set index_name in column_key\n");
            return -1;
        }
        if (index_set.find(table_info.column_key(idx).index_name()) != index_set.end()) {
            printf("duplicate index_name %s\n", table_info.column_key(idx).index_name().c_str());
            return -1;
        }
        index_set.insert(table_info.column_key(idx).index_name());
        std::string cur_key;
        if (table_info.column_key(idx).col_name_size() > 0) {
            for (const auto& name : table_info.column_key(idx).col_name()) {
                auto iter = name_map.find(name);
                if (iter == name_map.end()) {
                    printf("column :%s is not member of columns\n", name.c_str());
                    return -1;
                }
                if (iter->second == ::openmldb::type::kFloat || iter->second == ::openmldb::type::kDouble) {
                    printf("float or double column can not be index\n");
                    return -1;
                }
                if (cur_key.empty()) {
                    cur_key = name;
                } else {
                    cur_key += "|" + name;
                }
            }
        } else {
            cur_key = table_info.column_key(idx).index_name();
        }
        if (key_set.find(cur_key) != key_set.end()) {
            printf("duplicate column_key\n");
            return -1;
        }
        key_set.insert(cur_key);
        if (!table_info.column_key(idx).ts_name().empty()) {
            const auto& ts_name = table_info.column_key(idx).ts_name();
            auto iter = name_map.find(ts_name);
            if (iter == name_map.end()) {
                printf("invalid ts_name %s\n", ts_name.c_str());
                return -1;
            }
            if (iter->second != ::openmldb::type::kBigInt && iter->second != ::openmldb::type::kTimestamp) {
                printf("invalid ts type ts_name %s\n", ts_name.c_str());
                return -1;
            }
        }
        ::openmldb::common::ColumnKey* column_key = ns_table_info.add_column_key();
        column_key->CopyFrom(table_info.column_key(idx));
    }

    if (index_set.empty()) {
        std::cout << "no index" << std::endl;
        return -1;
    }
    return 0;
}

int GenTableInfo(const std::string& path,
                 ::openmldb::nameserver::TableInfo& ns_table_info) {  // NOLINT
    ::openmldb::client::TableInfo table_info;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cout << "can not open file " << path << std::endl;
        return -1;
    }
    google::protobuf::io::FileInputStream fileInput(fd);
    fileInput.SetCloseOnDelete(true);
    if (!google::protobuf::TextFormat::Parse(&fileInput, &table_info)) {
        std::cout << "table meta file format error" << std::endl;
        return -1;
    }

    ns_table_info.set_name(table_info.name());
    std::string compress_type = table_info.compress_type();
    std::transform(compress_type.begin(), compress_type.end(), compress_type.begin(), ::tolower);
    if (compress_type == "knocompress" || compress_type == "nocompress" || compress_type == "no") {
        ns_table_info.set_compress_type(::openmldb::type::CompressType::kNoCompress);
    } else if (compress_type == "ksnappy" || compress_type == "snappy") {
        ns_table_info.set_compress_type(::openmldb::type::CompressType::kSnappy);
    } else {
        printf("compress type %s is invalid\n", table_info.compress_type().c_str());
        return -1;
    }
    if (table_info.has_key_entry_max_height()) {
        if (table_info.key_entry_max_height() > FLAGS_skiplist_max_height) {
            printf(
                "Fail to create table. key_entry_max_height %u is greater than "
                "the max heght %u\n",
                table_info.key_entry_max_height(), FLAGS_skiplist_max_height);
            return -1;
        }
        if (table_info.key_entry_max_height() == 0) {
            printf(
                "Fail to create table. key_entry_max_height must be greater "
                "than 0\n");
            return -1;
        }
        ns_table_info.set_key_entry_max_height(table_info.key_entry_max_height());
    }
    ns_table_info.set_seg_cnt(table_info.seg_cnt());
    ns_table_info.set_format_version(table_info.format_version());
    if (SetTablePartition(table_info, ns_table_info) < 0) {
        return -1;
    }
    if (SetColumnDesc(table_info, ns_table_info) < 0) {
        return -1;
    }

    for (int idx = 0; idx < table_info.partition_key_size(); idx++) {
        ns_table_info.add_partition_key(table_info.partition_key(idx));
    }
    return 0;
}

void HandleNSCreateTable(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    ::openmldb::nameserver::TableInfo ns_table_info;
    if (parts.size() == 2) {
        if (GenTableInfo(parts[1], ns_table_info) < 0) {
            return;
        }
    } else if (parts.size() > 4) {
        ns_table_info.set_name(parts[1]);
        ::openmldb::common::TTLSt ttl_desc;
        try {
            std::vector<std::string> vec;
            ::openmldb::base::SplitString(parts[2], ":", vec);
            if (vec.size() == 2) {
                if ((vec[0] == "latest" || vec[0] == "kLatestTime")) {
                    ttl_desc.set_ttl_type(::openmldb::type::TTLType::kLatestTime);
                    ttl_desc.set_lat_ttl(boost::lexical_cast<uint64_t>(vec[vec.size() - 1]));
                    ttl_desc.set_abs_ttl(0);
                } else if ((vec[0] == "absolute" || vec[0] == "kAbsoluteTime")) {
                    ttl_desc.set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
                    ttl_desc.set_lat_ttl(0);
                    ttl_desc.set_abs_ttl(boost::lexical_cast<uint64_t>(vec[vec.size() - 1]));
                } else {
                    std::cout << "invalid ttl type" << std::endl;
                    return;
                }
            } else if (vec.size() == 3) {
                if ((vec[0] == "absandlat" || vec[0] == "kAbsAndLat")) {
                    ttl_desc.set_ttl_type(::openmldb::type::TTLType::kAbsAndLat);
                    ttl_desc.set_abs_ttl(boost::lexical_cast<uint64_t>(vec[vec.size() - 2]));
                    ttl_desc.set_lat_ttl(boost::lexical_cast<uint64_t>(vec[vec.size() - 1]));
                } else if ((vec[0] == "absorlat" || vec[0] == "kAbsOrLat")) {
                    ttl_desc.set_ttl_type(::openmldb::type::TTLType::kAbsOrLat);
                    ttl_desc.set_abs_ttl(boost::lexical_cast<uint64_t>(vec[vec.size() - 2]));
                    ttl_desc.set_lat_ttl(boost::lexical_cast<uint64_t>(vec[vec.size() - 1]));
                } else {
                    std::cout << "invalid ttl type" << std::endl;
                    return;
                }
            } else {
                ttl_desc.set_ttl_type(::openmldb::type::TTLType::kAbsoluteTime);
                ttl_desc.set_lat_ttl(0);
                ttl_desc.set_abs_ttl(boost::lexical_cast<uint64_t>(vec[vec.size() - 1]));
            }
            uint32_t partition_num = boost::lexical_cast<uint32_t>(parts[3]);
            if (partition_num == 0) {
                std::cout << "partition_num should be large than zero" << std::endl;
                return;
            }
            ns_table_info.set_partition_num(partition_num);
            uint32_t replica_num = boost::lexical_cast<uint32_t>(parts[4]);
            if (replica_num == 0) {
                std::cout << "replica_num should be large than zero" << std::endl;
                return;
            }
            ns_table_info.set_replica_num(replica_num);
        } catch (std::exception const& e) {
            std::cout << "Invalid args. pid should be uint32_t" << std::endl;
            return;
        }
        std::set<std::string> name_set;
        for (uint32_t i = 5; i < parts.size(); i++) {
            std::vector<std::string> kv;
            ::openmldb::base::SplitString(parts[i], ":", kv);
            if (kv.size() < 2) {
                std::cout << "create failed! schema format is illegal" << std::endl;
                return;
            }
            if (name_set.find(kv[0]) != name_set.end()) {
                printf("Duplicated column %s\n", kv[0].c_str());
                return;
            }
            std::string cur_type = kv[1];
            std::transform(cur_type.begin(), cur_type.end(), cur_type.begin(), ::tolower);
            auto type_iter = ::openmldb::codec::DATA_TYPE_MAP.find(cur_type);
            if (type_iter == ::openmldb::codec::DATA_TYPE_MAP.end()) {
                printf("type %s is invalid\n", kv[1].c_str());
                return;
            }
            name_set.insert(kv[0]);
            ::openmldb::common::ColumnDesc* column_desc = ns_table_info.add_column_desc();
            column_desc->set_name(kv[0]);
            column_desc->set_data_type(type_iter->second);

            if (kv.size() > 2 && kv[2] == "index") {
                if ((cur_type == "float") || (cur_type == "double")) {
                    printf("float or double column can not be index\n");
                    return;
                }
                ::openmldb::common::ColumnKey* column_key = ns_table_info.add_column_key();
                column_key->set_index_name(kv[0]);
                column_key->add_col_name(kv[0]);
                ::openmldb::common::TTLSt* ttl = column_key->mutable_ttl();
                ttl->CopyFrom(ttl_desc);
            }
        }
        if (parts.size() > 5 && ns_table_info.column_key_size() == 0) {
            std::cout << "create failed! schema has no index" << std::endl;
            return;
        }
    } else {
        std::cout << "create format error! ex: create table_meta_file | create "
                     "name ttl partition_num replica_num [name:type:index ...]"
                  << std::endl;
        return;
    }
    ns_table_info.set_db(client->GetDb());
    std::string msg;
    if (!client->CreateTable(ns_table_info, msg)) {
        std::cout << "Fail to create table. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "Create table ok" << std::endl;
}

void HandleNSClientHelp(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() == 1) {
        printf("addindex - add index to table \n");
        printf("addtablefield - add field to the schema table \n");
        printf("addreplica - add replica to leader\n");
        printf("cancelop - cancel the op\n");
        printf("create - create table\n");
        printf("confset - update conf\n");
        printf("confget - get conf\n");
        printf("count - count the num of data in specified key\n");
        printf(
            "changeleader - select leader again when the endpoint of leader "
            "offline\n");
        printf("delete - delete pk\n");
        printf("delreplica - delete replica from leader\n");
        printf("drop - drop table\n");
        printf("exit - exit client\n");
        printf("get - get only one record\n");
        printf("gettablepartition - get partition info\n");
        printf("help - get cmd info\n");
        printf("makesnapshot - make snapshot\n");
        printf("migrate - migrate partition form one endpoint to another\n");
        printf("man - get cmd info\n");
        printf(
            "offlineendpoint - select leader and delete replica when endpoint "
            "offline\n");
        printf("preview - preview data\n");
        printf("put -  insert data into table\n");
        printf("quit - exit client\n");
        printf("recovertable - recover only one table partition\n");
        printf("recoverendpoint - recover all tables in endpoint when online\n");
        printf("scan - get records for a period of time\n");
        printf("showtable - show table info\n");
        printf("showtablet - show tablet info\n");
        printf("showsdkendpoint - show sdkendpoint info\n");
        printf("showns - show nameserver info\n");
        printf("showdb - show all databases\n");
        printf("showschema - show schema info\n");
        printf("showopstatus - show op info\n");
        printf("settablepartition - update partition info\n");
        printf("setttl - set table ttl\n");
        printf("updatetablealive - update table alive status\n");
        printf("info - show information of the table\n");
        printf("addrepcluster - add remote replica cluster\n");
        printf("showrepcluster - show remote replica cluster\n");
        printf("removerepcluster - remove remote replica cluste \n");
        printf("switchmode - switch cluster mode\n");
        printf("synctable - synctable from leader cluster to replica cluster\n");
        printf("deleteindx - delete index of specified table\n");
        printf("setsdkendpoint - set sdkendpoint for external network sdk\n");
        printf("showcatalogversion - show catalog version\n");
    } else if (parts.size() == 2) {
        if (parts[1] == "create") {
            printf("desc: create table\n");
            printf("usage: create table_meta_file_path\n");
            printf(
                "usage: create table_name ttl partition_num replica_num "
                "[colum_name1:type:index colum_name2:type ...]\n");
            printf("ex: create ./table_meta.txt\n");
            printf("ex: create table1 144000 8 3\n");
            printf("ex: create table2 latest:10 8 3\n");
            printf(
                "ex: create table3 latest:10 8 3 card:string:index "
                "mcc:string:index value:float\n");
        } else if (parts[1] == "drop") {
            printf("desc: drop table\n");
            printf("usage: drop table_name\n");
            printf("ex: drop table1\n");
        } else if (parts[1] == "put") {
            printf("desc: insert data into table\n");
            printf("usage: put table_name pk ts value\n");
            printf("usage: put table_name ts key1 key2 ... value1 value2 ...\n");
            printf("ex: put table1 key1 1528872944000 value1\n");
            printf("ex: put table2 1528872944000 card0 mcc0 1.3\n");
        } else if (parts[1] == "scan") {
            printf("desc: get records for a period of time\n");
            printf("usage: scan table_name pk start_time end_time [limit]\n");
            printf(
                "usage: scan table_name key key_name start_time end_time "
                "[limit]\n");
            printf("ex: scan table1 key1 1528872944000 1528872930000\n");
            printf("ex: scan table1 key1 1528872944000 1528872930000 10\n");
            printf("ex: scan table1 key1 0 0 10\n");
            printf("ex: scan table2 card0 card 1528872944000 1528872930000\n");
            printf("ex: scan table2 card0 card 1528872944000 1528872930000 10\n");
            printf("ex: scan table2 card0 card  0 0 10\n");
        } else if (parts[1] == "get") {
            printf("desc: get only one record\n");
            printf("usage: get table_name key ts\n");
            printf("usage: get table_name key idx_name ts\n");
            printf("ex: get table1 key1 1528872944000\n");
            printf("ex: get table1 key1 0\n");
            printf("ex: get table2 card0 card 1528872944000\n");
            printf("ex: get table2 card0 card 0\n");
        } else if (parts[1] == "delete") {
            printf("desc: delete pk\n");
            printf("usage: delete table_name key idx_name\n");
            printf("ex: delete table1 key1\n");
            printf("ex: delete table2 card0 card\n");
        } else if (parts[1] == "count") {
            printf("desc: count the num of data in specified key\n");
            printf("usage: count table_name key [filter_expired_data]\n");
            printf("usage: count table_name key idx_name [filter_expired_data]\n");
            printf("ex: count table1 key1\n");
            printf("ex: count table1 key1 true\n");
            printf("ex: count table2 card0 card\n");
            printf("ex: count table2 card0 card true\n");
        } else if (parts[1] == "preview") {
            printf("desc: preview data in table\n");
            printf("usage: preview table_name [limit]\n");
            printf("ex: preview table1\n");
            printf("ex: preview table1 10\n");
        } else if (parts[1] == "showtable") {
            printf("desc: show table info\n");
            printf("usage: showtable [table_name]\n");
            printf("ex: showtable\n");
            printf("ex: showtable table1\n");
        } else if (parts[1] == "showtablet") {
            printf("desc: show tablet info\n");
            printf("usage: showtablet\n");
            printf("ex: showtablet\n");
        } else if (parts[1] == "showsdkendpoint") {
            printf("desc: show sdkendpoint info\n");
            printf("usage: showsdkendpoint\n");
            printf("ex: showsdkendpoint\n");
        } else if (parts[1] == "showns") {
            printf("desc: show nameserver info\n");
            printf("usage: showns\n");
            printf("ex: showns\n");
        } else if (parts[1] == "showschema") {
            printf("desc: show schema info\n");
            printf("usage: showschema table_name\n");
            printf("ex: showschema table1\n");
        } else if (parts[1] == "showopstatus") {
            printf("desc: show op info\n");
            printf("usage: showopstatus [table_name pid]\n");
            printf("ex: showopstatus\n");
            printf("ex: showopstatus table1\n");
            printf("ex: showopstatus table1 0\n");
        } else if (parts[1] == "makesnapshot") {
            printf("desc: make snapshot\n");
            printf("usage: makesnapshot name pid\n");
            printf("ex: makesnapshot table1 0\n");
        } else if (parts[1] == "addreplica") {
            printf("desc: add replica to leader\n");
            printf("usage: addreplica name pid_group endpoint\n");
            printf("ex: addreplica table1 0 172.27.128.31:9527\n");
            printf("ex: addreplica table1 0,3,5 172.27.128.31:9527\n");
            printf("ex: addreplica table1 1-5 172.27.128.31:9527\n");
        } else if (parts[1] == "delreplica") {
            printf("desc: delete replica from leader\n\n");
            printf("usage: delreplica name pid_group endpoint\n");
            printf("ex: delreplica table1 0 172.27.128.31:9527\n");
            printf("ex: delreplica table1 0,3,5 172.27.128.31:9527\n");
            printf("ex: delreplica table1 1-5 172.27.128.31:9527\n");
        } else if (parts[1] == "confset") {
            printf("desc: update conf\n");
            printf("usage: confset auto_failover true/false\n");
            printf("ex: confset auto_failover true\n");
        } else if (parts[1] == "confget") {
            printf("desc: get conf\n");
            printf("usage: confget\n");
            printf("usage: confget conf_name\n");
            printf("ex: confget\n");
            printf("ex: confget auto_failover\n");
        } else if (parts[1] == "changeleader") {
            printf(
                "desc: select leader again when the endpoint of leader "
                "offline\n");
            printf("usage: changeleader table_name pid [candidate_leader]\n");
            printf("ex: changeleader table1 0\n");
            printf("ex: changeleader table1 0 auto\n");
            printf("ex: changeleader table1 0 172.27.128.31:9527\n");
        } else if (parts[1] == "offlineendpoint") {
            printf(
                "desc: select leader and delete replica when endpoint "
                "offline\n");
            printf("usage: offlineendpoint endpoint [concurrency]\n");
            printf("ex: offlineendpoint 172.27.128.31:9527\n");
            printf("ex: offlineendpoint 172.27.128.31:9527 2\n");
        } else if (parts[1] == "recovertable") {
            printf("desc: recover only one table partition\n");
            printf("usage: recovertable table_name pid endpoint\n");
            printf("ex: recovertable table1 0 172.27.128.31:9527\n");
        } else if (parts[1] == "recoverendpoint") {
            printf("desc: recover all tables in endpoint when online\n");
            printf(
                "usage: recoverendpoint endpoint [need_restore] "
                "[concurrency]\n");
            printf("ex: recoverendpoint 172.27.128.31:9527\n");
            printf("ex: recoverendpoint 172.27.128.31:9527 false\n");
            printf("ex: recoverendpoint 172.27.128.31:9527 true 2\n");
        } else if (parts[1] == "migrate") {
            printf("desc: migrate partition form one endpoint to another\n");
            printf(
                "usage: migrate src_endpoint table_name pid_group "
                "des_endpoint\n");
            printf("ex: migrate 172.27.2.52:9991 table1 1 172.27.2.52:9992\n");
            printf("ex: migrate 172.27.2.52:9991 table1 1,3,5 172.27.2.52:9992\n");
            printf("ex: migrate 172.27.2.52:9991 table1 1-5 172.27.2.52:9992\n");
        } else if (parts[1] == "gettablepartition") {
            printf("desc: get partition info\n");
            printf("usage: gettablepartition table_name pid\n");
            printf("ex: gettablepartition table1 0\n");
        } else if (parts[1] == "settablepartition") {
            printf("desc: set partition info\n");
            printf("usage: settablepartition table_name partition_file_path\n");
            printf("ex: settablepartition table1 ./partition_file.txt\n");
        } else if (parts[1] == "showdb") {
            printf("desc: show all databases\n");
        } else if (parts[1] == "exit" || parts[1] == "quit") {
            printf("desc: exit client\n");
            printf("ex: quit\n");
            printf("ex: exit\n");
        } else if (parts[1] == "help" || parts[1] == "man") {
            printf("desc: get cmd info\n");
            printf("usage: help [cmd]\n");
            printf("usage: man [cmd]\n");
            printf("ex:help\n");
            printf("ex:help create\n");
            printf("ex:man\n");
            printf("ex:man create\n");
        } else if (parts[1] == "setttl") {
            printf("desc: set table ttl \n");
            printf("usage: setttl table_name ttl_type ttl [ts_name]\n");
            printf("ex: setttl t1 absolute 10\n");
            printf("ex: setttl t2 latest 5\n");
            printf("ex: setttl t3 latest 5 ts1\n");
        } else if (parts[1] == "cancelop") {
            printf("desc: cancel the op\n");
            printf("usage: cancelop op_id\n");
            printf("ex: cancelop 5\n");
        } else if (parts[1] == "updatetablealive") {
            printf("desc: update table alive status\n");
            printf("usage: updatetablealive table_name pid endppoint is_alive\n");
            printf("ex: updatetablealive t1 * 172.27.2.52:9991 no\n");
            printf("ex: updatetablealive t1 0 172.27.2.52:9991 no\n");
        } else if (parts[1] == "addtablefield") {
            printf("desc: add table field (max adding field count is 63)\n");
            printf("usage: addtablefield table_name col_name col_type\n");
            printf("ex: addtablefield test card string\n");
            printf("ex: addtablefield test money float\n");
        } else if (parts[1] == "info") {
            printf("desc: show information of the table\n");
            printf("usage: info table_name \n");
            printf("ex: info test\n");
        } else if (parts[1] == "addrepcluster") {
            printf("desc: add remote replica cluster\n");
            printf("usage: addrepcluster zk_endpoints zk_path cluster_alias\n");
            printf(
                "ex: addrepcluster 10.1.1.1:2181,10.1.1.2:2181 /openmldb_cluster "
                "prod_dc01\n");
        } else if (parts[1] == "showrepcluster") {
            printf("desc: show remote replica cluster\n");
            printf("usage: showrepcluster\n");
            printf("ex: showrepcluster\n");
        } else if (parts[1] == "removerepcluster") {
            printf("desc: remove remote replica cluster\n");
            printf("usage: removerepcluster cluster_alias\n");
            printf("ex: removerepcluster prod_dc01\n");
        } else if (parts[1] == "switchmode") {
            printf("desc: switch cluster mode\n");
            printf("usage: switchmode normal|leader\n");
            printf("ex: switchmode normal\n");
            printf("ex: switchmode leader\n");
        } else if (parts[1] == "synctable") {
            printf("desc: synctable from leader cluster to replica cluster\n");
            printf("usage: synctable table_name cluster_alias [pid]\n");
            printf("ex: synctable test bj\n");
            printf("ex: synctable test bj 0\n");
        } else if (parts[1] == "setsdkendpoint") {
            printf("desc: set sdkendpoint for external network sdk\n");
            printf("usage: setsdkendpoint server_name sdkendpoint\n");
            printf("eg: setsdkendpoint tb1 202.12.18.1:9527\n");
            printf("eg: setsdkendpoint tb1 null\n");
        } else if (parts[1] == "addindex") {
            printf("desc: add new index to table\n");
            printf("usage: addindex table_name index_name [col_name] [ts_name]\n");
            printf("ex: addindex test card\n");
            printf("ex: addindex test combine1 card,mcc\n");
            printf("ex: addindex test combine2 id,name ts1,ts2\n");
            printf("ex: addindex test combine3 id:string,name:int32 ts1,ts2\n");
        } else if (parts[1] == "deleteindex") {
            printf("desc: delete index of specified index\n");
            printf("usage: deleteindex table_name index_name");
            printf("usage: deleteindex test index0");
        } else if (parts[1] == "createdb") {
            printf("desc: create database\n");
            printf("usage: createdb database_name\n");
            printf("eg: createdb db1");
        } else if (parts[1] == "use") {
            printf("desc: use database\n");
            printf("usage: use database_name\n");
            printf("eg: use db1");
        } else if (parts[1] == "dropdb") {
            printf("desc: drop database\n");
            printf("usage: dropdb database_name\n");
            printf("eg: dropdb db1");
        } else {
            printf("unsupport cmd %s\n", parts[1].c_str());
        }
    } else {
        printf("help format error!\n");
        printf("usage: help [cmd]\n");
        printf("usage: man [cmd]\n");
        printf("ex: help\n");
        printf("ex: help create\n");
        printf("ex:man\n");
        printf("ex:man create\n");
    }
}

void HandleNSClientSetTablePartition(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::string name = parts[1];
    int fd = open(parts[2].c_str(), O_RDONLY);
    if (fd < 0) {
        std::cout << "can not open file " << parts[2] << std::endl;
        return;
    }
    ::openmldb::nameserver::TablePartition table_partition;
    google::protobuf::io::FileInputStream fileInput(fd);
    fileInput.SetCloseOnDelete(true);
    if (!google::protobuf::TextFormat::Parse(&fileInput, &table_partition)) {
        std::cout << "table partition file format error" << std::endl;
        return;
    }
    std::set<std::string> leader_set;
    std::set<std::string> follower_set;
    for (int idx = 0; idx < table_partition.partition_meta_size(); idx++) {
        std::string endpoint = table_partition.partition_meta(idx).endpoint();
        if (table_partition.partition_meta(idx).is_leader()) {
            if (leader_set.find(endpoint) != leader_set.end()) {
                std::cout << "has same leader " << endpoint << std::endl;
                return;
            }
            leader_set.insert(endpoint);
        } else {
            if (follower_set.find(endpoint) != follower_set.end()) {
                std::cout << "has same follower" << endpoint << std::endl;
                return;
            }
            follower_set.insert(endpoint);
        }
    }
    if (leader_set.empty()) {
        std::cout << "has no leader" << std::endl;
        return;
    }

    std::string msg;
    if (!client->SetTablePartition(name, table_partition, msg)) {
        std::cout << "Fail to set table partition. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "set table partition ok" << std::endl;
}

void HandleNsClientSQL(const std::string sql, ::openmldb::client::NsClient* client) {
    std::string msg;
    if (!client->ExecuteSQL(sql, msg)) {
        std::cout << "execute sql failed, sql:" << sql << " , msg: " << msg << std::endl;
    }
}

void HandleNSClientGetTablePartition(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::string name = parts[1];
    uint32_t pid = 0;
    try {
        pid = boost::lexical_cast<uint32_t>(parts[2]);
    } catch (std::exception const& e) {
        std::cout << "Invalid args. pid should be uint32_t" << std::endl;
        return;
    }
    ::openmldb::nameserver::TablePartition table_partition;
    std::string msg;
    if (!client->GetTablePartition(name, pid, table_partition, msg)) {
        std::cout << "Fail to get table partition. error msg: " << msg << std::endl;
        return;
    }
    std::string value;
    google::protobuf::TextFormat::PrintToString(table_partition, &value);
    std::string file_name = name + "_" + parts[2] + ".txt";
    FILE* fd_write = fopen(file_name.c_str(), "w");
    if (fd_write == NULL) {
        PDLOG(WARNING, "fail to open file %s", file_name.c_str());
        std::cout << "fail to open file" << file_name << std::endl;
        return;
    }
    bool io_error = false;
    if (fputs(value.c_str(), fd_write) == EOF) {
        std::cout << "write error" << std::endl;
        io_error = true;
    }
    if (!io_error && ((fflush(fd_write) == EOF) || fsync(fileno(fd_write)) == -1)) {
        std::cout << "flush error" << std::endl;
        io_error = true;
    }
    fclose(fd_write);
    if (!io_error) {
        std::cout << "get table partition ok" << std::endl;
    }
}

void HandleNSClientUpdateTableAlive(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    if (parts.size() < 5) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::string name = parts[1];
    std::string endpoint = parts[3];
    bool is_alive = false;
    if (parts[4] == "yes") {
        is_alive = true;
    } else if (parts[4] == "no") {
        is_alive = false;
    } else {
        std::cout << "is_alive should be yes or no" << std::endl;
        return;
    }
    uint32_t pid = UINT32_MAX;
    if (parts[2] != "*") {
        try {
            int pid_tmp = boost::lexical_cast<int32_t>(parts[2]);
            if (pid_tmp < 0) {
                std::cout << "Invalid args. pid should be uint32_t" << std::endl;
                return;
            }
            pid = pid_tmp;
        } catch (std::exception const& e) {
            std::cout << "Invalid args. pid should be uint32_t" << std::endl;
            return;
        }
    }
    std::string msg;
    if (!client->UpdateTableAliveStatus(endpoint, name, pid, is_alive, msg)) {
        std::cout << "Fail to update table alive. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "update ok" << std::endl;
}

void HandleNSShowOPStatus(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    std::vector<std::string> row;
    row.push_back("op_id");
    row.push_back("op_type");
    row.push_back("name");
    row.push_back("pid");
    row.push_back("status");
    row.push_back("start_time");
    row.push_back("execute_time");
    row.push_back("end_time");
    row.push_back("cur_task");
    row.push_back("for_replica_cluster");
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    ::openmldb::nameserver::ShowOPStatusResponse response;
    std::string msg;
    std::string name;
    uint32_t pid = ::openmldb::client::INVALID_PID;
    if (parts.size() > 1) {
        name = parts[1];
    }
    if (parts.size() > 2) {
        try {
            pid = boost::lexical_cast<uint32_t>(parts[2]);
        } catch (std::exception const& e) {
            std::cout << "Invalid args pid should be uint32_t" << std::endl;
            return;
        }
    }
    bool ok = client->ShowOPStatus(response, name, pid, msg);
    if (!ok) {
        std::cout << "Fail to show tablets. error msg: " << msg << std::endl;
        return;
    }
    for (int idx = 0; idx < response.op_status_size(); idx++) {
        std::vector<std::string> row;
        row.push_back(std::to_string(response.op_status(idx).op_id()));
        row.push_back(response.op_status(idx).op_type());
        if (response.op_status(idx).has_name()) {
            row.push_back(response.op_status(idx).name());
        } else {
            row.push_back("-");
        }
        if (response.op_status(idx).has_pid() && (response.op_status(idx).pid() != ::openmldb::client::INVALID_PID)) {
            row.push_back(std::to_string(response.op_status(idx).pid()));
        } else {
            row.push_back("-");
        }
        row.push_back(response.op_status(idx).status());
        if (response.op_status(idx).start_time() > 0) {
            time_t rawtime = (time_t)response.op_status(idx).start_time();
            tm* timeinfo = localtime(&rawtime);  // NOLINT
            char buf[20];
            strftime(buf, 20, "%Y%m%d%H%M%S", timeinfo);
            row.push_back(buf);
            if (response.op_status(idx).end_time() != 0) {
                row.push_back(
                    std::to_string(response.op_status(idx).end_time() - response.op_status(idx).start_time()) + "s");
                rawtime = (time_t)response.op_status(idx).end_time();
                timeinfo = localtime(&rawtime);  // NOLINT
                buf[0] = '\0';
                strftime(buf, 20, "%Y%m%d%H%M%S", timeinfo);
                row.push_back(buf);
            } else {
                uint64_t cur_time = ::baidu::common::timer::now_time();
                row.push_back(std::to_string(cur_time - response.op_status(idx).start_time()) + "s");
                row.push_back("-");
            }
        } else {
            row.push_back("-");
            row.push_back("-");
            row.push_back("-");
        }
        row.push_back(response.op_status(idx).task_type());
        if (response.op_status(idx).for_replica_cluster() == 1) {
            row.push_back("yes");
        } else {
            row.push_back("no");
        }
        tp.AddRow(row);
    }
    tp.Print(true);
}

void HandleNSClientDeleteIndex(const std::vector<std::string>& parts, ::openmldb::client::NsClient* client) {
    ::openmldb::nameserver::GeneralResponse response;
    if (parts.size() != 3) {
        std::cout << "Bad format" << std::endl;
        std::cout << "usage: deleteindex table_name index_name" << std::endl;
        return;
    }
    std::string msg;
    if (!client->DeleteIndex(parts[1], parts[2], msg)) {
        std::cout << "Fail to delete index. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "delete index ok" << std::endl;
}

void HandleClientDeleteIndex(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    ::openmldb::nameserver::GeneralResponse response;
    if (parts.size() < 4) {
        std::cout << "Bad format" << std::endl;
        std::cout << "usage: deleteindex tid pid index_name" << std::endl;
        return;
    }
    try {
        std::string msg;
        if (!client->DeleteIndex(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]),
                                 parts[3], &msg)) {
            std::cout << "Fail to delete index. error msg: " << msg << std::endl;
            return;
        }
    } catch (std::exception const& e) {
        std::cout << "Invalid args tid and pid should be uint32_t" << std::endl;
    }
    std::cout << "delete index ok" << std::endl;
}

void HandleClientSetTTL(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 5) {
        std::cout << "Bad setttl format, eg setttl tid pid type ttl [index_name]" << std::endl;
        return;
    }
    std::string index_name;
    try {
        uint64_t abs_ttl = 0;
        uint64_t lat_ttl = 0;
        ::openmldb::type::TTLType type = ::openmldb::type::kLatestTime;
        if (parts[3] == "absolute") {
            type = ::openmldb::type::TTLType::kAbsoluteTime;
            abs_ttl = boost::lexical_cast<uint64_t>(parts[4]);
            if (parts.size() == 6) {
                index_name = parts[5];
            } else if (parts.size() > 6) {
                std::cout << "Bad setttl format, eg setttl tid pid type ttl [index_name]" << std::endl;
                return;
            }
        } else if (parts[3] == "absandlat") {
            type = ::openmldb::type::TTLType::kAbsAndLat;
            abs_ttl = boost::lexical_cast<uint64_t>(parts[4]);
            lat_ttl = boost::lexical_cast<uint64_t>(parts[5]);
            if (parts.size() == 7) {
                index_name = parts[6];
            }
        } else if (parts[3] == "absorlat") {
            type = ::openmldb::type::TTLType::kAbsOrLat;
            abs_ttl = boost::lexical_cast<uint64_t>(parts[4]);
            lat_ttl = boost::lexical_cast<uint64_t>(parts[5]);
            if (parts.size() == 7) {
                index_name = parts[6];
            }
        } else if (parts[3] == "latest") {
            lat_ttl = boost::lexical_cast<uint64_t>(parts[4]);
            if (parts.size() == 6) {
                index_name = parts[5];
            } else if (parts.size() > 6) {
                std::cout << "Bad setttl format, eg setttl tid pid type ttl [index_name]" << std::endl;
                return;
            }
        }
        bool ok = client->UpdateTTL(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]),
                                    type, abs_ttl, lat_ttl, index_name);
        if (ok) {
            std::cout << "Set ttl ok !" << std::endl;
        } else {
            std::cout << "Set ttl failed! " << std::endl;
        }
    } catch (std::exception const& e) {
        std::cout << "Invalid args tid and pid should be uint32_t" << std::endl;
    }
}

void HandleClientGet(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 5) {
        std::cout << "Bad get format, eg get tid pid key time" << std::endl;
        return;
    }
    try {
        std::string value;
        uint64_t ts = 0;
        std::string msg;
        bool ok = client->Get(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]),
                              parts[3], boost::lexical_cast<uint64_t>(parts[4]), value, ts, msg);
        ::openmldb::api::TableStatus table_status;
        if (!client->GetTableStatus(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]),
                                    table_status)) {
            std::cout << "Fail to get table status" << std::endl;
            return;
        }
        ::openmldb::type::CompressType compress_type = ::openmldb::type::CompressType::kNoCompress;
        if (table_status.compress_type() == ::openmldb::type::CompressType::kSnappy) {
            compress_type = ::openmldb::type::CompressType::kSnappy;
        }
        if (compress_type == ::openmldb::type::CompressType::kSnappy) {
            std::string uncompressed;
            ::snappy::Uncompress(value.c_str(), value.length(), &uncompressed);
            value = uncompressed;
        }
        if (ok) {
            std::cout << "value :" << value << std::endl;
        } else {
            std::cout << "Get failed! error msg: " << msg << std::endl;
        }
    } catch (std::exception const& e) {
        std::cout << "Invalid args tid and pid should be uint32_t, ts should "
                     "be uint64_t"
                  << std::endl;
    }
}

void HandleClientBenGet(std::vector<std::string>& parts,  // NOLINT
                        ::openmldb::client::TabletClient* client) {
    try {
        uint32_t tid = boost::lexical_cast<uint32_t>(parts[1]);
        uint32_t pid = boost::lexical_cast<uint32_t>(parts[2]);
        uint64_t key_num = 1000000;
        if (parts.size() >= 4) {
            key_num = ::boost::lexical_cast<uint64_t>(parts[3]);
        }
        uint32_t times = 10000;
        if (parts.size() >= 5) {
            times = ::boost::lexical_cast<uint32_t>(parts[4]);
        }
        int num = 100;
        if (parts.size() >= 6) {
            num = ::boost::lexical_cast<int>(parts[5]);
        }
        std::string value;
        uint64_t base = 100000000;
        std::random_device rd;
        std::default_random_engine engine(rd());
        std::uniform_int_distribution<> dis(1, key_num);
        std::string msg;
        while (num > 0) {
            for (uint32_t i = 0; i < times; i++) {
                std::string key = std::to_string(base + dis(engine));
                uint64_t ts = 0;
                client->Get(tid, pid, key, 0, value, ts, msg);
            }
            client->ShowTp();
            num--;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "put argument error!" << std::endl;
    }
}

// the input format like put 1 1 key time value
void HandleClientPut(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 6) {
        std::cout << "Bad put format, eg put tid pid key time value" << std::endl;
        return;
    }
    try {
        bool ok = client->Put(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]),
                              parts[3], boost::lexical_cast<uint64_t>(parts[4]), parts[5]);
        if (ok) {
            std::cout << "Put ok" << std::endl;
        } else {
            std::cout << "Put failed" << std::endl;
        }
    } catch (std::exception const& e) {
        std::cout << "Invalid args tid and pid should be uint32_t" << std::endl;
    }
}

void HandleClientBenPut(std::vector<std::string>& parts,  // NOLINT
                        ::openmldb::client::TabletClient* client) {
    try {
        uint32_t tid = boost::lexical_cast<uint32_t>(parts[1]);
        uint32_t pid = boost::lexical_cast<uint32_t>(parts[2]);
        uint64_t key_num = 1000000;
        if (parts.size() >= 4) {
            key_num = ::boost::lexical_cast<uint64_t>(parts[3]);
        }
        uint32_t times = 10000;
        if (parts.size() >= 5) {
            times = ::boost::lexical_cast<uint32_t>(parts[4]);
        }
        int num = 100;
        if (parts.size() >= 6) {
            num = ::boost::lexical_cast<int>(parts[5]);
        }
        std::string value(128, 'a');
        uint64_t base = 100000000;
        std::random_device rd;
        std::default_random_engine engine(rd());
        std::uniform_int_distribution<> dis(1, key_num);
        while (num > 0) {
            for (uint32_t i = 0; i < times; i++) {
                std::string key = std::to_string(base + dis(engine));
                uint64_t ts = ::baidu::common::timer::get_micros() / 1000;
                client->Put(tid, pid, key, ts, value);
            }
            client->ShowTp();
            num--;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "put argument error!" << std::endl;
    }
}

// the input format like create name tid pid ttl leader endpoints
void HandleClientCreateTable(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 6) {
        std::cout << "Bad create format, input like create <name> <tid> <pid> "
                     "<ttl> <seg_cnt>"
                  << std::endl;
        return;
    }

    try {
        int64_t abs_ttl = 0;
        int64_t lat_ttl = 0;
        ::openmldb::type::TTLType type = ::openmldb::type::TTLType::kAbsoluteTime;
        if (parts.size() > 4) {
            std::vector<std::string> vec;
            ::openmldb::base::SplitString(parts[4], ":", vec);
            abs_ttl = boost::lexical_cast<int64_t>(vec[vec.size() - 1]);
            if (vec.size() > 1) {
                if (vec[0] == "latest") {
                    type = ::openmldb::type::TTLType::kLatestTime;
                    lat_ttl = abs_ttl;
                    abs_ttl = 0;
                    if (lat_ttl > FLAGS_latest_ttl_max) {
                        std::cout << "Create failed. The max num of latest "
                                     "LatestTime is "
                                  << FLAGS_latest_ttl_max << std::endl;
                        return;
                    }
                } else {
                    std::cout << "invalid ttl type" << std::endl;
                    return;
                }
            } else {
                if (abs_ttl > FLAGS_absolute_ttl_max) {
                    std::cout << "Create failed. The max num of AbsoluteTime ttl is " << FLAGS_absolute_ttl_max
                              << std::endl;
                    return;
                }
            }
        }
        if (abs_ttl < 0 || lat_ttl < 0) {
            std::cout << "ttl should be equal or greater than 0" << std::endl;
            return;
        }
        uint32_t seg_cnt = 16;
        if (parts.size() > 5) {
            seg_cnt = boost::lexical_cast<uint32_t>(parts[5]);
        }
        bool is_leader = true;
        if (parts.size() > 6 && parts[6] == "false") {
            is_leader = false;
        }
        std::vector<std::string> endpoints;
        ::openmldb::type::CompressType compress_type = ::openmldb::type::CompressType::kNoCompress;
        if (parts.size() > 7) {
            std::string raw_compress_type = parts[7];
            std::transform(raw_compress_type.begin(), raw_compress_type.end(), raw_compress_type.begin(), ::tolower);
            if (raw_compress_type == "knocompress" || raw_compress_type == "nocompress") {
                compress_type = ::openmldb::type::CompressType::kNoCompress;
            } else if (raw_compress_type == "ksnappy" || raw_compress_type == "snappy") {
                compress_type = ::openmldb::type::CompressType::kSnappy;
            } else {
                printf("compress type %s is invalid\n", parts[7].c_str());
                return;
            }
        }
        bool ok = client->CreateTable(parts[1], boost::lexical_cast<uint32_t>(parts[2]),
                                      boost::lexical_cast<uint32_t>(parts[3]), abs_ttl, lat_ttl, is_leader, endpoints,
                                      type, seg_cnt, 0, compress_type);
        if (!ok) {
            std::cout << "Fail to create table" << std::endl;
        } else {
            std::cout << "Create table ok" << std::endl;
        }
    } catch (std::exception const& e) {
        std::cout << "Invalid args, tid , pid or ttl should be uint32_t" << std::endl;
    }
}

void HandleClientDropTable(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad drop command, you should input like 'drop tid pid' " << std::endl;
        return;
    }
    try {
        bool ok = client->DropTable(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]));
        if (ok) {
            std::cout << "Drop table ok" << std::endl;
        } else {
            std::cout << "Fail to drop table" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad drop format" << std::endl;
    }
}

void HandleClientAddReplica(const std::vector<std::string> parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad addreplica format" << std::endl;
        return;
    }
    try {
        bool ok = client->AddReplica(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]),
                                     parts[3]);
        if (ok) {
            std::cout << "AddReplica ok" << std::endl;
        } else {
            std::cout << "Fail to Add Replica" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad addreplica format" << std::endl;
    }
}

void HandleClientDelReplica(const std::vector<std::string> parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad delreplica format" << std::endl;
        return;
    }
    try {
        bool ok = client->DelReplica(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]),
                                     parts[3]);
        if (ok) {
            std::cout << "DelReplica ok" << std::endl;
        } else {
            std::cout << "Fail to Del Replica" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad delreplica format" << std::endl;
    }
}

void HandleClientSetExpire(const std::vector<std::string> parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    try {
        bool ok = client->SetExpire(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]),
                                    parts[3] == "true" ? true : false);
        if (ok) {
            std::cout << "setexpire ok" << std::endl;
        } else {
            std::cout << "Fail to setexpire" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad format" << std::endl;
    }
}

void HandleClientConnectZK(const std::vector<std::string> parts, ::openmldb::client::TabletClient* client) {
    bool ok = client->ConnectZK();
    if (ok) {
        std::cout << "connect zk ok" << std::endl;
    } else {
        std::cout << "Fail to connect zk" << std::endl;
    }
}

void HandleClientDisConnectZK(const std::vector<std::string> parts, ::openmldb::client::TabletClient* client) {
    bool ok = client->DisConnectZK();
    if (ok) {
        std::cout << "disconnect zk ok" << std::endl;
    } else {
        std::cout << "Fail to disconnect zk" << std::endl;
    }
}

void HandleClientHelp(const std::vector<std::string> parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 2) {
        printf("addreplica - add replica to leader\n");
        printf("changerole - change role\n");
        printf("count - count the num of data in specified key\n");
        printf("create - create table\n");
        printf("delreplica - delete replica from leader\n");
        printf("delete - delete pk\n");
        printf("deleteindex - delete index\n");
        printf("drop - drop table\n");
        printf("exit - exit client\n");
        printf("get - get only one record\n");
        printf("gettablestatus - get table status\n");
        printf("getfollower - get follower\n");
        printf("help - get cmd info\n");
        printf("loadtable - create table and load data\n");
        printf("man - get cmd info\n");
        printf("makesnapshot - make snapshot\n");
        printf("pausesnapshot - pause snapshot\n");
        printf("preview - preview data\n");
        printf("put - insert data into table\n");
        printf("quit - exit client\n");
        printf("recoversnapshot - recover snapshot\n");
        printf("sput - insert data into table of multi dimension\n");
        printf("screate - create multi dimension table\n");
        printf("scan - get records for a period of time\n");
        printf(
            "sscan - get records for a period of time from multi dimension "
            "table\n");
        printf("sget - get only one record from multi dimension table\n");
        printf("sendsnapshot - send snapshot to another endpoint\n");
        printf("setexpire - enable or disable ttl\n");
        printf("showschema - show schema\n");
        printf("setttl - set ttl for partition\n");
    } else if (parts.size() == 2) {
        if (parts[1] == "create") {
            printf("desc: create table\n");
            printf(
                "usage: create name tid pid ttl segment_cnt [is_leader "
                "compress_type]\n");
            printf("ex: create table1 1 0 144000 8\n");
            printf("ex: create table1 1 0 144000 8 true snappy\n");
            printf("ex: create table1 1 0 144000 8 false\n");
        } else if (parts[1] == "screate") {
            printf("desc: create multi dimension table\n");
            printf(
                "usage: screate table_name tid pid ttl segment_cnt is_leader "
                "schema\n");
            printf(
                "ex: screate table1 1 0 144000 8 true card:string:index "
                "merchant:string:index amt:double\n");
        } else if (parts[1] == "drop") {
            printf("desc: drop table\n");
            printf("usage: drop tid pid\n");
            printf("ex: drop 1 0\n");
        } else if (parts[1] == "put") {
            printf("desc: insert data into table\n");
            printf("usage: put tid pid pk ts value\n");
            printf("ex: put 1 0 key1 1528858466000 value1\n");
        } else if (parts[1] == "sput") {
            printf("desc: insert data into table of multi dimension\n");
            printf("usage: sput tid pid ts key1 key2 ... value\n");
            printf("ex: sput 1 0 1528858466000 card0 merchant0 1.1\n");
        } else if (parts[1] == "scan") {
            printf("desc: get records for a period of time\n");
            printf("usage: scan tid pid pk starttime endtime [limit]\n");
            printf("ex: scan 1 0 key1 1528858466000 1528858300000\n");
            printf("ex: scan 1 0 key1 1528858466000 1528858300000 10\n");
            printf("ex: scan 1 0 key1 0 0 10\n");
        } else if (parts[1] == "sscan") {
            printf(
                "desc: get records for a period of time from multi dimension "
                "table\n");
            printf(
                "usage: sscan tid pid key key_name starttime endtime "
                "[limit]\n");
            printf("ex: sscan 1 0 card0 card 1528858466000 1528858300000\n");
            printf("ex: sscan 1 0 card0 card 1528858466000 1528858300000 10\n");
            printf("ex: sscan 1 0 card0 card 0 0 10\n");
        } else if (parts[1] == "get") {
            printf("desc: get only one record\n");
            printf("usage: get tid pid key ts\n");
            printf("ex: get 1 0 key1 1528858466000\n");
            printf("ex: get 1 0 key1 0\n");
        } else if (parts[1] == "sget") {
            printf("desc: get only one record from multi dimension table\n");
            printf("usage: sget tid pid key key_name ts\n");
            printf("ex: sget 1 0 card0 card 1528858466000\n");
            printf("ex: sget 1 0 card0 card 0\n");
        } else if (parts[1] == "delete") {
            printf("desc: delete pk\n");
            printf("usage: delete tid pid key [key_name]\n");
            printf("ex: delete 1 0 key1\n");
            printf("ex: delete 1 0 card0 card\n");
        } else if (parts[1] == "deleteindex") {
            printf("desc: delete index\n");
            printf("usage: deleteindex tid pid index_name\n");
            printf("ex: deleteindex 1 0 card\n");
        } else if (parts[1] == "count") {
            printf("desc: count the num of data in specified key\n");
            printf("usage: count tid pid key [filter_expired_data]\n");
            printf("usage: count tid pid key key_name [filter_expired_data]\n");
            printf("ex: count 1 0 key1\n");
            printf("ex: count 1 0 key1 true\n");
            printf("ex: count 2 0 card0 card\n");
            printf("ex: count 2 0 card0 card true\n");
        } else if (parts[1] == "preview") {
            printf("desc: preview data in table\n");
            printf("usage: preview tid pid [limit]\n");
            printf("ex: preview 1 0\n");
            printf("ex: preview 1 0 10\n");
        } else if (parts[1] == "addreplica") {
            printf("desc: add replica to leader\n");
            printf("usage: addreplica tid pid endpoint\n");
            printf("ex: addreplica 1 0 172.27.2.52:9992\n");
        } else if (parts[1] == "delreplica") {
            printf("desc: delete replica from leader\n");
            printf("usage: delreplica tid pid endpoint\n");
            printf("ex: delreplica 1 0 172.27.2.52:9992\n");
        } else if (parts[1] == "makesnapshot") {
            printf("desc: make snapshot\n");
            printf("usage: makesnapshot tid pid\n");
            printf("ex: makesnapshot 1 0\n");
        } else if (parts[1] == "pausesnapshot") {
            printf("desc: pause snapshot\n");
            printf("usage: pausesnapshot tid pid\n");
            printf("ex: pausesnapshot 1 0\n");
        } else if (parts[1] == "recoversnapshot") {
            printf("desc: recover snapshot\n");
            printf("usage: recoversnapshot tid pid\n");
            printf("ex: recoversnapshot 1 0\n");
        } else if (parts[1] == "sendsnapshot") {
            printf("desc: send snapshot\n");
            printf("usage: sendsnapshot tid pid endpoint\n");
            printf("ex: sendsnapshot 1 0 172.27.128.32:8541\n");
        } else if (parts[1] == "loadtable") {
            printf("desc: create table and load data\n");
            printf("usage: loadtable table_name tid pid ttl segment_cnt is_leader");
            printf("ex: loadtable table1 1 0 144000 8 true memory\n");
        } else if (parts[1] == "changerole") {
            printf("desc: change role\n");
            printf("usage: changerole tid pid role\n");
            printf("ex: changerole 1 0 leader\n");
            printf("ex: changerole 1 0 follower\n");
        } else if (parts[1] == "setexpire") {
            printf("desc: enable or disable ttl\n");
            printf("usage: setexpire tid pid is_expire\n");
            printf("ex: setexpire 1 0 true\n");
            printf("ex: setexpire 1 0 false\n");
        } else if (parts[1] == "showschema") {
            printf("desc: show schema\n");
            printf("usage: showschema tid pid\n");
            printf("ex: showschema 1 0\n");
        } else if (parts[1] == "gettablestatus") {
            printf("desc: get table status\n");
            printf("usage: gettablestatus [tid pid]\n");
            printf("ex: gettablestatus\n");
            printf("ex: gettablestatus 1 0\n");
        } else if (parts[1] == "getfollower") {
            printf("desc: get table follower\n");
            printf("usage: getfollower tid pid\n");
            printf("ex: getfollower 1 0\n");
        } else if (parts[1] == "exit" || parts[1] == "quit") {
            printf("desc: exit client\n");
            printf("ex: quit\n");
            printf("ex: exit\n");
        } else if (parts[1] == "help" || parts[1] == "man") {
            printf("desc: get cmd info\n");
            printf("usage: help [cmd]\n");
            printf("usage: man [cmd]\n");
            printf("ex:help\n");
            printf("ex:help create\n");
            printf("ex:man\n");
            printf("ex:man create\n");
        } else if (parts[1] == "setttl") {
            printf("desc: set table ttl \n");
            printf("usage: setttl tid pid ttl_type ttl [ts_name]\n");
            printf("ex: setttl 1 0 absolute 10\n");
            printf("ex: setttl 2 0 latest 10\n");
            printf("ex: setttl 3 0 latest 10 ts1\n");
        } else {
            printf("unsupport cmd %s\n", parts[1].c_str());
        }
    } else {
        printf("help format error!\n");
        printf("usage: help [cmd]\n");
        printf("ex: help\n");
        printf("ex: help create\n");
    }
}

void HandleClientGetTableStatus(const std::vector<std::string> parts, ::openmldb::client::TabletClient* client) {
    std::vector<::openmldb::api::TableStatus> status_vec;
    if (parts.size() == 3) {
        ::openmldb::api::TableStatus table_status;
        try {
            if (client->GetTableStatus(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]),
                                       table_status)) {
                status_vec.push_back(table_status);
            } else {
                std::cout << "gettablestatus failed" << std::endl;
            }
        } catch (boost::bad_lexical_cast& e) {
            std::cout << "Bad gettablestatus format" << std::endl;
        }
    } else if (parts.size() == 1) {
        ::openmldb::api::GetTableStatusResponse response;
        if (!client->GetTableStatus(response)) {
            std::cout << "gettablestatus failed" << std::endl;
            return;
        }
        for (int idx = 0; idx < response.all_table_status_size(); idx++) {
            status_vec.push_back(response.all_table_status(idx));
        }
    } else {
        std::cout << "Bad gettablestatus format" << std::endl;
        return;
    }
    ::openmldb::cmd::PrintTableStatus(status_vec);
}

void HandleClientMakeSnapshot(const std::vector<std::string> parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad MakeSnapshot format" << std::endl;
        return;
    }
    bool ok = client->MakeSnapshot(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]), 0);
    if (ok) {
        std::cout << "MakeSnapshot ok" << std::endl;
    } else {
        std::cout << "Fail to MakeSnapshot" << std::endl;
    }
}

void HandleClientPauseSnapshot(const std::vector<std::string> parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad PauseSnapshot format" << std::endl;
        return;
    }
    try {
        bool ok =
            client->PauseSnapshot(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]));
        if (ok) {
            std::cout << "PauseSnapshot ok" << std::endl;
        } else {
            std::cout << "Fail to PauseSnapshot" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad PauseSnapshot format" << std::endl;
    }
}

void HandleClientRecoverSnapshot(const std::vector<std::string> parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad RecoverSnapshot format" << std::endl;
        return;
    }
    try {
        bool ok =
            client->RecoverSnapshot(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]));
        if (ok) {
            std::cout << "RecoverSnapshot ok" << std::endl;
        } else {
            std::cout << "Fail to RecoverSnapshot" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad RecoverSnapshot format" << std::endl;
    }
}

void HandleClientSendSnapshot(const std::vector<std::string> parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad SendSnapshot format" << std::endl;
        return;
    }
    try {
        bool ok = client->SendSnapshot(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[1]),
                                       boost::lexical_cast<uint32_t>(parts[2]), parts[3]);
        if (ok) {
            std::cout << "SendSnapshot ok" << std::endl;
        } else {
            std::cout << "Fail to SendSnapshot" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad SendSnapshot format" << std::endl;
    }
}

void HandleClientLoadTable(const std::vector<std::string> parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 6) {
        std::cout << "Bad LoadTable format eg loadtable <name> <tid> <pid> "
                     "<ttl> <seg_cnt> [<is_leader>]"
                  << std::endl;
        return;
    }
    try {
        uint64_t ttl = 0;
        ttl = boost::lexical_cast<uint64_t>(parts[4]);
        uint32_t seg_cnt = 16;
        seg_cnt = boost::lexical_cast<uint32_t>(parts[5]);
        bool is_leader = true;
        if (parts.size() > 6) {
            if (parts[6] == "false") {
                is_leader = false;
            } else if (parts[6] == "true") {
                is_leader = true;
            } else {
                std::cout << "Bad LoadTable format eg loadtable <name> <tid> <pid> "
                             "<ttl> <seg_cnt> [<is_leader>]"
                          << std::endl;
                return;
            }
        }
        bool ok = client->LoadTable(parts[1], boost::lexical_cast<uint32_t>(parts[2]),
                                    boost::lexical_cast<uint32_t>(parts[3]), ttl, is_leader, seg_cnt);
        if (ok) {
            std::cout << "LoadTable ok" << std::endl;
        } else {
            std::cout << "Fail to LoadTable" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad LoadTable format" << std::endl;
    }
}

void HandleClientChangeRole(const std::vector<std::string> parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad changerole format" << std::endl;
        return;
    }
    try {
        uint64_t termid = 0;
        if (parts.size() > 4) {
            termid = boost::lexical_cast<uint64_t>(parts[4]);
        }
        if (parts[3].compare("leader") == 0) {
            bool ok = client->ChangeRole(boost::lexical_cast<uint32_t>(parts[1]),
                                         boost::lexical_cast<uint32_t>(parts[2]), true, termid);
            if (ok) {
                std::cout << "ChangeRole ok" << std::endl;
            } else {
                std::cout << "Fail to change leader" << std::endl;
            }
        } else if (parts[3].compare("follower") == 0) {
            bool ok = client->ChangeRole(boost::lexical_cast<uint32_t>(parts[1]),
                                         boost::lexical_cast<uint32_t>(parts[2]), false, termid);
            if (ok) {
                std::cout << "ChangeRole ok" << std::endl;
            } else {
                std::cout << "Fail to change follower" << std::endl;
            }
        } else {
            std::cout << "role must be leader or follower" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad changerole format" << std::endl;
    }
}

void HandleClientPreview(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "preview format error. eg: preview tid pid [limit]" << std::endl;
        return;
    }
    uint32_t limit = FLAGS_preview_default_limit;
    uint32_t tid, pid;
    try {
        tid = boost::lexical_cast<uint32_t>(parts[1]);
        pid = boost::lexical_cast<uint32_t>(parts[2]);
        if (parts.size() > 3) {
            int64_t tmp = boost::lexical_cast<int64_t>(parts[3]);
            if (tmp < 0) {
                printf("preview error. limit should be unsigned int\n");
                return;
            }
            limit = boost::lexical_cast<uint32_t>(parts[3]);
            if (limit > FLAGS_preview_limit_max_num) {
                printf("preview error. limit is greater than the max num %u\n", FLAGS_preview_limit_max_num);
                return;
            } else if (limit == 0) {
                printf("preview error. limit must be greater than zero\n");
                return;
            }
        }
    } catch (std::exception const& e) {
        printf("Invalid args. tid, pid and limit should be unsigned int\n");
        return;
    }
    ::openmldb::api::TableStatus table_status;
    if (!client->GetTableStatus(tid, pid, true, table_status)) {
        std::cout << "Fail to get table status" << std::endl;
        return;
    }
    /*std::string schema = table_status.schema();
    std::vector<::openmldb::codec::ColumnDesc> columns;
    if (!schema.empty()) {
        ::openmldb::codec::SchemaCodec codec;
        codec.Decode(schema, columns);
    }
    uint32_t column_num = columns.empty() ? 4 : columns.size() + 2;
    ::baidu::common::TPrinter tp(column_num, FLAGS_max_col_display_length);
    std::vector<std::string> row;
    if (schema.empty()) {
        row.push_back("#");
        row.push_back("key");
        row.push_back("ts");
        row.push_back("data");
    } else {
        row.push_back("#");
        row.push_back("ts");
        for (uint32_t i = 0; i < columns.size(); i++) {
            row.push_back(columns[i].name);
        }
    }
    tp.AddRow(row);
    uint32_t index = 1;
    uint32_t count = 0;
    ::openmldb::base::KvIterator* it =
        client->Traverse(tid, pid, "", "", 0, limit, count);
    if (it == NULL) {
        std::cout << "Fail to preview table" << std::endl;
        return;
    }
    while (it->Valid()) {
        row.clear();
        row.push_back(std::to_string(index));
        if (schema.empty()) {
            std::string value = it->GetValue().ToString();
            if (table_status.compress_type() ==
                ::openmldb::type::CompressType::kSnappy) {
                std::string uncompressed;
                ::snappy::Uncompress(value.c_str(), value.length(),
                                     &uncompressed);
                value = uncompressed;
            }
            row.push_back(it->GetPK());
            row.push_back(std::to_string(it->GetKey()));
            row.push_back(value);
        } else {
            row.push_back(std::to_string(it->GetKey()));
            ::openmldb::api::TableMeta table_meta;
            bool ok = client->GetTableSchema(tid, pid, table_meta);
            if (!ok) {
                std::cout << "No schema for table, please use command scan"
                          << std::endl;
                delete it;
                return;
            }
            std::string value;
            if (table_meta.compress_type() == ::openmldb::type::CompressType::kSnappy) {
                ::snappy::Uncompress(it->GetValue().data(),
                                     it->GetValue().size(), &value);
            } else {
                value.assign(it->GetValue().data(), it->GetValue().size());
            }
            if (table_meta.added_column_desc_size() == 0) {
                ::openmldb::codec::RowCodec::DecodeRow(
                    columns.size(), ::openmldb::base::Slice(value), &row);
            } else {
                ::openmldb::codec::RowCodec::DecodeRow(
                    columns.size() - table_meta.added_column_desc_size(),
                    columns.size(), ::openmldb::base::Slice(value), &row);
            }
        }
        tp.AddRow(row);
        index++;
        it->Next();
    }
    delete it;
    tp.Print(true);*/
}

// the input format like scan tid pid pk st et
void HandleClientScan(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 6) {
        std::cout << "Bad scan format! eg. scan tid pid pk start_time end_time "
                     "[limit]"
                  << std::endl;
        return;
    }
    try {
        uint32_t limit = 0;
        if (parts.size() > 6) {
            limit = boost::lexical_cast<uint32_t>(parts[6]);
        }
        std::string msg;
        ::openmldb::base::KvIterator* it = client->Scan(
            boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]), parts[3],
            boost::lexical_cast<uint64_t>(parts[4]), boost::lexical_cast<uint64_t>(parts[5]), limit, 0, msg);
        if (it == NULL) {
            std::cout << "Fail to scan table. error msg: " << msg << std::endl;
        } else {
            bool print = true;
            if (parts.size() >= 7) {
                if (parts[6] == "false") {
                    print = false;
                }
            }
            std::cout << "#\tTime\tData" << std::endl;
            uint32_t index = 1;
            while (it->Valid()) {
                if (print) {
                    std::cout << index << "\t" << it->GetKey() << "\t" << it->GetValue().ToString() << std::endl;
                }
                index++;
                it->Next();
            }
            delete it;
        }
    } catch (std::exception const& e) {
        std::cout << "Invalid args, tid pid should be uint32_t, st and et "
                     "should be uint64_t"
                  << std::endl;
    }
}

void HandleClientBenchmarkPut(uint32_t tid, uint32_t pid, uint32_t val_size, uint32_t run_times, uint32_t ns,
                              ::openmldb::client::TabletClient* client) {
    char val[val_size];  // NOLINT
    for (uint32_t i = 0; i < val_size; i++) {
        val[i] = '0';
    }
    std::string sval(val);
    for (uint32_t i = 0; i < run_times; i++) {
        std::string key = boost::lexical_cast<std::string>(ns) + "test" + boost::lexical_cast<std::string>(i);
        for (uint32_t j = 0; j < 4000; j++) {
            client->Put(tid, pid, key, j, sval);
        }
        client->ShowTp();
    }
}

void HandleClientBenchmarkScan(uint32_t tid, uint32_t pid, uint32_t run_times, uint32_t ns,
                               ::openmldb::client::TabletClient* client) {
    uint64_t st = 999;
    uint64_t et = 0;
    std::string msg;
    for (uint32_t j = 0; j < run_times; j++) {
        for (uint32_t i = 0; i < 500 * 4; i++) {
            std::string key = boost::lexical_cast<std::string>(ns) + "test" + boost::lexical_cast<std::string>(i);
            ::openmldb::base::KvIterator* it = client->Scan(tid, pid, key, st, et, 0, 0, msg);
            delete it;
        }
        client->ShowTp();
    }
}

void HandleClientBenchmark(::openmldb::client::TabletClient* client) {
    uint32_t size = 40;
    uint32_t times = 10;
    std::cout << "Percentile:Start benchmark put size:40" << std::endl;
    HandleClientBenchmarkPut(1, 1, size, times, 1, client);
    std::cout << "Percentile:Start benchmark put size:80" << std::endl;
    HandleClientBenchmarkPut(1, 1, 80, times, 2, client);
    std::cout << "Percentile:Start benchmark put size:200" << std::endl;
    HandleClientBenchmarkPut(1, 1, 200, times, 3, client);
    std::cout << "Percentile:Start benchmark put ha size:400" << std::endl;
    HandleClientBenchmarkPut(1, 1, 400, times, 4, client);

    std::cout << "Percentile:Start benchmark put with one replica size:40" << std::endl;
    HandleClientBenchmarkPut(2, 1, size, times, 1, client);
    std::cout << "Percentile:Start benchmark put with one replica size:80" << std::endl;
    HandleClientBenchmarkPut(2, 1, 80, times, 2, client);
    std::cout << "Percentile:Start benchmark put with one replica  size:200" << std::endl;
    HandleClientBenchmarkPut(2, 1, 200, times, 3, client);
    std::cout << "Percentile:Start benchmark put with one replica size:400" << std::endl;
    HandleClientBenchmarkPut(2, 1, 400, times, 4, client);

    std::cout << "Percentile:Start benchmark Scan 1000 records key size:40" << std::endl;
    HandleClientBenchmarkScan(1, 1, times, 1, client);
    std::cout << "Percentile:Start benchmark Scan 1000 records key size:80" << std::endl;
    HandleClientBenchmarkScan(1, 1, times, 2, client);
    std::cout << "Percentile:Start benchmark Scan 1000 records key size:200" << std::endl;
    HandleClientBenchmarkScan(1, 1, times, 3, client);
    std::cout << "Percentile:Start benchmark Scan 1000 records key size:400" << std::endl;
    HandleClientBenchmarkScan(1, 1, times, 4, client);
}

void HandleClientGetFollower(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad get follower format" << std::endl;
        return;
    }
    uint32_t tid = 0;
    uint32_t pid = 0;
    try {
        tid = boost::lexical_cast<uint32_t>(parts[1]);
        pid = boost::lexical_cast<uint32_t>(parts[2]);
    } catch (std::exception const& e) {
        std::cout << "Invalid args" << std::endl;
        return;
    }
    std::map<std::string, uint64_t> info_map;
    uint64_t offset = 0;
    std::string msg;
    if (!client->GetTableFollower(tid, pid, offset, info_map, msg)) {
        std::cout << "get failed. msg: " << msg << std::endl;
        return;
    }
    std::vector<std::string> header;
    header.push_back("#");
    header.push_back("tid");
    header.push_back("pid");
    header.push_back("leader_offset");
    header.push_back("follower");
    header.push_back("offset");
    ::baidu::common::TPrinter tp(header.size(), FLAGS_max_col_display_length);

    tp.AddRow(header);
    int idx = 0;
    for (const auto& kv : info_map) {
        std::vector<std::string> row;
        row.push_back(std::to_string(idx));
        idx++;
        row.push_back(std::to_string(tid));
        row.push_back(std::to_string(pid));
        row.push_back(std::to_string(offset));
        row.push_back(kv.first);
        row.push_back(std::to_string(kv.second));
        tp.AddRow(row);
    }
    tp.Print(true);
}

void HandleClientCount(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "count format error! eg. count tid pid key [col_name] "
                     "[filter_expired_data] | count tid=xxx pid=xxx key=xxx "
                     "index_name=xxx ts=xxx ts_name=xxx [filter_expired_data]"
                  << std::endl;
        return;
    }
    std::map<std::string, std::string> parameter_map;
    if (!GetParameterMap("tid", parts, "=", parameter_map)) {
        std::cout << "count format erro! eg. count tid=xxx pid=xxx key=xxx "
                     "index_name=xxx ts=xxx ts_name=xxx [filter_expired_data]"
                  << std::endl;
        return;
    }
    bool is_pair_format = parameter_map.empty() ? false : true;
    uint32_t tid = 0;
    uint32_t pid = 0;
    bool filter_expired_data = false;
    std::string key;
    std::string index_name;
    std::string ts_name;
    uint64_t value = 0;
    auto iter = parameter_map.begin();
    try {
        if (is_pair_format) {
            iter = parameter_map.find("tid");
            if (iter != parameter_map.end()) {
                tid = boost::lexical_cast<uint32_t>(iter->second);
            } else {
                std::cout << "count format error: tid does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("pid");
            if (iter != parameter_map.end()) {
                pid = boost::lexical_cast<uint32_t>(iter->second);
            } else {
                std::cout << "count format error: pid does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("key");
            if (iter != parameter_map.end()) {
                key = iter->second;
            } else {
                std::cout << "count format error: key does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("index_name");
            if (iter != parameter_map.end()) {
                index_name = iter->second;
            }
            iter = parameter_map.find("ts_name");
            if (iter != parameter_map.end()) {
                ts_name = iter->second;
            }
            iter = parameter_map.find("filter_expired_data");
            if (iter != parameter_map.end()) {
                std::string temp_str = iter->second;
                if (temp_str == "true") {
                    filter_expired_data = true;
                } else if (temp_str == "false") {
                    filter_expired_data = false;
                } else {
                    printf(
                        "filter_expired_data parameter should be true or "
                        "false\n");
                    return;
                }
            }
        } else {
            tid = boost::lexical_cast<uint32_t>(parts[1]);
            pid = boost::lexical_cast<uint32_t>(parts[2]);
            key = parts[3];
            if (parts.size() == 5) {
                if (parts[4] == "true") {
                    filter_expired_data = true;
                } else if (parts[4] == "false") {
                    filter_expired_data = false;
                } else {
                    index_name = parts[4];
                }
            } else if (parts.size() > 5) {
                index_name = parts[4];
                if (parts[5] == "true") {
                    filter_expired_data = true;
                } else if (parts[5] == "false") {
                    filter_expired_data = false;
                } else {
                    printf(
                        "filter_expired_data parameter should be true or "
                        "false\n");
                    return;
                }
            }
        }
    } catch (std::exception const& e) {
        std::cout << "Invalid args. tid and pid should be uint32" << std::endl;
        return;
    }
    std::string msg;
    bool ok = client->Count(tid, pid, key, index_name, ts_name, filter_expired_data, value, msg);
    if (ok) {
        std::cout << "count: " << value << std::endl;
    } else {
        std::cout << "Count failed. error msg: " << msg << std::endl;
    }
}

void HandleClientShowSchema(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad show schema format" << std::endl;
        return;
    }
    ::openmldb::api::TableMeta table_meta;
    try {
        bool ok = client->GetTableSchema(boost::lexical_cast<uint32_t>(parts[1]),
                                         boost::lexical_cast<uint32_t>(parts[2]), table_meta);
        if (!ok) {
            std::cout << "ShowSchema failed" << std::endl;
            return;
        }
    } catch (std::exception const& e) {
        std::cout << "Invalid args" << std::endl;
        return;
    }
    if (table_meta.column_desc_size() > 0) {
        ::openmldb::cmd::PrintSchema(table_meta.column_desc(), table_meta.added_column_desc());
        printf("\n#ColumnKey\n");
        ::openmldb::cmd::PrintColumnKey(table_meta.column_key());
    } else {
        std::cout << "No schema for table" << std::endl;
    }
}

void HandleClientSGet(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 5) {
        std::cout << "Bad sget format, eg. sget tid pid key index_name ts | sget "
                     "table_name=xxx key=xxx index_name=xxx ts=xxx ts_name=xxx"
                  << std::endl;
        return;
    }
    std::map<std::string, std::string> parameter_map;
    if (!GetParameterMap("tid", parts, "=", parameter_map)) {
        std::cout << "sget format erro! eg. sget table_name=xxx key=xxx "
                     "index_name=xxx ts=xxx ts_name=xxx"
                  << std::endl;
        return;
    }
    bool is_pair_format = parameter_map.empty() ? false : true;
    uint32_t tid = 0;
    uint32_t pid = 0;
    std::string key;
    std::string index_name;
    uint64_t timestamp = 0;
    std::string ts_name;
    auto iter = parameter_map.begin();
    try {
        if (is_pair_format) {
            iter = parameter_map.find("tid");
            if (iter != parameter_map.end()) {
                tid = boost::lexical_cast<uint32_t>(iter->second);
            } else {
                std::cout << "sget format error: tid does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("pid");
            if (iter != parameter_map.end()) {
                pid = boost::lexical_cast<uint32_t>(iter->second);
            } else {
                std::cout << "sget format error: pid does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("key");
            if (iter != parameter_map.end()) {
                key = iter->second;
            } else {
                std::cout << "sget format error: key does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("index_name");
            if (iter != parameter_map.end()) {
                index_name = iter->second;
            }
            iter = parameter_map.find("ts");
            if (iter != parameter_map.end()) {
                timestamp = boost::lexical_cast<uint64_t>(iter->second);
            }
            iter = parameter_map.find("ts_name");
            if (iter != parameter_map.end()) {
                ts_name = iter->second;
            }
        } else {
            tid = boost::lexical_cast<uint32_t>(parts[1]);
            pid = boost::lexical_cast<uint32_t>(parts[2]);
            key = parts[3];
            index_name = parts[4];
            if (parts.size() > 5) {
                timestamp = boost::lexical_cast<uint64_t>(parts[5]);
            }
        }
    } catch (std::exception const& e) {
        std::cout << "Invalid args. tid pid should be uint32_t, ts should be "
                     "uint64_t, "
                  << std::endl;
        return;
    }
    ::openmldb::api::TableMeta table_meta;
    bool ok = client->GetTableSchema(tid, pid, table_meta);
    if (!ok) {
        std::cout << "No schema for table ,please use command get" << std::endl;
        return;
    }
    std::string value;
    uint64_t ts = 0;
    std::string msg;
    ok = client->Get(tid, pid, key, timestamp, index_name, ts_name, value, ts, msg);
    if (!ok) {
        std::cout << "Fail to sget value! error msg: " << msg << std::endl;
        return;
    }
    ::openmldb::api::TableStatus table_status;
    if (!client->GetTableStatus(tid, pid, table_status)) {
        std::cout << "Fail to get table status" << std::endl;
        return;
    }
    ::openmldb::type::CompressType compress_type = ::openmldb::type::CompressType::kNoCompress;
    if (table_status.compress_type() == ::openmldb::type::CompressType::kSnappy) {
        compress_type = ::openmldb::type::CompressType::kSnappy;
    }
    if (compress_type == ::openmldb::type::CompressType::kSnappy) {
        std::string uncompressed;
        ::snappy::Uncompress(value.c_str(), value.length(), &uncompressed);
        value = uncompressed;
    }
    // TODO(denglong): display schema
    /*std::string schema = table_meta.schema();
    std::vector<::openmldb::codec::ColumnDesc> raw;
    ::openmldb::codec::SchemaCodec codec;
    codec.Decode(schema, raw);
    ::baidu::common::TPrinter tp(raw.size() + 2, FLAGS_max_col_display_length);
    std::vector<std::string> row;
    row.push_back("#");
    row.push_back("ts");
    for (uint32_t i = 0; i < raw.size(); i++) {
        row.push_back(raw[i].name);
    }
    tp.AddRow(row);
    row.clear();
    row.push_back("1");
    row.push_back(std::to_string(ts));
    if (table_meta.added_column_desc_size() == 0) {
        ::openmldb::codec::RowCodec::DecodeRow(raw.size(),
                                            ::openmldb::base::Slice(value), &row);
    } else {
        ::openmldb::codec::RowCodec::DecodeRow(
            raw.size() - table_meta.added_column_desc_size(), raw.size(),
            ::openmldb::base::Slice(value), &row);
    }
    tp.AddRow(row);
    tp.Print(true);*/
}

void HandleClientSScan(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 7) {
        std::cout << "Bad scan format! eg.sscan tid pid key col_name start_time "
                     "end_time [limit] | sscan table_name=xxx key=xxx index_name=xxx "
                     "st=xxx et=xxx ts_name=xxx [limit=xxx]"
                  << std::endl;
        return;
    }
    std::map<std::string, std::string> parameter_map;
    if (!GetParameterMap("tid", parts, "=", parameter_map)) {
        std::cout << "scan format erro! eg. sscan table_name=xxx key=xxx "
                     "index_name=xxx st=xxx et=xxx ts_name=xxx [limit=xxx]"
                  << std::endl;
        return;
    }
    bool is_pair_format = parameter_map.empty() ? false : true;
    uint32_t tid = 0;
    uint32_t pid = 0;
    std::string key;
    std::string index_name;
    uint64_t st = 0;
    uint64_t et = 0;
    std::string ts_name;
    uint32_t limit = 0;
    auto iter = parameter_map.begin();
    try {
        if (is_pair_format) {
            iter = parameter_map.find("tid");
            if (iter != parameter_map.end()) {
                tid = boost::lexical_cast<uint32_t>(iter->second);
            } else {
                std::cout << "sscan format error: tid does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("pid");
            if (iter != parameter_map.end()) {
                pid = boost::lexical_cast<uint32_t>(iter->second);
            } else {
                std::cout << "sscan format error: pid does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("key");
            if (iter != parameter_map.end()) {
                key = iter->second;
            } else {
                std::cout << "sscan format error: key does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("index_name");
            if (iter != parameter_map.end()) {
                index_name = iter->second;
            }
            iter = parameter_map.find("st");
            if (iter != parameter_map.end()) {
                st = boost::lexical_cast<uint64_t>(iter->second);
            } else {
                std::cout << "sscan format error: st does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("et");
            if (iter != parameter_map.end()) {
                et = boost::lexical_cast<uint64_t>(iter->second);
            } else {
                std::cout << "sscan format error: et does not exist!" << std::endl;
                return;
            }
            iter = parameter_map.find("ts_name");
            if (iter != parameter_map.end()) {
                ts_name = iter->second;
            }
            iter = parameter_map.find("limit");
            if (iter != parameter_map.end()) {
                limit = boost::lexical_cast<uint32_t>(iter->second);
            }
        } else {
            if (parts.size() > 7) {
                limit = boost::lexical_cast<uint32_t>(parts[7]);
            }
            tid = boost::lexical_cast<uint32_t>(parts[1]);
            pid = boost::lexical_cast<uint32_t>(parts[2]);
            key = parts[3];
            index_name = parts[4];
            st = boost::lexical_cast<uint64_t>(parts[5]);
            et = boost::lexical_cast<uint64_t>(parts[6]);
        }
    } catch (std::exception const& e) {
        std::cout << "Invalid args. tid pid should be uint32_t, st and et "
                     "should be uint64_t, limit should be uint32"
                  << std::endl;
        return;
    }
    std::string msg;
    std::shared_ptr<::openmldb::base::KvIterator> it(
        client->Scan(tid, pid, key, st, et, index_name, ts_name, limit, 0, msg));
    if (!it) {
        std::cout << "Fail to scan table. error msg: " << msg << std::endl;
        return;
    }
    ::openmldb::api::TableMeta table_meta;
    bool ok = client->GetTableSchema(tid, pid, table_meta);
    if (!ok) {
        std::cout << "table is not exist" << std::endl;
        return;
    }
    std::vector<std::shared_ptr<::openmldb::base::KvIterator>> iter_vec;
    iter_vec.push_back(std::move(it));
    ::openmldb::cmd::SDKIterator sdk_it(iter_vec, limit);
    ::openmldb::cmd::ShowTableRows(table_meta, &sdk_it);
}

void HandleClientSPut(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 5) {
        std::cout << "Bad put format, eg put tid pid time value" << std::endl;
        return;
    }
    try {
        uint32_t tid = boost::lexical_cast<uint32_t>(parts[1]);
        uint32_t pid = boost::lexical_cast<uint32_t>(parts[2]);
        ::openmldb::api::TableMeta table_meta;
        bool ok = client->GetTableSchema(tid, pid, table_meta);
        if (!ok) {
            std::cout << "Fail to get table schema" << std::endl;
            return;
        }
        ::openmldb::codec::SDKCodec codec(table_meta);
        std::vector<std::string> input_value(parts.begin() + 4, parts.end());
        std::map<uint32_t, ::openmldb::codec::Dimension> dimensions;
        if (codec.EncodeDimension(input_value, 0, &dimensions) < 0) {
            std::cout << "Encode dimension error" << std::endl;
            return;
        }
        std::string value;
        if (codec.EncodeRow(input_value, &value) < 0) {
            std::cout << "Encode data error" << std::endl;
            return;
        }

        if (table_meta.compress_type() == ::openmldb::type::CompressType::kSnappy) {
            std::string compressed;
            ::snappy::Compress(value.c_str(), value.length(), &compressed);
            value.swap(compressed);
        }
        ok = client->Put(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]),
                         boost::lexical_cast<uint64_t>(parts[3]), value, dimensions[0]);
        if (ok) {
            std::cout << "Put ok" << std::endl;
        } else {
            std::cout << "Put failed" << std::endl;
        }
    } catch (std::exception const& e) {
        std::cout << e.what() << std::endl;
    }
}

void HandleClientDelete(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad delete format" << std::endl;
        return;
    }
    try {
        uint32_t tid = boost::lexical_cast<uint32_t>(parts[1]);
        uint32_t pid = boost::lexical_cast<uint32_t>(parts[2]);
        std::string msg;
        std::string idx_name;
        if (parts.size() > 4) {
            idx_name = parts[4];
        }
        if (client->Delete(tid, pid, parts[3], idx_name, msg)) {
            std::cout << "Delete ok" << std::endl;
        } else {
            std::cout << "Delete failed" << std::endl;
        }
    } catch (std::exception const& e) {
        std::cout << "Invalid args, tid pid should be uint32_t" << std::endl;
    }
}

void HandleClientBenScan(const std::vector<std::string>& parts, ::openmldb::client::TabletClient* client) {
    uint64_t et = 0;
    uint32_t tid = 1;
    uint32_t pid = 1;
    uint64_t key_num = 1000000;
    uint32_t times = 10000;
    int num = 100;
    uint32_t limit = 0;
    if (parts.size() >= 3) {
        try {
            tid = ::boost::lexical_cast<uint32_t>(parts[1]);
            pid = ::boost::lexical_cast<uint32_t>(parts[2]);
            if (parts.size() >= 4) {
                key_num = ::boost::lexical_cast<uint64_t>(parts[3]);
            }
            if (parts.size() >= 5) {
                times = ::boost::lexical_cast<uint32_t>(parts[4]);
            }
            if (parts.size() >= 6) {
                num = ::boost::lexical_cast<int>(parts[5]);
            }
            if (parts.size() >= 7) {
                limit = ::boost::lexical_cast<uint32_t>(parts[6]);
            }
        } catch (boost::bad_lexical_cast& e) {
            std::cout << "Bad scan format" << std::endl;
            return;
        }
    }
    uint64_t base = 100000000;
    std::random_device rd;
    std::default_random_engine engine(rd());
    std::uniform_int_distribution<> dis(1, key_num);
    std::string msg;
    while (num > 0) {
        for (uint32_t i = 0; i < times; i++) {
            std::string key = std::to_string(base + dis(engine));
            uint64_t st = ::baidu::common::timer::get_micros() / 1000;
            msg.clear();
            // ::openmldb::base::KvIterator* it = client->Scan(tid, pid,
            // key.c_str(), st, et, msg, false);
            ::openmldb::base::KvIterator* it = client->Scan(tid, pid, key.c_str(), st, et, limit, 0, msg);
            delete it;
        }
        client->ShowTp();
        num--;
    }
}

void StartClient() {
    if (FLAGS_endpoint.empty()) {
        std::cout << "Start failed! not set endpoint" << std::endl;
        return;
    }
    if (FLAGS_interactive) {
        std::cout << "Welcome to openmldb with version " << OPENMLDB_VERSION << std::endl;
    }
    ::openmldb::client::TabletClient client(FLAGS_endpoint, "");
    client.Init();
    std::string display_prefix = FLAGS_endpoint + "> ";
    while (true) {
        std::string buffer;
        if (!FLAGS_interactive) {
            buffer = FLAGS_cmd;
        } else {
            char* line = ::openmldb::base::linenoise(display_prefix.c_str());
            if (line == NULL) {
                return;
            }
            if (line[0] != '\0' && line[0] != '/') {
                buffer.assign(line);
                boost::trim(buffer);
                if (!buffer.empty()) {
                    ::openmldb::base::linenoiseHistoryAdd(line);
                }
            }
            ::openmldb::base::linenoiseFree(line);
            if (buffer.empty()) {
                continue;
            }
        }
        std::vector<std::string> parts;
        ::openmldb::base::SplitString(buffer, " ", parts);
        if (parts.empty()) {
            continue;
        } else if (parts[0] == "put") {
            HandleClientPut(parts, &client);
        } else if (parts[0] == "sput") {
            HandleClientSPut(parts, &client);
        } else if (parts[0] == "create") {
            HandleClientCreateTable(parts, &client);
        } else if (parts[0] == "get") {
            HandleClientGet(parts, &client);
        } else if (parts[0] == "sget") {
            HandleClientSGet(parts, &client);
        } else if (parts[0] == "scan") {
            HandleClientScan(parts, &client);
        } else if (parts[0] == "sscan") {
            HandleClientSScan(parts, &client);
        } else if (parts[0] == "delete") {
            HandleClientDelete(parts, &client);
        } else if (parts[0] == "count") {
            HandleClientCount(parts, &client);
        } else if (parts[0] == "preview") {
            HandleClientPreview(parts, &client);
        } else if (parts[0] == "showschema") {
            HandleClientShowSchema(parts, &client);
        } else if (parts[0] == "getfollower") {
            HandleClientGetFollower(parts, &client);
        } else if (parts[0] == "benput") {
            HandleClientBenPut(parts, &client);
        } else if (parts[0] == "benscan") {
            HandleClientBenScan(parts, &client);
        } else if (parts[0] == "benget") {
            HandleClientBenGet(parts, &client);
        } else if (parts[0] == "benchmark") {
            HandleClientBenchmark(&client);
        } else if (parts[0] == "drop") {
            HandleClientDropTable(parts, &client);
        } else if (parts[0] == "addreplica") {
            HandleClientAddReplica(parts, &client);
        } else if (parts[0] == "delreplica") {
            HandleClientDelReplica(parts, &client);
        } else if (parts[0] == "makesnapshot") {
            HandleClientMakeSnapshot(parts, &client);
        } else if (parts[0] == "pausesnapshot") {
            HandleClientPauseSnapshot(parts, &client);
        } else if (parts[0] == "recoversnapshot") {
            HandleClientRecoverSnapshot(parts, &client);
        } else if (parts[0] == "sendsnapshot") {
            HandleClientSendSnapshot(parts, &client);
        } else if (parts[0] == "loadtable") {
            HandleClientLoadTable(parts, &client);
        } else if (parts[0] == "changerole") {
            HandleClientChangeRole(parts, &client);
        } else if (parts[0] == "gettablestatus") {
            HandleClientGetTableStatus(parts, &client);
        } else if (parts[0] == "setexpire") {
            HandleClientSetExpire(parts, &client);
        } else if (parts[0] == "connectzk") {
            HandleClientConnectZK(parts, &client);
        } else if (parts[0] == "disconnectzk") {
            HandleClientDisConnectZK(parts, &client);
        } else if (parts[0] == "deleteindex") {
            HandleClientDeleteIndex(parts, &client);
        } else if (parts[0] == "setttl") {
            HandleClientSetTTL(parts, &client);
        } else if (parts[0] == "exit" || parts[0] == "quit") {
            std::cout << "bye" << std::endl;
            return;
        } else if (parts[0] == "help" || parts[0] == "man") {
            HandleClientHelp(parts, &client);
        } else {
            std::cout << "unsupported cmd" << std::endl;
        }
        if (!FLAGS_interactive) {
            return;
        }
    }
}

void StartNsClient() {
    std::string endpoint;
    std::string real_endpoint;
    if (FLAGS_interactive) {
        std::cout << "Welcome to openmldb with version " << OPENMLDB_VERSION << std::endl;
    }
    std::shared_ptr<::openmldb::zk::ZkClient> zk_client;
    if (!FLAGS_zk_cluster.empty()) {
        zk_client = std::make_shared<::openmldb::zk::ZkClient>(FLAGS_zk_cluster, "", 1000, "", FLAGS_zk_root_path);
        if (!zk_client->Init()) {
            std::cout << "zk client init failed" << std::endl;
            return;
        }
        std::string node_path = FLAGS_zk_root_path + "/leader";
        std::vector<std::string> children;
        if (!zk_client->GetChildren(node_path, children) || children.empty()) {
            std::cout << "get children failed" << std::endl;
            return;
        }
        std::string leader_path = node_path + "/" + children[0];
        if (!zk_client->GetNodeValue(leader_path, endpoint)) {
            std::cout << "get leader failed" << std::endl;
            return;
        }
        std::cout << "ns leader: " << endpoint << std::endl;
        // real endpoint part
        std::string path = FLAGS_zk_root_path + "/map/names/" + endpoint;
        if (zk_client->IsExistNode(path) == 0) {
            if (!zk_client->GetNodeValue(path, real_endpoint)) {
                std::cout << "get real_endpoint failed" << std::endl;
                return;
            }
            std::string name_root_path = FLAGS_zk_root_path + "/map/names";
            std::vector<std::string> nodes;
            if (!zk_client->GetChildren(name_root_path, nodes) || nodes.empty()) {
                std::cout << "get server name nodes failed" << std::endl;
                return;
            }
            for (const auto& node : nodes) {
                std::string real_ep;
                std::string real_path = name_root_path + "/" + node;
                if (!zk_client->GetNodeValue(real_path, real_ep)) {
                    std::cout << "get zk server name failed" << std::endl;
                    return;
                }
                real_ep_map.insert(std::make_pair(node, real_ep));
            }
        }
    } else if (!FLAGS_endpoint.empty()) {
        endpoint = FLAGS_endpoint;
    } else {
        std::cout << "Start failed! not set endpoint or zk_cluster" << std::endl;
        return;
    }
    ::openmldb::client::NsClient client(endpoint, real_endpoint);
    if (client.Init() < 0) {
        std::cout << "client init failed" << std::endl;
        return;
    }
    std::string display_prefix = endpoint + " " + client.GetDb() + "> ";
    std::string multi_line_perfix = std::string(display_prefix.length() - 3, ' ') + "-> ";
    std::string sql;
    bool use_sql = false;
    bool multi_line = false;
    while (true) {
        std::string buffer;
        display_prefix = endpoint + " " + client.GetDb() + "> ";
        multi_line_perfix = std::string(display_prefix.length() - 3, ' ') + "-> ";
        if (!FLAGS_interactive) {
            buffer = FLAGS_cmd;
        } else {
            char* line = ::openmldb::base::linenoise(multi_line ? multi_line_perfix.c_str() : display_prefix.c_str());
            if (line == NULL) {
                return;
            }

            if (line[0] != '\0' && line[0] != '/') {
                buffer.assign(line);
                boost::trim(buffer);
                if (!buffer.empty()) {
                    ::openmldb::base::linenoiseHistoryAdd(line);
                }
            }
            ::openmldb::base::linenoiseFree(line);
            if (buffer.empty()) {
                continue;
            }
        }
        if (use_sql) {
            sql.append(buffer);
            if (sql == "exit" || sql == "quit") {
                use_sql = false;
                display_prefix = endpoint + " " + client.GetDb() + "> ";
                multi_line_perfix = std::string(display_prefix.length() - 3, ' ') + "-> ";
                ::openmldb::base::linenoiseSetMultiLine(0);
            } else if (sql.back() == ';') {
                HandleNsClientSQL(sql, &client);
                multi_line = false;
                sql.clear();
            } else {
                sql.append("\n");
                multi_line = true;
            }
            continue;
        }
        std::vector<std::string> parts;
        ::openmldb::base::SplitString(buffer, " ", parts);
        if (parts.empty()) {
            continue;
        } else if (parts[0] == "showtablet") {
            HandleNSShowTablet(parts, &client);
        } else if (parts[0] == "showsdkendpoint") {
            HandleNSShowSdkEndpoint(parts, &client);
        } else if (parts[0] == "showns") {
            HandleNSShowNameServer(parts, &client, zk_client);
        } else if (parts[0] == "showopstatus") {
            HandleNSShowOPStatus(parts, &client);
        } else if (parts[0] == "create") {
            HandleNSCreateTable(parts, &client);
        } else if (parts[0] == "put") {
            HandleNSPut(parts, &client);
        } else if (parts[0] == "scan") {
            HandleNSScan(parts, &client);
        } else if (parts[0] == "get") {
            HandleNSGet(parts, &client);
        } else if (parts[0] == "delete") {
            HandleNSDelete(parts, &client);
        } else if (parts[0] == "count") {
            HandleNSCount(parts, &client);
        } else if (parts[0] == "preview") {
            HandleNSPreview(parts, &client);
        } else if (parts[0] == "makesnapshot") {
            HandleNSMakeSnapshot(parts, &client);
        } else if (parts[0] == "addreplica") {
            HandleNSAddReplica(parts, &client);
        } else if (parts[0] == "delreplica") {
            HandleNSDelReplica(parts, &client);
        } else if (parts[0] == "drop") {
            HandleNSClientDropTable(parts, &client);
        } else if (parts[0] == "showtable") {
            HandleNSClientShowTable(parts, &client);
        } else if (parts[0] == "showschema") {
            HandleNSClientShowSchema(parts, &client);
        } else if (parts[0] == "confset") {
            HandleNSClientConfSet(parts, &client);
        } else if (parts[0] == "confget") {
            HandleNSClientConfGet(parts, &client);
        } else if (parts[0] == "changeleader") {
            HandleNSClientChangeLeader(parts, &client);
        } else if (parts[0] == "offlineendpoint") {
            HandleNSClientOfflineEndpoint(parts, &client);
        } else if (parts[0] == "migrate") {
            HandleNSClientMigrate(parts, &client);
        } else if (parts[0] == "recoverendpoint") {
            HandleNSClientRecoverEndpoint(parts, &client);
        } else if (parts[0] == "recovertable") {
            HandleNSClientRecoverTable(parts, &client);
        } else if (parts[0] == "connectzk") {
            HandleNSClientConnectZK(parts, &client);
        } else if (parts[0] == "disconnectzk") {
            HandleNSClientDisConnectZK(parts, &client);
        } else if (parts[0] == "gettablepartition") {
            HandleNSClientGetTablePartition(parts, &client);
        } else if (parts[0] == "settablepartition") {
            HandleNSClientSetTablePartition(parts, &client);
        } else if (parts[0] == "updatetablealive") {
            HandleNSClientUpdateTableAlive(parts, &client);
        } else if (parts[0] == "setttl") {
            HandleNSClientSetTTL(parts, &client);
        } else if (parts[0] == "cancelop") {
            HandleNSClientCancelOP(parts, &client);
        } else if (parts[0] == "addtablefield") {
            HandleNSAddTableField(parts, &client);
        } else if (parts[0] == "info") {
            HandleNSInfo(parts, &client);
        } else if (parts[0] == "addrepcluster") {
            HandleNSAddReplicaCluster(parts, &client);
        } else if (parts[0] == "showrepcluster") {
            HandleShowReplicaCluster(parts, &client);
        } else if (parts[0] == "removerepcluster") {
            HandleNSRemoveReplicaCluster(parts, &client);
        } else if (parts[0] == "switchmode") {
            HandleNSSwitchMode(parts, &client);
        } else if (parts[0] == "synctable") {
            HandleNSClientSyncTable(parts, &client);
        } else if (parts[0] == "setsdkendpoint") {
            HandleNSClientSetSdkEndpoint(parts, &client);
        } else if (parts[0] == "addindex") {
            HandleNSClientAddIndex(parts, &client);
        } else if (parts[0] == "deleteindex") {
            HandleNSClientDeleteIndex(parts, &client);
        } else if (parts[0] == "showdb") {
            HandleNSShowDB(&client);
        } else if (parts[0] == "showcatalogversion") {
            HandleNSShowCatalogVersion(&client);
        } else if (parts[0] == "use") {
            HandleNsUseDb(parts, &client);
        } else if (parts[0] == "dropdb") {
            HandleNsDropDb(parts, &client);
        } else if (parts[0] == "sql") {
            use_sql = true;
            display_prefix = endpoint + " " + client.GetDb() + " sql> ";
            multi_line_perfix = std::string(display_prefix.length() - 3, ' ') + "-> ";
            ::openmldb::base::linenoiseSetMultiLine(1);
            sql.clear();
        } else if (parts[0] == "exit" || parts[0] == "quit") {
            std::cout << "bye" << std::endl;
            return;
        } else if (parts[0] == "help" || parts[0] == "man") {
            HandleNSClientHelp(parts, &client);
        } else {
            std::cout << "unsupported cmd" << std::endl;
        }
        if (!FLAGS_interactive) {
            return;
        }
    }
}

void StartAPIServer() {
    SetupLog();
    std::string real_endpoint = FLAGS_endpoint;
    if (real_endpoint.empty()) {
        GetRealEndpoint(&real_endpoint);
    }

    auto api_service = std::make_unique<::openmldb::apiserver::APIServerImpl>();
    ::openmldb::sdk::ClusterOptions cluster_options;
    cluster_options.zk_cluster = FLAGS_zk_cluster;
    cluster_options.zk_path = FLAGS_zk_root_path;
    if (!api_service->Init(cluster_options)) {
        PDLOG(WARNING, "Fail to init");
        exit(1);
    }

    brpc::ServerOptions options;
    options.num_threads = FLAGS_thread_pool_size;
    brpc::Server server;
    if (server.AddService(api_service.get(), brpc::SERVER_DOESNT_OWN_SERVICE, "/* => Process") != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }

    if (server.Start(real_endpoint.c_str(), &options) != 0) {
        PDLOG(WARNING, "Fail to start server");
        exit(1);
    }
    PDLOG(INFO, "start apiserver on endpoint %s with version %s", real_endpoint.c_str(), OPENMLDB_VERSION.c_str());
    server.set_version(OPENMLDB_VERSION.c_str());
    server.RunUntilAskedToQuit();
}

int main(int argc, char* argv[]) {
    ::google::SetVersionString(OPENMLDB_VERSION.c_str());
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_role == "ns_client") {
        StartNsClient();
    } else if (FLAGS_role == "sql_client") {
        ::openmldb::cmd::HandleCli();
#if defined(__linux__) || defined(__mac_tablet__)
    } else if (FLAGS_role == "tablet") {
        StartTablet();
    } else if (FLAGS_role == "client") {
        StartClient();
    } else if (FLAGS_role == "nameserver") {
        StartNameServer();
    } else if (FLAGS_role == "apiserver") {
        StartAPIServer();
#endif
    } else {
        std::cout << "Start failed! FLAGS_role must be tablet, client, "
                     "nameserver, ns_client, apiserver"
                  << std::endl;
    }
    return 0;
}
