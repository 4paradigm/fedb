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

#ifndef SRC_CMD_SQL_CMD_H_
#define SRC_CMD_SQL_CMD_H_
#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/linenoise.h"
#include "base/texttable.h"
#include "catalog/schema_adapter.h"
#include "gflags/gflags.h"
#include "node/node_manager.h"
#include "plan/plan_api.h"
#include "proto/fe_type.pb.h"
#include "sdk/cluster_sdk.h"
#include "sdk/sql_cluster_router.h"
#include "version.h"  // NOLINT

DEFINE_string(database, "", "Set database");
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_bool(interactive);
DECLARE_string(cmd);

using ::openmldb::catalog::TTL_TYPE_MAP;

namespace openmldb {
namespace cmd {
using hybridse::plan::PlanAPI;
const std::string LOGO =  // NOLINT

    "  _____                    ______  _       _____   ______   \n"
    " / ___ \\                  |  ___ \\| |     (____ \\ (____  \\  \n"
    "| |   | |____   ____ ____ | | _ | | |      _   \\ \\ ____)  ) \n"
    "| |   | |  _ \\ / _  )  _ \\| || || | |     | |   | |  __  (  \n"
    "| |___| | | | ( (/ /| | | | || || | |_____| |__/ /| |__)  ) \n"
    " \\_____/| ||_/ \\____)_| |_|_||_||_|_______)_____/ |______/  \n"
    "        |_|                                                 \n";


const std::string VERSION = std::to_string(OPENMLDB_VERSION_MAJOR) + "." +  // NOLINT
                            std::to_string(OPENMLDB_VERSION_MINOR) + "." + std::to_string(OPENMLDB_VERSION_BUG) + "." +
                            OPENMLDB_COMMIT_ID;

std::string db = "";  // NOLINT
::openmldb::sdk::ClusterSDK *cs = NULL;
::openmldb::sdk::SQLClusterRouter *sr = NULL;

void PrintResultSet(std::ostream &stream, ::hybridse::sdk::ResultSet *result_set) {
    if (!result_set || result_set->Size() == 0) {
        stream << "Empty set" << std::endl;
        return;
    }
    ::hybridse::base::TextTable t('-', ' ', ' ');
    auto *schema = result_set->GetSchema();
    // Add Header
    for (int32_t i = 0; i < schema->GetColumnCnt(); i++) {
        t.add(schema->GetColumnName(i));
    }
    t.end_of_row();
    while (result_set->Next()) {
        for (int32_t i = 0; i < schema->GetColumnCnt(); i++) {
            if (result_set->IsNULL(i)) {
                t.add("NULL");
                continue;
            }
            auto data_type = schema->GetColumnType(i);
            switch (data_type) {
                case hybridse::sdk::kTypeInt16: {
                    int16_t value = 0;
                    result_set->GetInt16(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case hybridse::sdk::kTypeInt32: {
                    int32_t value = 0;
                    result_set->GetInt32(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case hybridse::sdk::kTypeInt64: {
                    int64_t value = 0;
                    result_set->GetInt64(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case hybridse::sdk::kTypeFloat: {
                    float value = 0;
                    result_set->GetFloat(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case hybridse::sdk::kTypeDouble: {
                    double value = 0;
                    result_set->GetDouble(i, &value);
                    t.add(std::to_string(value));
                    break;
                }
                case hybridse::sdk::kTypeString: {
                    std::string val;
                    result_set->GetString(i, &val);
                    t.add(val);
                    break;
                }
                case hybridse::sdk::kTypeTimestamp: {
                    int64_t ts = 0;
                    result_set->GetTime(i, &ts);
                    t.add(std::to_string(ts));
                    break;
                }
                case hybridse::sdk::kTypeDate: {
                    int32_t year = 0;
                    int32_t month = 0;
                    int32_t day = 0;
                    std::stringstream ss;
                    result_set->GetDate(i, &year, &month, &day);
                    ss << year << "-" << month << "-" << day;
                    t.add(ss.str());
                    break;
                }
                case hybridse::sdk::kTypeBool: {
                    bool value = false;
                    result_set->GetBool(i, &value);
                    t.add(value ? "true" : "false");
                    break;
                }
                default: {
                    t.add("NA");
                }
            }
        }
        t.end_of_row();
    }
    stream << t << std::endl;
    stream << result_set->Size() << " rows in set" << std::endl;
}

void PrintTableIndex(std::ostream &stream, const ::hybridse::vm::IndexList &index_list) {
    ::hybridse::base::TextTable t('-', ' ', ' ');
    t.add("#");
    t.add("name");
    t.add("keys");
    t.add("ts");
    t.add("ttl");
    t.add("ttl_type");
    t.end_of_row();
    for (int i = 0; i < index_list.size(); i++) {
        const ::hybridse::type::IndexDef &index = index_list.Get(i);
        t.add(std::to_string(i + 1));
        t.add(index.name());
        t.add(index.first_keys(0));
        std::string ts_name = index.second_key();
        if (ts_name.empty()) {
            t.add("-");
        } else {
            t.add(index.second_key());
        }
        std::ostringstream oss;
        for (int i = 0; i < index.ttl_size(); i++) {
            oss << index.ttl(i);
            if (i != index.ttl_size() - 1) {
                oss << "m"
                    << ",";
            }
        }
        t.add(oss.str());
        if (index.ttl_type() == ::hybridse::type::kTTLTimeLive) {
            t.add("kAbsolute");
        } else if (index.ttl_type() == ::hybridse::type::kTTLCountLive) {
            t.add("kLatest");
        } else if (index.ttl_type() == ::hybridse::type::kTTLTimeLiveAndCountLive) {
            t.add("kAbsAndLat");
        } else {
            t.add("kAbsOrLat");
        }
        t.end_of_row();
    }
    stream << t;
}

void PrintTableSchema(std::ostream &stream, const ::hybridse::vm::Schema &schema) {
    if (schema.empty()) {
        stream << "Empty set" << std::endl;
        return;
    }
    uint32_t items_size = schema.size();
    ::hybridse::base::TextTable t('-', ' ', ' ');
    t.add("#");
    t.add("Field");
    t.add("Type");
    t.add("Null");
    t.end_of_row();

    for (uint32_t i = 0; i < items_size; i++) {
        auto column = schema.Get(i);
        t.add(std::to_string(i + 1));
        t.add(column.name());
        t.add(::hybridse::type::Type_Name(column.type()));
        t.add(column.is_not_null() ? "NO" : "YES");
        t.end_of_row();
    }
    stream << t;
}

void PrintItems(std::ostream &stream, const std::string &head, const std::vector<std::string> &items) {
    if (items.empty()) {
        stream << "Empty set" << std::endl;
        return;
    }

    ::hybridse::base::TextTable t('-', ' ', ' ');
    t.add(head);
    t.end_of_row();
    for (auto item : items) {
        t.add(item);
        t.end_of_row();
    }
    stream << t;
    auto items_size = items.size();
    if (items_size > 1) {
        stream << items_size << " rows in set" << std::endl;
    } else {
        stream << items_size << " row in set" << std::endl;
    }
}

void PrintItems(const std::vector<std::pair<std::string, std::string>> &items, std::ostream &stream) {
    if (items.empty()) {
        stream << "Empty set" << std::endl;
        return;
    }

    ::hybridse::base::TextTable t('-', ' ', ' ');
    t.add("DB");
    t.add("SP");
    t.end_of_row();
    for (auto item : items) {
        t.add(item.first);
        t.add(item.second);
        t.end_of_row();
    }
    stream << t;
    auto items_size = items.size();
    if (items_size > 1) {
        stream << items_size << " rows in set" << std::endl;
    } else {
        stream << items_size << " row in set" << std::endl;
    }
}

void PrintProcedureSchema(const std::string &head, const ::hybridse::sdk::Schema &sdk_schema, std::ostream &stream) {
    try {
        const ::hybridse::sdk::SchemaImpl &schema_impl = dynamic_cast<const ::hybridse::sdk::SchemaImpl &>(sdk_schema);
        auto &schema = schema_impl.GetSchema();
        if (schema.empty()) {
            stream << "Empty set" << std::endl;
            return;
        }
        uint32_t items_size = schema.size();
        ::hybridse::base::TextTable t('-', ' ', ' ');

        t.add("#");
        t.add("Field");
        t.add("Type");
        t.add("IsConstant");
        t.end_of_row();

        for (uint32_t i = 0; i < items_size; i++) {
            auto column = schema.Get(i);
            t.add(std::to_string(i + 1));
            t.add(column.name());
            t.add(::hybridse::type::Type_Name(column.type()));
            t.add(column.is_constant() ? "YES" : "NO");
            t.end_of_row();
        }
        stream << t << std::endl;
    } catch (std::bad_cast) {
        return;
    }
}

void PrintProcedureInfo(const hybridse::sdk::ProcedureInfo &sp_info) {
    std::vector<std::pair<std::string, std::string>> vec;
    std::pair<std::string, std::string> pair = std::make_pair(sp_info.GetDbName(), sp_info.GetSpName());
    vec.push_back(pair);
    PrintItems(vec, std::cout);
    std::vector<std::string> items{sp_info.GetSql()};
    PrintItems(std::cout, "SQL", items);
    PrintProcedureSchema("Input Schema", sp_info.GetInputSchema(), std::cout);
    PrintProcedureSchema("Output Schema", sp_info.GetOutputSchema(), std::cout);
}

void HandleCmd(const hybridse::node::CmdPlanNode *cmd_node) {
    switch (cmd_node->GetCmdType()) {
        case hybridse::node::kCmdShowDatabases: {
            std::string error;
            std::vector<std::string> dbs;
            auto ns = cs->GetNsClient();
            if (!ns) {
                std::cout << "Fail to connect to db" << std::endl;
            }
            ns->ShowDatabase(&dbs, error);
            PrintItems(std::cout, "Databases", dbs);
            return;
        }

        case hybridse::node::kCmdShowTables: {
            if (db.empty()) {
                std::cout << "please enter database first" << std::endl;
                return;
            }
            auto tables = cs->GetTables(db);
            std::vector<std::string> table_names;
            auto it = tables.begin();
            for (; it != tables.end(); ++it) {
                table_names.push_back((*it)->name());
            }
            PrintItems(std::cout, "Tables", table_names);
            return;
        }

        case hybridse::node::kCmdDescTable: {
            if (db.empty()) {
                std::cout << "please enter database first" << std::endl;
                return;
            }
            auto table = cs->GetTableInfo(db, cmd_node->GetArgs()[0]);
            if (table == nullptr) {
                std::cerr << "table " << cmd_node->GetArgs()[0] << " does not exist" << std::endl;
                return;
            }
            ::hybridse::vm::Schema output_schema;
            ::openmldb::catalog::SchemaAdapter::ConvertSchema(table->column_desc(), &output_schema);
            PrintTableSchema(std::cout, output_schema);
            ::hybridse::vm::IndexList index_list;
            ::openmldb::catalog::SchemaAdapter::ConvertIndex(table->column_key(), &index_list);
            PrintTableIndex(std::cout, index_list);
            break;
        }

        case hybridse::node::kCmdCreateDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            auto ns = cs->GetNsClient();
            std::string error;
            bool ok = ns->CreateDatabase(name, error);
            if (ok) {
                std::cout << "Create database success" << std::endl;
            } else {
                std::cout << "Create database failed for " << error << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdUseDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            std::string error;
            auto ns = cs->GetNsClient();
            bool ok = ns->Use(name, error);
            if (!ok) {
                std::cout << error << std::endl;
            } else {
                db = name;
                std::cout << "Database changed" << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdDropDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            std::string error;
            auto ns = cs->GetNsClient();
            if (!ns->DropDatabase(name, error)) {
                std::cout << error << std::endl;
            } else {
                std::cout << "drop ok" << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdDropTable: {
            if (db.empty()) {
                std::cout << "please enter database first" << std::endl;
                return;
            }
            std::string name = cmd_node->GetArgs()[0];
            std::string error;
            printf("Drop table %s? yes/no\n", name.c_str());
            std::string input;
            std::cin >> input;
            std::transform(input.begin(), input.end(), input.begin(), ::tolower);
            if (input != "yes") {
                printf("'drop %s' cmd is canceled!\n", name.c_str());
                return;
            }
            auto ns = cs->GetNsClient();
            bool ok = ns->DropTable(name, error);
            if (ok) {
                std::cout << "drop ok" << std::endl;
                sr->RefreshCatalog();
            } else {
                std::cout << "failed to drop. error msg: " << error << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdDropIndex: {
            std::string index_name = cmd_node->GetArgs()[0];
            std::string table_name = cmd_node->GetArgs()[1];
            std::string error;
            printf("Drop index %s on %s? yes/no\n", index_name.c_str(), table_name.c_str());
            std::string input;
            std::cin >> input;
            std::transform(input.begin(), input.end(), input.begin(), ::tolower);
            if (input != "yes") {
                printf("'Drop index %s on %s' cmd is canceled!\n", index_name.c_str(), table_name.c_str());
                return;
            }
            auto ns = cs->GetNsClient();
            bool ok = ns->DeleteIndex(table_name, index_name, error);
            if (ok) {
                std::cout << "drop index ok" << std::endl;
            } else {
                std::cout << "Fail to drop index. error msg: " << error << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdShowCreateSp: {
            std::string db_name = cmd_node->GetArgs()[0];
            if (db_name.empty()) {
                if (db.empty()) {
                    std::cout << "please enter database first" << std::endl;
                    return;
                } else {
                    db_name = db;
                }
            }
            std::string sp_name = cmd_node->GetArgs()[1];
            std::string error;
            std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info = cs->GetProcedureInfo(db_name, sp_name, &error);
            if (!sp_info) {
                std::cout << "Fail to show procdure. error msg: " << error << std::endl;
                return;
            }
            PrintProcedureInfo(*sp_info);
            break;
        }
        case hybridse::node::kCmdShowProcedures: {
            std::string error;
            std::vector<std::shared_ptr<hybridse::sdk::ProcedureInfo>> sp_infos = cs->GetProcedureInfo(&error);
            std::vector<std::pair<std::string, std::string>> pairs;
            for (uint32_t i = 0; i < sp_infos.size(); i++) {
                auto &sp_info = sp_infos.at(i);
                pairs.push_back(std::make_pair(sp_info->GetDbName(), sp_info->GetSpName()));
            }
            PrintItems(pairs, std::cout);
            break;
        }
        case hybridse::node::kCmdDropSp: {
            if (db.empty()) {
                std::cout << "please enter database first" << std::endl;
                return;
            }
            std::string sp_name = cmd_node->GetArgs()[0];
            std::string error;
            printf("Drop store procedure %s? yes/no\n", sp_name.c_str());
            std::string input;
            std::cin >> input;
            std::transform(input.begin(), input.end(), input.begin(), ::tolower);
            if (input != "yes") {
                printf("'drop %s' cmd is canceled!\n", sp_name.c_str());
                return;
            }
            auto ns = cs->GetNsClient();
            bool ok = ns->DropProcedure(db, sp_name, error);
            if (ok) {
                std::cout << "drop ok" << std::endl;
            } else {
                std::cout << "failed to drop. error msg: " << error << std::endl;
            }
            break;
        }
        case hybridse::node::kCmdExit: {
            exit(0);
        }
        default: {
            return;
        }
    }
}

void HandleCreateIndex(const hybridse::node::CreateIndexNode *create_index_node) {
    ::openmldb::common::ColumnKey column_key;
    column_key.set_index_name(create_index_node->index_name_);
    for (const auto &key : create_index_node->index_->GetKey()) {
        column_key.add_col_name(key);
    }
    column_key.set_ts_name(create_index_node->index_->GetTs());

    std::string error;
    auto ns = cs->GetNsClient();
    bool ok = ns->AddIndex(create_index_node->table_name_, column_key, nullptr, error);
    if (ok) {
        std::cout << "create index ok" << std::endl;
    } else {
        std::cout << "failed to create index. error msg: " << error << std::endl;
        return;
    }
}

void HandleSQL(const std::string &sql) {
    hybridse::node::NodeManager node_manager;
    hybridse::base::Status sql_status;
    hybridse::node::PlanNodeList plan_trees;
    hybridse::plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, sql_status);

    if (0 != sql_status.code) {
        std::cout << sql_status.msg << std::endl;
        return;
    }
    hybridse::node::PlanNode *node = plan_trees[0];
    switch (node->GetType()) {
        case hybridse::node::kPlanTypeCmd: {
            hybridse::node::CmdPlanNode *cmd =
                dynamic_cast<hybridse::node::CmdPlanNode *>(node);
            HandleCmd(cmd);
            return;
        }
        case hybridse::node::kPlanTypeExplain: {
            std::string empty;
            std::string mu_script = sql;
            mu_script.replace(0u, 7u, empty);
            ::hybridse::sdk::Status status;
            auto info = sr->Explain(db, mu_script, &status);
            if (!info) {
                std::cout << "fail to get explain info" << std::endl;
                return;
            }
            std::cout << info->GetPhysicalPlan() << std::endl;
            return;
        }
        case hybridse::node::kPlanTypeCreate:
        case hybridse::node::kPlanTypeCreateSp: {
            if (db.empty()) {
                std::cout << "please use database first" << std::endl;
                return;
            }
            ::hybridse::sdk::Status status;
            bool ok = sr->ExecuteDDL(db, sql, &status);
            if (!ok) {
                std::cout << "fail to execute ddl" << std::endl;
            } else {
                sr->RefreshCatalog();
            }
            return;
        }
        case hybridse::node::kPlanTypeCreateIndex: {
            if (db.empty()) {
                std::cout << "please use database first" << std::endl;
                return;
            }
            hybridse::node::CreateIndexPlanNode *create_index_node =
                dynamic_cast<hybridse::node::CreateIndexPlanNode *>(node);
            HandleCreateIndex(create_index_node->create_index_node_);
            return;
        }
        case hybridse::node::kPlanTypeInsert: {
            if (db.empty()) {
                std::cout << "please use database first" << std::endl;
                return;
            }
            ::hybridse::sdk::Status status;
            bool ok = sr->ExecuteInsert(db, sql, &status);
            if (!ok) {
                std::cout << "fail to execute insert" << std::endl;
            }
            return;
        }
        case hybridse::node::kPlanTypeFuncDef:
        case hybridse::node::kPlanTypeQuery: {
            if (db.empty()) {
                std::cout << "please use database first" << std::endl;
                return;
            }
            ::hybridse::sdk::Status status;
            auto rs = sr->ExecuteSQL(db, sql, &status);
            if (!rs) {
                std::cout << "fail to execute query" << std::endl;
            } else {
                PrintResultSet(std::cout, rs.get());
            }
            return;
        }
        default: {
        }
    }
}

void HandleCli() {
    if (FLAGS_interactive) {
        std::cout << LOGO << std::endl;
        std::cout << "v" << VERSION << std::endl;
    }
    ::openmldb::sdk::ClusterOptions copt;
    copt.zk_cluster = FLAGS_zk_cluster;
    copt.zk_path = FLAGS_zk_root_path;
    cs = new ::openmldb::sdk::ClusterSDK(copt);
    bool ok = cs->Init();
    if (!ok) {
        std::cout << "Fail to connect to db" << std::endl;
        return;
    }
    sr = new ::openmldb::sdk::SQLClusterRouter(cs);
    if (!sr->Init()) {
        std::cout << "Fail to connect to db" << std::endl;
        return;
    }
    std::string ns_endpoint = cs->GetNsClient()->GetEndpoint();
    std::string display_prefix = ns_endpoint + "/" + db + "> ";
    std::string multi_line_perfix = std::string(display_prefix.length() - 3, ' ') + "-> ";
    std::string sql;
    bool multi_line = false;
    while (true) {
        std::string buffer;
        if (!FLAGS_interactive) {
            buffer = FLAGS_cmd;
            db = FLAGS_database;
        } else {
            char *line = ::openmldb::base::linenoise(multi_line ? multi_line_perfix.c_str() : display_prefix.c_str());
            if (line == NULL) {
                return;
            }
            if (line[0] != '\0' && line[0] != '/') {
                buffer.assign(line);
                if (!buffer.empty()) {
                    ::openmldb::base::linenoiseHistoryAdd(line);
                }
            }
            ::openmldb::base::linenoiseFree(line);
            if (buffer.empty()) {
                continue;
            }
        }
        sql.append(buffer);
        if (sql.back() == ';') {
            HandleSQL(sql);
            multi_line = false;
            display_prefix = ns_endpoint + "/" + db + "> ";
            multi_line_perfix = std::string(display_prefix.length() - 3, ' ') + "-> ";
            sql.clear();
        } else {
            sql.append("\n");
            multi_line = true;
        }
        if (!FLAGS_interactive) {
            return;
        }
    }
}

}  // namespace cmd
}  // namespace openmldb

#endif  // SRC_CMD_SQL_CMD_H_
