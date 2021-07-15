#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse


"""
from .version import version_args
from .import_table import import_table_args
from .upsert import upsert_args
from .batch_run import batch_run_args
from .batch_sql import batch_sql_args
from .deploy import deploy_args
from .online_extraction import online_extraction_args
from .gen_train import gen_train_args
from .gen_server import gen_server_args
"""
from version import version_args
from import_table import import_table_args
from upsert import upsert_args
from batch_run import batch_run_args
from batch_sql import batch_sql_args
from deploy import deploy_args
from online_extraction import online_extraction_args
from gen_train import gen_train_args
from gen_server import gen_server_args



def main():
    parser = argparse.ArgumentParser(prog="openmldb")
    subparsers = parser.add_subparsers()

    version_command = subparsers.add_parser("version", help="Print the version of openmldb")
    version_command.set_defaults(func=version_args)

    import_command = subparsers.add_parser("import", help="import $table_desc")
    import_command.add_argument("table_desc", type=str, help="The desc of table(e.g. t1:file:///tmp/)")
    import_command.set_defaults(func=import_table_args)

    upsert_command = subparsers.add_parser("upsert", help="upsert $row_desc")
    upsert_command.add_argument("row_desc", type=str, help="The desc of row")
    upsert_command.set_defaults(func=upsert_args)

    batch_run_command = subparsers.add_parser("batch_run", help="batch_run $yaml_path")
    batch_run_command.add_argument("yaml_path", type=str, help="The path of yaml file")
    batch_run_command.set_defaults(func=batch_run_args)

    batch_sql_command = subparsers.add_parser("batch_sql", help="batch_sql $sql_statement")
    batch_sql_command.add_argument("sql_statement", type=str, help="The statement of sql")
    batch_sql_command.set_defaults(func=batch_sql_args)

    deploy_command = subparsers.add_parser("deploy", help="deploy $sql_file")
    deploy_command.add_argument("sql_file", type=str, help="The SQL file")
    deploy_command.set_defaults(func=deploy_args)

    online_extraction_command = subparsers.add_parser("online_extraction", help="online_extraction $json_file")
    online_extraction_command.add_argument("sql_file", type=str, help="The SQL file")
    online_extraction_command.add_argument("json_file", type=str, help="The JSON file")
    online_extraction_command.set_defaults(func=online_extraction_args)

    gen_train_command = subparsers.add_parser("gen_train", help="gen_train $model $label")
    gen_train_command.add_argument("model", type=str, help="The model type")
    gen_train_command.add_argument("label", type=str, help="The label column for training")
    gen_train_command.set_defaults(func=gen_train_args)

    gen_server_command = subparsers.add_parser("gen_server", help="gen_server $model $sql_file")
    gen_server_command.add_argument("model", type=str, help="The model type")
    gen_server_command.add_argument("sql_file", type=str, help="The SQL file")
    gen_server_command.set_defaults(func=gen_server_args)


    args = parser.parse_args()
    if getattr(args, "func", None):
        args.func(args)
    else :
        # Print help messing if not passing any argument
        parser.print_help()


if __name__ == "__main__":
    main()
