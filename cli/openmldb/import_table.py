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


import os
import shutil
import logging
import json


def import_table_args(args):
    import_table(args.table_desc)


def import_table(table_desc):
    # Parse the table desc, example: "t1:file:///tmp/taxi_tour_parquet/"
    seperator_index = table_desc.index(":")
    table_name = table_desc[:seperator_index]
    table_path = table_desc[seperator_index+1:]

    # Create the config dir if not exist
    openmldb_config_dir = os.path.expanduser("~") + "/.openmldb/"
    if not os.path.isdir(openmldb_config_dir):
        os.makedirs(openmldb_config_dir, exist_ok=True)

    # Load the config if exist
    tables_config = {}
    tables_config_path = openmldb_config_dir + "tables.json"
    if os.path.isfile(tables_config_path):
        with open(tables_config_path) as f:
            tables_config = json.load(f)

    if table_name in tables_config:
        # Do not import for the same table name
        logging.warning("Table {} has been registered".format(table_name))
    else:
        # Import and write in config file
        with open(tables_config_path, "w") as f:
            tables_config[table_name] = table_path
            f.write(json.dumps(tables_config, indent=4, sort_keys=True))
            logging.info("Success to register table {}. Current tables config: {}".format(table_name, tables_config))
