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

import logging
import json
import os
from pyspark.sql import SparkSession


def batch_sql_args(args):
    batch_sql(args.sql_statement)


def batch_sql(sql_statement):
    # Create SparkSession
    spark_builder = SparkSession.builder.appName("BatchSql").master("local[*]")
    spark = spark_builder.getOrCreate()

    # Get imported tables
    tables_config = {}
    tables_config_path = os.path.expanduser("~") + "/.openmldb/tables.json"
    if os.path.isfile(tables_config_path):
        with open(tables_config_path) as f:
            tables_config = json.load(f)

    # Register tables
    for k, v in tables_config.items():
        spark.read.parquet(v).createOrReplaceTempView(k)

    # Run SQL
    output_df = spark.sql(sql_statement)
    output_df.show(10)
