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

import time
import logging
import yaml
import os
import json
import subprocess
from pyspark.sql import SparkSession


def batch_run_args(args):
    batch_run(args.yaml_path)


def submit_local(yaml_config):

    # Create SparkSession
    spark_builder = SparkSession.builder
    for k, v in yaml_config.get("sparkConfig", {}).items():
        if k == "name":
            spark_builder = spark_builder.appName(v)
        elif k == "master":
            spark_builder = spark_builder.master(v)
        else:
            spark_builder = spark_builder.config(k, v)
    spark = spark_builder.getOrCreate()

    # Get imported tables
    tables_config = {}
    tables_config_path = os.path.expanduser("~") + "/.openmldb/tables.json"
    if os.path.isfile(tables_config_path):
        with open(tables_config_path) as f:
            tables_config = json.load(f)
    
    # Register tables from yaml
    for k, v in yaml_config.get("tables", {}).items():
        if k in tables_config:
            logging.warning("The table {} has been imported, ignore now")
        else:
            tables_config[k] = v

    # Register tables from config
    for k, v in tables_config.items():
        spark.read.parquet(v).createOrReplaceTempView(k)

    # Read SQL file
    if "sql_file" in yaml_config:
        with open(yaml_config.get("sql_file"), "r") as f:
            sql_text = f.read()
    else:
        sql_text = yaml_config.get("sql_text")

    # Read output path
    output_path = yaml_config.get("output_path", "file:///tmp/openmldb_output/")
    output_format = yaml_config.get("output_format", "parquet")

    # Run SparkSQL
    output_df = spark.sql(sql_text)
    if output_format == "parquet":
        output_df.write.mode("overwrite").parquet(output_path)
    elif output_format == "csv":
        output_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
    else:
        logging.error("Unsupport output format: {}".format(output_format))



def submit_yarn_cluster(yaml_config):
    print("Sumbit to yarn")

    spark_submit_cmd = """
    $SPARK_HOME/bin/spark-submit \
     --master yarn --deploy-mode cluster \
     --num-executors 1 \
     --executor-cores 1 \
     --driver-memory 1g \
     --executor-memory 1g \
     --conf spark.yarn.maxAppAttempts=1 \
     --files /Users/tobe/.openmldb/tables.json,../examples/taxi_tour.sql \
     ./openmldb_batch_main.py taxi_tour.sql
    """.format()

    print(spark_submit_cmd)

    cmd_output = subprocess.run(spark_submit_cmd, shell=True, check=True, stderr=subprocess.PIPE)
    cmd_stderr = cmd_output.stderr.decode("utf-8")

    print(cmd_stderr)


def batch_run(yaml_path):
    # TODO: Check if necessary keys are exist

    # Read yaml file
    try:
        yaml_config = yaml.load(open(yaml_path, "r"))
    except yaml.YAMLError as exc:
        mark = exc.problem_mark
        logging.error("Error position: (%s:%s)" % (mark.line + 1, mark.column + 1))
        exit(-1)

    master = yaml_config.get("sparkConfig", {}).get("master", "local")

    if master.startswith("local"):
        submit_local(yaml_config)
    elif master == "yarn-cluster":
        submit_yarn_cluster(yaml_config)
    else:
        logging.error("Unsupported master config: {}".format(master))
