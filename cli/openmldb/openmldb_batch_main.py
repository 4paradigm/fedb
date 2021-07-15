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
import sys
import json
from pyspark.sql import SparkSession


def main():
    spark_builder = SparkSession.builder.appName("OpenMLDB-Batch")
    spark = spark_builder.getOrCreate()

    # Get imported tables
    tables_config = {}
    tables_config_path = "./tables.json"
    if os.path.isfile(tables_config_path):
        with open(tables_config_path) as f:
            tables_config = json.load(f)

    # Register tables
    for k, v in tables_config.items():
        spark.read.parquet(v).createOrReplaceTempView(k)

    
    # Read SQL file
    sql_file = sys.argv[1]

    with open(sql_file, "r") as f:
        sql_text = f.read()
        print(sql_text)

    
    # Read output path
    output_path = "hdfs:///user/tobe/openmldb_batch_output/"
    output_format = "csv"

    # Run SparkSQL
    output_df = spark.sql(sql_text)
    if output_format == "parquet":
        output_df.write.mode("overwrite").parquet(output_path)
    elif output_format == "csv":
        output_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
    else:
        logging.error("Unsupport output format: {}".format(output_format))
    
    
if __name__ == "__main__":
    main()