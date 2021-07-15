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

import sys
import datetime
import sqlalchemy as db


def deploy_args(args):
    deploy_sql(args.sql_file)


def deploy_sql(sql_file):
    engine = db.create_engine('fedb:///db_test?zk=127.0.0.1:2181&zkPath=/fedb')
    connection = engine.connect()

    # TODO: Generate DDL
    with open(sql_file, "r") as f:
        sql_text = f.read()

    # TODO: Create table and index


    ddl="""
create table t1(
id string,
vendor_id int,
pickup_datetime timestamp,
dropoff_datetime timestamp,
passenger_count int,
pickup_longitude double,
pickup_latitude double,
dropoff_longitude double,
dropoff_latitude double,
store_and_fwd_flag string,
trip_duration int,
index(key=vendor_id, ts=pickup_datetime),
index(key=passenger_count, ts=pickup_datetime)
);
"""

    try:
        connection.execute("create database db_test;");
    except Exception as e:
        print(e)
    try:
        connection.execute(ddl);
    except Exception as e:
        print(e)



    def insert_row(line):
        row = line.split(',')
        row[2] = '%dl'%int(datetime.datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
        row[3] = '%dl'%int(datetime.datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
        insert = "insert into t1 values('%s', %s, %s, %s, %s, %s, %s, %s, %s, '%s', %s);"% tuple(row)
        connection.execute(insert)


    # TODO: Import data from offline
    
    train_csv_path = "/OpenMLDB/cli/examples/taxi_tour_table_train_simple.csv"

    with open(train_csv_path, 'r') as fd:
        idx = 0
        for line in fd:
            if idx == 0:
                idx = idx + 1
                continue
            insert_row(line.replace('\n', ''))
            idx = idx + 1