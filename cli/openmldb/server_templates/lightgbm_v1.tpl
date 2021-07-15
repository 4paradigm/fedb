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
import numpy as np
import tornado.web
import tornado.ioloop
import json
import lightgbm as lgb
import sqlalchemy as db
from sqlalchemy_fedb.fedbapi import Type as feType


sql = """{{sql_text}}"""

model_path = sys.argv[1]
bst = lgb.Booster(model_file=model_path)

engine = db.create_engine('fedb:///db_test?zk=127.0.0.1:2181&zkPath=/fedb')
connection = engine.connect()

TypeDict = {feType.Bool:"bool", feType.Int16:"smallint", feType.Int32:"int", feType.Int64:"bigint", feType.Float:"float", feType.Double:"double", feType.String:"string", feType.Date:"date", feType.Timestamp:"timestamp"}

table_schema = [
	("id", "string"),
	("vendor_id", "int"),
	("pickup_datetime", "timestamp"),
	("dropoff_datetime", "timestamp"),
	("passenger_count", "int"),
	("pickup_longitude", "double"),
	("pickup_latitude", "double"),
	("dropoff_longitude", "double"),
	("dropoff_latitude", "double"),
	("store_and_fwd_flag", "string"),
	("trip_duration", "int"),
]

def get_schema():
    dict_schema = {}
    for i in table_schema:
        dict_schema[i[0]] = i[1]
    return dict_schema

dict_schema = get_schema()
json_schema = json.dumps(dict_schema)

def build_feature(rs):
    var_Y = [rs[0]]
    var_X = [rs[1:12]]
    return np.array(var_X)

class SchemaHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(json_schema)

class PredictHandler(tornado.web.RequestHandler):
    def post(self):
        row = json.loads(self.request.body)
        data = {}
        for i in table_schema:
            if i[1] == "string":
                data[i[0]] = row.get(i[0], "")
            elif i[1] == "int" or i[1] == "double" or i[1] == "timestamp" or i[1] == "bigint":
                data[i[0]] = row.get(i[0], 0)
            else:
                data[i[0]] = None
        rs = connection.execute(sql, data)
        for r in rs:
            ins = build_feature(r)
            self.write("----------------ins---------------\n")
            self.write(str(ins) + "\n")
            duration = bst.predict(ins)
            self.write("---------------predict trip_duration -------------\n")
            self.write("%s s"%str(duration[0]))

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("real time execute sparksql demo")

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/schema", SchemaHandler),
        (r"/predict", PredictHandler),
    ])

if __name__ == "__main__":
    app = make_app()
    app.listen(8887)
    tornado.ioloop.IOLoop.current().start()