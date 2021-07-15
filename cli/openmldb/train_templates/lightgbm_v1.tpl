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
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV


def main():
    #csv_file_path = "/tmp/openmldb_output_csv/part-00000-ccbae5f7-de06-4a40-9684-e8af6dba6455-c000.csv"

    intput_path = sys.argv[1]

    train_set = pd.read_csv(intput_path)

    label_col = "{{ label }}"

    y_train = train_set[label_col]
    x_train = train_set.drop(columns=[label_col])
    y_predict = train_set[label_col]
    x_predict = train_set.drop(columns=[label_col])

    lgb_train = lgb.Dataset(x_train, y_train)
    lgb_eval = lgb.Dataset(x_predict, y_predict, reference=lgb_train)

    params = {
        'boosting_type': 'gbdt',
        'objective': 'regression',
        'metric': {'l2', 'l1'},
        'num_leaves': 31,
        'learning_rate': 0.05,
        'feature_fraction': 0.9,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'verbose': 0
    }

    print('Starting training...')
    gbm = lgb.train(params,
                    lgb_train,
                    num_boost_round=20,
                    valid_sets=lgb_eval,
                    early_stopping_rounds=5)
    model_file_path = '/tmp/lightgbm_gbdt_model.txt'
    gbm.save_model(model_file_path)

    print("save model file in {}".format(model_file_path))


if __name__ == "__main__":
    main()