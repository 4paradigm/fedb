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
import logging
import json
import jinja2
import pathlib


def gen_train_args(args):
    gen_train(args.model, args.label)

def gen_train(model, label):

    supported_models = ["lightgbm_v1"]

    if model not in supported_models:
        logging.error("Use unsupported model, should be one of {}".format(supported_models))
        return
    
    template_file = "{}.tpl".format(model)
    template_vars = {"label": label}
    
    search_path = os.path.join(pathlib.Path(__file__).parent.resolve(), "./train_templates")
    templateLoader = jinja2.FileSystemLoader(searchpath=search_path)
    templateEnv = jinja2.Environment(loader=templateLoader)

    template = templateEnv.get_template(template_file)
    outputText = template.render(template_vars)

    output_file = "train_{}.py".format(model)
    with open(output_file, 'w') as f:
        f.write(outputText)






    
