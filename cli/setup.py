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

from setuptools import setup, find_packages

from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="openmldb",
    version="0.1.4.1",
    author="4Paradigm",
    author_email="chendihao@4paradigm.com",
    url="https://github.com/4paradigm/OpenMLDB",
    description= "The client of OpenMLDB",
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=["openmldb"],
    install_requires=["pyspark==3.0.0", "prettytable>=2.1.0", "sqlalchemy"],
    include_package_data=True,
    zip_safe=False,
    license="Apache License 2.0",
    entry_points={
        "console_scripts": [
            "openmldb=openmldb.openmldb:main"
        ]
    }
)
