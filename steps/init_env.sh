#!/bin/sh
#
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

# init_env.sh

cd /depends
if [[ ! -d thirdparty && -r thirdparty.tar.gz ]]; then
    mkdir -p thirdparty
    tar xzf thirdparty.tar.gz -C thirdparty --strip-components=1
    curl -SL -o libzetasql.tar.gz https://github.com/jingchen2222/zetasql/releases/download/v0.2.0/libzetasql-0.2.0-linux-x86_64.tar.gz
    tar xzf libzetasql.tar.gz -C  thirdparty --strip-components 1
    rm libzetasql.tar.gz
fi


rm -rf thirdparty/hybridse
mkdir -p thirdparty/hybridse
PACKAGE_NAME=hybridse-0.2.0-linux-x86_64
curl -LO https://github.com/4paradigm/HybridSE/releases/download/v0.2.0/hybridse-0.2.0-linux-x86_64.tar.gz
tar zxf ${PACKAGE_NAME}.tar.gz > /dev/null
mv ${PACKAGE_NAME}/* thirdparty/hybridse
cd -
ln -sf /depends/thirdparty thirdparty
ln -sf /depends/thirdsrc thirdsrc
