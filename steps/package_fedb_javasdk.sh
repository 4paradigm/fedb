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

#! /bin/sh
#
# package.sh
#
WORKDIR=`pwd`
set -e
sdk_vesion=$1-SNAPSHOT
mkdir -p src/sdk/java/sql-native/src/main/resources/
sh steps/install_hybridse.sh
test -f build/src/sdk/libsql_jsdk.dylib && cp build/src/sdk/libsql_jsdk.dylib  src/sdk/java/sql-native/src/main/resources/
mkdir -p build && cd build &&  cmake -DSQL_JAVASDK_ENABLE=ON .. && make -j4 sql_jsdk
cd ${WORKDIR}
cp build/src/sdk/libsql_jsdk.so  src/sdk/java/sql-native/src/main/resources/
cd src/sdk/java/ &&  /opt/bin/mvn versions:set -DnewVersion=${sdk_vesion} && /opt/bin/mvn deploy -Dmaven.test.skip=true
