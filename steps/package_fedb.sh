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
ln -sf /usr/workdir/thirdparty thirdparty  || :
ln -sf /usr/workdir/thirdsrc thirdsrc || :

package=fedb-cluster-$1
rm -rf ${package} || :
mkdir ${package} || :
cp -r release/conf ${package}/conf
cp -r release/bin ${package}/bin
IP=127.0.0.1
sed -i "s/--endpoint=.*/--endpoint=${IP}:6527/g" ${package}/conf/nameserver.flags
sed -i "s/--zk_cluster=.*/--zk_cluster=${IP}:2181/g" ${package}/conf/nameserver.flags
sed -i "s/--zk_root_path=.*/--zk_root_path=\/fedb/g" ${package}/conf/nameserver.flags

sed -i "s/--endpoint=.*/--endpoint=${IP}:9527/g" ${package}/conf/tablet.flags
sed -i "s/#--zk_cluster=.*/--zk_cluster=${IP}:2181/g" ${package}/conf/tablet.flags
sed -i "s/#--zk_root_path=.*/--zk_root_path=\/fedb/g" ${package}/conf/tablet.flags

cp -r tools ${package}/tools
ls -l build/bin/
cp -r build/bin/fedb ${package}/bin/fedb
test -e build/bin/fedb_mac &&  cp -r build/bin/fedb_mac ${package}/bin/fedb_mac_cli
cd ${package}/bin
cd ../..
tar -cvzf ${package}.tar.gz ${package}
echo "package at ./${package}"

