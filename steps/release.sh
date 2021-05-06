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
# release.sh
#
set -x

VERSION=`git describe --always --tag`
VERSION=${VERSION:1}
if [[ ! ($VERSION =~ ^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}$) ]]; then
    echo "$VERSION is not release version"
    exit 0
fi

cmake_file="./CMakeLists.txt"
i=1
while((1==1))
do
    value=`echo $1|cut -d "." -f$i`
    if [ "$value" != "" ]
    then
        case "$i" in 
            "1")
                sed -i 's/FEDB_VERSION_MAJOR .*/FEDB_VERSION_MAJOR '${value}')/g' ${cmake_file};;
            "2")
                sed -i 's/FEDB_VERSION_MINOR .*/FEDB_VERSION_MINOR '${value}')/g' ${cmake_file};;
            "3")
                sed -i 's/FEDB_VERSION_BUG .*/FEDB_VERSION_BUG '${value}')/g' ${cmake_file};;
            *)
                echo "xx";;
        esac        
        ((i++))
    else
        break
    fi
done
sed -i "s/version=.*/version='$1',/g" src/sdk/python/sqlalchemy-fedb/setup.py