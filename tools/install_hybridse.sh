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

# install_hybridse.sh
INSTALL_FROM_SRC=$1
CMAKE_TYPE=$2

HYRBID_VESION=release-0.1.0
if [[ "${CMAKE_TYPE}" != "Debug" ]]; then
        CMAKE_TYPE="RelWithDebInfo"
fi
echo "CMake Type "${CMAKE_TYPE}

export FEDB_THIRDPARTY=${FEDB_DEV_THIRDPARTY:-/depends/thirdparty}
WORK_DIR=`pwd`


# Install hybridse from src
if [[ "${INSTALL_FROM_SRC}" != "SRC" ]]; then
  # Download hybridse lib and include directly
  PACKAGE_NAME=hybridse-release-0.1.0.tar.gz
    echo "get ${PACKAGE_NAME}"
  fi
else
  git clone https://github.com/4paradigm/HybridSE.git
  cd HybridSE && ln -sf ${FEDB_THIRDPARTY} thirdparty && mkdir -p build
  cd build && cmake -DCMAKE_BUILD_TYPE=${CMAKE_TYPE} -DCMAKE_INSTALL_PREFIX="${FEDB_THIRDPARTY}/hybridse" -DTESTING_ENABLE=OFF -DBENCHMARK_ENABLE=OFF -DEXAMPLES_ENABLE=OFF -DCOVERAGE_ENABLE=OFF -DPYSDK_ENABLE=OFF -DJAVASDK_ENABLE=ON ..  && make -j10 install
  # cd ${WORK_DIR}/HybridSE/java/ && mvn install -pl hybridse-common -am
fi