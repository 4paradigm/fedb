/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "base/time_series_pool.h"

#include <vector>

#include "gtest/gtest.h"

namespace openmldb {
namespace base {

class TimeSeriesPoolTest : public ::testing::Test {
 public:
    TimeSeriesPoolTest() {}
    ~TimeSeriesPoolTest() {}
};

TEST_F(TimeSeriesPoolTest, FreeToEmpty) {
    TimeSeriesPool pool(1024);
    std::vector<uint64_t> times;
    const int datasize = 1024 / 2;
    char *data = new char[datasize];
    for (int i = 0; i < datasize; ++i) data[i] = i * i * i;
    for (int i = 0; i < 1000; ++i) {
        auto time = (i * i % 7) * (60 * 60 * 1000);
        auto ptr = pool.Alloc(datasize, time);
        memcpy(ptr, data, datasize);
        times.push_back(time);
    }

    for (auto time : times) pool.Free(time);

    ASSERT_TRUE(pool.Empty());
}

}  // namespace base
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
