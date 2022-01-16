__author__ = "saeedamen"  # Saeed Amen

#
# Copyright 2016 Cuemacro
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on a "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pytest
import pandas as pd

from findatapy.market.ioengine import IOEngine

from findatapy.util.dataconstants import DataConstants

data_constants = DataConstants()

redis_server = data_constants.db_cache_server
redis_port = data_constants.db_cache_port


def test_redis_caching():
    # Note: you need to install Redis in order for this to work!

    # read CSV from disk, and make sure to parse dates
    df = pd.read_csv("S&P500.csv", parse_dates=['Date'], index_col=['Date'])
    df.index = pd.to_datetime(df.index)

    io = IOEngine()

    use_cache_compression = [True, False]

    for u in use_cache_compression:
        # Write DataFrame to Redis (using pyarrow format)
        io.write_time_series_cache_to_disk('test_key', df, engine='redis', db_server=redis_server, db_port=redis_port,
                                           use_cache_compression=u)

        # Read back DataFrame from Redis (using pyarrow format)
        df_out = io.read_time_series_cache_from_disk('test_key', engine='redis', db_server=redis_server, db_port=redis_port)

        pd.testing.assert_frame_equal(df, df_out)


def test_path_join():

    io = IOEngine()

    path = io.path_join("/home/hello", "hello", "hello")

    assert path == "/home/hello/hello/hello"

    path = io.path_join("s3://home/hello", "hello", "hello")

    assert path == "s3://home/hello/hello/hello"

if __name__ == '__main__':
    pytest.main()
