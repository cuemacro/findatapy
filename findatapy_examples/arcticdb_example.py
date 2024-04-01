__author__ = "saeedamen"  # Saeed Amen

#
# Copyright 2024 Cuemacro
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

import datetime
import time

if __name__ == '__main__':
    ###### below line CRUCIAL when running Windows, otherwise multiprocessing doesn't work! (not necessary on Linux)
    from findatapy.util import SwimPool;

    SwimPool()

    # This is the new ArcticDB, which uses disk based storage, as opposed
    # to a database engine like MongoDB as a backend
    from findatapy.market import Market, MarketDataRequest, \
        MarketDataGenerator, IOEngine

    market = Market(market_data_generator=MarketDataGenerator())

    # In the config file, we can use keywords 'open', 'high', 'low', 'close'
    # and 'volume' for Yahoo data

    # Download equities data from Yahoo
    md_request = MarketDataRequest(
        start_date="01 Mar 2020",  # start date
        data_source="yahoo",  # use yahoo as data source
        tickers=["Apple", 'Nvidia'],  # ticker (findatapy)
        fields=["close"],  # which fields to download
        vendor_tickers=["aapl", "nvda"],  # ticker (Yahoo)
        vendor_fields=["Close"])  # which Yahoo fields to download)

    df = market.fetch_market(md_request)

    io = IOEngine()

    arcticdb_conn_str = "arcticdb:lmdb://tempdatabase?map_size=2GB"

    # To switch between local storage or s3, it's a matter of changing the
    # connection string (also you need to make sure your AWS S3 authentication is set etc.)
    if local_storage:
        arcticdb_conn_str = "arcticdb:lmdb://tempdatabase?map_size=2GB"
    else:
        # https://docs.arcticdb.io/latest/#s3-configuration gives more details
        # Note we need to prefix arcticdb: to the front so findatapy
        # knows what backend engine to use
        region = "eu-west-2"
        bucket_name = "burger_king_whopper"  # Not sure, if this name is taken :-)
        path_prefix = "test"

        arcticdb_conn_str = f"arcticdb:s3s://s3.{region}.amazonaws.com:{bucket_name}?path_prefix={path_prefix}&aws_auth=true"

    # Set various parameters to govern how we write to ArcticDB, to use
    # versioning
    arcticdb_dict = {
        # If this is set to true removes previous versions (so we only record
        # the final version). Not pruning versions will take more disk space
        "prune_previous_versions": False,

        # Do we want to append to existing records or write
        # If you attempt to append with an overlapping chunk, you'll
        # get an assertion failure, "update" allows you to change existing data
        "write_style": "write",  # "write" / "append" / "update"

        # If set to true will remove any existing library, before writing
        "force_create_library": False,

        # This enables us to take advantage of ArcticDB's filtering of columns/dates
        # otherwise we would download the full dataset, and then filter
        # in Pandas
        "allow_on_disk_filter": True,

        # You can also specify your own custom queries for ArcticDB
        "query_builder": None
    }

    current_time_list = []

    for i in range(0, 3):

        # Perturb by a small amount to differientate time series versions
        df_to_write = df * (1 + i / 100)

        # Note: we will use the lmdb interface which is for local filestorage
        # with arcticdb (it also supports s3, azure etc.)
        io.write_time_series_cache_to_disk(
            "stocks", df_to_write, engine=arcticdb_conn_str,
            arcticdb_dict=arcticdb_dict)

        current_time = datetime.datetime.now().utcnow()
        current_time_list.append(current_time)
        time.sleep(5)

    for i in range(0, 3):
        # Read back from Arctic
        df_arcticdb = io.read_time_series_cache_from_disk(
            "stocks", engine=arcticdb_conn_str,
            arcticdb_dict=arcticdb_dict,
            as_of=current_time_list[i])

        print(f"Read version as of {str(current_time_list[i])}")
        print(df_arcticdb.tail(n=5))

    print("Read without as of parameter - will return the last version")

    # If we do not specific the as_of, we get the final version
    df_arcticdb = io.read_time_series_cache_from_disk(
        "stocks", engine=arcticdb_conn_str,
        arcticdb_dict=arcticdb_dict)

    print(df_arcticdb.tail(n=5))
