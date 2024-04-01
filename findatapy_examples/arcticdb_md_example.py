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

    from findatapy.market import Market, MarketDataRequest

    # In this case we are saving predefined tick tickers to disk, and then reading back
    from findatapy.market.ioengine import IOEngine

    md_request_download = MarketDataRequest(
        start_date="04 Jan 2021",
        finish_date="06 Jan 2021",
        category="fx",
        data_source="dukascopy",
        freq="tick",
        tickers=["USDJPY"],
        fields=["bid", "ask", "bidv", "askv"],
        data_engine=None
    )

    market = Market()

    df_tick_earlier = market.fetch_market(md_request=md_request_download)

    io = IOEngine()

    local_storage = True

    # To switch between local storage or s3, it's a matter of changing the
    # connection string (also you need to make sure your AWS S3 authentication is set etc.)
    if local_storage:
        arcticdb_conn_str = "arcticdb:lmdb://tempdatabase?map_size=2GB"
    else:
        # https://docs.arcticdb.io/latest/#s3-configuration gives more details
        # Note we need to prefix arcticdb: to the front so findatapy
        # knows what backend engine to use
        region = "eu-west-2"
        bucket_name = "burger_king_whopper" # Not sure, if this name is taken :-)
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
        "write_style": "write", # "write" / "append" / "update"

        # If set to true will remove any existing library, before writing
        "force_create_library": False,

        # This enables us to take advantage of ArcticDB's filtering of columns/dates
        # otherwise we would download the full dataset, and then filter
        # in Pandas
        "allow_on_disk_filter": True,

        # You can also specify your own custom queries for ArcticDB
        "query_builder": None
    }

    md_request_download.arcticdb_dict = arcticdb_dict

    IOEngine().write_time_series_cache_to_disk(data_frame=df_tick_earlier,
                                               engine=arcticdb_conn_str,
                                               md_request=md_request_download)
    earlier_download_time = datetime.datetime.now().utcnow()

    # Let's read directly from ArcticDB without the Market wrapper using IOEngine
    # This would also be the symbol you can use if you want to directly
    # interact with Arctic
    symbol = "backtest.fx.dukascopy.tick.NYC.USDJPY"
    df_read_tick = IOEngine().read_time_series_cache_from_disk(
        symbol, engine=arcticdb_conn_str)

    print(df_read_tick)

    # Now download a second set of data and write it for "append"
    # Note: we'll get an assertion error, if we try to append before the end
    # of the existing time series on disk
    md_request_download.start_date = "06 Jan 2021"
    md_request_download.finish_date = "07 Jan 2021"
    md_request_download.arcticdb_dict["write_style"] = "append"

    df_tick_later = market.fetch_market(md_request=md_request_download)
    IOEngine().write_time_series_cache_to_disk(data_frame=df_tick_later,
                                               engine=arcticdb_conn_str,
                                               md_request=md_request_download)

    # This time we use the Market wrapper to download data
    # Given we don't specify an "as_of" property, we'll get the later version
    later_download_time = datetime.datetime.now().utcnow()

    md_request_local_cache = MarketDataRequest(
        md_request=md_request_download
    )

    md_request_local_cache.start_date = "04 Jan 2021 10:00"
    md_request_local_cache.finish_date = "06 Jan 2021 14:00"
    md_request_local_cache.data_engine = arcticdb_conn_str
    md_request_local_cache.cache_algo = "cache_algo_return"

    df_read_tick = Market().fetch_market(md_request=md_request_local_cache)

    # We should see the 1st write and 2nd append combined, ie. latest write
    print("No as_of specified, so we'll get the latest write!")
    print(df_read_tick)

    # Let's instead take the first vintage
    md_request_local_cache.as_of = earlier_download_time
    df_read_tick = Market().fetch_market(md_request=md_request_local_cache)

    # We should only see the earlier vintage
    print("See the earlier vintage write!")
    print(df_read_tick)

    # We can also specify the later write time
    md_request_local_cache.as_of = later_download_time
    df_read_tick = Market().fetch_market(md_request=md_request_local_cache)

    # We should only see the latest vintage
    print("See the latest vintage write!")
    print(df_read_tick)

    # Finally let's try doing an update of an existing continuous chunk
    # Note: if part of our update ends up being before/after the existing
    # dataset, it will fail
    md_request_download.start_date = "06 Jan 2021"
    md_request_download.finish_date = "07 Jan 2021"
    md_request_download.arcticdb_dict["write_style"] = "update"

    df_tick_later = market.fetch_market(md_request=md_request_download)

    # Modify the date, so we can see the obvious difference!
    df_tick_later = df_tick_later * 10.0
    IOEngine().write_time_series_cache_to_disk(data_frame=df_tick_later,
                                               engine=arcticdb_conn_str,
                                               md_request=md_request_download)

    md_request_local_cache.start_date = "06 Jan 2021 10:00"
    md_request_local_cache.finish_date = "08 Jan 2021 14:00"
    md_request_local_cache.as_of = None
    df_read_updated_tick = Market().fetch_market(md_request=md_request_local_cache)

    print("Updated tick (should be 10 larger!)")
    print(df_read_updated_tick)


