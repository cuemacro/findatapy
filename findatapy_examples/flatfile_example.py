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

if __name__ == "__main__":
    ###### below line CRUCIAL when running Windows, otherwise multiprocessing doesn"t work! (not necessary on Linux)
    from findatapy.util import SwimPool;

    SwimPool()

    from findatapy.market import Market, MarketDataRequest, MarketDataGenerator

    market = Market(market_data_generator=MarketDataGenerator())

    # choose run_example = 0 for everything
    # run_example = 1 - read in CSV file (daily)
    # run_example = 2 - read in HD5 file in market data (intraday)
    # run_example = 3 - save to disk for Quandl as Parquet (defined tickers)
    # and read back with MarketDataRequest
    # run_example = 4 - save to disk for Dukascopy as Parquet (defined tickers
    # for EURUSD) and read back with MarketDataRequest

    # NOTE: you need to make sure you have the correct data licences before
    # storing data on disk (and whether other
    # users can access it)

    run_example = 4

    if run_example == 1 or run_example == 0:
        md_request = MarketDataRequest(
            start_date="01 Jan 2002", finish_date="31 Jan 2016",
            tickers="S&P500",
            fields="close",
            data_source="../tests/S&P500.csv",
            freq="daily",
        )

        market = Market(market_data_generator=MarketDataGenerator())

        df = market.fetch_market(md_request=md_request)

        print(df)

    if run_example == 2 or run_example == 0:
        # load tick data from DukasCopy and then resample to 15s buckets
        md_request = MarketDataRequest(
            start_date="01 Jun 2016", finish_date="31 Jul 2016",
            tickers="EURUSD",
            fields="bid",
            data_source="../tests/EURUSD_tick.h5",
            freq="intraday",
            resample="15s",
            resample_how="last_dropna"
        )

        market = Market(market_data_generator=MarketDataGenerator())

        df = market.fetch_market(md_request=md_request)

        print(df)

        # the second time we call it, if we have Redis installed, we will fetch
        # from memory, so it will be quicker
        # also don"t need to run the resample operation again

        # need to specify cache_algo_return
        md_request.cache_algo = "cache_algo_return"

        df = market.fetch_market(md_request)

        print(df)

    if run_example == 3:
        # In this case we are saving predefined daily tickers to disk, and then
        # reading back
        from findatapy.util.dataconstants import DataConstants
        from findatapy.market.ioengine import IOEngine
        import os

        quandl_api_key = DataConstants().quandl_api_key  # change with your own
        # Quandl API key!

        md_request = MarketDataRequest(
            category="fx",
            data_source="quandl",
            freq="daily",
            quandl_api_key=quandl_api_key
        )

        market = Market(market_data_generator=MarketDataGenerator())

        df = market.fetch_market(md_request=md_request)

        print(df)

        folder = "../tests/"

        # Save to disk in a file name format friendly for reading later via
        # MarketDataRequest (ie. ../tests/backtest.fx.daily.quandl.NYC.parquet)
        IOEngine().write_time_series_cache_to_disk(folder, df,
                                                   engine="parquet",
                                                   md_request=md_request)

        md_request.data_engine = "../tests/*.parquet"

        df = market.fetch_market(md_request)

        print(df)

    if run_example == 4:
        # In this case we are saving predefined tick data tickers to disk, and
        # then reading back using the MarketDataRequest interface
        from findatapy.util.dataconstants import DataConstants
        from findatapy.market.ioengine import IOEngine

        md_request = MarketDataRequest(
            start_date="01 Jan 2021",
            finish_date="05 Jan 2021",
            category="fx",
            data_source="dukascopy",
            freq="tick",
            tickers=["EURUSD"],
            fields=["bid", "ask", "bidv", "askv"],
        )

        market = Market(market_data_generator=MarketDataGenerator())

        df = market.fetch_market(md_request=md_request)

        print(df)

        folder = "../tests/"

        # Save to disk in a format friendly for reading later
        # (ie. ../tests/backtest.fx.tick.dukascopy.NYC.EURUSD.parquet)
        # Here it will automatically generate the filename from the folder we
        # gave and the MarketDataRequest we made (altenatively, we could have
        # just given the filename directly)
        IOEngine().write_time_series_cache_to_disk(folder, df,
                                                   engine="parquet",
                                                   md_request=md_request)

        md_request.data_engine = "../tests/*.parquet"
        df = market.fetch_market(md_request)

        print(df)

        # Let's try the same thing with CSV too! But this will be slower given
        # it's CSV format...!
        IOEngine().write_time_series_cache_to_disk(folder, df, engine="csv",
                                                   md_request=md_request)

        md_request.data_engine = "../tests/*.csv"
        df = market.fetch_market(md_request)

        print(df)
