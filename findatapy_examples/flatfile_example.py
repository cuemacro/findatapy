__author__ = 'saeedamen'  # Saeed Amen

#
# Copyright 2016-2020 Cuemacro
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
# License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and limitations under the License.
#


if __name__ == '__main__':
    ###### below line CRUCIAL when running Windows, otherwise multiprocessing doesn't work! (not necessary on Linux)
    from findatapy.util import SwimPool; SwimPool()

    from findatapy.market import Market, MarketDataRequest, MarketDataGenerator

    market = Market(market_data_generator=MarketDataGenerator())

    # choose run_example = 0 for everything
    # run_example = 1 - read in CSV file (daily)
    # run_example = 2 - read in HD5 file in market data (intraday)

    run_example = 0

    if run_example == 1 or run_example == 0:

        md_request = MarketDataRequest(
            start_date='01 Jan 2002', finish_date='31 Jan 2016',
            tickers='S&P500',
            fields='close',
            data_source="../tests/S&P500.csv",
            freq='daily',
        )

        market = Market(market_data_generator=MarketDataGenerator())

        df = market.fetch_market(md_request=md_request)

        print(df)

    if run_example == 2 or run_example == 0:
        # load tick data from DukasCopy and then resample to 15s buckets
        md_request = MarketDataRequest(
            start_date='01 Jun 2016', finish_date='31 Jul 2016',
            tickers='EURUSD',
            fields='bid',
            data_source="../tests/EURUSD_tick.h5",
            freq='intraday',
            resample='15s',
            resample_how='last_dropna'
        )

        market = Market(market_data_generator=MarketDataGenerator())

        df = market.fetch_market(md_request=md_request)

        print(df)

        # the second time we call it, if we have Redis installed, we will fetch from memory, so it will be quicker
        # also don't need to run the resample operation again

        # need to specify cache_algo_return
        md_request.cache_algo = 'cache_algo_return'

        df = market.fetch_market(md_request)

        print(df)