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


# TODO: Needs fixing, given change in APIs

if __name__ == "__main__":
    ###### below line CRUCIAL when running Windows, otherwise multiprocessing 
    # doesn"t work! (not necessary on Linux)
    from findatapy.util import SwimPool;

    SwimPool()

    from findatapy.market import Market, MarketDataRequest, MarketDataGenerator

    market = Market(market_data_generator=MarketDataGenerator())

    # Choose run_example   (0 = all examples)
    # Example 1: bitcoincharts
    # Example 2: poloniex (needs to be fixed)
    # Example 3: binance
    # Example 4: bitfinex
    # Example 5: gdax
    # Example 6: kraken
    #
    # Check findatapy/conf/time_series_tickers_list.csv for all possible tickers
    # Note we use XBT instead of BTC.  Same for XET (ETH) and XLC (LTC).
    #
    # Note: there will generally be a limit of how many data points you can 
    # pull in one call, so you might
    # have to choose smaller time periods

    run_example = 0

    if run_example == 1 or run_example == 0:
        ### Download data from bitcoincharts ###
        # fields contains ["close","volume"]
        # return tick data

        md_request = MarketDataRequest(start_date="11 Nov 2015",
                                       finish_date="02 Feb 2018", cut="LOC",
                                       freq="tick",
                                       data_source="bitcoincharts",
                                       category="crypto",
                                       fields=["close", "volume"],
                                       tickers=["XBTUSD_itbit"])

        df = market.fetch_market(md_request)
        print(df.head(5))
        print(df.tail(5))

    if run_example == 2 or run_example == 0:
        ### Download data from poloniex ###
        # freq : daily or intraday, where intraday - returns 5 minutes data.
        # fields contains ["close","high","low","open","quote-volume","volume","weighted-average"]

        md_request = MarketDataRequest(start_date="18 Feb 2017",
                                       finish_date="20 Feb 2017", cut="LOC",
                                       freq="intraday", data_source="poloniex",
                                       category="crypto",
                                       fields=["close", "volume",
                                               "weighted-average"],
                                       tickers=["STRXBT"])

        df = market.fetch_market(md_request)
        print(df.head(5))
        print(df.tail(5))

    if run_example == 3 or run_example == 0:
        ### download data from binance ###
        # freq: daily or intraday.  intraday - return 1 minute data
        # fields contain ["open","high","low","close","volume","quote-asset-volume",
        #                 "trade-numbers","taker-buy-base-asset-volume","taker-buy-quote-asset-volume"]
        # Note it may take a while due to the limit of numbers per second calling the API

        md_request = MarketDataRequest(start_date="01 Feb 2017",
                                       finish_date="20 Feb 2018", cut="LOC",
                                       freq="daily", data_source="binance",
                                       category="crypto",
                                       fields=["close", "volume",
                                               "quote-asset-volume"],
                                       tickers=["WTCXBT"])

        df = market.fetch_market(md_request)
        print(df.head(5))
        print(df.tail(5))

    if run_example == 4 or run_example == 0:
        ### download data from bitfinex ###
        # freq: daily or intraday.  intraday - return 1 minute data
        # fields contain ["open","close","high","low","volume"]
        # Note it may take a while due to the limit of numbers per second calling the API

        md_request = MarketDataRequest(start_date="11 Feb 2018",
                                       finish_date="20 Feb 2018", cut="LOC",
                                       freq="intraday", data_source="bitfinex",
                                       category="crypto",
                                       fields=["close", "volume", "high",
                                               "open"],
                                       tickers=["XLCUSD"])

        df = market.fetch_market(md_request)
        print(df.head(5))
        print(df.tail(5))

    if run_example == 5 or run_example == 0:
        ### Download data from gdax (now called coinbase pro) ###
        # freq: daily or intraday.  intraday - 1 minute data
        # fields contain ["low","high","open","close","volume"]
        # Note it may take a while due to the limit of numbers per second calling the API

        md_request = MarketDataRequest(start_date="01 Jan 2018 00:00",
                                       finish_date="01 Jan 2018 01:50",
                                       cut="LOC",
                                       freq="intraday", data_source="gdax",
                                       category="crypto",
                                       fields=["close", "volume", "low",
                                               "high"],
                                       tickers=["XBTUSD"])

        df = market.fetch_market(md_request)
        print(df.head(5))
        print(df.tail(5))

    if run_example == 6:
        ### Download data from kraken ###
        # freq: tick (all trade data)
        # fields contain ["close","volume","buy-sell","market-limit"]
        # buy-sell : 1 means buy, and -1 means sell
        # market-limit: 1 means market, and -1 means limit

        # Note it may take a while due to the limit of numbers per second for
        # calling the API

        md_request = MarketDataRequest(start_date="19 Feb 2018",
                                       finish_date="20 Feb 2018", cut="LOC",
                                       freq="tick", data_source="kraken",
                                       category="crypto",
                                       fields=["close", "volume", "buy-sell",
                                               "market-limit"],
                                       tickers=["XBTUSD"])

        df = market.fetch_market(md_request)
        print(df.head(5))
        print(df.tail(5))
