"""
Showing how we can take advantage of in memory caching via Redis if we are making repeated market data calls externally.
This memory cache is designed to be temporary (and relatively transparent to the user), rather than long term storage.

For longer term storage, can use IOEngine combined with MongoDB

See below for example of speed increase 2ms when fetching from cache vs nearly a second for downloading from Yahoo directly.

There is a much bigger difference when we download large amounts of data (eg. intraday minute data)

2017-02-11 13:54:53,639 - __main__ - INFO - Load data from Yahoo directly
2017-02-11 13:54:54,119 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:54,120 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:54,120 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:54,127 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:54,127 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:54,128 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:54,129 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:54,680 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:54,687 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:54,691 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:54,693 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:54,749 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:54,765 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:54,770 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:54,780 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:54,791 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:54,938 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:55,013 - findatapy.market.ioengine - INFO - Pushed MarketDataRequest_443__cache_algo-internet_load_return__category-None__cut-NYC__data_source-yahoo__environment-backtest__expiry_date-None__fields-close__finish_date-2017-02-05 00:00:00__freq-daily__freq_mult-1__futures_curve-None__futures_curve_key-None__gran_freq-None__start_date-2002-01-01 00:00:00__tickers-Apple_Citigroup_Microsoft_Oracle_IBM_Walmart_Amazon_UPS_Exxon__trade_side-trade__vendor_fields-Close__vendor_tickers-aapl_c_msft_orcl_ibm_wmt_amzn_ups_xom to Redis
2017-02-11 13:54:55,014 - __main__ - INFO - Loaded data from Yahoo directly, now try reading from Redis in-memory cache
2017-02-11 13:54:55,025 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:55,026 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:55,028 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:55,029 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:55,030 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:55,030 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:55,031 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:55,033 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:55,336 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:55,345 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-11 13:54:55,368 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:55,425 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:55,436 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:55,440 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:55,445 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:55,451 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:55,455 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:55,555 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-11 13:54:55,642 - findatapy.market.ioengine - INFO - Pushed MarketDataRequest_440__cache_algo-cache_algo_return__category-None__cut-NYC__data_source-yahoo__environment-backtest__expiry_date-None__fields-close__finish_date-2017-02-05 00:00:00__freq-daily__freq_mult-1__futures_curve-None__futures_curve_key-None__gran_freq-None__start_date-2002-01-01 00:00:00__tickers-Apple_Citigroup_Microsoft_Oracle_IBM_Walmart_Amazon_UPS_Exxon__trade_side-trade__vendor_fields-Close__vendor_tickers-aapl_c_msft_orcl_ibm_wmt_amzn_ups_xom to Redis
2017-02-11 13:54:55,643 - __main__ - INFO - Read from Redis cache.. that was a lot quicker!

"""

__author__ = 'saeedamen'  # Saeed Amen

#
# Copyright 2016 Cuemacro
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
    from findatapy.util import LoggerManager

    market = Market(market_data_generator=MarketDataGenerator())
    logger = LoggerManager().getLogger(__name__)

    # in the config file, we can use keywords 'open', 'high', 'low', 'close' and 'volume' for Google finance data

    # download equities data from Google
    md_request = MarketDataRequest(
        start_date="01 Jan 2002",       # start date
        finish_date="05 Feb 2017",      # finish date
        data_source='google',           # use Google Finance as data source
        tickers=['Apple', 'Citigroup', 'Microsoft', 'Oracle', 'IBM', 'Walmart', 'Amazon', 'UPS', 'Exxon'],  # ticker (findatapy)
        fields=['close'],               # which fields to download
        vendor_tickers=['aapl', 'c', 'msft', 'orcl', 'ibm', 'wmt', 'amzn', 'ups', 'xom'],                   # ticker (Yahoo)
        vendor_fields=['Close'],        # which Google Finance fields to download
        cache_algo='internet_load_return')

    logger.info("Load data from Google directly")
    df = market.fetch_market(md_request)

    logger.info("Loaded data from Google directly, now try reading from Redis in-memory cache")
    md_request.cache_algo = 'cache_algo_return' # change flag to cache algo so won't attempt to download via web

    df = market.fetch_market(md_request)

    logger.info("Read from Redis cache.. that was a lot quicker!")
