"""
Showing how we can take advantage of in memory caching via Redis if we are
making repeated market data calls externally.

This memory cache is designed to be temporary (and relatively transparent to
the user), rather than long term storage.

For longer term storage, can use IOEngine combined with MongoDB

"""

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
    ###### below line CRUCIAL when running Windows, otherwise multiprocessing
    # doesn"t work! (not necessary on Linux)
    from findatapy.util import SwimPool;

    SwimPool()

    from findatapy.market import Market, MarketDataRequest, MarketDataGenerator
    from findatapy.util import LoggerManager

    market = Market(market_data_generator=MarketDataGenerator())
    logger = LoggerManager().getLogger(__name__)

    # In the config file, we can use keywords "open", "high", "low", "close"
    # and "volume" for alphavantage data

    # Download equities data from yahoo
    md_request = MarketDataRequest(
        start_date="01 Jan 2002",  # start date
        finish_date="05 Feb 2017",  # finish date
        data_source="yahoo",  # use alphavantage as data source
        tickers=["Apple", "Citigroup", "Microsoft", "Oracle", "IBM", "Walmart",
                 "Amazon", "UPS", "Exxon"],  # ticker (findatapy)
        fields=["close"],  # which fields to download
        vendor_tickers=["aapl", "c", "msft", "orcl", "ibm", "wmt", "amzn",
                        "ups", "xom"],  # ticker (yahoo)
        vendor_fields=["Close"],  # which yahoo fields to download
        cache_algo="internet_load_return")

    logger.info("Load data from yahoo directly")
    df = market.fetch_market(md_request)

    print(df)

    logger.info(
        "Loaded data from yahoo directly, now try reading from Redis "
        "in-memory cache")
    md_request.cache_algo = "cache_algo_return"  # change flag to cache algo
    # so won"t attempt to download via web

    df = market.fetch_market(md_request)
    print(df)

    logger.info("Read from Redis cache.. that was a lot quicker!")
