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

    market = Market(market_data_generator=MarketDataGenerator())

    # in the config file, we can use keywords "open", "high", "low", "close"
    # and "volume" for Yahoo and Google finance data

    # download equities data from Alpha Vantage
    md_request = MarketDataRequest(
        start_date="decade",  # start date
        data_source="alphavantage",  # use Bloomberg as data source
        tickers=["Apple", "Microsoft", "Citigroup"],  # ticker (findatapy)
        fields=["close"],  # which fields to download
        vendor_tickers=["aapl", "msft", "c"],  # ticker (Alpha Vantage)
        vendor_fields=["Close"])  # which Bloomberg fields to download)

    df = market.fetch_market(md_request)

    print(df.tail(n=10))

    # NOTE: uses yfinance for Yahoo API

    # download equities data from Yahoo
    md_request = MarketDataRequest(
        start_date="decade",  # start date
        data_source="yahoo",  # use Bloomberg as data source
        tickers=["Apple", "Citigroup"],  # ticker (findatapy)
        fields=["close"],  # which fields to download
        vendor_tickers=["aapl", "c"],  # ticker (Yahoo)
        vendor_fields=["Close"])  # which Bloomberg fields to download)

    df = market.fetch_market(md_request)

    print(df.tail(n=10))

    # download equities data from Google
    md_request = MarketDataRequest(
        start_date="decade",  # start date
        data_source="yahoo",  # use Bloomberg as data source
        tickers=["Apple", "S&P500-ETF"],  # ticker (findatapy)
        fields=["close"],  # which fields to download
        vendor_tickers=["aapl", "spy"],  # ticker (Yahoo)
        vendor_fields=["Close"])  # which Bloomberg fields to download)

    df = market.fetch_market(md_request)

    print(df.tail(n=10))
