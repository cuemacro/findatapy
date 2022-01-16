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

    # choose run_example = 0 for everything
    # run_example = 1 - download FX forwards curve data from Bloomberg for FX 
    # (and FX spot and base depos) for EURUSD
    # run_example = 2 - download FX forwards curve data from Bloomberg for FX 
    # (and FX spot and base depos) for USDBRL

    run_example = 0

    cache_algo = "cache_algo_return"

    ###### Download FX forwards data for EURUSD
    if run_example == 1 or run_example == 0:
        ####### Bloomberg examples (you need to have a Bloomberg Terminal 
        # installed for this to work!)

        # We can use shortcuts given that forwards for most major crosses have 
        # been defined
        # Get EURUSD 1M forward points
        md_request = MarketDataRequest(start_date="01 Oct 2020",
                                       finish_date="18 Dec 2020",
                                       data_source="bloomberg", cut="NYC",
                                       category="fx-forwards",
                                       tickers=["EURUSD1M"],
                                       cache_algo=cache_algo)

        df = market.fetch_market(md_request)
        print(df.tail(n=10))

        # Get whole of EURUSD FX forwards curve + spot + base depos
        md_request = MarketDataRequest(start_date="01 Oct 2020",
                                       finish_date="18 Dec 2020",
                                       data_source="bloomberg", cut="NYC",
                                       category="fx-forwards-market",
                                       tickers=["EURUSD"],
                                       cache_algo=cache_algo)

        df = market.fetch_market(md_request)
        print(df.tail(n=10))

    ###### Download FX forwards data for USDBRL
    if run_example == 2 or run_example == 0:
        ####### Bloomberg examples (you need to have a Bloomberg Terminal 
        # installed for this to work!)

        # We can use shortcuts given that forwards for most major crosses have 
        # been defined
        # Get USDBRL 3M forward points
        md_request = MarketDataRequest(start_date="01 Oct 2020",
                                       finish_date="18 Dec 2020",
                                       data_source="bloomberg", cut="NYC",
                                       category="fx-forwards",
                                       tickers=["USDBRL3M"],
                                       cache_algo=cache_algo)

        df = market.fetch_market(md_request)
        print(df.tail(n=10))

        # Get whole of EURUSD FX forwards curve + spot + base depos
        md_request = MarketDataRequest(start_date="01 Oct 2020",
                                       finish_date="18 Dec 2020",
                                       data_source="bloomberg", cut="NYC",
                                       category="fx-forwards-market",
                                       tickers=["USDBRL"],
                                       cache_algo=cache_algo)

        df = market.fetch_market(md_request)
        print(df.tail(n=10))
