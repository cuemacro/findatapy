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
    # run_example = 1 - download minute crypto data from Bloomberg
    # coming soon other crypto data sources!

    run_example = 1

    if run_example == 1 or run_example == 0:
        ####### Bloomberg

        # Now we will try various examples with Bloomberg
        # only works if you have Bloomberg terminal installed and the 
        # Python API!
        md_request = MarketDataRequest(start_date="week", freq="intraday",
                                       category="fx", data_source="bloomberg",
                                       cut="BSTP",
                                       tickers=["XBTUSD", "XETUSD", "XLCUSD",
                                                "XRPUSD"])

        df = market.fetch_market(md_request)

        print(df.tail(n=10))
