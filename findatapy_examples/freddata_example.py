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

    md_request = MarketDataRequest(
        start_date="01 Jun 2000",
        # start date (download data over past decade)
        data_source="alfred",  # use ALFRED/FRED as data source
        tickers=["US CPI YoY", "EZ CPI YoY"],  # ticker
        fields=["close"],  # which fields to download
        vendor_tickers=["CPIAUCSL", "CP0000EZ17M086NEST"],  # ticker (FRED)
        vendor_fields=["close"])  # which ALFRED fields to download

    df = market.fetch_market(md_request)

    print(df.tail(n=10))
