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


if __name__ == '__main__':
    ###### below line CRUCIAL when running Windows, otherwise multiprocessing
    # doesn't work! (not necessary on Linux)
    from findatapy.util import SwimPool;

    SwimPool()

    from findatapy.market import Market, MarketDataRequest, MarketDataGenerator
    from findatapy.timeseries import DataQuality

    market = Market(market_data_generator=MarketDataGenerator())
    dq = DataQuality()

    # in the config file, we can use keywords 'open', 'high', 'low', 'close'
    # and 'volume' for Yahoo and Google finance data

    # download equities data from Yahoo
    md_request = MarketDataRequest(
        start_date="decade",  # start date
        data_source='yahoo',  # use Bloomberg as data source
        tickers=['Apple', 'Citigroup'],  # ticker (findatapy)
        fields=['close'],  # which fields to download
        vendor_tickers=['aapl', 'c'],  # ticker (Yahoo)
        vendor_fields=['Close'])  # which Bloomberg fields to download)

    df = market.fetch_market(md_request)

    # create duplicated DataFrame
    df = df.append(df)
    count, dups = dq.count_repeated_dates(df)

    print("Number of duplicated elements")
    print(count)

    print("Duplicated dates")
    print(dups)
