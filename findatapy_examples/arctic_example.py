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
    ###### below line CRUCIAL when running Windows, otherwise multiprocessing doesn't work! (not necessary on Linux)
    from findatapy.util import SwimPool;

    SwimPool()

    from findatapy.market import Market, MarketDataRequest, \
        MarketDataGenerator, IOEngine

    market = Market(market_data_generator=MarketDataGenerator())

    # In the config file, we can use keywords 'open', 'high', 'low', 'close'
    # and 'volume' for Yahoo and Google finance data

    # Download equities data from Yahoo
    md_request = MarketDataRequest(
        start_date="decade",  # start date
        data_source='yahoo',  # use Bloomberg as data source
        tickers=['Apple', 'Citigroup'],  # ticker (findatapy)
        fields=['close'],  # which fields to download
        vendor_tickers=['aapl', 'c'],  # ticker (Yahoo)
        vendor_fields=['Close'])  # which Bloomberg fields to download)

    df = market.fetch_market(md_request)

    io = IOEngine()

    # Note: you need to set up Man-AHL's Arctic and MongoDB database for this
    # to work
    # write to Arctic (to MongoDB) - by default use's Arctic's VersionStore
    io.write_time_series_cache_to_disk('stocks', df, engine='arctic',
                                       db_server='127.0.0.1')

    # Read back from Arctic
    df_arctic = io.read_time_series_cache_from_disk('stocks', engine='arctic',
                                                    db_server='127.0.0.1')

    print(df_arctic.tail(n=5))
