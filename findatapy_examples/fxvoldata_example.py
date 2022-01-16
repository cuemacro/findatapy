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

    market = Market(market_data_generator=MarketDataGenerator())

    # choose run_example = 0 for everything
    # run_example = 1 - download implied volatility data from Bloomberg for FX
    # run_example = 2 - download implied volatility data (not in configuration
    # file) from Bloomberg for FX

    run_example = 0

    cache_algo = 'cache_algo_return'

    ###### Download FX volatility quotations from Bloomberg
    if run_example == 1 or run_example == 0:
        ####### Bloomberg examples (you need to have a Bloomberg Terminal
        # installed for this to work!)
        # let's download of 1M ATM data for EURUSD

        # we can use shortcuts given that implied vol surfaces for most major
        # crosses have been defined
        md_request = MarketDataRequest(start_date='01 Jan 2020',
                                       finish_date='15 Feb 2020',
                                       data_source='bloomberg', cut='NYC',
                                       category='fx-implied-vol',
                                       tickers=['EURUSDV1M'],
                                       cache_algo=cache_algo)

        df = market.fetch_market(md_request)
        print(df.tail(n=10))

        # we can also download the whole volatility surface for EURUSD, this
        # way.. without having to define every point!
        md_request = MarketDataRequest(start_date='01 Jan 2020',
                                       finish_date='15 Feb 2020',
                                       data_source='bloomberg',
                                       cut='LDN', category='fx-implied-vol',
                                       tickers=['EURUSD'],
                                       cache_algo=cache_algo)

        df = market.fetch_market(md_request)

        # we can also download the whole all market data for EURUSD for pricing
        # options (vol surface)
        md_request = MarketDataRequest(start_date='01 Jan 2020',
                                       finish_date='15 Feb 2020',
                                       data_source='bloomberg',
                                       cut='LDN', category='fx-vol-market',
                                       tickers=['EURUSD'],
                                       cache_algo=cache_algo)

        df = market.fetch_market(md_request)
        print(df.tail(n=10))

###### Download FX volatility quotations from Bloomberg defining all fields
if run_example == 2 or run_example == 0:
    ####### Bloomberg examples (you need to have a Bloomberg Terminal installed
    # for this to work!)
    # now we define the vendor_tickers and vendor_fields (we don't need to have
    # these in the configuration file)
    # we use NOK/SEK vol quotations, because these haven't been predefined

    # we can use shortcuts given that implied vol surfaces for most major
    # crosses have been defined
    md_request = MarketDataRequest(start_date='01 Jan 2020',
                                   finish_date='15 Feb 2020',
                                   data_source='bloomberg',
                                   tickers=['NOKSEKV1M'],
                                   vendor_tickers=['NOKSEKV1M BGN Curncy'],
                                   fields=['close'], vendor_fields=['PX_LAST'],
                                   cache_algo=cache_algo)

    df = market.fetch_market(md_request)
    print(df.tail(n=10))
