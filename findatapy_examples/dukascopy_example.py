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

    # choose run_example = 0 for everything
    # run_example = 1 - download EURUSD free tick data from DukasCopy example
    # run_example = 2 - download S&P500 free tick data from DukasCopy example

    run_example = 0

    if run_example == 1 or run_example == 0:
        ####### DukasCopy examples
        # let's download data for 14 Jun 2016 for EUR/USD - the raw data has bid/ask, if we specify close, we calculate
        # it as the average

        from findatapy.market import Market, MarketDataRequest, \
            MarketDataGenerator

        market = Market(market_data_generator=MarketDataGenerator())

        # first we can do it by defining all the vendor fields, tickers etc. so we bypass the configuration file
        md_request = MarketDataRequest(start_date='14 Jun 2016',
                                       finish_date='20 Jun 2016',
                                       fields=['bid'], vendor_fields=['bid'],
                                       freq='tick', data_source='dukascopy',
                                       tickers=['EURUSD'],
                                       vendor_tickers=['EURUSD'])

        df = market.fetch_market(md_request)
        print(df.tail(n=10))

    if run_example == 2 or run_example == 0:
        ####### Dukascopy S&P500 example
        from findatapy.market import Market, MarketDataRequest, \
            MarketDataGenerator

        market = Market(market_data_generator=MarketDataGenerator())

        md_request = MarketDataRequest(start_date='14 Jun 2016',
                                       finish_date='15 Jun 2016',
                                       fields=['bid', 'ask'],
                                       vendor_fields=['bid', 'ask'],
                                       freq='tick', data_source='dukascopy',
                                       tickers=['S&P500'],
                                       vendor_tickers=['USA500IDXUSD'])

        # Careful need to divide by 1000.0
        df = market.fetch_market(md_request) / 1000.0

        print(df)
