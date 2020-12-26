__author__ = 'saeedamen' # Saeed Amen

#
# Copyright 2016-2020 Cuemacro
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

    market = Market(market_data_generator=MarketDataGenerator())

    # download event data from Bloomberg
    # we have to use the special category "events" keyword for economic data events
    # so findatapy can correctly identify them (given the underlying Bloomberg API calls are all different, however,
    # this will appear transparent to the user)
    market_data_request = MarketDataRequest(
                    start_date = "year",
                    category = "events",
                    data_source = 'bloomberg',                                                              # use Bloomberg as data source
                    tickers = ['FOMC', 'NFP'],
                    fields = ['release-date-time-full', 'release-dt', 'actual-release'],                    # which fields to download
                    vendor_tickers = ['FDTR Index', 'NFP TCH Index'],                                       # ticker (Bloomberg)
                    vendor_fields = ['ECO_FUTURE_RELEASE_DATE_LIST', 'ECO_RELEASE_DT', 'ACTUAL_RELEASE'])   # which Bloomberg fields to download

    df = market.fetch_market(market_data_request)

    print(df)

    # we also have a few events defined in our configuation file
    # those tickers/fields which are predefined this way are easier to download
    # note how we don't have to use the vendor_tickers and vendor_fields for examples
    market_data_request = MarketDataRequest(
                    start_date = "year",
                    category = "events",
                    data_source = 'bloomberg',                      # use Bloomberg as data source
                    tickers = ['USD-US Employees on Nonfarm Payrolls Total MoM Net Change SA'],
                    fields = ['release-date-time-full', 'release-dt', 'actual-release', 'number-observations'])

    df = market.fetch_market(market_data_request)

    print(df)

    # now just download the event day
    market_data_request = MarketDataRequest(
                    start_date = "year",
                    category = "events",
                    data_source = 'bloomberg',              # use Bloomberg as data source
                    tickers = ['NFP'],
                    fields = ['release-date-time-full'],                # which fields to download
                    vendor_tickers = ['NFP TCH Index'],     # ticker (Bloomberg)
                    vendor_fields = ['ECO_FUTURE_RELEASE_DATE_LIST'])     # which Bloomberg fields to download

    df = market.fetch_market(market_data_request)

    print(df)