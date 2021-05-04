__author__ = 'saeedamen'  # Saeed Amen

#
# Copyright 2016-2021 Cuemacro
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

    from findatapy.util import ConfigManager

    # findatapy has predefined tickers and categories in the conf folder in CSV files which you can modify, that
    # map from tickers/categories to vendor tickers/vendor fields
    #
    # time_series_categories_fields.csv
    # time_series_fields_list.csv
    # time_series_tickers_list.csv
    #
    # You can also add you own files with tickers, like we have done for FX vol like fx_vol_tickers.csv
    #
    # Having these ticker mappings makes it easier to mix and match data from different data sources, and saves us
    # having to remember vendor specific tickers

    cm = ConfigManager().get_instance()

    # Get all the categories for raw data (note this won't include generated categories like fx-vol-market,
    # which aggregate from many other categories)
    categories = list(cm.get_categories_from_tickers())

    print(categories)

    # Filter those categories which include quandl
    quandl_category = [x for x in categories if 'quandl' in x]

    print(quandl_category[0])

    # For this category, get all the tickers and fields which are available
    tickers = cm.get_tickers_list_for_category_str(categories[0])
    fields = cm.get_fields_list_for_category_str(categories[0])

    # We don't need to add the environment (eg. backtest)
    print("For category " + quandl_category[0] + ", tickers = " + str(tickers) + ", fields = " + str(fields))

    market = Market(market_data_generator=MarketDataGenerator())

    from findatapy.util import DataConstants

    constants = DataConstants()

    # You will likely need to put your own Quandl API key (free from Quandl's website!)
    quandl_api_key = constants.quandl_api_key
    print(market.fetch_market(md_request_str=categories[0], md_request=MarketDataRequest(quandl_api_key=quandl_api_key)))


