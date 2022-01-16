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

    from findatapy.util import ConfigManager

    # findatapy has predefined tickers and categories in the conf folder in
    # CSV files which you can modify, that
    # map from tickers/categories to vendor tickers/vendor fields
    #
    # time_series_categories_fields.csv
    # time_series_fields_list.csv
    # time_series_tickers_list.csv
    #
    # You can also add you own files with tickers, like we have done for FX vol
    # like fx_vol_tickers.csv
    #
    # Having these ticker mappings makes it easier to mix and match data from
    # different data sources, and saves us
    # having to remember vendor specific tickers

    cm = ConfigManager().get_instance()

    # Get all the categories for raw data (note this won't include generated
    # categories like fx-vol-market,
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
    print("For category " + quandl_category[0] + ", tickers = " + str(
        tickers) + ", fields = " + str(fields))

    # Do a more complicated query, get any combinations
    df = cm.free_form_tickers_regex_query(category='fx/*|equities/*',
                                          ret_fields=['category', 'freq'])

    print(df.head(min(5, len(df.index))))

    # Let's search for all the category.data_source.freq.cut.tickers.fields
    # combinations
    # where the category matches the regular expression for which has 'fx' or
    # 'equities' in it and smart group
    df = cm.free_form_tickers_regex_query(category='fx/*|equities/*',
                                          ret_fields=['category',
                                                      'data_source', 'freq',
                                                      'cut', 'tickers',
                                                      'fields'],
                                          smart_group=True)

    print(df.head(min(5, len(df.index))))

    # We'll do the same query again, but this time, we'll convert it into a
    # DataFrame which can use to create
    # a MarketDataRequest
    df_ungrouped = cm.free_form_tickers_regex_query(category='fx/*|equities/*',
                                                    ret_fields=['category',
                                                                'data_source',
                                                                'freq', 'cut',
                                                                'tickers',
                                                                'fields'])

    print(df_ungrouped)

    # We can also do exact matches for multiple queries, where findatapy
    # guesses the what parameters we were intending to find
    # so they don't necessarily have to be in the order
    # category.data_source.freq.cut.ticker etc. They can be in whatever
    # order you want. Note that for this to work, we can not have values common
    # across multiple fields, like tickers and category
    #
    # Here we are searching for the tickers/vendor_tickers combination which
    # matches
    # - fx.daily.quandl (ie. findatapy will guess category='fx', freq='daily'
    # and data_source = 'quandl')
    # - fx.daily.bloomberg (ie. findatapy
    df_guess = cm.free_form_tickers_query(
        ['fx.daily.quandl', 'fx.daily.bloomberg'],
        ret_fields=['tickers', 'vendor_tickers'], list_query=True,
        smart_group=True)

    print(df_guess)

    market = Market(market_data_generator=MarketDataGenerator())

    md_request_list = market.create_md_request_from_dataframe(df_ungrouped)

    print(md_request_list)

    df_tickers = ConfigManager.get_dataframe_tickers()

    print(df_tickers)

    from findatapy.util import DataConstants

    constants = DataConstants()

    # You will likely need to put your own Quandl API key (free from Quandl's
    # website!)
    quandl_api_key = constants.quandl_api_key
    print(market.fetch_market(md_request_str=categories[0],
                              md_request=MarketDataRequest(
                                  quandl_api_key=quandl_api_key)))

    # Or we could create this download of FX Quandl data with a more free form
    # query
    df_quandl_tickers = cm.free_form_tickers_regex_query(category='fx/*',
                                                         data_source='quandl',
                                                         cut='NYC')
    print(market.fetch_market(md_request_df=df_quandl_tickers,
                              md_request=MarketDataRequest(
                                  quandl_api_key=quandl_api_key)))

    # Now lets get all Bloomberg data and Quandl FX data in our predefined
    # tickers for the past week
    # we use ^fx$ as a regular expression, so it will only match with fx, and
    # not any variations like fx-implied-vol
    md_request = MarketDataRequest(start_date='week',
                                   quandl_api_key=quandl_api_key)

    md_request_list = market.create_md_request_from_dataframe(
        cm.free_form_tickers_regex_query(category='^fx$', freq='daily',
                                         data_source='quandl|bloomberg',
                                         cut='NYC'),
        md_request=md_request)

    for m in md_request_list:
        print(market.fetch_market(m))
