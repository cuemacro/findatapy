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

    market = Market(market_data_generator=MarketDataGenerator())

    # We can grab market data, by using MarketDataRequest objects
    # However, sometimes we might wish to create more freeform requests (eg. if we have tickers stored in a spreadsheet)
    # Here, we show how to this, and also how to call tickers which are already predefined in the CSV config files

    # Now we will try various examples with Bloomberg
    # only works if you have Bloomberg terminal installed and the Python API!

    # In this case, we are using the special 'fx' category
    md_request = MarketDataRequest(start_date='week', category='fx', data_source='bloomberg')

    # Let's try creating a MarketDataRequest overwrites in the tickers field
    md_request.freeform_md_request = [{'tickers' : ['AUDJPY']}, {'tickers' : ['AUDJPY', 'AUDUSD']}]
    md_request_list = market.create_md_request_from_freeform(md_request)

    print(md_request_list)

    # Now, we can do the same thing with a more complicated overwrite
    md_request.freeform_md_request = [{'tickers' : ['AUDJPY'], 'fields' : 'close'},
                                      {'tickers' : ['AUDJPY', 'AUDUSD'], 'fields' : 'close'}]

    md_request_list = market.create_md_request_from_freeform(md_request)

    print(md_request_list)

    # We can also create MarketDataRequest by passing a str that relates to predefined tickers
    md_request = market.create_md_request_from_str('backtest.fx.bloomberg.daily.NYC.EURUSD.close')

    print(md_request)

    # Or we can do a raw request using any properties we want. At a minimum, we need to have the data_source, tickers,
    # and vendor_tickers field
    md_request = market.create_md_request_from_str('raw.data_source.bloomberg.tickers.EURUSD.vendor_tickers.EURUSD Curncy')

    print(md_request)

    # Or we can directly call fetch_market with it
    # Format of the call is environment.category.freq.cut.ticker.field

    # Note, we can choose to omit the environment if we want and findatapy will choose the default environment
    # which is backtest.
    print(market.fetch_market('backtest.fx.bloomberg.daily.NYC.EURUSD.close'))
    print(market.fetch_market('backtest.fx.quandl.daily.NYC.EURUSD.close'))

    # We can also pass in a MarketDataRequest which can contain many more parameters, which we can't specify
    # with a string of the above form
    print(market.fetch_market(md_request_str='backtest.fx.bloomberg.intraday.NYC.EURUSD',
                              md_request=MarketDataRequest(fields=['open', 'high', 'low', 'close'])))

    # Let's do that for whole vol surface for EURUSD (note, we have omitted backtest here, which findatapy assumes as default)
    print(market.fetch_market('fx-implied-vol.bloomberg.daily.BGN.EURUSD.close',
                              start_date='30 Apr 2021', finish_date='30 Apr 2021'))

    # We can also download any arbitary tickers/vendor tickers which aren't predefined
    print(market.fetch_market('raw.data_source.bloomberg.tickers.VIX.vendor_tickers.VIX Index',
                              start_date='01 Apr 2021', finish_date='30 Apr 2021'))

