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

    # We can grab market data, by using MarketDataRequest objects
    # However, sometimes we might wish to create more freeform requests
    #   eg. if we have tickers stored in a spreadsheet)
    #   eg. from a single string (either for predefined tickers or any
    #   tickers/vendor_tickers combinations we want)
    # Here, we show how to this, and also how to call tickers which are
    # already predefined in the CSV config files

    # Now we will try various examples with Bloomberg
    # only works if you have Bloomberg terminal installed and the Python API!

    # In this case, we are using the special "fx" category
    md_request = MarketDataRequest(start_date="week", category="fx",
                                   data_source="bloomberg")

    # Let"s try creating a MarketDataRequest overwrites in the tickers field
    md_request.freeform_md_request = [{"tickers": ["AUDJPY"]},
                                      {"tickers": ["AUDJPY", "AUDUSD"]}]
    md_request_list = market.create_md_request_from_freeform(md_request)

    print(md_request_list)

    # Now, we can do the same thing with a more complicated overwrite
    md_request.freeform_md_request = [
        {"tickers": ["AUDJPY"], "fields": "close"},
        {"tickers": ["AUDJPY", "AUDUSD"], "fields": "close"}]

    md_request_list = market.create_md_request_from_freeform(md_request)

    print(md_request_list)

    # We can also create MarketDataRequest by passing a str that relates to
    # predefined tickers
    md_request = market.create_md_request_from_str(
        "backtest.fx.bloomberg.daily.NYC.EURUSD.close")

    print(md_request)

    # We can do an approximate match for predefined tickers, and findatapy will
    # "guess" the closest match
    md_request = market.create_md_request_from_str("_.bloomberg.EURUSD.NYC")

    print(md_request)

    # We get *all* the predefined tickers which match in any column with quandl
    # and fx, and these be smart grouped
    # into the smallest number of requests
    md_request = market.create_md_request_from_str("_.quandl.fx",
                                                   best_match_only=False,
                                                   smart_group=True)

    print(md_request)

    # We can also create MarketDataRequest by passing a str for an arbitrary
    # Parquet, note, if we have dots in the path
    # we need to use {} to denote them (we can use either "raw" to denote this
    # or just "r")
    md_request = market.create_md_request_from_str(
        "raw.data_source.{c:\parquet_files\dump.parquet}.tickers.EURUSD")

    print(md_request)

    # We can also create a MarketDataRequest using a JSON string (we must be
    # careful to adhere to correct format)
    # where the keys match those available of a MarketDataRequest
    md_request = market.create_md_request_from_str(
        """{"data_source" : "bloomberg",
        "category" : "fx",
        "freq" : "daily",
        "tickers" : ["EURUSD", "GBPUSD"],
        "cut" : "NYC",
        "fields" : "close"}""")

    print(md_request)

    # Or directly from a dict where the keys match those available of a
    # MarketDataRequest (again we need to be careful
    # with the keys, so they match those of a MarketDataRequest
    md_request = market.create_md_request_from_dict({
        "data_source": "bloomberg",
        "category": "fx",
        "freq": "daily",
        "tickers": ["EURUSD", "GBPUSD"],
        "cut": "NYC",
        "fields": "close"})

    print(md_request)

    # Or we can do a raw request using any properties we want. At a minimum,
    # we need to have the data_source, tickers,
    # and vendor_tickers field, any ones we don"t include, will just be taken
    # from the defaults
    md_request = market.create_md_request_from_str(
        "raw.data_source.bloomberg.tickers.EURUSD.vendor_tickers.EURUSD Curncy")

    print(md_request)

    # Or we can directly call fetch_market with it
    # Format of the call is environment.category.freq.cut.ticker.field which is
    # for predefined tickers

    # Note, we can choose to omit the environment if we want and findatapy will
    # choose the default environment
    # which is backtest (in this case, we have included it)
    print(market.fetch_market("backtest.fx.bloomberg.daily.NYC.EURUSD.close"))
    print(market.fetch_market("backtest.fx.quandl.daily.NYC.EURUSD.close"))

    # We can also pass in a MarketDataRequest which can contain many more
    # parameters, which we can"t specify
    # with a string of the above form. Given we"ve ignored adding the
    # environment here, findatapy will automatically
    # add the default environment which is backtest
    print(
        market.fetch_market(md_request_str="fx.bloomberg.intraday.NYC.EURUSD",
                            md_request=MarketDataRequest(
                                fields=["open", "high", "low", "close"])))

    # Let"s do that for whole vol surface for EURUSD (note, we have omitted
    # backtest here, which findatapy assumes as default)
    print(
        market.fetch_market("fx-implied-vol.bloomberg.daily.BGN.EURUSD.close",
                            start_date="30 Apr 2021",
                            finish_date="30 Apr 2021"))

    # We can also download any arbitary properties such as tickers/vendor
    # tickers which aren"t predefined
    print(market.fetch_market(
        "raw.data_source.bloomberg.tickers.VIX.vendor_tickers.VIX Index",
        start_date="01 Apr 2021", finish_date="30 Apr 2021"))

    # Or let's give it a Python dict like above, which is likely to be easier
    # to structure, when we have many properties
    print(market.fetch_market({
        "start_date": "01 Apr 2021",
        "finish_date": "30 Apr 2021",
        "category": "fx",
        "data_source": "bloomberg",
        "freq": "daily",
        "tickers": ["EURUSD", "GBPUSD"],
        "cut": "NYC",
        "fields": "close"}))
