from findatapy.market import Market, MarketDataRequest, MarketDataGenerator

market = Market(market_data_generator=MarketDataGenerator())

# choose run_example = 0 for everything
# run_example = 1 - download free tick data from DukasCopy example
# run_example = 2 - download free FX daily data from Quandl
# run_example = 3 - download FX data from Bloomberg
# run_example = 4 - download second FX data from Bloomberg

run_example = 0

###### backtest simple trend following strategy for FX spot basket
if run_example == 1 or run_example == 0:

    ####### DukasCopy examples
    # let's download data for 14 Jun 2016 for EUR/USD - the raw data has bid/ask, if we specify close, we calculate
    # it as the average

    # first we can do it by defining all the vendor fields, tickers etc. so we bypass the configuration file
    md_request = MarketDataRequest(start_date='14 Jun 2016', finish_date='15 Jun 2016',
                                   fields=['bid','ask'], vendor_fields=['bid', 'ask'],
                                   freq='tick', data_source='dukascopy',
                                   tickers=['EURUSD'], vendor_tickers=['EURUSD'])

    df = market.fetch_market(md_request)
    print(df.tail(n=10))

    # now let's do it using the category keyword, which goes into our config files (only works for predefined tickers!)
    # simplifies our calling procedure a lot!
    md_request = MarketDataRequest(start_date='14 Jun 2016', finish_date='15 Jun 2016',
                                   category='fx', fields=['close'], freq='tick', data_source='dukascopy', tickers=['EURUSD'])

    df = market.fetch_market(md_request)
    print(df.tail(n=10))

    # we can also get the bid/ask quotes directly
    md_request = MarketDataRequest(start_date='14 Jun 2016', finish_date='15 Jun 2016',
                                   category='fx', fields=['bid', 'ask'], freq='tick', data_source='dukascopy', tickers=['EURUSD'])

    df = market.fetch_market(md_request)
    print(df.tail(n=10))


if run_example == 2 or run_example == 0:
    ####### Quandl data examples

    # for this to work make sure you edit the Quandl API key in DataConstants file

    # already defined for us are the tickers for G10 USD FX in various CSV files in conf folder
    # by default, assume data is for 'close' field only, cut as being 'NYC' etc.
    # we can use keywords, 'month' and 'year' to specify the last year of data, alternatively, we can put a specific date
    md_request = MarketDataRequest(start_date='year', category='fx', data_source='quandl', tickers=['EURUSD'])

    df = market.fetch_market(md_request)
    print(df.tail(n=10))

    # if we give it a cross which doesn't exist in the database directly, it will try to synthesis from the USD rates
    # this might be ok for daily data, but should not be used for tick data
    # eg. AUD/JPY
    md_request = MarketDataRequest(start_date='year', category='fx', data_source='quandl', tickers=['AUDJPY'])

    df = market.fetch_market(md_request)
    print(df.tail(n=10))

    # if you don't specify tickers, it will download all the tickers in that data_source.freq.category.cut combination
    # ie. quandl.daily.fx.NYC (all G10 USD crosses)
    md_request = MarketDataRequest(start_date='month', category='fx', data_source='quandl', cut='NYC')

    df = market.fetch_market(md_request)
    print(df.tail(n=10))

if run_example == 3 or run_example == 0:
    ####### FRED example

    # if we give it a cross which doesn't exist in the database directly, it will try to synthesis from the USD rates
    # this might be ok for daily data, but should not be used for tick data
    # eg. AUD/JPY
    md_request = MarketDataRequest(start_date='year', category='fx', data_source='quandl', tickers=['AUDJPY'])

    df = market.fetch_market(md_request)
    print(df.tail(n=10))

    # if you don't specify tickers, it will download all the tickers in that data_source.freq.category.cut combination
    # ie. quandl.daily.fx.NYC (all G10 USD crosses)
    md_request = MarketDataRequest(start_date='month', category='fx', data_source='quandl', cut='NYC')

    df = market.fetch_market(md_request)
    print(df.tail(n=10))

if run_example == 3 or run_example == 0:
    ####### Bloomberg

    # now we will try various examples with Bloomberg
    # only works if you have Bloomberg terminal installed and the Python API!
    md_request = MarketDataRequest(start_date='week', category='fx', data_source='bloomberg', tickers=['AUDJPY'])
    # df = market.fetch_market(md_request)

    # print(df.tail(n=10))

    # let's now try downloading 1 minute intraday data for the past week from Bloomberg
    md_request = MarketDataRequest(start_date='week', freq='intraday', category='fx', data_source='bloomberg', tickers=['AUDJPY'])

    # df = market.fetch_market(md_request)
    # print(df.tail(n=10))

    # let's now try downloading 1 minute intraday data for the past week from Bloomberg, directly specifying tickers
    # bypassing our config file
    # this is handy when you want to download something which isn't predefined in the configuration files
    md_request = MarketDataRequest(start_date='week', freq='intraday', data_source='bloomberg',
                                   tickers=['AUDJPY'], vendor_tickers=['AUDJPY BGN Curncy'],
                                   fields=['close'], vendor_fields=['close'])

    df = market.fetch_market(md_request)
    print(df.tail(n=10))


if run_example == 4 or run_example == 0:
    ####### Bloomberg

    # let's now try downloading tick data for the past hour from Bloomberg, directly specifying tickers
    # bypassing our config file
    # this is handy when you want to download something which isn't predefined in the configuration files
    # then convert into seconds data using pandas
    md_request = MarketDataRequest(start_date='hour', freq='tick', data_source='bloomberg',
                                   tickers=['AUDJPY'], vendor_tickers=['AUDJPY BGN Curncy'],
                                   fields=['close'], vendor_fields=['close'])

    df = market.fetch_market(md_request)
    df = df.resample('1s').mean()

    print(df.tail(n=60))