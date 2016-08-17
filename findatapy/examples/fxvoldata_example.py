from findatapy.market import Market, MarketDataRequest, MarketDataGenerator

market = Market(market_data_generator=MarketDataGenerator())

# choose run_example = 0 for everything
# run_example = 1 - download implied volatility data from Bloomberg for FX
# run_example = 2 - download implied volatility data (not in configuration file) from Bloomberg for FX

run_example =0

###### download FX volatility quotations from Bloomberg
if run_example == 1 or run_example == 0:

    ####### Bloomberg examples (you need to have a Bloomberg Terminal installed for this to work!)
    # let's download past month of 1M ATM data for EURUSD

    # we can use shortcuts given that implied vol surfaces for most major crosses have been defined
    md_request = MarketDataRequest(start_date='month', data_source='bloomberg', cut='NYC', category='fx-implied-vol',
                                   tickers=['EURUSDV1M'])

    df = market.fetch_market(md_request)
    print(df.tail(n=10))

    # we can also download the whole volatility surface for EURUSD, this way.. without having to define every point!
    md_request = MarketDataRequest(start_date='month', data_source='bloomberg', cut='LDN', category='fx-implied-vol',
                                   tickers=['EURUSD'])

    df = market.fetch_market(md_request)
    print(df.tail(n=10))

###### download FX volatility quotations from Bloomberg defining all fields
if run_example == 2 or run_example == 0:

    ####### Bloomberg examples (you need to have a Bloomberg Terminal installed for this to work!)
    # now we define the vendor_tickers and vendor_fields (we don't need to have these in the configuration file)
    # we use NOK/SEK vol quotations, because these haven't been predefined

    # we can use shortcuts given that implied vol surfaces for most major crosses have been defined
    md_request = MarketDataRequest(start_date='month', data_source='bloomberg',
                                   tickers=['NOKSEKV1M'], vendor_tickers=['NOKSEKV1M BGN Curncy'],
                                   fields=['close'], vendor_fields=['PX_LAST'])

    df = market.fetch_market(md_request)
    print(df.tail(n=10))