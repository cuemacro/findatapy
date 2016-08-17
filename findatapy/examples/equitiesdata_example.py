from findatapy.market import Market, MarketDataRequest, MarketDataGenerator

market = Market(market_data_generator=MarketDataGenerator())

# in the config file, we can use keywords 'open', 'high', 'low', 'close' and 'volume' for Yahoo and Google finance data

# download equities data from Yahoo
md_request = MarketDataRequest(
    start_date="decade",            # start date
    data_source='yahoo',            # use Bloomberg as data source
    tickers=['Apple', 'Citigroup'], # ticker (findatapy)
    fields=['close'],               # which fields to download
    vendor_tickers=['aapl', 'c'],   # ticker (Yahoo)
    vendor_fields=['Close'])        # which Bloomberg fields to download)

df = market.fetch_market(md_request)

print(df.tail(n=10))

# download equities data from Google
md_request = MarketDataRequest(
    start_date="decade",            # start date
    data_source='yahoo',            # use Bloomberg as data source
    tickers=['Apple', 'S&P500-ETF'], # ticker (findatapy)
    fields=['close'],               # which fields to download
    vendor_tickers=['aapl', 'spy'],   # ticker (Yahoo)
    vendor_fields=['Close'])        # which Bloomberg fields to download)

df = market.fetch_market(md_request)

print(df.tail(n=10))