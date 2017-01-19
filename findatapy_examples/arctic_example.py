from findatapy.market import Market, MarketDataRequest, MarketDataGenerator, IOEngine

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

io = IOEngine()

# note: you need to set up Man-AHL's Arctic and MongoDB database for this to work
# write to Arctic (to MongoDB) - by default use's Arctic's VersionStore
io.write_time_series_cache_to_disk('stocks', df, engine='arctic', db_server='127.0.0.1')

# read back from Arctic
df_arctic = io.read_time_series_cache_from_disk('stocks', engine='arctic', db_server='127.0.0.1')

print(df_arctic.tail(n=5))
