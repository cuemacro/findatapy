from findatapy.market import Market, MarketDataRequest, MarketDataGenerator

market = Market(market_data_generator=MarketDataGenerator())

md_request = MarketDataRequest(
    start_date="01 Jun 2000",                   # start date (download data over past decade)
    data_source='fred',                         # use FRED as data source
    tickers=['US CPI YoY', 'EZ CPI YoY'],       # ticker
    fields=['close'],                           # which fields to download
    vendor_tickers=['CPIAUCSL', 'CP0000EZ17M086NEST'],  # ticker (FRED)
    vendor_fields=['close'])                    # which FRED fields to download

df = market.fetch_market(md_request)

print(df.tail(n=10))