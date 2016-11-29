from findatapy.market import Market, MarketDataRequest, MarketDataGenerator

market = Market(market_data_generator=MarketDataGenerator())

# get the first release for GDP and also print the release date of that
md_request = MarketDataRequest(
    start_date="01 Jun 2000",                                                      # start date (download data over past decade)
    data_source='alfred',                                                          # use ALFRED/FRED as data source
    tickers=['US GDP'],                                                            # ticker
    fields=['actual-release', 'release-date-time-full'],                           # which fields to download
    vendor_tickers=['GDP'],                                                        # ticker (FRED)
    vendor_fields=['actual-release', 'release-date-time-full'])                    # which FRED fields to download

df = market.fetch_market(md_request)

print(df)

# comapre the close and actual release of US GDP (and the final)
md_request = MarketDataRequest(
    start_date="01 Jun 2000",                                                      # start date (download data over past decade)
    data_source='alfred',                                                          # use ALFRED/FRED as data source
    tickers=['US GDP'],                                                            # ticker
    fields=['actual-release', 'close'],                                            # which fields to download
    vendor_tickers=['GDP'],                                                        # ticker (FRED)
    vendor_fields=['actual-release', 'close'])                                     # which FRED fields to download

df = market.fetch_market(md_request)

from chartpy import Chart

Chart().plot(df)