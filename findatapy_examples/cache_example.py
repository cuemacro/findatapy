"""
Showing how we can take advantage of in memory caching via Redis if we are making repeated market data calls externally.
This memory cache is designed to be temporary (and relatively transparent to the user), rather than long term storage.

For longer term storage, can use IOEngine combined with MongoDB

See below for example of speed increase 2ms when fetching from cache vs nearly a second for downloading from Yahoo directly.

There is a much bigger difference when we download large amounts of data (eg. intraday minute data)

2017-02-08 14:08:57,592 - __main__ - INFO - Load data from Yahoo directly
2017-02-08 14:08:57,778 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-08 14:08:57,781 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-08 14:08:57,782 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-08 14:08:57,782 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-08 14:08:57,785 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-08 14:08:57,788 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-08 14:08:57,788 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-08 14:08:58,099 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-08 14:08:58,100 - findatapy.market.datavendorweb - INFO - Request Pandas Web data
2017-02-08 14:08:58,253 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-08 14:08:58,267 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-08 14:08:58,315 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-08 14:08:58,374 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-08 14:08:58,377 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-08 14:08:58,380 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-08 14:08:58,382 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-08 14:08:58,554 - findatapy.market.datavendorweb - INFO - Completed request from Pandas Web.
2017-02-08 14:08:58,626 - findatapy.market.ioengine - INFO - Pushed MarketDataRequest_6eef6648406c333a4035cd5e60d0bf2ecf2606d7 to Redis
2017-02-08 14:08:58,627 - __main__ - INFO - Loaded data from Yahoo directly, now try reading from Redis in-memory cache
2017-02-08 14:08:58,628 - findatapy.market.ioengine - INFO - Load Redis cache: MarketDataRequest_6eef6648406c333a4035cd5e60d0bf2ecf2606d7
2017-02-08 14:08:58,629 - __main__ - INFO - Read from Redis cache.. that was a lot quicker!

"""
from findatapy.market import Market, MarketDataRequest, MarketDataGenerator, IOEngine
from findatapy.util import LoggerManager

market = Market(market_data_generator=MarketDataGenerator())
logger = LoggerManager().getLogger(__name__)

# in the config file, we can use keywords 'open', 'high', 'low', 'close' and 'volume' for Yahoo and Google finance data

# download equities data from Yahoo
md_request = MarketDataRequest(
    start_date="01 Jan 2002",       # start date
    finish_date="05 Feb 2017",      # finish date
    data_source='yahoo',            # use Bloomberg as data source
    tickers=['Apple', 'Citigroup', 'Microsoft', 'Oracle', 'IBM', 'Walmart', 'Amazon', 'UPS', 'Exxon'],  # ticker (findatapy)
    fields=['close'],               # which fields to download
    vendor_tickers=['aapl', 'c', 'msft', 'orcl', 'ibm', 'wmt', 'amzn', 'ups', 'xom'],                   # ticker (Yahoo)
    vendor_fields=['Close'],
    cache_algo='internet_load_return')        # which Bloomberg fields to download)

logger.info("Load data from Yahoo directly")
df = market.fetch_market(md_request)

logger.info("Loaded data from Yahoo directly, now try reading from Redis in-memory cache")
md_request.cache_algo = 'cache_algo_return' # change flag to cache algo so won't attempt to download via web

df = market.fetch_market(md_request)

logger.info("Read from Redis cache.. that was a lot quicker!")
