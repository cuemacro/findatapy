<img src="findatapy_logo.png?raw=true" width="300"/>

# [findatapy](https://github.com/cuemacro/findatapy)

findatapy creates an easy to use Python API to download market data from many sources including Quandl, Bloomberg, Yahoo, Google etc. using
a unified high level interface. Users can also define their own custom tickers, using configuration files. There is also functionality which
is particularly useful for those downloading FX market data. Below example shows how to download AUDJPY data from Quandl (and automatically 
calculates this via USD crosses).

*Contributors for the project are very much welcome, see below!*

```
from findatapy.market import Market, MarketDataRequest, MarketDataGenerator

market = Market(market_data_generator=MarketDataGenerator())

md_request = MarketDataRequest(start_date='year', category='fx', data_source='quandl', tickers=['AUDJPY'])

df = market.fetch_market(md_request)
print(df.tail(n=10))
```

Here we see how to download tick data from DukasCopy, wih the same API calls and minimal changes in the code.

```
md_request = MarketDataRequest(start_date='14 Jun 2016', finish_date='15 Jun 2016',
                                   category='fx', fields=['bid', 'ask'], freq='tick', 
                                   data_source='dukascopy', tickers=['EURUSD'])

df = market.fetch_market(md_request)
print(df.tail(n=10))
```

I had previously written the open source PyThalesians financial library. This new findatapy library has similar functionality to the 
market data part of that library. However, I've totally rewritten the API to make it much cleaner and easier to use. It is also now a fully
standalone package, so you can more easily use it with whatever libraries you have for analysing market data or doing your backtesting (although I'd recommend
my own finmarketpy package if you are doing backtesting of trading strategies!).

A few things to note:
* Please bear in mind at present findatapy is currently a highly experimental alpha project and isn't yet fully 
documented
* Uses Apache 2.0 licence

# Contributors

Contributors are always welcome for finmarketpy, findatapy and chartpy. If you'd like to contribute, have a look at
[Planned Features](PLANNED_FEATURES.md] for areas we're looking for help on. Or if you have any ideas for improvements
to the libriares please let us know too!

# Gallery

To appear

# Requirements

Major requirements
* Required: Python 3.4, 3.5
* Required: pandas, numpy etc.
* Recommended: blpapi - Bloomberg Python Open API
* Recommended: chartpy - for funky interactive plots ([https://github.com/cuemacro/chartpy](https://github.com/cuemacro/chartpy)) and
* Recommended: arctic - AHL library for managing time series in MongoDB
* Recommended: multiprocessor_on_dill - standard multiprocessing library pickle causes issues ([https://github.com/sixty-north/multiprocessing_on_dill](https://github.com/sixty-north/multiprocessing_on_dill))
* Recommended: fredapi - ALFRED/FRED has been rewritten and added to the project directly (from https://github.com/mortada/fredapi)

# Installation

For detailed installation instructions for chartpy, findatapy & finmarketpy and its associated Python libraries go to
[https://github.com/cuemacro/finmarketpy/blob/master/INSTALL.md](https://github.com/cuemacro/finmarketpy/blob/master/INSTALL.md). The tutorial includes details on how to setup your entire Python environment.

You can install the library using the below. After installation:
* Make sure you edit the DataConstants class for the correct Quandl API and Twitter API keys etc

```
pip install git+https://github.com/cuemacro/findatapy.git
```

# findatapy examples

In findatapy/examples you will find several demos

# Release Notes

* No formal releases yet

# Coding log

* 02 Nov 2019
    * Added BoE as a data source
    * Removed blosc/msgpack (msgpack deprecated in pandas) and replaced with pyarrow for caching 
    * Uses keyring library for API keys (unless specified in DataCred)
    * Began to add tests for IO
* 03 Oct 2019
    * Remove API key from cache
    * Remove timezone when storing in Arctic (can cause issues with later versions of Pandas)
* 14 Aug 2019
    * Bloomberg downloaders now works with Pandas 0.25
    * Fixed Yahoo downloader to work with yfinance (replacing pandas_datareader for Yahoo)
* 06 Aug 2019
    * Adding parameters to MarketDataRequest for user specified API keys (Quandl, FRED & Alpha Vantage)
* 23 Jul 2019
    * Changed some rolling calculations in Calculation class to work with newer pandas
* 12 Jul 2019
    * Fixed issues with DukasCopy downloading when using multi-threading
* 01 Mar 2019
    * Added read/write Parquet
    * Added concat dataframes
* 15 Nov 2018
    * Fixed aggregation by hour/day etc. with pandas > 0.23
    * Filter data frame columns by multiple keywords
* 20 Sep 2018 - Fixed bug in ALFRED
* 25 Jul 2018 - Better timezone handling when filtering by holidays
* 23 Jul 2018 - Fixed additional bug in filter
* 27 Jun 2018 - Added note about installing blpapi via pip
* 23 Jun 2018 - Fixed bug filtering dataframes with timezones
* 29 May 2018 - Added port
* 11 May 2018
    * Allow filtering of dataframes by user defined holidays
* 25 Apr 2018
    * Added transaction costs by asset
    * Fixed bug with Redis caching
* 21 Apr 2018 - New features
    * use CSV/HDF5 files with MarketDataRequest (includes flatfile_example.py)
    * allow resample parameter for MarketDataRequest
    * added AlphaVantage as a data source
    * added fxcmpy as a a data source (unfinished)
* 20 Apr 2018 - Remove rows where all NaNs for daily data when returning from MarketDataGenerator
* 26 Mar 2018 - Change logging level for downloading dates of DukasCopy
* 20 Mar 2018 - Added insert_sparse_time_series in Calculation, and mask_time_series_by_time in Filter.
* 07 Mar 2018 - Fixed bugs for date_parser.
* 20 Feb 2018 - Added cryptocurrency data generators and example
* 22 Jan 2018 - Added function to remove duplicate consecutive data
* 05 Jan 2018 - Fixed bug when downloading BBG reference data
* 18 Dec 2017 - Fixed FXCM downloader bug
* 24 Nov 2017 - Minor bug fixes for DukasCopy downloader
* 10 Oct 2017 - Added handling of username and password for arctic
* 26 Aug 2017 - Improved threading for FXCM and DukasCopy downloaders
* 25 Aug 2017 - Added FXCM downloader (partially finished)
* 23 Aug 2017 - Improved overwritting of constants by cred file
* 10 Jul 2017 - Added method for calculation of autocorrelation in Calculations
* 07 Jun 2017 - Added methods for calendar day seasonality in Calculations
* 25 May 2017 - Removed unneeded dependency in DataQuality
* 22 May 2017 - Began to replace pandas OLS with statsmodels
* 03 May 2017 - Added section for contributors
* 28 Apr 2017 - Issues with returning weekend data for FX spot fixed
* 18 Apr 2017 - Fixed FX spot calc
* 13 Apr 2017 - Fixed issues with FX cross calculations (and refactored)
* 07 Apr 2017 - Fix issue with returned Quandl labels in returned time series, downloading of Bloomberg tick data
* 06 Apr 2017 - Fixed issue with not specifying field
* 13 Mar 2017 - Changed examples to use SwimPool
* 08 Mar 2017 - Fixed bug with DukasCopy data (was getting wrong month) and added blpapi pre-built
* 28 Feb 2017 - Added passthrough for BBG overrides via MarketDataRequest
* 23 Feb 2017 - Added ability to specify tickers with wildcards
* 21 Feb 2017 - Optimised code to speed up downloading Bloomberg data considerably
* 17 Feb 2017 - Added switch between multiprocess and multiprocessing on dill libraries in SpeedCache
* 15 Feb 2017 - Added multiprocessing_example, switched to using multiprocess library and improved SpeedCache (for deletion of keys)
* 14 Feb 2017 - Speeded up returns statistic computation and created DataQuality class
* 13 Feb 2017 - Added SwimPool class
* 12 Feb 2017 - Fixed small filtering bug (for start/finish date) and began adding tests
* 11 Feb 2017 - Added example to show how to use Redis caching
* 09 Feb 2017 - Added in-memory caching when loading market data (via Redis)
* 08 Feb 2017 - Pad columns now returns columns in same order as input
* 07 Feb 2017 - Added Redis to IOEngine
* 05 Feb 2017 - Added openpyxl as a dependency
* 01 Feb 2017 - Added method for aligning left and right dataframes (with fill down) and rolling_corr (to work with pandas <= 0.13)
* 25 Jan 2017 - Work on stop losses for multiple assets in DataFrame and extra documentation for IOEngine
* 24 Jan 2017 - Extra method for calculating signal * returns (multiplying matrices)
* 19 Jan 2017 - Changed examples location in project, added future based variables to Market
* 18 Jan 2017 - Fixed returning of bid/ask in DukasCopy
* 16 Jan 2017 - Added override for stop/take profit signals (& allow dynamic levels), speed up for filtering of time series by column
* 13 Jan 2017 - Added "expiry" for tickers (optional to add), so can handle futures data better when downloading
and various bugs fixed for getting Bloomberg reference data fetching
* 11 Jan 2017 - Added extra documentation and method for assessing stop loss/take profit
* 10 Jan 2017 - Added better handling for downloading of Bloomberg reference requests
* 05 Jan 2017 - Fixed fxspotdata_example example, fixed singleton mechanism in ConfigManager
* 24 Dec 2016 - Added more error handling for Quandl
* 20 Dec 2016 - Updated deprecated some pandas deprecated methods in Calculations class & various bug fixes
* 14 Dec 2016 - Bug fixes for DukasCopy downloader (@kalaytan) and added delete ticker from disk (Arctic)
* 09 Dec 2016 - Speeded up ALFRED/FRED downloader
* 30 Nov 2016 - Rewrote fredapi downloader (added helped methods) and added to project
* 29 Nov 2016 - Added ALFRED/FRED as a data source
* 28 Nov 2016 - Bug fixes on MarketDataGenerator and BBGLowLevelTemplate (@spyamine)
* 04 Nov 2016 - Added extra field converters for Quandl
* 02 Nov 2016 - Changed timeouts for accessing MongoDB via arctic
* 17 Oct 2016 - Functions for filtering time series by period
* 13 Oct 2016 - Added YoY metric in RetStats, by default pad missing returned columns for MarketDataGenerator
* 07 Oct 2016 - Add .idea from .gitignore
* 06 Oct 2016 - Fixed downloading of tick count for FX
* 04 Oct 2016 - Added arctic_example for writing pandas DataFrames
* 02 Oct 2016 - Added read/write dataframes via AHL's Arctic (MongoDB), added multi-threaded outer join, speeded up downloading intraday FX
* 28 Sep 2016 - Added more data types to download for vol
* 23 Sep 2016 - Fixed issue with downloading events
* 20 Sep 2016 - Removed deco dependency, fixed issue downloading Quandl fields, fixed issue with setup files
* 02 Sep 2016 - Edits around Bloomberg event download, fixed issues with data downloading threading
* 23 Aug 2016 - Added skeletons for ONS and BOE data
* 22 Aug 2016 - Added credentials file
* 17 Aug 2016 - Uploaded first code

End of note
