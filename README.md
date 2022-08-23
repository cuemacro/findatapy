<img src="findatapy_logo.png?raw=true" width="300"/>

# [findatapy](https://github.com/cuemacro/findatapy)

[![Downloads](https://pepy.tech/badge/findatapy)](https://pepy.tech/project/findatapy)

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
[Planned Features](PLANNED_FEATURES.md) for areas we're looking for help on. Or if you have any ideas for improvements
to the libriares please let us know too!

# Gallery

To appear

# Requirements

Major requirements
* Required: Python 3.8 (Python 2 is not supported)
* Required: pandas, numpy etc.
* Recommended: blpapi - Bloomberg Python Open API
* Recommended: chartpy - for funky interactive plots ([https://github.com/cuemacro/chartpy](https://github.com/cuemacro/chartpy)) and
* Recommended: arctic - AHL library for managing time series in MongoDB

# Installation

For detailed installation instructions for chartpy, findatapy & finmarketpy and its associated Python libraries go to
[https://github.com/cuemacro/finmarketpy/blob/master/INSTALL.md](https://github.com/cuemacro/finmarketpy/blob/master/INSTALL.md). The tutorial includes details on how to setup your entire Python environment.

You can install the library using the below. After installation:
* Make sure you edit the dataconstants class for the correct Eikon API, Quandl API and Twitter API keys etc.
* Or you can run set_api_keys.py script to set the API keys via storing in your keyring
* Or you can create a datacred.py file which overwrites these keys
* Or some of these API keys can be passed via MarketDataRequest on demand

To install via pip (latest release):
```
pip install findatapy
```

To install newest repo copy:
```
pip install git+https://github.com/cuemacro/findatapy.git
```

# Couldn't push MarketDataRequest message

You might often get an error like the below, when you are downloading market data with
findatapy, and you don't have Redis installed.

```
Couldn't push MarketDataRequest
```

findatapy includes an in-memory caching mechanism, which uses Redis
a key/value in-memory store. The idea is that if we do exactly the same data download
call with the same parameters of a MarketDataRequest it will check this volatile cache
first, before going out to our external data provider (eg. Quandl).

Note, that Redis is usually set up as volatile cache, so once your computer is turned off, this cache
will be lost.

If Redis is not installed, this caching will fail and you'll
get this error. However, all other functionality aside from the caching will be fine. All
findatapy will do is to always go externally to download market data. Redis is available for Linux.
There is also an unsupported (older) Windows version available, which I've found works fine, 
although it lacks some functionality of later Redis versions.

# findatapy examples

In findatapy/examples you will find several demos on how to download data from many different sources. Note, 
for some such as Bloomberg or Eikon, you'll need to have a licence/subscription for it to work. Also there might be 
certain limits of the history you can download for intraday data from certain sources (you will need to check with
individual data providers)

# Release Notes

* 0.1.28 - findatapy (19 Jul 2022)
* 0.1.27 - findatapy (20 May 2022)
* 0.1.26 - findatapy (07 Oct 2021)
* 0.1.25 - findatapy (07 Oct 2021)
* 0.1.24 - findatapy (06 Oct 2021)
* 0.1.23 - findatapy (03 Jun 2021)
* 0.1.22 - findatapy (01 Jun 2021)
* 0.1.21 - findatapy (04 May 2021)
* 0.1.20 - findatapy (11 Feb 2021)
* 0.1.19 - findatapy (22 Jan 2021)
* 0.1.18 - findatapy (02 Oct 2020)
* 0.1.17 - findatapy (01 Oct 2020)
* 0.1.16 - findatapy (13 Sep 2020)
* 0.1.15 - findatapy (10 Sep 2020)
* 0.1.14 - findatapy (25 Aug 2020)
* 0.1.13 - findatapy (24 Aug 2020)
* 0.1.12 - findatapy (06 May 2020)

# Coding log

* 23 Aug 2022
  * Simplified MarketDataRequest
* 12 Aug 2022
  * Minor changes in MarketDataGenerator for non-standard columns
* 11 Aug 2022
  * Fixed various bug in MarketDataRequest, when updating constants and 
  freeform str ticker queries with kwargs
* 19 Jul 2022
  * Various fixes for data download
* 20 May 2022
  * Added more customisation for data vendors
* 25 Jan 2022
  * Fixed delete key problem with Redis
* 20 Jan 2022
  * Fixed path join for s3
* 17 Jan 2022
  * Fixed bug in BBG wrapper
* 16 Jan 2022
  * Started to refactor code for PEP8 standards
  * Added BBG tests
* 15 Jan 2022
  * Changed read/write on Redis to use Pickle instead of Arrow
* 14 Dec 2021
  * Fixed bug in overrides for BBG
  * Patched ticker for EUR1Y deposit rate
* 07 Oct 2021
  * Fixed bug in downloading data for unusual categories 
  * Fixed missing ticker in time_series_tickers.csv
* 27 Sep 2021
  * Fixed bug in numeric conversion in DataFrame
  * Error trapping when downloading web pages to DataFrame
  * Added ignore case in filter columns
  * Removed lz4 compression for Arrow caching
* 23 Sep 2021
  * Fixed bug in YoY calculation
* 29 Jul 2021
  * Minor changes to `Market` for managing tickers
* 28 Jul 2021
  * Improved freeform ticker queries and fixed bug with downloading whole categories
* 22 Jul 2021
  * Fixed S3 credentials management and added S3 file copy method
  * Added roll costs
* 19 Jul 2021
  * Added delete file method in `IOEngine` for S3
* 12 Jul 2021
  * Can now read CSV conf files for tickers from S3 buckets and improved S3 support (can now specify AWS credentials, as parameter)
  * Additional file functions (eg. list_files)
* 05 Jul 2021
  * Now (optionally) writes Parquet files in chunks (user specified size) to avoid memory issues with pyarrow
  * Default is to use pandas.to_parquet (with pyarrow), and to fall back on chunked writing if that fails
  * Added multithreaded reading for DataVendorFlatFile
* 04 Jul 2021
  * Added extra support for reading/writing to S3 buckets of Parquet files
* 02 Jul 2021
  * Can download multiple CSVs in ZIP with time series data (DataVendorWeb) 
* 29 Jun 2021
  * Added downloads badge
* 17 Jun 2021
  * Can download different GDP releases from Bloomberg, without having to specify overrides
* 03 Jun 2021
  * Fix bug in ConfigManager
* 29 May 2021
  * Improved freeform queries for MarketDataRequest
* 26 May 2021
  * Added more flexible ticker calls (with a '_' prefix)
* 23 May 2021
  * Fixed various bugs with reading ECO_RELEASE_DT etc. dates from Bloomberg
  * Fixed bugs when predefined ticker is defined differently in different categories  
  * Can now write Parquet without date truncation errors (eg. ns to us)
  * Reads/writes Parquet from S3
* 22 May 2021
  * Better reading/writing of files to disk
* 21 May 2021
  * Added revision periods to config tickers
* 20 May 2021
  * Added ability to query stored tickers by regular expressions
  * Made field names/code consistent for tickers/vendor_tickers etc.
* 17 May 2021
  * Changed named conventions for CSV conf ticker files to be consistent with MarketDataRequest
* 08 May 2021
  * Added more ways to create a market data request
* 07 May 2021
  * Fixed freeform market data requests when contain different file formats
* 06 May 2021
  * Fixed bug when querying from files with dot in them for parquet
* 04 May 2021
  * Made fetching market data more flexible (can use a string for tickers which are not predefined)
  * Added ability to call predefined tickers with a string
* 29 Apr 2021
  * Bug fix when getting empty ticker from Bloomberg
* 22 Apr 2021
  * Added 404 error for downloading from Dukascopy
* 15 Apr 2021
  * Constant overrides now persist
* 13 Apr 2021
  * Fix issue when conversion of date/time releases for certain Bloomberg economic events
* 25 Mar 2021
  * Fixed empty column download from data vendor downloading as object (rather than NaN)
* 20 Mar 2021
  * Improved Redis caching
  * Can now fetch multiple MarketDataRequests
* 11 Feb 2021
  * Fixed timezone issues in Seasonality
  * Add extra Dukascopy error checking/retry functionality/parameters
  * Added Parquet to MarketDataRequest
  * Started to write Numba implementations for join and align
  * Added fields for downloading eg. FX vol data
* 22 Jan 2021
  * Fixed caching for tick
* 14 Jan 2021
  * Fixed ON FX option date expiry
* 10 Jan 2021
  * Added Dukascopy non-equities example
* 08 Jan 2021
  * Added extra calendar example
* 05 Jan 2021
  * MarketDataRequest accepts parsing of full month names
* 28 Dec 2020
  * Spun out Calendar into separate Python script
* 26 Dec 2020
  * Added missing holiday file
  * Refactored Calendar (so is no longer dependent on Filter)
* 24 Dec 2020
  * Remove logger as field variable in IOEngine
  * Fixed Calendar methods so can take single input
* 19 Dec 2020
  * Added functionality to download FX forwards based total indices from BBG
  * Fixed downloading of forward points for NDFs
  * Fixed missing timestamp issue with DukasCopy
  * Adding holidays functionality and calculation of FX options expiries (and FX delivery dates)
* 10 Dec 2020
    * Added resample method on Calculations for tick data
    * Fixed logger in DataVendorWeb
    * Fixed setting no timezone method
* 11 Nov 2020
    * Added cumulative additive index returns 
    * Removed log as field variable in DataVendorBBG
    * Added 10am NYC cut for FX vol surface download
* 02 Oct 2020
    * Fix vol ticker mapping for 4M points
    * Fix Bloomberg downloader for events
* 30 Sep 2020
    * Fix crypto downloaders (added tickers, fields etc. to CSV files)
* 24 Sep 2020
    * Refactoring of Calculations
* 13 Sep 2020
    * Removed multiprocessing_on_dill as dependency, which is no longer being used
* 10 Sep 2020
    * Adding Eikon as a market data source (daily, intraday and tick market data)
* 25 Aug 2020
    * Fixes for newer Pandas eg. 1.0.5
    * Fixes for ALFRED downloading of economic data
* 24 Aug 2020
    * Removed .ix references (to work with newer Pandas)
* 06 May 2020
    * Amended function to remove points outside FX hours to exclude 1 Jan every year
    * RetStats can now resample time series (removed kurtosis)
    * Tidy up some code comments
* 07 Apr 2020
    * Bug fix in constants
* 06 Apr 2020
    * Minor changes to ConfigManager
* 05 Apr 2020
    * Added push to cache parameter for MarketDataRequest
* 04 Apr 2020
    * Added timeout for Dukascopy download
* 14 Mar 2020
    * Fixed bug with downloading short intervals of Dukascopy tick data
* 20 Feb 2020
    * Made Redis optional dependency
* 30 Dec 2019
    * Added message about lack of Redis
* 17 Dec 2019
    * Fix issue with Redis cache if two similar elements cached (takes the last now)
* 16 Dec 2019
    * Fix problem with missing Redis dependency when reading from market
* 04 Dec 2019
    * Allow usage on Azure Notebooks, by making keyring dependency optional
* 03 Nov 2019
    * Added script to set API keys with keyring
* 02 Nov 2019
    * Added BoE as a data source
    * Removed blosc/msgpack (msgpack deprecated in pandas) and replaced with pyarrow for caching 
    * Uses keyring library for API keys (unless specified in DataCred)
    * Began to add tests for IO and market data download
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
