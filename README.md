# findatapy

findatapy creates an easy to use Python API to download market data from many sources including Quandl, Bloomberg, Yahoo, Google etc. using
a unified high level interface. Users can also define their own custom tickers, using configuraiton files. There is also functionality which
is particularly useful for those downloading FX market data. Below example shows how to download AUDJPY data from Quandl (and automatically 
calculates this via USD crosses).

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

# Gallery

To appear

# Requirements

Major requirements
* Required: Python 3.4, 3.5
* Required: pandas, numpy etc.
* Recommended: Bloomberg Python Open API
    * To use Bloomberg you will need to have a licence (either to access Desktop API or Server API)
    * Use experimental Python 3.4 version from Bloomberg http://www.bloomberglabs.com/api/libraries/
    * Also download C++ version of Bloomberg API and extract into any location
        * eg. C:\blp\blpapi_cpp_3.9.10.1
    * For Python 3.5 - need to compile blpapi source using Microsoft Visual Studio 2015 yourself
        * Install Microsoft Visual Studio 2015 (Community Edition is free)
        * Before doing do be sure to add environment variables for the Bloomberg DLL (blpapi3_64.dll) to PATH variable
            * eg. C:\blp\blpapi_cpp_3.9.10.1\bin
        * Make sure BLPAPI_ROOT root is set as an environmental variable in Windows
            * eg. C:\blp\blpapi_cpp_3.9.10.1
        * python setup.py build
        * python setup.py install
    * For Python 3.4 - prebuilt executable can be run, which means we can skip the build steps above
        * Might need to tweak registry to avoid "Python 3.4 not found in registry error" (blppython.reg example) when using this executable 
    * The library doesn't support the old Bloomberg COM API (since much slower than the new Open API)
* Recommended: chartpy for funky interactive plots (https://github.com/cuemacro/chartpy) and
* Recommended: arctic (AHL library for managing time series in MongoDB)
* Recommended: multiprocessor_on_dill because standard multiprocessing library pickle causes issues 
(from https://github.com/sixty-north/multiprocessing_on_dill)
* Recommended: fredapi from ALFRED/FRED has been rewritten and added to the project directly (from https://github.com/mortada/fredapi)

# Installation

You can install the library using the below. After installation:
* Make sure you edit the DataConstants class for the correct Quandl API and Twitter API keys

```
pip install git+https://github.com/cuemacro/findatapy.git
```

# findatapy examples

In findatapy/examples you will find several demos

# Release Notes

* No formal releases yet

# Coding log

* 20 Dec 2016 - Updated deprecated some pandas deprecated methods in Calculations class
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
