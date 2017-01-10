__author__ = 'saeedamen' # Saeed Amen

#
# Copyright 2016 Cuemacro
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
# License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and limitations under the License.
#

"""
MarketDataGenerator

Returns market data time series by directly calling data sources like Bloomberg (bloomberg), Yahoo (yahoo), Quandl (quandl),
FRED (fred) etc. which are implemented in subclasses of LoaderTemplate. This provides a common wrapper for all these data sources.

"""

import copy

from findatapy.market.ioengine import IOEngine
from findatapy.timeseries import Filter, Calculations
from findatapy.util import DataConstants, LoggerManager, ConfigManager

class MarketDataGenerator(object):
    _time_series_cache = {} # shared across all instances of object!

    def __init__(self):
        self.config = ConfigManager().get_instance()
        self.logger = LoggerManager().getLogger(__name__)
        self.filter = Filter()
        self.calculations = Calculations()
        self.io_engine = IOEngine()
        self._intraday_code = -1

        return

    def flush_cache(self):
        """
        flush_cache - Flushs internal cache of time series
        """

        self._time_series_cache = {}

    def set_intraday_code(self, code):
        self._intraday_code = code

    def get_data_vendor(self, source):
        """
        get_loader - Loads appropriate data service class

        Parameters
        ----------
        source : str
            the data service to use "bloomberg", "quandl", "yahoo", "google", "fred" etc.
            we can also have forms like "bloomberg-boe" separated by hyphens

        Returns
        -------
        DataVendor
        """

        data_vendor = None

        source = source.split("-")[0]

        if source == 'bloomberg':
            from findatapy.market.datavendorbbg import DataVendorBBGOpen
            data_vendor = DataVendorBBGOpen()

        elif source == 'quandl':
            from findatapy.market.datavendorweb import DataVendorQuandl
            data_vendor = DataVendorQuandl()

        elif source == 'ons':
            from findatapy.market.datavendorweb import DataVendorONS
            data_vendor = DataVendorONS()

        elif source == 'boe':
            from findatapy.market.datavendorweb import DataVendorBOE
            data_vendor = DataVendorBOE()

        elif source == 'dukascopy':
            from findatapy.market.datavendorweb  import DataVendorDukasCopy
            data_vendor = DataVendorDukasCopy()

        elif source == 'alfred':
            from findatapy.market.datavendorweb  import DataVendorALFRED
            data_vendor = DataVendorALFRED()

        elif source in ['yahoo', 'google', 'fred', 'oecd', 'eurostat', 'edgar-index']:
            from findatapy.market.datavendorweb  import DataVendorPandasWeb
            data_vendor = DataVendorPandasWeb()

        # TODO add support for other data sources (like Reuters)

        return data_vendor

    def fetch_market_data(self, market_data_request, kill_session = True):
        """
        fetch_market_data - Loads time series from specified data provider

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains various properties describing time series to fetched, including ticker, start & finish date etc.

        Returns
        -------
        pandas.DataFrame
        """

        tickers = market_data_request.tickers
        data_vendor = self.get_data_vendor(market_data_request.data_source)

        # check if tickers have been specified (if not load all of them for a category)
        # also handle single tickers/list tickers
        create_tickers = False

        if tickers is None :
            create_tickers = True
        elif isinstance(tickers, str):
            if tickers == '': create_tickers = True
        elif isinstance(tickers, list):
            if tickers == []: create_tickers = True

        if create_tickers:
            market_data_request.tickers = ConfigManager().get_instance().get_tickers_list_for_category(
            market_data_request.category, market_data_request.data_source, market_data_request.freq, market_data_request.cut)

        # intraday or tick: only one ticker per cache file
        if (market_data_request.freq in ['intraday', 'tick', 'second', 'hour', 'minute']):
            data_frame_agg = self.download_intraday_tick(market_data_request, data_vendor)

        # daily: multiple tickers per cache file - assume we make one API call to vendor library
        else: data_frame_agg = self.download_daily(market_data_request, data_vendor)

        if('internet_load' in market_data_request.cache_algo):
            self.logger.debug("Internet loading.. ")

            # signal to data_vendor template to exit session
            # if data_vendor is not None and kill_session == True: data_vendor.kill_session()

        if(market_data_request.cache_algo == 'cache_algo'):
            self.logger.debug("Only caching data in memory, do not return any time series."); return

        # only return time series if specified in the algo
        if 'return' in market_data_request.cache_algo:
            # special case for events/events-dt which is not indexed like other tables (also same for downloading futures
            # contracts dates)
            if market_data_request.category is not None:
                if 'events' in market_data_request.category:
                    return data_frame_agg

            try:
                return self.filter.filter_time_series(market_data_request, data_frame_agg, pad_columns=True)
            except:
                if data_frame_agg is not None:
                    return data_frame_agg

                import traceback

                self.logger.error(traceback.format_exc())

                return None

    def get_market_data_cached(self, market_data_request):
        """
        get_time_series_cached - Loads time series from cache (if it exists)

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains various properties describing time series to fetched, including ticker, start & finish date etc.

        Returns
        -------
        pandas.DataFrame
        """

        if (market_data_request.freq == "intraday"):
            ticker = market_data_request.tickers
        else:
            ticker = None

        fname = self.create_time_series_hash_key(market_data_request, ticker)

        if (fname in self._time_series_cache):
            data_frame = self._time_series_cache[fname]

            return self.filter.filter_time_series(market_data_request, data_frame)

        return None

    def create_time_series_hash_key(self, market_data_request, ticker = None):
        """
        create_time_series_hash_key - Creates a hash key for retrieving the time series

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains various properties describing time series to fetched, including ticker, start & finish date etc.

        Returns
        -------
        str
        """

        if(isinstance(ticker, list)):
            ticker = ticker[0]

        return self.create_cache_file_name(self.create_category_key(market_data_request, ticker))

    def download_intraday_tick(self, market_data_request, data_vendor):
        """
        download_intraday_tick - Loads intraday time series from specified data provider

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains various properties describing time series to fetched, including ticker, start & finish date etc.

        Returns
        -------
        pandas.DataFrame
        """

        data_frame_agg = None
        calcuations = Calculations()

        ticker_cycle = 0

        data_frame_group = []

        # single threaded version
        # handle intraday ticker calls separately one by one
        if len(market_data_request.tickers) == 1 or DataConstants().market_thread_no['other'] == 1:
            for ticker in market_data_request.tickers:
                market_data_request_single = copy.copy(market_data_request)
                market_data_request_single.tickers = ticker

                if market_data_request.vendor_tickers is not None:
                    market_data_request_single.vendor_tickers = [market_data_request.vendor_tickers[ticker_cycle]]
                    ticker_cycle = ticker_cycle + 1

                # we downscale into float32, to avoid memory problems in Python (32 bit)
                # data is stored on disk as float32 anyway
                data_frame_single = data_vendor.load_ticker(market_data_request_single)

                # if the vendor doesn't provide any data, don't attempt to append
                if data_frame_single is not None:
                    if data_frame_single.empty == False:
                        data_frame_single.index.name = 'Date'
                        data_frame_single = data_frame_single.astype('float32')

                        data_frame_group.append(data_frame_single)

                        # # if you call for returning multiple tickers, be careful with memory considerations!
                        # if data_frame_agg is not None:
                        #     data_frame_agg = data_frame_agg.join(data_frame_single, how='outer')
                        # else:
                        #     data_frame_agg = data_frame_single

                # key = self.create_category_key(market_data_request, ticker)
                # fname = self.create_cache_file_name(key)
                # self._time_series_cache[fname] = data_frame_agg  # cache in memory (disable for intraday)


            # if you call for returning multiple tickers, be careful with memory considerations!
            if data_frame_group is not None:
                data_frame_agg = calcuations.pandas_outer_join(data_frame_group)

            return data_frame_agg
        else:
            market_data_request_list = []

            # create a list of MarketDataRequests
            for ticker in market_data_request.tickers:
                market_data_request_single = copy.copy(market_data_request)
                market_data_request_single.tickers = ticker

                if hasattr(market_data_request, 'vendor_tickers'):
                    market_data_request_single.vendor_tickers = [market_data_request.vendor_tickers[ticker_cycle]]
                    ticker_cycle = ticker_cycle + 1

                market_data_request_list.append(market_data_request_single)

            return self.fetch_group_time_series(market_data_request_list)

    def fetch_single_time_series(self, market_data_request):
        data_frame_single = self.get_data_vendor(market_data_request.data_source).load_ticker(market_data_request)

        if data_frame_single is not None:
            if data_frame_single.empty == False:
                data_frame_single.index.name = 'Date'

                # will fail for dataframes which includes dates
                try:
                    data_frame_single = data_frame_single.astype('float32')
                except: pass

                if market_data_request.freq == "second":
                    data_frame_single = data_frame_single.resample("1s")

        return data_frame_single

    def fetch_group_time_series(self, market_data_request_list):

        data_frame_agg = None

        # depends on the nature of operation as to whether we should use threading or multiprocessing library
        if DataConstants().market_thread_technique is "thread":
            from multiprocessing.dummy import Pool
        else:
            # most of the time is spend waiting for Bloomberg to return, so can use threads rather than multiprocessing
            # must use the multiprocessing_on_dill library otherwise can't pickle objects correctly
            # note: currently not very stable
            from multiprocessing_on_dill import Pool

        thread_no = DataConstants().market_thread_no['other']

        if market_data_request_list[0].data_source in DataConstants().market_thread_no:
            thread_no = DataConstants().market_thread_no[market_data_request_list[0].data_source]

        if thread_no > 0:
            pool = Pool(thread_no)

            # open the market data downloads in their own threads and return the results
            result = pool.map_async(self.fetch_single_time_series, market_data_request_list)
            data_frame_group = result.get()

            pool.close()
            pool.join()
        else:
            data_frame_group = []

            for md_request in market_data_request_list:
                data_frame_group.append(self.fetch_single_time_series(md_request))

        # collect together all the time series
        if data_frame_group is not None:
            data_frame_group = [i for i in data_frame_group if i is not None]

            if data_frame_group is not None:
                data_frame_agg = self.calculations.pandas_outer_join(data_frame_group)

        return data_frame_agg

    def download_daily(self, market_data_request, data_vendor):
        """
        download_daily - Loads daily time series from specified data provider

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains various properties describing time series to fetched, including ticker, start & finish date etc.

        Returns
        -------
        pandas.DataFrame
        """

        # daily data does not include ticker in the key, as multiple tickers in the same file

        if DataConstants().market_thread_no['other'] == 1:
            data_frame_agg = data_vendor.load_ticker(market_data_request)
        else:
            market_data_request_list = []
            
            # when trying your example 'equitiesdata_example' I had a -1 result so it went out of the comming loop and I had errors in execution
            group_size = max(int(len(market_data_request.tickers) / DataConstants().market_thread_no['other'] - 1),0) 

            if group_size == 0: group_size = 1

            # split up tickers into groups related to number of threads to call
            for i in range(0, len(market_data_request.tickers), group_size):
                market_data_request_single = copy.copy(market_data_request)
                market_data_request_single.tickers = market_data_request.tickers[i:i + group_size]

                if market_data_request.vendor_tickers is not None:
                    market_data_request_single.vendor_tickers = \
                        market_data_request.vendor_tickers[i:i + group_size]

                market_data_request_list.append(market_data_request_single)

            data_frame_agg = self.fetch_group_time_series(market_data_request_list)

        key = self.create_category_key(market_data_request)
        fname = self.create_cache_file_name(key)
        self._time_series_cache[fname] = data_frame_agg  # cache in memory (ok for daily data)

        return data_frame_agg

    def create_category_key(self, market_data_request, ticker=None):
        """
        create_category_key - Returns a category key for the associated MarketDataRequest

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains various properties describing time series to fetched, including ticker, start & finish date etc.

        Returns
        -------
        str
        """

        category = 'default-cat'
        cut = 'default-cut'

        if market_data_request.category is not None: category = market_data_request.category

        environment = market_data_request.environment
        source = market_data_request.data_source
        freq = market_data_request.freq

        if market_data_request.cut is not None: cut = market_data_request.cut

        if (ticker is not None): key = environment + "." + category + '.' + source + '.' + freq + '.' + cut + '.' + ticker
        else: key = environment + "." + category + '.' + source + '.' + freq + '.' + cut

        return key

    def create_cache_file_name(self, filename):
        return DataConstants().folder_time_series_data + "/" + filename
