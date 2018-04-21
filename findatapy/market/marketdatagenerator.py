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

import copy

import pandas

from findatapy.market.ioengine import IOEngine
from findatapy.market.marketdatarequest import MarketDataRequest
from findatapy.timeseries import Filter, Calculations
from findatapy.util import DataConstants, LoggerManager, ConfigManager, SwimPool

class MarketDataGenerator(object):
    """Returns market data time series by directly calling market data sources.

    At present it supports Bloomberg (bloomberg), Yahoo (yahoo), Quandl (quandl), FRED (fred) etc. which are implemented
    in subclasses of DataVendor class. This provides a common wrapper for all these data sources.

    """

    def __init__(self):
        self.config = ConfigManager().get_instance()
        self.logger = LoggerManager().getLogger(__name__)
        self.filter = Filter()
        self.calculations = Calculations()
        self.io_engine = IOEngine()
        self._intraday_code = -1
        self.days_expired_intraday_contract_download = -1

        return

    def set_intraday_code(self, code):
        self._intraday_code = code

    def get_data_vendor(self, source):
        """Loads appropriate data service class

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

        try:
            source = source.split("-")[0]
        except:
            self.logger.error("Was data source specified?")

            return None

        if source == 'bloomberg':
            try:
                from findatapy.market.datavendorbbg import DataVendorBBGOpen
                data_vendor = DataVendorBBGOpen()
            except:
                self.logger.warn("Bloomberg needs to be installed")

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

        elif source == 'fxcm':
            from findatapy.market.datavendorweb  import DataVendorFXCM
            data_vendor = DataVendorFXCM()

        elif source == 'alfred':
            from findatapy.market.datavendorweb  import DataVendorALFRED
            data_vendor = DataVendorALFRED()

        elif source in ['yahoo', 'google', 'fred', 'oecd', 'eurostat', 'edgar-index']:
            from findatapy.market.datavendorweb  import DataVendorPandasWeb
            data_vendor = DataVendorPandasWeb()

        elif source == 'bitcoincharts':
            from findatapy.market.datavendorweb import DataVendorBitcoincharts
            data_vendor = DataVendorBitcoincharts()
        elif source == 'poloniex':
            from findatapy.market.datavendorweb import DataVendorPoloniex
            data_vendor = DataVendorPoloniex()
        elif source == 'binance':
            from findatapy.market.datavendorweb import DataVendorBinance
            data_vendor = DataVendorBinance()
        elif source == 'bitfinex':
            from findatapy.market.datavendorweb import DataVendorBitfinex
            data_vendor = DataVendorBitfinex()
        elif source == 'gdax':
            from findatapy.market.datavendorweb import DataVendorGdax
            data_vendor = DataVendorGdax()
        elif source == 'kraken':
            from findatapy.market.datavendorweb import DataVendorKraken
            data_vendor = DataVendorKraken()
        elif source == 'bitmex':
            from findatapy.market.datavendorweb import DataVendorBitmex
            data_vendor = DataVendorBitmex()
        elif '.csv' in source or '.h5' in source:
            from findatapy.market.datavendorweb import DataVendorFlatFile
            data_vendor = DataVendorFlatFile()
        elif source == 'alphavantage':
            from findatapy.market.datavendorweb import DataVendorAlphaVantage
            data_vendor = DataVendorAlphaVantage()
        elif source == 'huobi':
            from findatapy.market.datavendorweb import DataVendorHuobi
            data_vendor = DataVendorHuobi()

        # TODO add support for other data sources (like Reuters)

        return data_vendor

    def fetch_market_data(self, market_data_request, kill_session = True):
        """Loads time series from specified data provider

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains various properties describing time series to fetched, including ticker, start & finish date etc.

        Returns
        -------
        pandas.DataFrame
        """

        # data_vendor = self.get_data_vendor(market_data_request.data_source)

        # check if tickers have been specified (if not load all of them for a category)
        # also handle single tickers/list tickers
        create_tickers = False

        if market_data_request.vendor_tickers is not None and market_data_request.tickers is None:
            market_data_request.tickers = market_data_request.vendor_tickers

        tickers = market_data_request.tickers

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
            data_frame_agg = self.download_intraday_tick(market_data_request)
     #       return data_frame_agg

        # daily: multiple tickers per cache file - assume we make one API call to vendor library
        else:
            data_frame_agg = self.download_daily(market_data_request)

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

            # pad columns a second time (is this necessary to do here again?)
            # TODO only do this for not daily data?
            try:
                data_frame_agg = self.filter.filter_time_series(market_data_request, data_frame_agg, pad_columns=True)\
                    .dropna(how = 'all')

                # resample data using pandas if specified in the MarketDataRequest
                if market_data_request.resample is not None:
                    if 'last' in market_data_request.resample_how:
                        data_frame_agg = data_frame_agg.resample(market_data_request.resample).last()
                    elif 'first' in market_data_request.resample_how:
                        data_frame_agg = data_frame_agg.resample(market_data_request.resample).first()

                    if 'dropna' in market_data_request.resample_how:
                        data_frame_agg = data_frame_agg.dropna(how = 'all')

                return data_frame_agg
            except:
                if data_frame_agg is not None:
                    return data_frame_agg

                import traceback

                self.logger.warn("No data returned for " + str(market_data_request.tickers))

                return None

    def create_time_series_hash_key(self, market_data_request, ticker = None):
        """Creates a hash key for retrieving the time series

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

        return self.create_cache_file_name(MarketDataRequest().create_category_key(market_data_request, ticker))

    def download_intraday_tick(self, market_data_request):
        """Loads intraday time series from specified data provider

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
                # old_finish_date = market_data_request_single.finish_date
                #
                # market_data_request_single.finish_date = self.refine_expiry_date(market_data_request)
                #
                # if market_data_request_single.finish_date >= market_data_request_single.start_date:
                #     data_frame_single = data_vendor.load_ticker(market_data_request_single)
                # else:
                #     data_frame_single = None
                #
                # market_data_request_single.finish_date = old_finish_date
                #
                # data_frame_single = data_vendor.load_ticker(market_data_request_single)

                data_frame_single = self.fetch_single_time_series(market_data_request)

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

                if market_data_request.vendor_tickers is not None:
                    market_data_request_single.vendor_tickers = [market_data_request.vendor_tickers[ticker_cycle]]
                    ticker_cycle = ticker_cycle + 1

                market_data_request_list.append(market_data_request_single)

            return self.fetch_group_time_series(market_data_request_list)

    def fetch_single_time_series(self, market_data_request):

        market_data_request = MarketDataRequest(md_request=market_data_request)

        # only includes those tickers have not expired yet!
        start_date = pandas.Timestamp(market_data_request.start_date).date()

        import datetime

        current_date = datetime.datetime.utcnow().date()

        from datetime import timedelta

        tickers = market_data_request.tickers
        vendor_tickers = market_data_request.vendor_tickers

        expiry_date = market_data_request.expiry_date

        config = ConfigManager().get_instance()

        # in many cases no expiry is defined so skip them
        for i in range(0, len(tickers)):
            try:
                expiry_date = config.get_expiry_for_ticker(market_data_request.data_source, tickers[i])
            except:
                pass

            if expiry_date is not None:
                expiry_date = pandas.Timestamp(expiry_date).date()

                # use pandas Timestamp, a bit more robust with weird dates (can fail if comparing date vs datetime)
                # if the expiry is before the start date of our download don't bother downloading this ticker
                if  expiry_date < start_date:
                    tickers[i] = None

                # special case for futures-contracts which are intraday
                # avoid downloading if the expiry date is very far in the past
                # (we need this before there might be odd situations where we run on an expiry date, but still want to get
                # data right till expiry time)
                if market_data_request.category == 'futures-contracts' and market_data_request.freq == 'intraday' \
                        and self.days_expired_intraday_contract_download > 0:

                    if expiry_date + timedelta(days=self.days_expired_intraday_contract_download) < current_date:
                        tickers[i] = None

                if vendor_tickers is not None and tickers[i] is None:
                    vendor_tickers[i] = None

        market_data_request.tickers = [e for e in tickers if e != None]

        if vendor_tickers is not None:
            market_data_request.vendor_tickers = [e for e in vendor_tickers if e != None]

        data_frame_single = None

        if len(market_data_request.tickers) > 0:
            data_frame_single = self.get_data_vendor(market_data_request.data_source).load_ticker(market_data_request)
            #print(data_frame_single.head(n=10))

        if data_frame_single is not None:
            if data_frame_single.empty == False:
                data_frame_single.index.name = 'Date'

                # will fail for dataframes which includes dates/strings (eg. futures contract names)
                try:
                    data_frame_single = data_frame_single.astype('float32')
                except:
                    self.logger.warning('Could not convert to float')

                if market_data_request.freq == "second":
                    data_frame_single = data_frame_single.resample("1s")

        return data_frame_single

    def fetch_group_time_series(self, market_data_request_list):

        data_frame_agg = None

        thread_no = DataConstants().market_thread_no['other']

        if market_data_request_list[0].data_source in DataConstants().market_thread_no:
            thread_no = DataConstants().market_thread_no[market_data_request_list[0].data_source]

        if thread_no > 0:
            pool = SwimPool().create_pool(thread_technique = DataConstants().market_thread_technique, thread_no=thread_no)

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

            # for debugging!
            # import pickle
            # import datetime
            # pickle.dump(data_frame_group, open(str(datetime.datetime.now()).replace(':', '-').replace(' ', '-').replace(".", "-") + ".p", "wb"))

            if data_frame_group is not None:
                try:
                    data_frame_agg = self.calculations.pandas_outer_join(data_frame_group)
                except Exception as e:
                    self.logger.warning('Possible overlap of columns? Have you specifed same ticker several times: ' + str(e))

        return data_frame_agg

    def download_daily(self, market_data_request):
        """Loads daily time series from specified data provider

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains various properties describing time series to fetched, including ticker, start & finish date etc.

        Returns
        -------
        pandas.DataFrame
        """

        key = MarketDataRequest().create_category_key(market_data_request)

        is_key_overriden = False

        for k in DataConstants().override_multi_threading_for_categories:
            if k in key:
                is_key_overriden = True
                break

        # by default use other
        thread_no = DataConstants().market_thread_no['other']

        if market_data_request.data_source in DataConstants().market_thread_no:
            thread_no = DataConstants().market_thread_no[market_data_request.data_source]

        # daily data does not include ticker in the key, as multiple tickers in the same file
        if thread_no == 1:
            # data_frame_agg = data_vendor.load_ticker(market_data_request)
            data_frame_agg = self.fetch_single_time_series(market_data_request)
        else:
            market_data_request_list = []
            
            # when trying your example 'equitiesdata_example' I had a -1 result so it went out of the comming loop and I had errors in execution
            group_size = max(int(len(market_data_request.tickers) / thread_no - 1),0)

            if group_size == 0: group_size = 1

            # split up tickers into groups related to number of threads to call
            for i in range(0, len(market_data_request.tickers), group_size):
                market_data_request_single = copy.copy(market_data_request)
                market_data_request_single.tickers = market_data_request.tickers[i:i + group_size]

                if market_data_request.vendor_tickers is not None:
                    market_data_request_single.vendor_tickers = \
                        market_data_request.vendor_tickers[i:i + group_size]

                market_data_request_list.append(market_data_request_single)

            # special case where we make smaller calls one after the other
            if is_key_overriden:

                data_frame_list = []

                for md in market_data_request_list:
                    data_frame_list.append(self.fetch_single_time_series(md))

                data_frame_agg = self.calculations.pandas_outer_join(data_frame_list)
            else:
                data_frame_agg = self.fetch_group_time_series(market_data_request_list)

        # fname = self.create_cache_file_name(key)
        # self._time_series_cache[fname] = data_frame_agg  # cache in memory (ok for daily data)

        return data_frame_agg

    def refine_expiry_date(self, market_data_request):

        # expiry date
        if market_data_request.expiry_date is None:
            ConfigManager().get_instance().get_expiry_for_ticker(market_data_request.data_source, market_data_request.ticker)

        return market_data_request

    def create_cache_file_name(self, filename):
        return DataConstants().folder_time_series_data + "/" + filename
