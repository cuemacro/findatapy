__author__ = "saeedamen"  # Saeed Amen

#
# Copyright 2016 Cuemacro
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on a "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.
#

from findatapy.util.dataconstants import DataConstants
from findatapy.util.loggermanager import LoggerManager

from datetime import timedelta
import datetime

import copy

class MarketDataRequest(object):
    """Provides parameters for requesting market data.

    Includes parameters to define the ticker we'd like to fetch, the start and
    finish dates for our request, as well as
    the various fields we would like and also the frequency of the data.

    """

    # properties
    #
    # data_source eg. bbg, yahoo, quandl
    # start_date
    
    # finish_date
    # tickers (can be list) eg. EURUSD
    # category (eg. fx, equities, fixed_income, cal_event, fundamental)
    # freq_mult (eg. 1)
    # freq (tick, intraday or daily)
    # gran_freq (minute, daily, hourly, daily, weekly, monthly, yearly)
    # fields (can be list)
    # vendor_tickers (optional)
    # vendor_fields (optional)
    # cache_algo (eg. internet, disk, memory) - internet will forcibly download 
    # from the internet
    # abstract_curve (optional)
    # environment (eg. prod, backtest) - old data is saved with prod, backtest 
    # will overwrite the last data point
    # overrides (optional) - if you need to specify any data overrides 
    # (eg. for BBG)

    def generate_key(self):
        """Generate a key to describe this MarketDataRequest object, which can 
        be used in a cache, as a hash-style key

        Returns
        -------
        str
            Key to describe this MarketDataRequest

        """
        from findatapy.market.ioengine import SpeedCache

        if self.freq == "daily":
            ticker = None
        else:
            ticker = self.tickers[0]

        self.__category_key = self.create_category_key(
            md_request=self, ticker=ticker)

        return SpeedCache().generate_key(
                self,
             ["logger",
              "_MarketDataRequest__abstract_curve",
              "_MarketDataRequest__cache_algo",
              "_MarketDataRequest__overrides",
              "_MarketDataRequest__data_vendor_custom"]) + "_df"

    def __init__(self, data_source=None,
                 start_date="year", finish_date=datetime.datetime.utcnow(),
                 tickers=None, category=None, freq_mult=1, freq="daily",
                 gran_freq=None, cut="NYC",
                 fields=["close"], cache_algo="internet_load_return",
                 vendor_tickers=None, vendor_fields=None,
                 environment=None,
                 trade_side="trade", expiry_date=None, 
                 resample=None, resample_how="last",

                 split_request_chunks=0,
                 list_threads=1,

                 fx_vol_part=None,
                 fx_vol_tenor=None,
                 fx_forwards_tenor=None,
                 base_depos_currencies=None,
                 base_depos_tenor=None,

                 data_engine=None,

                 md_request=None, abstract_curve=None, 
                 quandl_api_key=None,
                 fred_api_key=None,
                 alpha_vantage_api_key=None,
                 eikon_api_key=None,

                 pretransformation=None,
                 vintage_as_index=None,
                 
                 push_to_cache=True,
                 overrides={},
                 freeform_md_request={},
                 data_vendor_custom=None):

        data_constants = DataConstants()

        if environment is None:
            environment = data_constants.default_data_environment
        if fx_vol_part is None:
            fx_vol_part = data_constants.fx_vol_part
        if fx_vol_tenor is None:
            fx_vol_tenor = data_constants.fx_vol_tenor
        if fx_forwards_tenor is None:
            fx_forwards_tenor = data_constants.fx_forwards_tenor
        if base_depos_currencies is None:
            base_depos_currencies = data_constants.base_depos_currencies
        if base_depos_tenor is None:
            base_depos_tenor = data_constants.base_depos_tenor
        if data_engine is None:
            data_engine = data_constants.default_data_engine
        if quandl_api_key is None:
            quandl_api_key = data_constants.quandl_api_key
        if fred_api_key is None:
            fred_api_key = data_constants.fred_api_key
        if alpha_vantage_api_key is None:
            alpha_vantage_api_key = data_constants.alpha_vantage_api_key
        if eikon_api_key is None:
            eikon_api_key = data_constants.eikon_api_key
        if data_vendor_custom is None:
            data_vendor_custom = data_constants.data_vendor_custom

        # Can deep copy MarketDataRequest (use a lock, so can be used with 
        # threading when downloading time series)
        if md_request is not None:
            import threading
            lock = threading.Lock()

            with lock:
                import copy

                self.freq_mult = copy.deepcopy(md_request.freq_mult)

                # define frequency of data
                self.gran_freq = copy.deepcopy(md_request.gran_freq)
                self.freq_mult = copy.deepcopy(md_request.freq_mult)
                self.freq = copy.deepcopy(md_request.freq)

                # data source, start and fin
                self.data_source = copy.deepcopy(md_request.data_source)
                self.start_date = copy.deepcopy(md_request.start_date)
                self.finish_date = copy.deepcopy(md_request.finish_date)

                self.category = copy.deepcopy(md_request.category)  # special predefined categories

                self.cut = copy.deepcopy(md_request.cut)  # closing time of the data (eg. NYC, LDN, TOK etc)
                self.fields = copy.deepcopy(md_request.fields)  # fields, eg. close, high, low, open
                self.cache_algo = copy.deepcopy(
                    md_request.cache_algo)  # internet_load_return (cache_algo_return is for future use)
                self.vendor_tickers = copy.deepcopy(md_request.vendor_tickers)  # define vendor tickers
                self.vendor_fields = copy.deepcopy(md_request.vendor_fields)  # define vendor fields
                self.environment = copy.deepcopy(
                    md_request.environment)  # backtest environment only supported at present
                self.trade_side = copy.deepcopy(md_request.trade_side)
                self.expiry_date = copy.deepcopy(md_request.expiry_date)
                self.resample = copy.deepcopy(md_request.resample)
                self.resample_how = copy.deepcopy(md_request.resample_how)

                self.split_request_chunks = \
                    copy.deepcopy(md_request.split_request_chunks)
                self.list_threads = copy.deepcopy(md_request.list_threads)

                self.fx_vol_part = copy.deepcopy(md_request.fx_vol_part)
                self.fx_vol_tenor = copy.deepcopy(md_request.fx_vol_tenor)
                self.fx_forwards_tenor = copy.deepcopy(md_request.fx_forwards_tenor)
                self.base_depos_currencies = \
                    copy.deepcopy(md_request.base_depos_currencies)
                self.base_depos_tenor = \
                    copy.deepcopy(md_request.base_depos_tenor)
                
                self.data_engine = copy.deepcopy(md_request.data_engine)

                self.abstract_curve = copy.deepcopy(md_request.abstract_curve)
                self.quandl_api_key = copy.deepcopy(md_request.quandl_api_key)
                self.fred_api_key = copy.deepcopy(md_request.fred_api_key)
                self.alpha_vantage_api_key = \
                    copy.deepcopy(md_request.alpha_vantage_api_key)
                self.eikon_api_key = copy.deepcopy(md_request.eikon_api_key)
                
                self.pretransformation = copy.deepcopy(md_request.pretransformation)
                self.vintage_as_index = copy.deepcopy(md_request.vintage_as_index)

                self.overrides = copy.deepcopy(md_request.overrides)
                self.push_to_cache = copy.deepcopy(md_request.push_to_cache)
                self.freeform_md_request = \
                    copy.deepcopy(md_request.freeform_md_request)

                self.tickers = copy.deepcopy(md_request.tickers)  # Need this after category in case have wildcard
                self.data_vendor_custom = md_request.data_vendor_custom
        else:
            self.freq_mult = freq_mult

            # define frequency of data
            self.gran_freq = gran_freq
            self.freq_mult = freq_mult
            self.freq = freq

            # data source, start and fin
            self.data_source = data_source
            self.start_date = start_date
            self.finish_date = finish_date
            self.category = category  # special predefined categories

            self.cut = cut  # closing time of the data (eg. NYC, LDN, TOK etc)
            self.fields = fields  # fields, eg. close, high, low, open
            self.cache_algo = cache_algo  # internet_load_return (cache_algo_return is for future use)
            self.vendor_tickers = vendor_tickers  # define vendor tickers
            self.vendor_fields = vendor_fields  # define vendor fields
            self.environment = environment  # backtest environment only supported at present
            self.trade_side = trade_side
            self.expiry_date = expiry_date
            self.resample = resample
            self.resample_how = resample_how
            
            self.split_request_chunks = split_request_chunks
            self.list_threads = list_threads

            self.fx_vol_part = fx_vol_part
            self.fx_vol_tenor = fx_vol_tenor
            self.fx_forwards_tenor = fx_forwards_tenor
            self.base_depos_currencies = base_depos_currencies
            self.base_depos_tenor = base_depos_tenor
            
            self.data_engine = data_engine

            self.abstract_curve = abstract_curve
            
            self.quandl_api_key = quandl_api_key
            self.fred_api_key = fred_api_key
            self.alpha_vantage_api_key = alpha_vantage_api_key
            self.eikon_api_key = eikon_api_key
            
            self.pretransformation = pretransformation
            self.vintage_as_index = vintage_as_index

            self.overrides = overrides
            self.push_to_cache = push_to_cache

            self.freeform_md_request = freeform_md_request

            self.tickers = tickers

            if self.tickers is None:
                self.tickers = vendor_tickers

            self.old_tickers = self.tickers
            self.data_vendor_custom = data_vendor_custom

    def __str__(self):
        return "MarketDataRequest summary - " + self.generate_key()

    def create_category_key(self, md_request=None, ticker=None):
        """Returns a category key for the associated MarketDataRequest, which 
        can be used to create filenames (or as part of a storage key in a cache)

        Parameters
        ----------
        md_request : MarketDataRequest
            contains various properties describing time series to fetched, 
            including ticker, start & finish date etc.

        Returns
        -------
        str
        """

        category = "default-cat"
        cut = "default-cut"

        if md_request is None:
            md_request = self

        if md_request.category is not None: category = md_request.category

        environment = md_request.environment
        source = md_request.data_source
        freq = md_request.freq

        if ticker is None and (md_request.freq == "intraday" 
                               or md_request.freq == "tick"):
            ticker = md_request.tickers[0]

        if md_request.cut is not None: cut = md_request.cut

        if (ticker is not None):
            key = str(environment) + "." + str(category) + "." + str(source) \
                  + "." + str(freq) + "." + str(cut) \
                  + "." + str(ticker)
        else:
            key = str(environment) + "." + str(category) + "." + str(source) \
                  + "." + str(freq) + "." + str(cut)

        return key

    @property
    def data_source(self):
        return self.__data_source

    @data_source.setter
    def data_source(self, data_source):
        try:
            valid_data_source = ["ats", "bloomberg", "dukascopy", "fred", 
                                 "gain", "google", "quandl", "yahoo",
                                 "boe", "eikon"]

            if not data_source in valid_data_source:
                LoggerManager().getLogger(__name__).warning(
                    data_source & " is not a defined data source.")
        except:
            pass

        self.__data_source = data_source

    @property
    def category(self):
        return self.__category

    @category.setter
    def category(self, category):
        self.__category = category

    @property
    def tickers(self):
        return self.__tickers

    @tickers.setter
    def tickers(self, tickers):
        if tickers is not None:
            if not isinstance(tickers, list):
                tickers = [tickers]

        config = None

        new_tickers = []

        if tickers is not None:
            for tick in tickers:
                if "*" in tick:
                    start = ""

                    if tick[-1] == "*" and tick[0] != "*":
                        start = "^"

                    tick = start + "(" + tick.replace("*", "") + ")"

                    if config is None:
                        from findatapy.util import ConfigManager
                        config = ConfigManager().get_instance()

                    new_tickers.append(
                        config.get_filtered_tickers_list_for_category(
                        self.__category, self.__data_source, self.__freq, 
                            self.__cut, tick))
                else:
                    new_tickers.append(tick)

            new_tickers = self._flatten_list(new_tickers)

            self.__tickers = new_tickers
        else:
            self.__tickers = tickers
    
    
    @property
    def old_tickers(self):
        return self.__old_tickers

    @old_tickers.setter
    def old_tickers(self, old_tickers):
        self.__old_tickers = old_tickers
        
    @property
    def fields(self):
        return self.__fields

    @fields.setter
    def fields(self, fields):
        valid_fields = ["open", "high", "low", "close", "volume", "numEvents"]

        if not isinstance(fields, list):
            fields = [fields]

        for field_entry in fields:
            if not field_entry in valid_fields:
                i = 0
                # self.logger.warning(field_entry + " is not a valid field.")

        # add error checking

        self.__fields = fields

    @property
    def vendor_tickers(self):
        return self.__vendor_tickers

    @vendor_tickers.setter
    def vendor_tickers(self, vendor_tickers):
        if vendor_tickers is not None:
            if not isinstance(vendor_tickers, list):
                vendor_tickers = [vendor_tickers]

        self.__vendor_tickers = vendor_tickers

    @property
    def vendor_fields(self):
        return self.__vendor_fields

    @vendor_fields.setter
    def vendor_fields(self, vendor_fields):
        if vendor_fields is not None:
            if not isinstance(vendor_fields, list):
                vendor_fields = [vendor_fields]

        self.__vendor_fields = vendor_fields

    @property
    def freq(self):
        return self.__freq

    @freq.setter
    def freq(self, freq):
        freq = freq.lower()

        valid_freq = ["tick", "second", "minute", "intraday", "hourly", 
                      "daily", "weekly", "monthly", "quarterly",
                      "annually"]

        if not freq in valid_freq:
            LoggerManager().getLogger(__name__).warning(freq + 
                                                        " is not a defined frequency")

        self.__freq = freq

    @property
    def gran_freq(self):
        return self.__gran_freq

    @gran_freq.setter
    def gran_freq(self, gran_freq):
        try:
            gran_freq = gran_freq.lower()

            valid_gran_freq = ["tick", "second", "minute", "hourly", 
                               "pseudodaily", "daily", "weekly", "monthly",
                               "quarterly", "annually"]

            if not gran_freq in valid_gran_freq:
                LoggerManager().getLogger(__name__).warning(
                    gran_freq + " is not a defined frequency")

            if gran_freq in ["minute", "hourly"]:
                self.__freq = "intraday"
            elif gran_freq in ["tick", "second"]:
                self.__freq = "tick"
            else:
                self.__freq = "daily"
        except:
            pass

        self.__gran_freq = gran_freq

    @property
    def freq_mult(self):
        return self.__freq_mult

    @freq_mult.setter
    def freq_mult(self, freq_mult):
        self.__freq_mult = freq_mult

    @property
    def start_date(self):
        return self.__start_date

    @start_date.setter
    def start_date(self, start_date):
        self.__start_date = self.date_parser(start_date)

    @property
    def finish_date(self):
        return self.__finish_date

    @finish_date.setter
    def finish_date(self, finish_date):
        self.__finish_date = self.date_parser(finish_date)

    @property
    def cut(self):
        return self.__cut

    @cut.setter
    def cut(self, cut):
        self.__cut = cut

    @property
    def resample(self):
        return self.__resample

    @resample.setter
    def resample(self, resample):
        self.__resample = resample

    @property
    def resample_how(self):
        return self.__resample_how

    @resample_how.setter
    def resample_how(self, resample_how):
        self.__resample_how = resample_how
        
    @property
    def split_request_chunks(self):
        return self.__split_request_chunks

    @split_request_chunks.setter
    def split_request_chunks(self, split_request_chunks):
        self.__split_request_chunks = split_request_chunks
        
    @property
    def list_threads(self):
        return self.__list_threads

    @list_threads.setter
    def list_threads(self, list_threads):
        self.__list_threads = list_threads

    def date_parser(self, date):
        if isinstance(date, str):

            date1 = datetime.datetime.utcnow()

            if date == "midnight":
                date1 = datetime.datetime(date1.year, date1.month, 
                                          date1.day, 0, 0, 0)
            elif date == "decade":
                date1 = date1 - timedelta(days=365 * 10)
            elif date == "year":
                date1 = date1 - timedelta(days=365)
            elif date == "month":
                date1 = date1 - timedelta(days=30)
            elif date == "week":
                date1 = date1 - timedelta(days=7)
            elif date == "day":
                date1 = date1 - timedelta(days=1)
            elif date == "hour":
                date1 = date1 - timedelta(hours=1)
            else:
                # format expected "Jun 1 2005 01:33", "%b %d %Y %H:%M"
                try:
                    date1 = datetime.datetime.strptime(date, "%b %d %Y %H:%M")
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                # format expected "1 Jun 2005 01:33", "%d %b %Y %H:%M"
                try:
                    date1 = datetime.datetime.strptime(date, "%d %b %Y %H:%M")
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                # format expected "1 June 2005 01:33", "%d %B %Y %H:%M"
                try:
                    date1 = datetime.datetime.strptime(date, "%d %B %Y %H:%M")
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, "%b %d %Y")
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, "%d %b %Y")
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, "%B %d %Y")
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, "%d %B %Y")
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0
        else:
            import pandas

            date1 = pandas.Timestamp(date)

        return date1

    @property
    def cache_algo(self):
        return self.__cache_algo

    @cache_algo.setter
    def cache_algo(self, cache_algo):
        cache_algo = cache_algo.lower()

        valid_cache_algo = ["internet_load", "internet_load_return", 
                            "cache_algo", "cache_algo_return"]

        if not cache_algo in valid_cache_algo:
            LoggerManager().getLogger(__name__).warning(cache_algo + 
                                                        " is not a defined caching scheme")

        self.__cache_algo = cache_algo

    @property
    def environment(self):
        return self.__environment

    @environment.setter
    def environment(self, environment):
        environment = environment.lower()

        valid_environment = DataConstants().possible_data_environment

        if not environment in valid_environment:
            LoggerManager().getLogger(__name__).warning(
                environment + " is not a defined environment.")

        self.__environment = environment

    @property
    def trade_side(self):
        return self.__trade_side

    @trade_side.setter
    def trade_side(self, trade_side):
        trade_side = trade_side.lower()

        valid_trade_side = ["trade", "bid", "ask"]

        if not trade_side in valid_trade_side:
            LoggerManager().getLogger(__name__).warning(
                trade_side + " is not a defined trade side.")

        self.__trade_side = trade_side

    @property
    def expiry_date(self):
        return self.__expiry_date

    @expiry_date.setter
    def expiry_date(self, expiry_date):
        self.__expiry_date = self.date_parser(expiry_date)
        
    ###### For FX vol, FX forwards and base depos ######
    @property
    def fx_vol_part(self):
        return self.__fx_vol_part

    @fx_vol_part.setter
    def fx_vol_part(self, fx_vol_part):
        self.__fx_vol_part = fx_vol_part
        
    @property
    def fx_vol_tenor(self):
        return self.__fx_vol_tenor

    @fx_vol_tenor.setter
    def fx_vol_tenor(self, fx_vol_tenor):
        self.__fx_vol_tenor = fx_vol_tenor
        
    @property
    def fx_forwards_tenor(self):
        return self.__fx_forwards_tenor

    @fx_forwards_tenor.setter
    def fx_forwards_tenor(self, fx_forwards_tenor):
        self.__fx_forwards_tenor = fx_forwards_tenor

    @property
    def base_depos_currencies(self):
        return self.__base_depos_currencies

    @base_depos_currencies.setter
    def base_depos_currencies(self, base_depos_currencies):
        self.__base_depos_currencies = base_depos_currencies
        
    @property
    def base_depos_tenor(self):
        return self.__base_depos_tenor

    @base_depos_tenor.setter
    def base_depos_tenor(self, base_depos_tenor):
        self.__base_depos_tenor = base_depos_tenor
        
    @property
    def data_engine(self):
        return self.__data_engine

    @data_engine.setter
    def data_engine(self, data_engine):
        self.__data_engine = data_engine

    ######
    @property
    def abstract_curve(self):
        return self.__abstract_curve

    @abstract_curve.setter
    def abstract_curve(self, abstract_curve):
        if abstract_curve is not None:
            self.__abstract_curve_key = abstract_curve.generate_key()
        else:
            self.__abstract_curve_key = None

        self.__abstract_curve = abstract_curve
        
    @property
    def quandl_api_key(self):
        return self.__quandl_api_key

    @quandl_api_key.setter
    def quandl_api_key(self, quandl_api_key):
        self.__quandl_api_key = quandl_api_key

    @property
    def fred_api_key(self):
        return self.__fred_api_key

    @fred_api_key.setter
    def fred_api_key(self, fred_api_key):
        self.__fred_api_key = fred_api_key
        
    @property
    def alpha_vantage_api_key(self):
        return self.__alpha_vantage_api_key

    @alpha_vantage_api_key.setter
    def alpha_vantage_api_key(self, alpha_vantage_api_key):
        self.__alpha_vantage_api_key = alpha_vantage_api_key
        
    @property
    def eikon_api_key(self):
        return self.__eikon_api_key

    @eikon_api_key.setter
    def eikon_api_key(self, eikon_api_key):
        self.__eikon_api_key = eikon_api_key

    @property
    def pretransformation(self):
        return self.__pretransformation

    @pretransformation.setter
    def pretransformation(self, pretransformation):
        if not isinstance(pretransformation, list):
            pretransformation = [pretransformation]

        self.__pretransformation = pretransformation
        
    @property
    def vintage_as_index(self):
        return self.__vintage_as_index

    @vintage_as_index.setter
    def vintage_as_index(self, vintage_as_index):
        self.__vintage_as_index = vintage_as_index
        
    @property
    def overrides(self):
        return self.__overrides

    @overrides.setter
    def overrides(self, overrides):
        self.__overrides = overrides
        
    @property
    def push_to_cache(self):
        return self.__push_to_cache

    @push_to_cache.setter
    def push_to_cache(self, push_to_cache):
        self.__push_to_cache = push_to_cache
        
    @property
    def freeform_md_request(self):
        return self.__freeform_md_request

    @freeform_md_request.setter
    def freeform_md_request(self, freeform_md_request):
        self.__freeform_md_request = freeform_md_request

    @property
    def data_vendor_custom(self):
        return self.__data_vendor_custom

    @data_vendor_custom.setter
    def data_vendor_custom(self, data_vendor_custom):
        self.__data_vendor_custom = data_vendor_custom

    def _flatten_list(self, list_of_lists):
        """Flattens list, particularly useful for combining baskets

        Parameters
        ----------
        list_of_lists : str (list)
            List to be flattened

        Returns
        -------

        """
        result = []

        for i in list_of_lists:
            # Only append if i is a basestring (superclass of string)
            if isinstance(i, str):
                result.append(i)
            # Otherwise call this function recursively
            else:
                result.extend(self._flatten_list(i))
        return result
