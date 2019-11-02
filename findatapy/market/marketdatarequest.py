__author__ = 'saeedamen'

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

from findatapy.util.dataconstants import DataConstants
from findatapy.util.loggermanager import LoggerManager

from datetime import timedelta
import datetime

import copy

data_constants = DataConstants()


class MarketDataRequest(object):
    """Provides parameters for requesting market data.

    Includes parameters to define the ticker we'd like to fetch, the start and finish dates for our request, as well as
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
    # cache_algo (eg. internet, disk, memory) - internet will forcibly download from the internet
    # abstract_curve (optional)
    # environment (eg. prod, backtest) - old data is saved with prod, backtest will overwrite the last data point
    # overrides (optional) - if you need to specify any data overrides (eg. for BBG)

    def generate_key(self):
        """Generate a key to describe this MarketDataRequest object, which can be used in a cache, as a hash-style key

        Returns
        -------
        str
            Key to describe this MarketDataRequest

        """
        from findatapy.market.ioengine import SpeedCache

        if self.freq == 'daily':
            ticker = None
        else:
            ticker = self.tickers[0]

        self.__category_key = self.create_category_key(self, ticker=ticker)

        return SpeedCache().generate_key(self, ['logger', '_MarketDataRequest__abstract_curve',
                                                '_MarketDataRequest__cache_algo',
                                                '_MarketDataRequest__overrides'])

    def __init__(self, data_source=None,
                 start_date='year', finish_date=datetime.datetime.utcnow(),
                 tickers=None, category=None, freq_mult=1, freq="daily",
                 gran_freq=None, cut="NYC",
                 fields=['close'], cache_algo="internet_load_return",
                 vendor_tickers=None, vendor_fields=None,
                 environment="backtest", trade_side='trade', expiry_date=None, resample=None, resample_how='last',
                 md_request=None, abstract_curve=None, quandl_api_key=data_constants.quandl_api_key,
                 fred_api_key=data_constants.fred_api_key, alpha_vantage_api_key=data_constants.alpha_vantage_api_key, 
                 overrides={}
                 ):

        # can deep copy MarketDataRequest (use a lock, so can be used with threading when downloading time series)
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
                self.abstract_curve = copy.deepcopy(md_request.abstract_curve)
                self.quandl_api_key = copy.deepcopy(md_request.quandl_api_key)
                self.fred_api_key = copy.deepcopy(md_request.fred_api_key)
                self.alpha_vantage_api_key = copy.deepcopy(md_request.alpha_vantage_api_key)
                self.overrides = copy.deepcopy(md_request.overrides)

                self.tickers = copy.deepcopy(md_request.tickers)  # need this after category in case have wildcard
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
            self.abstract_curve = abstract_curve
            
            self.quandl_api_key = quandl_api_key
            self.fred_api_key = fred_api_key
            self.alpha_vantage_api_key = alpha_vantage_api_key

            self.overrides = overrides

            self.tickers = tickers

    def create_category_key(self, market_data_request, ticker=None):
        """Returns a category key for the associated MarketDataRequest, which can be used to create filenames (or
        as part of a storage key in a cache)

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

        if (ticker is not None):
            key = str(environment) + "." + str(category) + '.' + str(source) + '.' + str(freq) + '.' + str(cut) \
                  + '.' + str(ticker)
        else:
            key = str(environment) + "." + str(category) + '.' + str(source) + '.' + str(freq) + '.' + str(cut)

        return key

    @property
    def data_source(self):
        return self.__data_source

    @data_source.setter
    def data_source(self, data_source):
        try:
            valid_data_source = ['ats', 'bloomberg', 'dukascopy', 'fred', 'gain', 'google', 'quandl', 'yahoo',
                                 'boe']

            if not data_source in valid_data_source:
                LoggerManager().getLogger(__name__).warning(data_source & " is not a defined data source.")
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
                if '*' in tick:
                    start = ''

                    if tick[-1] == "*" and tick[0] != "*":
                        start = "^"

                    tick = start + "(" + tick.replace('*', '') + ")"

                    if config is None:
                        from findatapy.util import ConfigManager
                        config = ConfigManager().get_instance()

                    new_tickers.append(config.get_filtered_tickers_list_for_category(
                        self.__category, self.__data_source, self.__freq, self.__cut, tick))
                else:
                    new_tickers.append(tick)

            new_tickers = self._flatten_list(new_tickers)

            self.__tickers = new_tickers
        else:
            self.__tickers = tickers

    @property
    def fields(self):
        return self.__fields

    @fields.setter
    def fields(self, fields):
        valid_fields = ['open', 'high', 'low', 'close', 'volume', 'numEvents']

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

        valid_freq = ['tick', 'second', 'minute', 'intraday', 'hourly', 'daily', 'weekly', 'monthly', 'quarterly',
                      'annually']

        if not freq in valid_freq:
            LoggerManager().getLogger(__name__).warning(freq + " is not a defined frequency")

        self.__freq = freq

    @property
    def gran_freq(self):
        return self.__gran_freq

    @gran_freq.setter
    def gran_freq(self, gran_freq):
        try:
            gran_freq = gran_freq.lower()

            valid_gran_freq = ['tick', 'second', 'minute', 'hourly', 'pseudodaily', 'daily', 'weekly', 'monthly',
                               'quarterly', 'annually']

            if not gran_freq in valid_gran_freq:
                LoggerManager().getLogger(__name__).warning(gran_freq & " is not a defined frequency")

            if gran_freq in ['minute', 'hourly']:
                self.__freq = 'intraday'
            elif gran_freq in ['tick', 'second']:
                self.__freq = 'tick'
            else:
                self.__freq = 'daily'
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

    def date_parser(self, date):
        if isinstance(date, str):

            date1 = datetime.datetime.utcnow()

            if date is 'midnight':
                date1 = datetime.datetime(date1.year, date1.month, date1.day, 0, 0, 0)
            elif date is 'decade':
                date1 = date1 - timedelta(days=365 * 10)
            elif date is 'year':
                date1 = date1 - timedelta(days=365)
            elif date is 'month':
                date1 = date1 - timedelta(days=30)
            elif date is 'week':
                date1 = date1 - timedelta(days=7)
            elif date is 'day':
                date1 = date1 - timedelta(days=1)
            elif date is 'hour':
                date1 = date1 - timedelta(hours=1)
            else:
                # format expected 'Jun 1 2005 01:33', '%b %d %Y %H:%M'
                try:
                    date1 = datetime.datetime.strptime(date, '%b %d %Y %H:%M')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                # format expected '1 Jun 2005 01:33', '%d %b %Y %H:%M'
                try:
                    date1 = datetime.datetime.strptime(date, '%d %b %Y %H:%M')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, '%b %d %Y')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, '%d %b %Y')
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

        valid_cache_algo = ['internet_load', 'internet_load_return', 'cache_algo', 'cache_algo_return']

        if not cache_algo in valid_cache_algo:
            LoggerManager().getLogger(__name__).warning(cache_algo + " is not a defined caching scheme")

        self.__cache_algo = cache_algo

    @property
    def environment(self):
        return self.__environment

    @environment.setter
    def environment(self, environment):
        environment = environment.lower()

        valid_environment = ['prod', 'backtest']

        if not environment in valid_environment:
            LoggerManager().getLogger(__name__).warning(environment + " is not a defined environment.")

        self.__environment = environment

    @property
    def trade_side(self):
        return self.__trade_side

    @trade_side.setter
    def trade_side(self, trade_side):
        trade_side = trade_side.lower()

        valid_trade_side = ['trade', 'bid', 'ask']

        if not trade_side in valid_trade_side:
            LoggerManager().getLogger(__name__).warning(trade_side + " is not a defined trade side.")

        self.__trade_side = trade_side

    @property
    def expiry_date(self):
        return self.__expiry_date

    @expiry_date.setter
    def expiry_date(self, expiry_date):
        self.__expiry_date = self.date_parser(expiry_date)

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
    def overrides(self):
        return self.__overrides

    @overrides.setter
    def overrides(self, overrides):
        self.__overrides = overrides

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
