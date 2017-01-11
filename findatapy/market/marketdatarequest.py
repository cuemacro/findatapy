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

from findatapy.util.loggermanager import LoggerManager
from datetime import timedelta
import datetime

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
    # freq
    # gran_freq (minute, daily, hourly, daily, weekly, monthly, yearly)
    # fields (can be list)
    # vendor_tickers (optional)
    # vendor_fields (optional)
    # cache_algo (eg. internet, disk, memory) - internet will forcibly download from the internet
    # environment (eg. prod, backtest) - old data is saved with prod, backtest will overwrite the last data point
    def __init__(self, data_source = None,
                 start_date ='year', finish_date = datetime.datetime.utcnow(),
                 tickers = None, category = None, freq_mult = 1, freq = "daily",
                 gran_freq = None, cut = "NYC",
                 fields = ['close'], cache_algo = "internet_load_return",
                 vendor_tickers = None, vendor_fields = None,
                 environment = "backtest", trade_side = 'trade', md_request = None
                 ):

        self.logger = LoggerManager().getLogger(__name__)

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
                self.tickers = copy.deepcopy(md_request.tickers)
                self.category = copy.deepcopy(md_request.category)  # special predefined categories

                self.cut = copy.deepcopy(md_request.cut)                        # closing time of the data (eg. NYC, LDN, TOK etc)
                self.fields = copy.deepcopy(md_request.fields)                  # fields, eg. close, high, low, open
                self.cache_algo = copy.deepcopy(md_request.cache_algo)          # internet_load_return (cache_algo_return is for future use)
                self.vendor_tickers = copy.deepcopy(md_request.vendor_tickers)  # define vendor tickers
                self.vendor_fields = copy.deepcopy(md_request.vendor_fields)    # define vendor fields
                self.environment = copy.deepcopy(md_request.environment)        # backtest environment only supported at present
                self.trade_side = copy.deepcopy(md_request.trade_side)
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
            self.tickers = tickers
            self.category = category                # special predefined categories

            self.cut = cut                          # closing time of the data (eg. NYC, LDN, TOK etc)
            self.fields = fields                    # fields, eg. close, high, low, open
            self.cache_algo = cache_algo            # internet_load_return (cache_algo_return is for future use)
            self.vendor_tickers = vendor_tickers    # define vendor tickers
            self.vendor_fields = vendor_fields      # define vendor fields
            self.environment = environment          # backtest environment only supported at present
            self.trade_side = trade_side

    @property
    def data_source(self):
        return self.__data_source

    @data_source.setter
    def data_source(self, data_source):
        try:
            valid_data_source = ['ats', 'bloomberg', 'dukascopy', 'fred', 'gain', 'google', 'quandl', 'yahoo']

            if not data_source in valid_data_source:
                self.logger.warning(data_source & " is not a defined data source.")
        except: pass

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

        valid_freq = ['tick', 'second', 'minute', 'intraday', 'hourly', 'daily', 'weekly', 'monthly', 'quarterly', 'annually']

        if not freq in valid_freq:
            self.logger.warning(freq + " is not a defined frequency")

        self.__freq = freq

    @property
    def gran_freq(self):
        return self.__gran_freq

    @gran_freq.setter
    def gran_freq(self, gran_freq):
        try:
            gran_freq = gran_freq.lower()

            valid_gran_freq = ['tick', 'second', 'minute', 'hourly', 'pseudodaily', 'daily', 'weekly', 'monthly', 'quarterly', 'annually']

            if not gran_freq in valid_gran_freq:
                self.logger.warning(gran_freq & " is not a defined frequency")

            if gran_freq in ['minute', 'hourly']:
                self.__freq = 'intraday'
            elif gran_freq in ['tick', 'second']:
                self.__freq = 'tick'
            else:
                self.__freq = 'daily'
        except: pass

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

    def date_parser(self, date):
        if isinstance(date, str):

            date1 = datetime.datetime.utcnow()

            if date is 'midnight':
                date1 = datetime.datetime(date1.year, date1.month, date1.day, 0, 0, 0)
            elif date is 'decade':
                date1 = date1 - timedelta(days=360 * 10)
            elif date is 'year':
                date1 = date1 - timedelta(days=360)
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
            date1 = date

        return date1

    @property
    def cache_algo(self):
        return self.__cache_algo

    @cache_algo.setter
    def cache_algo(self, cache_algo):
        cache_algo = cache_algo.lower()

        valid_cache_algo = ['internet_load', 'internet_load_return', 'cache_algo', 'cache_algo_return']


        if not cache_algo in valid_cache_algo:
            self.logger.warning(cache_algo + " is not a defined caching scheme")

        self.__cache_algo = cache_algo

    @property
    def environment(self):
        return self.__environment

    @environment.setter
    def environment(self, environment):
        environment = environment.lower()

        valid_environment= ['prod', 'backtest']

        if not environment in valid_environment:
            self.logger.warning(environment + " is not a defined environment.")

        self.__environment = environment

    @property
    def trade_side(self):
        return self.__trade_side

    @trade_side.setter
    def trade_side(self, trade_side):
        trade_side = trade_side.lower()

        valid_trade_side = ['trade', 'bid', 'ask']

        if not trade_side in valid_trade_side:
            self.logger.warning(trade_side + " is not a defined trade side.")

        self.__trade_side = trade_side