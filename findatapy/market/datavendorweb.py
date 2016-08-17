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
loaderweb

Contains implementations of LoaderTemplate for

Quandl (free/premium data source) - LoaderQuandl
Pandas Data Reader (free data source - includes FRED, World Bank, Yahoo) - LoaderPandasWeb
DukasCopy (retail FX broker - has historical tick data) - LoaderDukasCopy
"""

#######################################################################################################################

"""
LoaderQuandl

Class for reading in data from Quandl into Pyfindatapy library

"""

# support Quandl 3.x.x
try:
    import quandl as Quandl
except:
    # if import fails use Quandl 2.x.x
    import Quandl

from findatapy.market.datavendor import DataVendor

class DataVendorQuandl(DataVendor):

    def __init__(self):
        super(DataVendorQuandl, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        self.logger.info("Request Quandl data")

        data_frame = self.download_daily(market_data_request_vendor)

        if data_frame is None or data_frame.index is []: return None

        # convert from vendor to findatapy tickers/fields
        if data_frame is not None:
            returned_tickers = data_frame.columns

        if data_frame is not None:
            # tidy up tickers into a format that is more easily translatable
            # we can often get multiple fields returned (even if we don't ask for them!)
            # convert to lower case
            returned_fields = [(x.split(' - ')[1]).lower().replace(' ', '-') for x in returned_tickers]
            returned_fields = [x.replace('value', 'close') for x in returned_fields]    # special case for close

            returned_tickers = [x.replace('.', '/') for x in returned_tickers]
            returned_tickers = [x.split(' - ')[0] for x in returned_tickers]

            fields = self.translate_from_vendor_field(returned_fields, market_data_request)
            tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)

            ticker_combined = []

            for i in range(0, len(fields)):
                ticker_combined.append(tickers[i] + "." + fields[i])

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

        self.logger.info("Completed request from Quandl.")

        return data_frame

    def download_daily(self, market_data_request):
        trials = 0

        data_frame = None

        while(trials < 5):
            try:
                data_frame = Quandl.get(market_data_request.tickers, authtoken=DataConstants().quandl_api_key, trim_start=market_data_request.start_date,
                                        trim_end=market_data_request.finish_date)

                break
            except:
                trials = trials + 1
                self.logger.info("Attempting... " + str(trials) + " request to download from Quandl")

        if trials == 5:
            self.logger.error("Couldn't download from Quandl after several attempts!")

        return data_frame

#######################################################################################################################

"""
LoaderPandasWeb

Class for reading in data from various web sources into Pyfindatapy library including

- Yahoo! Finance - yahoo
- Google Finance - google
- St. Louis FED (FRED) - fred
- Kenneth French data library - famafrench
- World Bank - wb

"""

import pandas_datareader.data as web

from findatapy.market.datavendor import DataVendor

class DataVendorPandasWeb(DataVendor):

    def __init__(self):
        super(DataVendorPandasWeb, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        self.logger.info("Request Pandas Web data")

        data_frame = self.download_daily(market_data_request_vendor)

        if market_data_request_vendor.data_source == 'fred':
            returned_fields = ['close' for x in data_frame.columns.values]
            returned_tickers = data_frame.columns.values
        else:
            data_frame = data_frame.to_frame().unstack()

            # print(data_frame.tail())

            if data_frame.index is []: return None

            # convert from vendor to findatapy tickers/fields
            if data_frame is not None:
                returned_fields = data_frame.columns.get_level_values(0)
                returned_tickers = data_frame.columns.get_level_values(1)

        if data_frame is not None:
            fields = self.translate_from_vendor_field(returned_fields, market_data_request)
            tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)

            ticker_combined = []

            for i in range(0, len(fields)):
                ticker_combined.append(tickers[i] + "." + fields[i])

            ticker_requested = []

            for f in market_data_request.fields:
                for t in market_data_request.tickers:
                    ticker_requested.append(t + "." + f)

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

            # only return the requested tickers
            data_frame = pandas.DataFrame(data = data_frame[ticker_requested],
                                          index = data_frame.index, columns = ticker_requested)

        self.logger.info("Completed request from Pandas Web.")

        return data_frame

    def download_daily(self, market_data_request):
        return web.DataReader(market_data_request.tickers, market_data_request.data_source, market_data_request.start_date, market_data_request.finish_date)

########################################################################################################################

"""
LoaderDukascopy

Class for downloading tick data from DukasCopy (note: past month of data is not available). Selecting very large
histories is not recommended as you will likely run out memory given the amount of data requested.

Parsing of files is rewritten version https://github.com/nelseric/ticks/
- parsing has been speeded up considerably
- on-the-fly downloading/parsing

"""

import os
from datetime import timedelta

import pandas
import requests

try:
    from numba import jit
finally:
    pass

# decompress binary files fetched from Dukascopy
try:
    import lzma
except ImportError:
    from backports import lzma

# abstract class on which this is based
from findatapy.market.datavendor import DataVendor

# for logging and constants
from findatapy.util import ConfigManager, DataConstants, LoggerManager

class DataVendorDukasCopy(DataVendor):
    tick_name  = "{symbol}/{year}/{month}/{day}/{hour}h_ticks.bi5"

    def __init__(self):
        super(DataVendor, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

        import logging
        logging.getLogger("requests").setLevel(logging.WARNING)
        self.config = ConfigManager()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        """
        load_ticker - Retrieves market data from external data source (in this case Bloomberg)

        Parameters
        ----------
        market_data_request : TimeSeriesRequest
            contains all the various parameters detailing time series start and finish, tickers etc

        Returns
        -------
        DataFrame
        """

        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        data_frame = None
        self.logger.info("Request Dukascopy data")

        # doesn't support non-tick data
        if (market_data_request.freq in ['daily', 'weekly', 'monthly', 'quarterly', 'yearly', 'intraday', 'minute', 'hourly']):
            self.logger.warning("Dukascopy loader is for tick data only")

            return None

        # assume one ticker only (MarketDataGenerator only calls one ticker at a time)
        if (market_data_request.freq in ['tick']):
            # market_data_request_vendor.tickers = market_data_request_vendor.tickers[0]

            data_frame = self.get_tick(market_data_request, market_data_request_vendor)

            if data_frame is not None: data_frame.tz_localize('UTC')

        self.logger.info("Completed request from Dukascopy")

        return data_frame

    def kill_session(self):
        return

    def get_tick(self, market_data_request, market_data_request_vendor):

        data_frame = self.download_tick(market_data_request_vendor)

        # convert from vendor to findatapy tickers/fields
        if data_frame is not None:
            returned_fields = data_frame.columns
            returned_tickers = [market_data_request_vendor.tickers[0]] * (len(returned_fields))

        if data_frame is not None:
            fields = self.translate_from_vendor_field(returned_fields, market_data_request)
            tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)

            ticker_combined = []

            for i in range(0, len(fields)):
                ticker_combined.append(tickers[i] + "." + fields[i])

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

        return data_frame

    def download_tick(self, market_data_request):

        symbol = market_data_request.tickers[0]
        df_list = []

        self.logger.info("About to download from Dukascopy... for " + symbol)

        # single threaded
        df_list = [self.fetch_file(time, symbol) for time in
                  self.hour_range(market_data_request.start_date, market_data_request.finish_date)]

        # TODO parallel (has pickle issues)
        # time_list = self.hour_range(market_data_request.start_date, market_data_request.finish_date)
        # import multiprocessing_on_dill as multiprocessing
        #
        # pool = multiprocessing.Pool(processes=4)
        # results = [pool.apply_async(self.fetch_file, args=(time, symbol)) for time in time_list]
        # df_list = [p.get() for p in results]

        try:
            return pandas.concat(df_list)
        except:
            return None

    def fetch_file(self, time, symbol):
        if time.hour % 24 == 0: self.logger.info("Downloading... " + str(time))

        tick_path = self.tick_name.format(
                symbol = symbol,
                year = str(time.year).rjust(4, '0'),
                month = str(time.month).rjust(2, '0'),
                day = str(time.day).rjust(2, '0'),
                hour = str(time.hour).rjust(2, '0')
            )

        tick = self.fetch_tick(DataConstants().dukascopy_base_url + tick_path)

        if DataConstants().dukascopy_write_temp_tick_disk:
            out_path = DataConstants().temp_folder + "/dkticks/" + tick_path

            if not os.path.exists(out_path):
                if not os.path.exists(os.path.dirname(out_path)):
                    os.makedirs(os.path.dirname(out_path))

            self.write_tick(tick, out_path)

        try:
            return self.retrieve_df(lzma.decompress(tick), symbol, time)
        except:
            return None

    def fetch_tick(self, tick_url):
        i = 0
        tick_request = None

        # try up to 5 times to download
        while i < 5:
            try:
                tick_request = requests.get(tick_url)
                i = 5
            except:
                i = i + 1

        if (tick_request is None):
            self.logger("Failed to download from " + tick_url)
            return None

        return tick_request.content

    def write_tick(self, content, out_path):
        data_file = open(out_path, "wb+")
        data_file.write(content)
        data_file.close()

    def chunks(self, list, n):
        if n < 1:
            n = 1
        return [list[i:i + n] for i in range(0, len(list), n)]

    def retrieve_df(self, data, symbol, epoch):
        date, tuple = self.parse_tick_data(data, epoch)

        df = pandas.DataFrame(data = tuple, columns=['temp', 'ask', 'bid', 'askv', 'bidv'], index = date)
        df.drop('temp', axis = 1)
        df.index.name = 'Date'

        divisor = 100000

        # where JPY is the terms currency we have different divisor
        if symbol[3:6] == 'JPY':
            divisor = 1000

        # prices are returned without decimal point
        df['bid'] =  df['bid'] /  divisor
        df['ask'] =  df['ask'] / divisor

        return df

    def hour_range(self, start_date, end_date):
          delta_t = end_date - start_date

          delta_hours = (delta_t.days *  24.0) + (delta_t.seconds / 3600.0)

          for n in range(int (delta_hours)):
              yield start_date + timedelta(0, 0, 0, 0, 0, n) # Hours

    def parse_tick_data(self, data, epoch):
        import struct

        # tick = namedtuple('Tick', 'Date ask bid askv bidv')

        chunks_list = self.chunks(data, 20)
        parsed_list = []
        date = []

        # note: Numba can speed up for loops
        for row in chunks_list:
            d = struct.unpack(">LLLff", row)
            date.append((epoch + timedelta(0,0,0, d[0])))

            # SLOW: no point using named tuples!
            # row_data = tick._asdict(tick._make(d))
            # row_data['Date'] = (epoch + timedelta(0,0,0,row_data['Date']))

            parsed_list.append(d)

        return date, parsed_list

    def chunks(self, list, n):
        if n < 1: n = 1

        return [list[i:i + n] for i in range(0, len(list), n)]

    def get_daily_data(self):
        pass