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

"""Contains implementations of DataVendor for

Quandl (free/premium data source) - DataVendorQuandl
ALFRED (free datasource) - DataVendorALFRED
Pandas Data Reader (free data source - includes FRED, World Bank, Yahoo) - DataVendorPandasWeb
DukasCopy (retail FX broker - has historical tick data) - DataVendorDukasCopy
ONS (free datasource) - DataVendorONS (incomplete)
BOE (free datasource) - DataVendorBOE (incomplete)
Bitcoinchart - DataVendorBitcoincharts



"""

#######################################################################################################################

# support Quandl 3.x.x
try:
    import quandl as Quandl
except:
    # if import fails use Quandl 2.x.x
    import Quandl

from findatapy.market.datavendor import DataVendor

from findatapy.market import IOEngine

import pandas

class DataVendorQuandl(DataVendor):
    """Reads in data from Quandl into findatapy library

    """

    def __init__(self):
        super(DataVendorQuandl, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        logger.info("Request Quandl data")

        data_frame = self.download_daily(market_data_request_vendor)

        if data_frame is None or data_frame.index is []: return None

        # convert from vendor to findatapy tickers/fields
        if data_frame is not None:
            returned_tickers = data_frame.columns

        if data_frame is not None:
            # tidy up tickers into a format that is more easily translatable
            # we can often get multiple fields returned (even if we don't ask for them!)
            # convert to lower case
            returned_fields = [(x.split(' - ')[1]).lower().replace(' ', '-').replace('.', '-').replace('--', '-') for x in returned_tickers]

            returned_fields = [x.replace('value', 'close') for x in returned_fields]    # special case for close

            # quandl doesn't always return the field name
            for i in range(0,len(returned_fields)):
                ticker = returned_tickers[i].split('/')[1].split(' - ')[0].lower()

                if ticker == returned_fields[i]:
                    returned_fields[i] = 'close'

            # replace time fields (can cause problems later for times to start with 0)
            for i in range(0, 10):
                returned_fields = [x.replace('0'+ str(i) + ':00', str(i) + ':00') for x in returned_fields]

            returned_tickers = [x.replace('.', '/') for x in returned_tickers]
            returned_tickers = [x.split(' - ')[0] for x in returned_tickers]

            try:
                fields = self.translate_from_vendor_field(returned_fields, market_data_request)
                tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)
            except:
                print('error')

            ticker_combined = []

            for i in range(0, len(tickers)):
                try:
                    ticker_combined.append(tickers[i] + "." + fields[i])
                except:
                    ticker_combined.append(tickers[i] + ".close")

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

        logger.info("Completed request from Quandl for " + str(ticker_combined))

        return data_frame

    def download_daily(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        
        trials = 0

        data_frame = None

        while(trials < 5):
            try:
                data_frame = Quandl.get(market_data_request.tickers,
                                        authtoken=market_data_request.quandl_api_key, trim_start=market_data_request.start_date,
                                        trim_end=market_data_request.finish_date)

                break
            except SyntaxError:
                logger.error("The tickers %s do not exist on Quandl." % market_data_request.tickers)
                break
            except Exception as e:
                trials = trials + 1
                logger.info("Attempting... " + str(trials) + " request to download from Quandl due to following error: " + str(e))

        if trials == 5:
            logger.error("Couldn't download from Quandl after several attempts!")

        return data_frame

#######################################################################################################################

# # support Quandl 3.x.x
# try:
#     import fredapi
#     from fredapi import Fred
# except:
#     pass

from findatapy.market.datavendor import DataVendor
from findatapy.timeseries import Filter, Calculations

class DataVendorALFRED(DataVendor):
    """Class for reading in data from ALFRED (and FRED) into findatapy library (based upon fredapi from
    https://github.com/mortada/fredapi

    """

    def __init__(self):
        super(DataVendorALFRED, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        logger.info("Request ALFRED/FRED data")

        data_frame = self.download_daily(market_data_request_vendor)

        if data_frame is None or data_frame.index is []: return None

        # convert from vendor to findatapy tickers/fields
        if data_frame is not None:
            returned_tickers = data_frame.columns

        if data_frame is not None:
            # tidy up tickers into a format that is more easily translatable
            # we can often get multiple fields returned (even if we don't ask for them!)
            # convert to lower case
            returned_fields = [(x.split('.')[1]) for x in returned_tickers]
            returned_tickers = [(x.split('.')[0]) for x in returned_tickers]

            try:
                fields = self.translate_from_vendor_field(returned_fields, market_data_request)
                tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)
            except:
                print('error')

            ticker_combined = []

            for i in range(0, len(fields)):
                ticker_combined.append(tickers[i] + "." + fields[i])

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

        logger.info("Completed request from ALFRED/FRED for " + str(ticker_combined))

        return data_frame

    def download_daily(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        
        trials = 0

        data_frame_list = []
        data_frame_release = []

        # TODO refactor this code, a bit messy at the moment!
        for i in range(0, len(market_data_request.tickers)):
            while (trials < 5):
                try:
                    fred = Fred(api_key=market_data_request.fred_api_key)

                    # acceptable fields: close, actual-release, release-date-time-full
                    if 'close' in market_data_request.fields and 'release-date-time-full' in market_data_request.fields:
                        data_frame = fred.get_series_all_releases(market_data_request.tickers[i],
                                                                  observation_start=market_data_request.start_date,
                                                                  observation_end=market_data_request.finish_date)

                        data_frame.columns = ['Date', market_data_request.tickers[i] + '.release-date-time-full',
                                              market_data_request.tickers[i] + '.close']

                        data_frame = data_frame.sort_values(by=['Date', market_data_request.tickers[i] + '.release-date-time-full'])
                        data_frame = data_frame.drop_duplicates(subset=['Date'], keep='last')
                        data_frame = data_frame.set_index(['Date'])

                        filter = Filter()
                        data_frame = filter.filter_time_series_by_date(market_data_request.start_date,
                                                                       market_data_request.finish_date, data_frame)

                        data_frame_list.append(data_frame)
                    elif 'close' in market_data_request.fields:

                        data_frame = fred.get_series(series_id=market_data_request.tickers[i],
                                                     observation_start=market_data_request.start_date,
                                                     observation_end=market_data_request.finish_date)

                        data_frame = pandas.DataFrame(data_frame)
                        data_frame.columns = [market_data_request.tickers[i] + '.close']
                        data_frame_list.append(data_frame)

                    if 'first-revision' in market_data_request.fields:
                        data_frame = fred.get_series_first_revision(market_data_request.tickers[i],
                                                                    observation_start=market_data_request.start_date,
                                                                    observation_end=market_data_request.finish_date)

                        data_frame = pandas.DataFrame(data_frame)
                        data_frame.columns = [market_data_request.tickers[i] + '.first-revision']

                        filter = Filter()
                        data_frame = filter.filter_time_series_by_date(market_data_request.start_date,
                                                                       market_data_request.finish_date, data_frame)

                        data_frame_list.append(data_frame)

                    if 'actual-release' in market_data_request.fields and 'release-date-time-full' in market_data_request.fields:
                        data_frame = fred.get_series_all_releases(market_data_request.tickers[i],
                                                                  observation_start=market_data_request.start_date,
                                                                  observation_end=market_data_request.finish_date)

                        data_frame.columns = ['Date', market_data_request.tickers[i] + '.release-date-time-full',
                                              market_data_request.tickers[i] + '.actual-release']

                        data_frame = data_frame.sort_values(by=['Date', market_data_request.tickers[i] + '.release-date-time-full'])
                        data_frame = data_frame.drop_duplicates(subset=['Date'], keep='first')
                        data_frame = data_frame.set_index(['Date'])

                        filter = Filter()
                        data_frame = filter.filter_time_series_by_date(market_data_request.start_date,
                                                                       market_data_request.finish_date, data_frame)

                        data_frame_list.append(data_frame)

                    elif 'actual-release' in market_data_request.fields:
                        data_frame = fred.get_series_first_release(market_data_request.tickers[i],
                                                                   observation_start=market_data_request.start_date,
                                                                   observation_end=market_data_request.finish_date)

                        data_frame = pandas.DataFrame(data_frame)
                        data_frame.columns = [market_data_request.tickers[i] + '.actual-release']

                        filter = Filter()
                        data_frame = filter.filter_time_series_by_date(market_data_request.start_date,
                                                                       market_data_request.finish_date, data_frame)

                        data_frame_list.append(data_frame)

                    elif 'release-date-time-full' in market_data_request.fields:
                        data_frame = fred.get_series_all_releases(market_data_request.tickers[i],
                                                                  observation_start=market_data_request.start_date,
                                                                  observation_end=market_data_request.finish_date)

                        data_frame = data_frame['realtime_start']

                        data_frame = pandas.DataFrame(data_frame)
                        data_frame.columns = [market_data_request.tickers[i] + '.release-date-time-full']

                        data_frame.index = data_frame[market_data_request.tickers[i] + '.release-date-time-full']
                        data_frame = data_frame.sort()
                        data_frame = data_frame.drop_duplicates()

                        filter = Filter()
                        data_frame_release.append(filter.filter_time_series_by_date(market_data_request.start_date,
                                                                       market_data_request.finish_date, data_frame))

                    break
                except Exception as e:
                    trials = trials + 1
                    logger.info("Attempting... " + str(trials) + " request to download from ALFRED/FRED" + str(e))

            if trials == 5:
                logger.error("Couldn't download from ALFRED/FRED after several attempts!")

        calc = Calculations()

        data_frame1 = calc.pandas_outer_join(data_frame_list)
        data_frame2 = calc.pandas_outer_join(data_frame_release)

        data_frame = pandas.concat([data_frame1, data_frame2], axis=1)

        return data_frame


#######################################################################################################################

class DataVendorONS(DataVendor):

    def __init__(self):
        super(DataVendorONS, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        logger.info("Request ONS data")

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

        logger.info("Completed request from ONS.")

        return data_frame

    def download_daily(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        trials = 0

        data_frame = None

        while(trials < 5):
            try:
                # TODO

                break
            except:
                trials = trials + 1
                logger.info("Attempting... " + str(trials) + " request to download from ONS")

        if trials == 5:
            logger.error("Couldn't download from ONS after several attempts!")

        return data_frame

#######################################################################################################################

import pandas as pd

class DataVendorBOE(DataVendor):

    def __init__(self):
        super(DataVendorBOE, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        logger.info("Request BOE data")


        data_frame = self.download_daily(market_data_request_vendor)

        if data_frame is None or data_frame.index is []:
            return None

        # convert from vendor to findatapy tickers/fields
        if data_frame is not None:
            if len(market_data_request.fields) == 1:
                data_frame.columns = data_frame.columns.str.cat(
                    market_data_request.fields * len(data_frame.columns), sep='.')
            else:
                logger.warning("Inconsistent number of fields and tickers.")
                data_frame.columns = data_frame.columns.str.cat(
                    market_data_request.fields, sep='.')
            data_frame.index.name = 'Date'

        logger.info("Completed request from BOE.")

        return data_frame

    def download_daily(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        trials = 0

        data_frame = None

        boe_url = ("http://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp"
                   "?csv.x=yes&Datefrom={start_date}&Dateto={end_date}"
                   "&SeriesCodes={tickers}"
                   "&CSVF=TN&UsingCodes=Y&VPD=Y&VFD=N")
        start_time = market_data_request.start_date.strftime("%d/%b/%Y")
        end_time = market_data_request.finish_date.strftime("%d/%b/%Y")

        while (trials < 5):
            try:
                data_frame = pd.read_csv(boe_url.format(start_date=start_time, end_date=end_time,
                                                        tickers=','.join(market_data_request.tickers)),
                                         index_col='DATE')
                break
            except:
                trials = trials + 1
                logger.info("Attempting... " + str(trials) + " request to download from BOE")

        if trials == 5:
            logger.error("Couldn't download from BoE after several attempts!")


        return data_frame

#######################################################################################################################

try:
    import yfinance as yf
except:
    pass

class DataVendorYahoo(DataVendor):

    def __init__(self):
        super(DataVendorYahoo, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        logger.info("Request Yahoo data")

        data_frame = self.download_daily(market_data_request_vendor)

        if data_frame is None or data_frame.index is []:
            return None

        # convert from vendor to findatapy tickers/fields
        if data_frame is not None:
            try:
                if len(market_data_request.tickers) > 1:
                    data_frame.columns = ['/'.join(col) for col in data_frame.columns.values]
            except:
                pass

            returned_tickers = data_frame.columns
        if data_frame is not None:
            # tidy up tickers into a format that is more easily translatable
            # we can often get multiple fields returned (even if we don't ask for them!)
            # convert to lower case
            #returned_fields = [(x.split(' - ')[1]).lower().replace(' ', '-') for x in returned_tickers]
            #returned_fields = [x.replace('value', 'close') for x in returned_fields]  # special case for close


            returned_fields = [x.split('/')[0].lower() for x in returned_tickers]

            #returned_tickers = [x.replace('.', '/') for x in returned_tickers]
            returned_tickers = [x.split('/')[1] for x in returned_tickers]

            fields = self.translate_from_vendor_field(returned_fields, market_data_request)
            tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)

            ticker_combined = []

            for i in range(0, len(fields)):
                ticker_combined.append(tickers[i] + "." + fields[i])

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

        logger.info("Completed request from Yahoo.")

        return data_frame

    def download_daily(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)

        trials = 0

        data_frame = None

        ticker_list = ' '.join(market_data_request.tickers)
        data_frame = yf.download(ticker_list, start=market_data_request.start_date, end=market_data_request.finish_date)

        while (trials < 5):


            try:
                data_frame = yf.download(ticker_list, start=market_data_request.start_date, end=market_data_request.finish_date)

                break
            except Exception as e:
                print(str(e))
                trials = trials + 1
                logger.info("Attempting... " + str(trials) + " request to download from Yahoo")

        if trials == 5:
            logger.error("Couldn't download from ONS after several attempts!")

        if len(market_data_request.tickers) == 1:
            data_frame.columns = [x + '/' + market_data_request.tickers[0] for x in data_frame.columns]

        return data_frame

#######################################################################################################################

# for pandas 0.23 (necessary for older versions of pandas_datareader
try:
    import pandas

    pandas.core.common.is_list_like = pandas.api.types.is_list_like
except:
    pass

try:
    import pandas_datareader.data as web
except:
    pass

from findatapy.market.datavendor import DataVendor

class DataVendorPandasWeb(DataVendor):
    """Class for reading in data from various web sources into findatapy library including

        Yahoo! Finance - yahoo
        Google Finance - google
        St. Louis FED (FRED) - fred
        Kenneth French data library - famafrench
        World Bank - wb

    """

    def __init__(self):
        super(DataVendorPandasWeb, self).__init__()


    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        logger.info("Request Pandas Web data")

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

            # return all the tickers (this might be imcomplete list, but we will pad the list later)
            # data_frame = pandas.DataFrame(data = data_frame[ticker_requested],
            #                               index = data_frame.index, columns = ticker_requested)

        logger.info("Completed request from Pandas Web.")

        return data_frame

    def download_daily(self, market_data_request):
        return web.DataReader(market_data_request.tickers, market_data_request.data_source, market_data_request.start_date, market_data_request.finish_date)

########################################################################################################################



####Bitcoin####################################################################################################################

from findatapy.market.datavendor import DataVendor

class DataVendorBitcoincharts(DataVendor):
    """Class for reading in data from various web sources into findatapy library including
    """


    def __init__(self):
        super(DataVendorBitcoincharts, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        logger.info("Request data from Bitcoincharts")
        

        data_website = 'http://api.bitcoincharts.com/v1/csv/' + market_data_request_vendor.tickers[0] + '.csv.gz'
        data_frame = pandas.read_csv(data_website, names = ['datetime','close','volume'])
        data_frame = data_frame.set_index('datetime')
        data_frame.index = pandas.to_datetime(data_frame.index, unit='s')
        data_frame.index.name = 'Date'
        data_frame = data_frame[(data_frame.index >= market_data_request_vendor.start_date) & (data_frame.index <= market_data_request_vendor.finish_date)]
#        data_frame = df[~df.index.duplicated(keep='last')]
        if (len(data_frame) == 0):
            print('###############################################################')
            print('Warning: No data. Please change the start_date and finish_date.')
            print('###############################################################')

        data_frame.columns = [market_data_request.tickers[0]+'.close', market_data_request.tickers[0]+'.volume']
        logger.info("Completed request from Bitcoincharts.")

        return data_frame


########################################################################################################################


from findatapy.market.datavendor import DataVendor

class DataVendorPoloniex(DataVendor):
    """Class for reading in data from various web sources into findatapy library including
    """


    def __init__(self):
        super(DataVendorPoloniex, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)

        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        logger.info("Request data from Poloniex")

        poloniex_url = 'https://poloniex.com/public?command=returnChartData&currencyPair={}&start={}&end={}&period={}'
        if market_data_request_vendor.freq == 'intraday':
            period = 300
        if market_data_request_vendor.freq == 'daily':
            period = 86400

        json_url = poloniex_url.format(market_data_request_vendor.tickers[0],
                                       int(market_data_request_vendor.start_date.timestamp()),
                                       int(market_data_request_vendor.finish_date.timestamp()), period)
        data_frame = pandas.read_json(json_url)
        data_frame = data_frame.set_index('date')
        data_frame.index.name = 'Date'

        if (data_frame.index[0] == 0):
            print('###############################################################')
            print('Warning: No data. Please change the start_date and finish_date.')
            print('###############################################################')

        data_frame.columns = [market_data_request.tickers[0]+'.close',
                              market_data_request.tickers[0]+'.high',
                              market_data_request.tickers[0]+'.low',
                              market_data_request.tickers[0]+'.open',
                              market_data_request.tickers[0]+'.quote-volume',
                              market_data_request.tickers[0]+'.volume',
                              market_data_request.tickers[0]+'.weighted-average']

        field_selected = []
        for i in range(0,len(market_data_request_vendor.fields)):
            field_selected.append(0)
            field_selected[-1] = market_data_request.tickers[0]+'.'+market_data_request_vendor.fields[i]

        logger.info("Completed request from Poloniex")

        return data_frame[field_selected]


#############################################################################################


from findatapy.market.datavendor import DataVendor


class DataVendorBinance(DataVendor):
    """Class for reading in data from various web sources into findatapy library including
    """
    # Data limit = 500

    def __init__(self):
        super(DataVendorBinance, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)

        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        logger.info("Request data from Binance")

        import time
        binance_url = 'https://www.binance.com/api/v1/klines?symbol={}&interval={}&startTime={}&endTime={}'
        if market_data_request_vendor.freq == 'intraday':
            period = '1m'
        if market_data_request_vendor.freq == 'daily':
            period = '1d'

        data_frame = pandas.DataFrame(columns=[0, 1, 2, 3, 4, 5])
        start_time = int(market_data_request_vendor.start_date.timestamp() * 1000)
        finish_time = int(market_data_request_vendor.finish_date.timestamp() * 1000)

        stop_flag = 0
        while stop_flag == 0:
            if stop_flag == 1:
                break
            json_url = binance_url.format(market_data_request_vendor.tickers[0],period,start_time,finish_time)
            data_read = pandas.read_json(json_url)
            if (len(data_read) < 500):
                if ((len(data_read)== 0) & (len(data_frame)==0)):
                    print('###############################################################')
                    print('Warning: No data. Please change the start_date and finish_date.')
                    print('###############################################################')
                    break
                else:
                    stop_flag = 1
            data_frame = data_frame.append(data_read)
            start_time = int(data_frame[0].tail(1))
            time.sleep(2)

        if (len(data_frame) == 0):
            return data_frame

        data_frame.columns = ['open-time','open','high','low','close','volume','close-time','quote-asset-volume',
                                  'trade-numbers','taker-buy-base-asset-volume','taker-buy-quote-asset-volume','ignore']
        data_frame['open-time'] = data_frame['open-time']/1000
        data_frame = data_frame.set_index('open-time')
        data_frame = data_frame.drop(['close-time','ignore'], axis=1)
        data_frame.index.name = 'Date'
        data_frame.index = pandas.to_datetime(data_frame.index, unit='s')
        data_frame.columns = [market_data_request.tickers[0] + '.open',
                              market_data_request.tickers[0] + '.high',
                              market_data_request.tickers[0] + '.low',
                              market_data_request.tickers[0] + '.close',
                              market_data_request.tickers[0] + '.volume',
                              market_data_request.tickers[0] + '.quote-asset-volume',
                              market_data_request.tickers[0] + '.trade-numbers',
                              market_data_request.tickers[0] + '.taker-buy-base-asset-volume',
                              market_data_request.tickers[0] + '.taker-buy-quote-asset-volume']

        field_selected = []
        for i in range(0, len(market_data_request_vendor.fields)):
            field_selected.append(0)
            field_selected[-1] = market_data_request.tickers[0] + '.' + market_data_request_vendor.fields[i]

        logger.info("Completed request from Binance")

        return data_frame[field_selected]

#########################################################################################################


from findatapy.market.datavendor import DataVendor


class DataVendorBitfinex(DataVendor):
    """Class for reading in data from various web sources into findatapy library including
    """
    # Data limit = 1000

    def __init__(self):
        super(DataVendorBitfinex, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        logger.info("Request data from Bitfinex.")

        
        import time
        bitfinex_url = 'https://api.bitfinex.com/v2/candles/trade:{}:t{}/hist?start={}&end={}&limit=1000&sort=1'
        if market_data_request_vendor.freq == 'intraday':
            period = '1m'
        if market_data_request_vendor.freq == 'daily':
            period = '1D'

        data_frame = pandas.DataFrame(columns=[0, 1, 2, 3, 4, 5])
        start_time = int(market_data_request_vendor.start_date.timestamp() * 1000)
        finish_time = int(market_data_request_vendor.finish_date.timestamp() * 1000)
        stop_flag = 0
        while stop_flag == 0:
            if stop_flag == 1:
                break
            json_url = bitfinex_url.format(period, market_data_request_vendor.tickers[0], start_time, finish_time)
            data_read = pandas.read_json(json_url)
            if (len(data_read) < 1000):
                if ((len(data_read)== 0) & (len(data_frame)==0)):
                    break
                else:
                    stop_flag = 1
            data_frame = data_frame.append(data_read)
            start_time = int(data_frame[0].tail(1))
            time.sleep(2)

        if (len(data_frame) == 0):
            print('###############################################################')
            print('Warning: No data. Please change the start_date and finish_date.')
            print('###############################################################')
        #    return data_frame

        data_frame.columns = ['mts', 'open', 'close', 'high', 'low', 'volume']
        data_frame = data_frame.set_index('mts')

        data_frame = data_frame[~data_frame.index.duplicated(keep='first')]

        data_frame.index.name = 'Date'
        data_frame.index = pandas.to_datetime(data_frame.index, unit='ms')
        data_frame.columns = [market_data_request.tickers[0] + '.open',
                              market_data_request.tickers[0] + '.close',
                              market_data_request.tickers[0] + '.high',
                              market_data_request.tickers[0] + '.low',
                              market_data_request.tickers[0] + '.volume']


        field_selected = []
        for i in range(0, len(market_data_request_vendor.fields)):
            field_selected.append(0)
            field_selected[-1] = market_data_request.tickers[0] + '.' + market_data_request_vendor.fields[i]

        logger.info("Completed request from Bitfinex.")

        return data_frame[field_selected]




#########################################################################################################


from findatapy.market.datavendor import DataVendor


class DataVendorGdax(DataVendor):
    """Class for reading in data from various web sources into findatapy library including
    """
    # Data limit = 350

    def __init__(self):
        super(DataVendorGdax, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)

        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        logger.info("Request data from Gdax.")

        
        from datetime import datetime
        from datetime import timedelta
        import time

        gdax_url = 'https://api.gdax.com/products/{}/candles?start={}&end={}&granularity={}'
        start_time = market_data_request_vendor.start_date
        end_time = market_data_request_vendor.finish_date
        if market_data_request_vendor.freq == 'intraday':
            period = '60'
            dt = timedelta(minutes=1)
        if market_data_request_vendor.freq == 'daily':
            period = '86400'
            dt = timedelta(days=1)
        limit = 350

        data_frame = pandas.DataFrame(columns=[0, 1, 2, 3, 4, 5])
        stop_flag = 0
        while stop_flag == 0:
            if stop_flag == 1:
                break
            data_end_time = start_time + (limit - 1) * dt
            if data_end_time > end_time:
                data_end_time = end_time
                stop_flag = 1
            json_url = gdax_url.format(market_data_request_vendor.tickers[0], start_time.isoformat(), data_end_time.isoformat(), period)
            data_read = pandas.read_json(json_url)
            data_frame = data_frame.append(data_read)
            if (len(data_read)==0):
                start_time = data_end_time
            else:
                start_time = pandas.to_datetime(int(data_read[0].head(1)), unit='s')
            time.sleep(2)

        if (len(data_frame) == 0):
            print('###############################################################')
            print('Warning: No data. Please change the start_date and finish_date.')
            print('###############################################################')

        data_frame.columns = ['time', 'low', 'high', 'open', 'close', 'volume']
        data_frame = data_frame.set_index('time')
        data_frame.index = pandas.to_datetime(data_frame.index, unit='s')
        data_frame.index.name = 'Date'
        data_frame = data_frame[~data_frame.index.duplicated(keep='first')]
        data_frame = data_frame.sort_index(ascending=True)
        data_frame.columns = [market_data_request.tickers[0] + '.low',
                              market_data_request.tickers[0] + '.high',
                              market_data_request.tickers[0] + '.open',
                              market_data_request.tickers[0] + '.close',
                              market_data_request.tickers[0] + '.volume']

        field_selected = []
        for i in range(0, len(market_data_request_vendor.fields)):
            field_selected.append(0)
            field_selected[-1] = market_data_request.tickers[0] + '.' + market_data_request_vendor.fields[i]

        logger.info("Completed request from Gdax.")

        return data_frame[field_selected]


########################################################################################################################


from findatapy.market.datavendor import DataVendor

class DataVendorKraken(DataVendor):
    """Class for reading in data from various web sources into findatapy library including
    """
    # Data limit : can only get the most recent 720 rows for klines
    # Collect data from all trades data

    def __init__(self):
        super(DataVendorKraken, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        logger.info("Request data from Kraken.")

        
        import json
        import requests
        import time
        import datetime
        from datetime import datetime

        #kraken_url = 'https://api.kraken.com/0/public/OHLC?pair={}&interval={}&since=0'
        #if market_data_request_vendor.freq == 'intraday':
        #    period = 1
        #if market_data_request_vendor.freq == 'daily':
        #    period = 1440
        start_time = int(market_data_request_vendor.start_date.timestamp() * 1e9)
        end_time = int(market_data_request_vendor.finish_date.timestamp() * 1e9)

        kraken_url = 'https://api.kraken.com/0/public/Trades?pair={}&since={}'
        data_frame = pandas.DataFrame(columns=['close', 'volume', 'time', 'buy-sell', 'market-limit', 'miscellaneous'])
        stop_flag = 0

        while stop_flag == 0:
            if stop_flag == 1:
                break

            json_url = kraken_url.format(market_data_request_vendor.tickers[0], start_time)
            data_read = json.loads(requests.get(json_url).text)
            if (len(list(data_read)) == 1):
                time.sleep(10)
                data_read = json.loads(requests.get(json_url).text)

            data_list = list(data_read['result'])[0]
            data_read = data_read['result'][data_list]
            df = pandas.DataFrame(data_read,
                              columns=['close', 'volume', 'time', 'buy-sell', 'market-limit', 'miscellaneous'])
            start_time = int(df['time'].tail(1) * 1e9)
            if (start_time > end_time):
                stop_flag = 1
            if (end_time < int(df['time'].head(1)*1e9)):
                stop_flag = 1
            data_frame = data_frame.append(df)
            time.sleep(5)

        data_frame = data_frame.set_index('time')
        data_frame.index = pandas.to_datetime(data_frame.index, unit='s')
        data_frame.index.name = 'Date'
        data_frame = data_frame.drop(['miscellaneous'], axis=1)
        data_frame.replace(['b', 's', 'm', 'l'], [1, -1, 1, -1], inplace=True)
        data_frame = data_frame[(data_frame.index >= market_data_request_vendor.start_date) & (
                                 data_frame.index <= market_data_request_vendor.finish_date)]
        if (len(data_frame) == 0):
            print('###############################################################')
            print('Warning: No data. Please change the start_date and finish_date.')
            print('###############################################################')

        data_frame.columns = [market_data_request.tickers[0]+'.close',
                              market_data_request.tickers[0]+'.volume',
                              market_data_request.tickers[0]+'.buy-sell',
                              market_data_request.tickers[0]+'.market-limit']

        field_selected = []
        for i in range(0,len(market_data_request_vendor.fields)):
            field_selected.append(0)
            field_selected[-1] = market_data_request.tickers[0]+'.'+market_data_request_vendor.fields[i]


        self.logger.info("Completed request from Kraken.")

        return data_frame[field_selected]


#########################################################################################################

class DataVendorBitmex(DataVendor):
    """Class for reading in data from various web sources into findatapy library including
    """
    # Data limit = 500,  150 calls / 5 minutes

    def __init__(self):
        super(DataVendorBitmex, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        logger.info("Request data from Bitmex.")

        
        import time

        bitMEX_url = 'https://www.bitmex.com/api/v1/quote?symbol={}&count=500&reverse=false&startTime={}&endTime={}'
        data_frame = pandas.DataFrame(columns=['askPrice', 'askSize', 'bidPrice', 'bidSize', 'symbol', 'timestamp'])
        start_time = market_data_request_vendor.start_date.timestamp()
        finish_time = market_data_request_vendor.finish_date.timestamp()
        symbol = market_data_request_vendor.tickers[0]
        
        stop_flag = 0
        while stop_flag == 0:
            if stop_flag == 1:
                break
            json_url = bitMEX_url.format(symbol, start_time.isoformat(), finish_time.isoformat())
            data_read = pandas.read_json(json_url)
            if (len(data_read) < 500):
                    stop_flag = 1
            data_frame = data_frame.append(data_read)
            start_time = data_read['timestamp'][data_frame.index[-1]]
            time.sleep(2)

        if (len(data_frame) == 0):
            print('###############################################################')
            print('Warning: No data. Please change the start_date and finish_date.')
            print('###############################################################')

        data_frame = data_frame.drop(columns=['symbol'])
        col = ['ask-price', 'ask-size', 'bid-price', 'bid-size', 'timestamp']
        data_frame.columns = col
        data_frame = data_frame.set_index('timestamp')
        data_frame.index = pandas.to_datetime(data_frame.index, unit='ms')
        data_frame = data_frame[~data_frame.index.duplicated(keep='first')]
        data_frame.columns = [market_data_request.tickers[0] + '.ask-price',
                              market_data_request.tickers[0] + '.ask-size',
                              market_data_request.tickers[0] + '.bid-price',
                              market_data_request.tickers[0] + '.bid-size']


        field_selected = []
        for i in range(0, len(market_data_request_vendor.fields)):
            field_selected.append(0)
            field_selected[-1] = market_data_request.tickers[0] + '.' + market_data_request_vendor.fields[i]

        logger.info("Completed request from Bitfinex.")

        return data_frame[field_selected]


class DataVendorHuobi(DataVendor):
    """Class for reading in data from various web sources into findatapy library including
    """
    # Data limit = 500,  150 calls / 5 minutes

    def __init__(self):
        super(DataVendorHuobi, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        logger = LoggerManager().getLogger(__name__)
        
        import requests
        
        import json
        
        def _calc_period_size(freq, start_dt, finish_dt):
            actual_window = finish_dt - start_dt
            extra_window = datetime.datetime.now() - finish_dt
            request_window = actual_window + extra_window

            if freq == 'daily':
                return int(request_window.days), '1day'

            if freq == 'tick':
                request_minutes = request_window.total_seconds() / 60
                return int(request_minutes), '1min'

            raise ValueError("Unsupported freq: '{}'".format(freq))

        # need to trick huobi to think we are a web-browser
        header = {
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest"
        }

        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        logger.info("Request data from Huobi.")

        request_size, period = _calc_period_size(
            market_data_request_vendor.freq,
            market_data_request_vendor.start_date,
            market_data_request_vendor.finish_date)

        if request_size > 2000:
            raise ValueError("Requested data too old for candle-stick frequency of '{}'".format(market_data_request_vendor.freq))

        url = "https://api.huobi.pro/market/history/kline?period={period}&size={size}&symbol={symbol}".format(
            period=period,
            size=request_size,
            symbol=market_data_request_vendor.tickers[0]
        )

        response = requests.get(url, headers=header)
        raw_data = json.loads(response.text)
        df = pandas.DataFrame(raw_data["data"])
        df["timestamp"] = pandas.to_datetime(df["id"], unit="s")

        df = df.set_index("timestamp").sort_index(ascending=True)
        df = df[~df.index.duplicated(keep='first')]

        df.drop(["id"], axis=1, inplace=True)

        if df.empty:
            self.logger.info('###############################################################')
            self.logger.info('Warning: No data. Please change the start_date and finish_date.')
            self.logger.info('###############################################################')

        df.columns = ["{}.{}".format(market_data_request.tickers[0], col) for col in df.columns]

        field_selected = []
        for i in range(0, len(market_data_request_vendor.fields)):
            field_selected.append(0)
            field_selected[-1] = market_data_request.tickers[0] + '.' + market_data_request_vendor.fields[i]

        logger.info("Completed request from Huobi.")

        df = df[field_selected]
        return df




#########################################################################################################


import os
from datetime import timedelta

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
    """Class for downloading tick data from DukasCopy (note: past month of data is not available). Selecting very large
    histories is not recommended as you will likely run out memory given the amount of data requested.

    Parsing of files is re-written version https://github.com/nelseric/ticks/
        parsing has been speeded up considerably
        on-the-fly downloading/parsing

    """
    tick_name  = "{symbol}/{year}/{month}/{day}/{hour}h_ticks.bi5"

    def __init__(self):
        super(DataVendor, self).__init__()
        # self.logger = LoggerManager.getLogger(__name__)
        import logging
        logging.getLogger("requests").setLevel(logging.WARNING)
        self.config = ConfigManager()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        """Retrieves market data from external data source (in this case Bloomberg)

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
        logger = LoggerManager.getLogger(__name__)
        logger.info("Request Dukascopy data")

        # doesn't support non-tick data
        if (market_data_request.freq in ['daily', 'weekly', 'monthly', 'quarterly', 'yearly', 'intraday', 'minute', 'hourly']):
            logger.warning("Dukascopy loader is for tick data only")

            return None

        # assume one ticker only (MarketDataGenerator only calls one ticker at a time)
        if (market_data_request.freq in ['tick']):
            # market_data_request_vendor.tickers = market_data_request_vendor.tickers[0]

            data_frame = self.get_tick(market_data_request, market_data_request_vendor)

            import pytz

            if data_frame is not None: data_frame.tz_localize(pytz.utc)

        logger.info("Completed request from Dukascopy")

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
        logger = LoggerManager.getLogger(__name__)

        logger.info("About to download from Dukascopy... for " + symbol)

        # single threaded
        # df_list = [self.fetch_file(time, symbol) for time in
        #        self.hour_range(market_data_request.start_date, market_data_request.finish_date)]

        # parallel threaded (even with GIL, fast because lots of waiting for IO!)
        from findatapy.util import SwimPool
        time_list = self.hour_range(market_data_request.start_date, market_data_request.finish_date)

        do_retrieve_df = True  # convert inside loop?
        multi_threaded = True # multithreading (can sometimes get errors but it's fine when retried)

        if multi_threaded:
            # use threading (not process interface)
            pool = SwimPool().create_pool('thread', DataConstants().market_thread_no['dukascopy'])
            results = [pool.apply_async(self.fetch_file, args=(time, symbol, do_retrieve_df,)) for time in time_list]
            tick_list = [p.get() for p in results]

            pool.close()
            pool.join()

        else:
            # fully single threaded

            tick_list = []

            for time in time_list:
                tick_list.append(self.fetch_file(time, symbol, do_retrieve_df=do_retrieve_df))

        if do_retrieve_df:
            df_list = tick_list
        else:
            df_list = []

            i = 0

            time_list = self.hour_range(market_data_request.start_date, market_data_request.finish_date)

            for time in time_list:
                try:
                    temp_df = self.retrieve_df(lzma.decompress(tick_list[i]), symbol, time)
                except Exception as e:
                    print(str(time) + ' ' + str(e))
                    # print(str(e))
                    temp_df = None

                df_list.append(temp_df)

                i = i + 1

        df_list = [x for x in df_list if x is not None]

        try:
            return pandas.concat(df_list)
        except:
            return None

    def fetch_file(self, time, symbol, do_retrieve_df):
        logger = LoggerManager.getLogger(__name__)
        if time.hour % 24 == 0:
            logger.info("Downloading... " + str(time))

        tick_path = self.tick_name.format(
                symbol = symbol,
                year = str(time.year).rjust(4, '0'),
                month = str(time.month-1).rjust(2, '0'),
                day = str(time.day).rjust(2, '0'),
                hour = str(time.hour).rjust(2, '0')
            )

        tick = self.fetch_tick(DataConstants().dukascopy_base_url + tick_path)

        #print(tick_path)
        if DataConstants().dukascopy_write_temp_tick_disk:
            out_path = DataConstants().temp_folder + "/dkticks/" + tick_path

            if not os.path.exists(out_path):
                if not os.path.exists(os.path.dirname(out_path)):
                    os.makedirs(os.path.dirname(out_path))

            self.write_tick(tick, out_path)

        if do_retrieve_df:
            try:
                return self.retrieve_df(lzma.decompress(tick), symbol, time)
            except Exception as e:
                #print(tick_path + ' ' + str(e))
                #print(str(e))
                return None

        return tick

    def fetch_tick(self, tick_url):
        i = 0

        tick_request_content = None

        logger = LoggerManager.getLogger(__name__)
        logger.debug("Loading URL " + tick_url)

        # try up to 5 times to download
        while i < 20:
            try:
                tick_request = requests.get(tick_url, timeout = 10)

                tick_request_content = tick_request.content
                tick_request.close()

                # can sometimes get back an error HTML page, in which case retry
                if 'error' not in str(tick_request_content):
                    break
            except Exception as e:
                logger.warning("Problem downloading.. " + str(e))
                i = i + 1
                import time
                time.sleep(i * 2)

        if (tick_request_content is None):
            logger.warning("Failed to download from " + tick_url)

            return None

        return tick_request_content

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

        divisor = 100000.0

        # where JPY is the terms currency we have different divisor
        if symbol[3:6] == 'JPY':
            divisor = 1000.0

        # special case!
        if symbol == 'BRENTCMDUSD':
            divisor = 1000.0

        # prices are returned without decimal point (need to divide)
        df['bid'] =  df['bid'] / divisor
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
            #d = struct.unpack('>3i2f', row)
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

########################################################################################################################

##from StringIO import StringIO
from io import BytesIO
import gzip
import urllib
import datetime



##Available Currencies
##AUDCAD,AUDCHF,AUDJPY, AUDNZD,CADCHF,EURAUD,EURCHF,EURGBP
##EURJPY,EURUSD,GBPCHF,GBPJPY,GBPNZD,GBPUSD,GBPCHF,GBPJPY
##GBPNZD,NZDCAD,NZDCHF.NZDJPY,NZDUSD,USDCAD,USDCHF,USDJPY

class DataVendorFXCM(DataVendor):
    """Class for downloading tick data from FXCM. Selecting very large
    histories is not recommended as you will likely run out memory given the amount of data requested. Loads csv.gz
    files from FXCM and then converts into pandas DataFrames locally.

    Based on https://github.com/FXCMAPI/FXCMTickData/blob/master/TickData34.py

    """

    url_suffix = '.csv.gz' ##Extension of the file name

    def __init__(self):
        super(DataVendor, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

        import logging
        logging.getLogger("requests").setLevel(logging.WARNING)
        self.config = ConfigManager()

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        """Retrieves market data from external data source (in this case Bloomberg)

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
        self.logger.info("Request FXCM data")

        # doesn't support non-tick data
        if (market_data_request.freq in ['daily', 'weekly', 'monthly', 'quarterly', 'yearly', 'intraday', 'minute', 'hourly']):
            self.logger.warning("FXCM loader is for tick data only")

            return None

        # assume one ticker only (MarketDataGenerator only calls one ticker at a time)
        if (market_data_request.freq in ['tick']):
            # market_data_request_vendor.tickers = market_data_request_vendor.tickers[0]

            data_frame = self.get_tick(market_data_request, market_data_request_vendor)

            import pytz

            if data_frame is not None: data_frame.tz_localize(pytz.utc)

        self.logger.info("Completed request from FXCM")

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

        self.logger.info("About to download from FXCM... for " + symbol)

        # single threaded
        # df_list = [self.fetch_file(week_year, symbol) for week_year in
        #           self.week_range(market_data_request.start_date, market_data_request.finish_date)]

        # parallel threaded (note: lots of waiting on IO, so even with GIL quicker!)
        week_list = self.week_range(market_data_request.start_date, market_data_request.finish_date)
        from findatapy.util import SwimPool

        pool = SwimPool().create_pool('thread', DataConstants().market_thread_no['fxcm'])
        results = [pool.apply_async(self.fetch_file, args=(week, symbol)) for week in week_list]
        df_list = [p.get() for p in results]
        pool.close()

        try:
            return pandas.concat(df_list)
        except:
            return None

    def fetch_file(self, week_year, symbol):
        self.logger.info("Downloading... " + str(week_year))

        week = week_year[0]
        year = week_year[1]

        tick_path = symbol + '/' + str(year) + '/' + str(week) + self.url_suffix

        return self.retrieve_df(DataConstants().fxcm_base_url + tick_path)

    def parse_datetime(self):
        pass

    def retrieve_df(self, tick_url):
        i = 0

        data_frame = None

        # Python 2 vs. 3
        try:
            from StringIO import StringIO
        except:
            from io import StringIO

        # try up to 5 times to download
        while i < 5:
            try:

                requests = urllib.request.urlopen(tick_url)
                buf = BytesIO(requests.read())

                with gzip.GzipFile(fileobj=buf, mode='rb') as f:

                    # slightly awkward date parser (much faster than using other Python methods)
                    # TODO use ciso8601 library (uses C parser, slightly quicker)
                    dateparse = lambda x: datetime.datetime(int(x[6:10]), int(x[0:2]), int(x[3:5]),
                                                   int(x[11:13]), int(x[14:16]), int(x[17:19]), int(x[20:23])*1000)

                    data_frame = pandas.read_csv(StringIO(f.read().decode('utf-16')), index_col=0, parse_dates=True,
                                                 date_parser=dateparse)

                    data_frame.columns = ['bid', 'ask']

                    f.close()

                i = 5
            except:
                i = i + 1

        if (data_frame is None):
            self.logger.warning("Failed to download from " + tick_url)
            return None

        return data_frame

    def week_range(self, start_date, finish_date):

        weeks = pandas.bdate_range(start_date - timedelta(days=7), finish_date+timedelta(days=7), freq='W')

        week_year = []

        for w in weeks:
            # week = w.week

            # if week != 52:
            #    year = w.year

            # week_year.append((week, year))
            year, week = w.isocalendar()[0:2]
            week_year.append((week, year))

        # if less than a week a
        if week_year == []:
            week_year.append((start_date.week, start_date.year))

        return week_year

    def get_daily_data(self):
        pass

########################################################################################################################

import os
import sys

if sys.version_info[0] >= 3:
    from urllib.request import urlopen
    from urllib.parse import quote_plus
    from urllib.parse import urlencode
    from urllib.error import HTTPError
else:
    from urllib2 import urlopen
    from urllib2 import HTTPError
    from urllib import quote_plus
    from urllib import urlencode

import xml.etree.ElementTree as ET

class Fred(object):
    """Auxillary class for getting access to ALFRED/FRED directly.

    Based on https://github.com/mortada/fredapi (with minor edits)

    """
    earliest_realtime_start = '1776-07-04'
    latest_realtime_end = '9999-12-31'
    nan_char = '.'
    max_results_per_request = 1000

    def __init__(self,
                 api_key=None,
                 api_key_file=None):
        """Initialize the Fred class that provides useful functions to query the Fred dataset. You need to specify a valid
        API key in one of 3 ways: pass the string via api_key, or set api_key_file to a file with the api key in the
        first line, or set the environment variable 'FRED_API_KEY' to the value of your api key. You can sign up for a
        free api key on the Fred website at http://research.stlouisfed.org/fred2/
        """
        self.api_key = None
        if api_key is not None:
            self.api_key = api_key
        elif api_key_file is not None:
            f = open(api_key_file, 'r')
            self.api_key = f.readline().strip()
            f.close()
        else:
            self.api_key = os.environ.get('FRED_API_KEY')
        self.root_url = 'https://api.stlouisfed.org/fred'

        if self.api_key is None:
            import textwrap

            raise ValueError(textwrap.dedent("""\
                    You need to set a valid API key. You can set it in 3 ways:
                    pass the string with api_key, or set api_key_file to a
                    file with the api key in the first line, or set the
                    environment variable 'FRED_API_KEY' to the value of your
                    api key. You can sign up for a free api key on the Fred
                    website at http://research.stlouisfed.org/fred2/"""))

    def __fetch_data(self, url):
        """Helper function for fetching data given a request URL
        """
        try:
            response = urlopen(url)
            root = ET.fromstring(response.read())
        except HTTPError as exc:
            root = ET.fromstring(exc.read())
            raise ValueError(root.get('message'))
        return root

    def _parse(self, date_str, format='%Y-%m-%d'):
        """Helper function for parsing FRED date string into datetime

        """
        from pandas import to_datetime
        rv = to_datetime(date_str, format=format)
        if hasattr(rv, 'to_datetime'):
            rv = rv.to_pydatetime()         # to_datetime is depreciated
        return rv

    def get_series_info(self, series_id):
        """Get information about a series such as its title, frequency, observation start/end dates, units, notes, etc.

        Parameters
        ----------
        series_id : str
            Fred series id such as 'CPIAUCSL'

        Returns
        -------
        info : Series
            a pandas Series containing information about the Fred series
        """
        url = "%s/series?series_id=%s&api_key=%s" % (self.root_url, series_id,
                                                     self.api_key)
        root = self.__fetch_data(url)
        if root is None:
            raise ValueError('No info exists for series id: ' + series_id)
        from pandas import Series
        info = Series(root.getchildren()[0].attrib)
        return info

    def get_series(self, series_id, observation_start=None, observation_end=None, **kwargs):
        """Get data for a Fred series id. This fetches the latest known data, and is equivalent to get_series_latest_release()

        Parameters
        ----------
        series_id : str
            Fred series id such as 'CPIAUCSL'
        observation_start : datetime or datetime-like str such as '7/1/2014', optional
            earliest observation date
        observation_end : datetime or datetime-like str such as '7/1/2014', optional
            latest observation date
        kwargs : additional parameters
            Any additional parameters supported by FRED. You can see https://api.stlouisfed.org/docs/fred/series_observations.html for the full list

        Returns
        -------
        data : Series
            a Series where each index is the observation date and the value is the data for the Fred series
        """
        url = "%s/series/observations?series_id=%s&api_key=%s" % (self.root_url,
                                                                  series_id,
                                                                  self.api_key)
        from pandas import to_datetime, Series

        if observation_start is not None:
            observation_start = to_datetime(observation_start, errors='raise')
            url += '&observation_start=' + observation_start.strftime('%Y-%m-%d')
        if observation_end is not None:
            observation_end = to_datetime(observation_end, errors='raise')
            url += '&observation_end=' + observation_end.strftime('%Y-%m-%d')

        if kwargs is not None:
            url += '&' + urlencode(kwargs)

        root = self.__fetch_data(url)
        if root is None:
            raise ValueError('No data exists for series id: ' + series_id)
        data = {}
        for child in root.getchildren():
            val = child.get('value')
            if val == self.nan_char:
                val = float('NaN')
            else:
                val = float(val)
            data[self._parse(child.get('date'))] = val
        return Series(data)

    def get_series_latest_release(self, series_id, observation_start=None, observation_end=None):
        """Get data for a Fred series id. This fetches the latest known data, and is equivalent to get_series()

        Parameters
        ----------
        series_id : str
            Fred series id such as 'CPIAUCSL'

        Returns
        -------
        info : Series
            a Series where each index is the observation date and the value is the data for the Fred series
        """
        return self.get_series(series_id, observation_start=observation_start, observation_end=observation_end)

    def get_series_first_release(self, series_id, observation_start=None, observation_end=None):
        """Get first-release data for a Fred series id.

        This ignores any revision to the data series. For instance,
        The US GDP for Q1 2014 was first released to be 17149.6, and then later revised to 17101.3, and 17016.0.
        This will ignore revisions after the first release.

        Parameters
        ----------
        series_id : str
            Fred series id such as 'GDP'

        Returns
        -------
        data : Series
            a Series where each index is the observation date and the value is the data for the Fred series
        """
        df = self.get_series_all_releases(series_id, observation_start=observation_start, observation_end=observation_end)
        first_release = df.groupby('date').head(1)
        data = first_release.set_index('date')['value']
        return data

    def get_series_first_revision(self, series_id, observation_start=None, observation_end=None):
        """Get first-revision data for a Fred series id.

        This will give the first revision to the data series. For instance,
        The US GDP for Q1 2014 was first released to be 17149.6, and then later revised to 17101.3, and 17016.0.
        This will take the first revision ie. 17101.3 in this case.

        Parameters
        ----------
        series_id : str
            Fred series id such as 'GDP'

        Returns
        -------
        data : Series
            a Series where each index is the observation date and the value is the data for the Fred series
        """
        df = self.get_series_all_releases(series_id, observation_start=observation_start, observation_end=observation_end)
        first_revision = df.groupby('date').head(2)
        data = first_revision.set_index('date')['value']
        data = data[~data.index.duplicated(keep='last')]

        return data

    def get_series_as_of_date(self, series_id, as_of_date):
        """Get latest data for a Fred series id as known on a particular date.

        This includes any revision to the data series
        before or on as_of_date, but ignores any revision on dates after as_of_date.

        Parameters
        ----------
        series_id : str
            Fred series id such as 'GDP'
        as_of_date : datetime, or datetime-like str such as '10/25/2014'
            Include data revisions on or before this date, and ignore revisions afterwards

        Returns
        -------
        data : Series
            a Series where each index is the observation date and the value is the data for the Fred series
        """
        from pandas import to_datetime
        as_of_date = to_datetime(as_of_date)
        df = self.get_series_all_releases(series_id)
        data = df[df['realtime_start'] <= as_of_date]
        return data

    def get_series_all_releases(self, series_id, observation_start=None, observation_end=None):
        """Get all data for a Fred series id including first releases and all revisions.

        This returns a DataFrame
        with three columns: 'date', 'realtime_start', and 'value'. For instance, the US GDP for Q4 2013 was first released
        to be 17102.5 on 2014-01-30, and then revised to 17080.7 on 2014-02-28, and then revised to 17089.6 on
        2014-03-27. You will therefore get three rows with the same 'date' (observation date) of 2013-10-01 but three
        different 'realtime_start' of 2014-01-30, 2014-02-28, and 2014-03-27 with corresponding 'value' of 17102.5, 17080.7
        and 17089.6

        Parameters
        ----------
        series_id : str
            Fred series id such as 'GDP'

        Returns
        -------
        data : DataFrame
            a DataFrame with columns 'date', 'realtime_start' and 'value' where 'date' is the observation period and 'realtime_start'
            is when the corresponding value (either first release or revision) is reported.
        """
        url = "%s/series/observations?series_id=%s&api_key=%s&realtime_start=%s&realtime_end=%s" % (self.root_url,
                                                                                                    series_id,
                                                                                                    self.api_key,
                                                                                                    self.earliest_realtime_start,
                                                                                                    self.latest_realtime_end)

        from pandas import to_datetime

        if observation_start is not None:
            observation_start = to_datetime(observation_start, errors='raise')
            url += '&observation_start=' + observation_start.strftime('%Y-%m-%d')
        if observation_end is not None:
            observation_end = to_datetime(observation_end, errors='raise')
            url += '&observation_end=' + observation_end.strftime('%Y-%m-%d')

        root = self.__fetch_data(url)
        if root is None:
            raise ValueError('No data exists for series id: ' + series_id)
        data = {}
        i = 0
        for child in root.getchildren():
            val = child.get('value')
            if val == self.nan_char:
                val = float('NaN')
            else:
                val = float(val)
            realtime_start = self._parse(child.get('realtime_start'))
            # realtime_end = self._parse(child.get('realtime_end'))
            date = self._parse(child.get('date'))

            data[i] = {'realtime_start': realtime_start,
                       # 'realtime_end': realtime_end,
                       'date': date,
                       'value': val}
            i += 1
        from pandas import DataFrame
        data = DataFrame(data).T
        return data

    def get_series_vintage_dates(self, series_id):
        """Get a list of vintage dates for a series.

        Vintage dates are the dates in history when a series' data values were revised or new data values were released.

        Parameters
        ----------
        series_id : str
            Fred series id such as 'CPIAUCSL'

        Returns
        -------
        dates : list
            list of vintage dates
        """
        url = "%s/series/vintagedates?series_id=%s&api_key=%s" % (self.root_url,
                                                                  series_id,
                                                                  self.api_key)
        root = self.__fetch_data(url)
        if root is None:
            raise ValueError('No vintage date exists for series id: ' + series_id)
        dates = []
        for child in root.getchildren():
            dates.append(self._parse(child.text))
        return dates

    def __do_series_search(self, url):
        """Helper function for making one HTTP request for data, and parsing the returned results into a DataFrame
        """
        root = self.__fetch_data(url)

        series_ids = []
        data = {}

        num_results_returned = 0  # number of results returned in this HTTP request
        num_results_total = int(root.get('count'))  # total number of results, this can be larger than number of results returned
        for child in root.getchildren():
            num_results_returned += 1
            series_id = child.get('id')
            series_ids.append(series_id)
            data[series_id] = {"id": series_id}
            fields = ["realtime_start", "realtime_end", "title", "observation_start", "observation_end",
                      "frequency", "frequency_short", "units", "units_short", "seasonal_adjustment",
                      "seasonal_adjustment_short", "last_updated", "popularity", "notes"]
            for field in fields:
                data[series_id][field] = child.get(field)

        if num_results_returned > 0:
            from pandas import DataFrame
            data = DataFrame(data, columns=series_ids).T
            # parse datetime columns
            for field in ["realtime_start", "realtime_end", "observation_start", "observation_end", "last_updated"]:
                data[field] = data[field].apply(self._parse, format=None)
            # set index name
            data.index.name = 'series id'
        else:
            data = None
        return data, num_results_total

    def __get_search_results(self, url, limit, order_by, sort_order):
        """Helper function for getting search results up to specified limit on the number of results. The Fred HTTP API
        truncates to 1000 results per request, so this may issue multiple HTTP requests to obtain more available data.
        """

        order_by_options = ['search_rank', 'series_id', 'title', 'units', 'frequency',
                            'seasonal_adjustment', 'realtime_start', 'realtime_end', 'last_updated',
                            'observation_start', 'observation_end', 'popularity']
        if order_by is not None:
            if order_by in order_by_options:
                url = url + '&order_by=' + order_by
            else:
                raise ValueError('%s is not in the valid list of order_by options: %s' % (order_by, str(order_by_options)))

        sort_order_options = ['asc', 'desc']
        if sort_order is not None:
            if sort_order in sort_order_options:
                url = url + '&sort_order=' + sort_order
            else:
                raise ValueError('%s is not in the valid list of sort_order options: %s' % (sort_order, str(sort_order_options)))

        data, num_results_total = self.__do_series_search(url)
        if data is None:
            return data

        if limit == 0:
            max_results_needed = num_results_total
        else:
            max_results_needed = limit

        if max_results_needed > self.max_results_per_request:
            for i in range(1, max_results_needed // self.max_results_per_request + 1):
                offset = i * self.max_results_per_request
                next_data, _ = self.__do_series_search(url + '&offset=' + str(offset))
                data = data.append(next_data)
        return data.head(max_results_needed)

    def search(self, text, limit=1000, order_by=None, sort_order=None):
        """Do a fulltext search for series in the Fred dataset.

        Returns information about matching series in a DataFrame.

        Parameters
        ----------
        text : str
            text to do fulltext search on, e.g., 'Real GDP'
        limit : int, optional
            limit the number of results to this value. If limit is 0, it means fetching all results without limit.
        order_by : str, optional
            order the results by a criterion. Valid options are 'search_rank', 'series_id', 'title', 'units', 'frequency',
            'seasonal_adjustment', 'realtime_start', 'realtime_end', 'last_updated', 'observation_start', 'observation_end',
            'popularity'
        sort_order : str, optional
            sort the results by ascending or descending order. Valid options are 'asc' or 'desc'

        Returns
        -------
        info : DataFrame
            a DataFrame containing information about the matching Fred series
        """
        url = "%s/series/search?search_text=%s&api_key=%s" % (self.root_url,
                                                              quote_plus(text),
                                                              self.api_key)
        info = self.__get_search_results(url, limit, order_by, sort_order)
        return info

    def search_by_release(self, release_id, limit=0, order_by=None, sort_order=None):
        """Search for series that belongs to a release id.

        Returns information about matching series in a DataFrame.

        Parameters
        ----------
        release_id : int
            release id, e.g., 151
        limit : int, optional
            limit the number of results to this value. If limit is 0, it means fetching all results without limit.
        order_by : str, optional
            order the results by a criterion. Valid options are 'search_rank', 'series_id', 'title', 'units', 'frequency',
            'seasonal_adjustment', 'realtime_start', 'realtime_end', 'last_updated', 'observation_start', 'observation_end',
            'popularity'
        sort_order : str, optional
            sort the results by ascending or descending order. Valid options are 'asc' or 'desc'

        Returns
        -------
        info : DataFrame
            a DataFrame containing information about the matching Fred series
        """
        url = "%s/release/series?release_id=%d&&api_key=%s" % (self.root_url,
                                                               release_id,
                                                               self.api_key)
        info = self.__get_search_results(url, limit, order_by, sort_order)
        if info is None:
            raise ValueError('No series exists for release id: ' + str(release_id))
        return info

    def search_by_category(self, category_id, limit=0, order_by=None, sort_order=None):
        """Search for series that belongs to a category id.

        Returns information about matching series in a DataFrame.

        Parameters
        ----------
        category_id : int
            category id, e.g., 32145
        limit : int, optional
            limit the number of results to this value. If limit is 0, it means fetching all results without limit.
        order_by : str, optional
            order the results by a criterion. Valid options are 'search_rank', 'series_id', 'title', 'units', 'frequency',
            'seasonal_adjustment', 'realtime_start', 'realtime_end', 'last_updated', 'observation_start', 'observation_end',
            'popularity'
        sort_order : str, optional
            sort the results by ascending or descending order. Valid options are 'asc' or 'desc'

        Returns
        -------
        info : DataFrame
            a DataFrame containing information about the matching Fred series
        """
        url = "%s/category/series?category_id=%d&api_key=%s" % (self.root_url,
                                                                category_id,
                                                                self.api_key)
        info = self.__get_search_results(url, limit, order_by, sort_order)
        if info is None:
            raise ValueError('No series exists for category id: ' + str(category_id))
        return info

########################################################################################################################

class DataVendorFlatFile(DataVendor):
    """Reads in data from a user-specifed CSV or HDF5 flat file via findatapy library. Does not do any ticker/field
    mapping, as this could vary significantly between CSV/HDF5 files.

    Users need to know the tickers/fields they wish to collect.

    """

    def __init__(self):
        super(DataVendorFlatFile, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):

        self.logger.info("Request " + market_data_request.data_source + " data")

        if ".csv" in market_data_request.data_source:
            data_frame = pandas.read_csv(market_data_request.data_source, index_col = 0, parse_dates = True,
                                         infer_datetime_format = True)
        elif ".h5" in market_data_request.data_source:
            data_frame = IOEngine().read_time_series_cache_from_disk(market_data_request.data_source, engine = 'hdf5')

        if data_frame is None or data_frame.index is []: return None

        if data_frame is not None:
            tickers = data_frame.columns

        if data_frame is not None:
            # tidy up tickers into a format that is more easily translatable
            # we can often get multiple fields returned (even if we don't ask for them!)
            # convert to lower case
            ticker_combined = []

            for i in range(0, len(tickers)):
                if "." in tickers[i]:
                    ticker_combined.append(tickers[i])
                else:
                    ticker_combined.append(tickers[i] + ".close")

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

        self.logger.info("Completed request from " + market_data_request.data_source + " for " + str(ticker_combined))

        return data_frame

########################################################################################################################

from alpha_vantage.timeseries import TimeSeries

class DataVendorAlphaVantage(DataVendor):
    """Reads in data from Alpha Vantage into findatapy library

    """

    def __init__(self):
        super(DataVendorAlphaVantage, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        self.logger.info("Request AlphaVantage data")

        data_frame, _ = self.download(market_data_request_vendor)

        if data_frame is None or data_frame.index is []: return None

        # convert from vendor to findatapy tickers/fields
        if data_frame is not None:
            returned_tickers = data_frame.columns

        if data_frame is not None:
            # tidy up tickers into a format that is more easily translatable
            # we can often get multiple fields returned (even if we don't ask for them!)
            # convert to lower case
            returned_fields = [(x.split('. ')[1]).lower() for x in returned_tickers]

            import numpy as np
            returned_tickers = np.repeat(market_data_request_vendor.tickers, len(returned_fields))

            try:
                fields = self.translate_from_vendor_field(returned_fields, market_data_request)
                tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)
            except:
                print('error')

            ticker_combined = []

            for i in range(0, len(tickers)):
                try:
                    ticker_combined.append(tickers[i] + "." + fields[i])
                except:
                    ticker_combined.append(tickers[i] + ".close")

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

        self.logger.info("Completed request from Alpha Vantage for " + str(ticker_combined))

        return data_frame

    def download(self, market_data_request):
        trials = 0

        ts = TimeSeries(key=market_data_request.alpha_vantage_api_key, output_format='pandas', indexing_type='date')

        data_frame = None

        while(trials < 5):
            try:
                if market_data_request.freq == 'intraday':
                    data_frame = ts.get_intraday(symbol=market_data_request.tickers,interval='1min', outputsize='full')
                else:
                    data_frame = ts.get_daily(symbol=market_data_request.tickers, outputsize='full')

                break
            except Exception as e:
                trials = trials + 1
                self.logger.info("Attempting... " + str(trials) + " request to download from Alpha Vantage due to following error: " + str(e))

        if trials == 5:
            self.logger.error("Couldn't download from Alpha Vantage after several attempts!")

        return data_frame

########################################################################################################################

try:
    import fxcmpy
except: pass

class DataVendorFXCMPY(DataVendor):
    """Reads in data from FXCM data using fxcmpy into findatapy library. Can be used for minute or daily data. For
    tick data we should use DataVendorFXCM (but this data is delayed).

    NOTE: NOT TESTED YET

    """

    def __init__(self):
        super(DataVendorFXCM, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        self.logger.info("Request FXCM data")

        data_frame, _ = self.download(market_data_request_vendor)

        if data_frame is None or data_frame.index is []: return None

        # convert from vendor to findatapy tickers/fields
        if data_frame is not None:
            returned_tickers = data_frame.columns

        if data_frame is not None:
            # tidy up tickers into a format that is more easily translatable
            # we can often get multiple fields returned (even if we don't ask for them!)
            # convert to lower case
            returned_fields = [(x.split('. ')[1]).lower() for x in returned_tickers]

            import numpy as np
            returned_tickers = np.repeat(market_data_request_vendor.tickers, len(returned_fields))

            try:
                fields = self.translate_from_vendor_field(returned_fields, market_data_request)
                tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)
            except:
                print('error')

            ticker_combined = []

            for i in range(0, len(tickers)):
                try:
                    ticker_combined.append(tickers[i] + "." + fields[i])
                except:
                    ticker_combined.append(tickers[i] + ".close")

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

        self.logger.info("Completed request from FXCM for " + str(ticker_combined))

        return data_frame

    def download(self, market_data_request):
        trials = 0

        con = fxcmpy.fxcmpy(access_token=DataConstants().fxcm_API, log_level='error')

        data_frame = None

        if market_data_request.freq == 'intraday':
            per = 'm1'
        else:
            per = 'D1'

        tickers = [t[0:4] +"/" + t[4:7] for t in market_data_request.tickers]

        while(trials < 5):
            try:
                data_frame = con.get_candles(tickers, period=per,
                                start=market_data_request.start_date, stop=market_data_request.finish_date)
                break
            except Exception as e:
                trials = trials + 1
                self.logger.info("Attempting... " + str(trials) + " request to download from FXCM due to following error: " + str(e))

        if trials == 5:
            self.logger.error("Couldn't download from FXCM after several attempts!")

        return data_frame