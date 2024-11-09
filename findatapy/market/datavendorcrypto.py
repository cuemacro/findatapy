__author__ = "shihhau"  # Shih Hau

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

import pandas

from findatapy.market.datavendor import DataVendor
from findatapy.util import LoggerManager


class DataVendorBitcoincharts(DataVendor):
    """Class for reading in data from various web sources into findatapy
    library including
    """

    def __init__(self):
        super(DataVendorBitcoincharts, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, md_request):
        logger = LoggerManager().getLogger(__name__)
        md_request_vendor = self.construct_vendor_md_request(
            md_request)

        logger.info("Request data from Bitcoincharts")

        data_website = 'http://api.bitcoincharts.com/v1/csv/' + \
                       md_request_vendor.tickers[0] + '.csv.gz'
        data_frame = pandas.read_csv(data_website,
                                     names=['datetime', 'close', 'volume'])
        data_frame = data_frame.set_index('datetime')
        data_frame.index = pandas.to_datetime(data_frame.index, unit='s')
        data_frame.index.name = 'Date'
        data_frame = data_frame[
            (data_frame.index >= md_request_vendor.start_date) & (
                    data_frame.index <= md_request_vendor.finish_date)]
        #        data_frame = df[~df.index.duplicated(keep='last')]
        if len(data_frame) == 0:
            logger.warning(
                "Warning: No data. Please change the start_date and finish_date.")

        data_frame.columns = [md_request.tickers[0] + '.close',
                              md_request.tickers[0] + '.volume']
        logger.info("Completed request from Bitcoincharts.")

        return data_frame


###############################################################################

class DataVendorPoloniex(DataVendor):
    """Class for reading in data from various web sources into findatapy
    library including
    """

    def __init__(self):
        super(DataVendorPoloniex, self).__init__()

    # Implement method in abstract superclass
    def load_ticker(self, md_request):
        logger = LoggerManager().getLogger(__name__)

        md_request_vendor = self.construct_vendor_md_request(
            md_request)

        logger.info("Request data from Poloniex")

        poloniex_url = 'https://poloniex.com/public?command=returnChartData&currencyPair={}&start={}&end={}&period={}'

        if md_request_vendor.freq == 'intraday':
            period = 300
        if md_request_vendor.freq == 'daily':
            period = 86400

        json_url = poloniex_url.format(
            md_request_vendor.tickers[0],
            int(md_request_vendor.start_date.timestamp()),
            int(md_request_vendor.finish_date.timestamp()),
            period)
        data_frame = pandas.read_json(json_url)
        data_frame = data_frame.set_index('date')
        data_frame.index.name = 'Date'

        if data_frame.index[0] == 0:
            logger.warning(
                "Warning: No data. Please change the start_date and finish_date.")

        data_frame.columns = [md_request.tickers[0] + '.close',
                              md_request.tickers[0] + '.high',
                              md_request.tickers[0] + '.low',
                              md_request.tickers[0] + '.open',
                              md_request.tickers[0] + '.quote-volume',
                              md_request.tickers[0] + '.volume',
                              md_request.tickers[
                                  0] + '.weighted-average']

        field_selected = []
        for i in range(0, len(md_request_vendor.fields)):
            field_selected.append(0)
            field_selected[-1] = md_request.tickers[0] + '.' + \
                                 md_request_vendor.fields[i]

        logger.info("Completed request from Poloniex")

        return data_frame[field_selected]


###############################################################################

class DataVendorBinance(DataVendor):
    """Class for reading in data from various web sources into findatapy
    library including
    """

    # Data limit = 500

    def __init__(self):
        super(DataVendorBinance, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, md_request):
        logger = LoggerManager().getLogger(__name__)

        md_request_vendor = self.construct_vendor_md_request(
            md_request)

        logger.info("Request data from Binance")

        import time
        binance_url = 'https://www.binance.com/api/v1/klines?symbol={}&interval={}&startTime={}&endTime={}'
        if md_request_vendor.freq == 'intraday':
            period = '1m'
        if md_request_vendor.freq == 'daily':
            period = '1d'

        data_frame = pandas.DataFrame(columns=[0, 1, 2, 3, 4, 5])
        start_time = int(
            md_request_vendor.start_date.timestamp() * 1000)
        finish_time = int(
            md_request_vendor.finish_date.timestamp() * 1000)

        stop_flag = 0
        while stop_flag == 0:
            if stop_flag == 1:
                break
            json_url = binance_url.format(
                md_request_vendor.tickers[0], period, start_time,
                finish_time)
            data_read = pandas.read_json(json_url)

            if len(data_read) < 500:
                if len(data_read) == 0 & len(data_frame) == 0:
                    logger.warning(
                        "Warning: No data. Please change the start_date and finish_date.")
                    break
                else:
                    stop_flag = 1
            data_frame = data_frame.append(data_read)
            start_time = int(data_frame[0].tail(1))
            time_library.sleep(2)

        if (len(data_frame) == 0):
            return data_frame

        data_frame.columns = ['open-time', 'open', 'high', 'low', 'close',
                              'volume', 'close-time', 'quote-asset-volume',
                              'trade-numbers', 'taker-buy-base-asset-volume',
                              'taker-buy-quote-asset-volume', 'ignore']
        data_frame['open-time'] = data_frame['open-time'] / 1000
        data_frame = data_frame.set_index('open-time')
        data_frame = data_frame.drop(['close-time', 'ignore'], axis=1)
        data_frame.index.name = 'Date'
        data_frame.index = pandas.to_datetime(data_frame.index, unit='s')
        data_frame.columns = [md_request.tickers[0] + '.open',
                              md_request.tickers[0] + '.high',
                              md_request.tickers[0] + '.low',
                              md_request.tickers[0] + '.close',
                              md_request.tickers[0] + '.volume',
                              md_request.tickers[
                                  0] + '.quote-asset-volume',
                              md_request.tickers[
                                  0] + '.trade-numbers',
                              md_request.tickers[
                                  0] + '.taker-buy-base-asset-volume',
                              md_request.tickers[
                                  0] + '.taker-buy-quote-asset-volume']

        field_selected = []
        for i in range(0, len(md_request_vendor.fields)):
            field_selected.append(0)
            field_selected[-1] = md_request.tickers[0] + '.' + \
                                 md_request_vendor.fields[i]

        logger.info("Completed request from Binance")

        return data_frame[field_selected]


###############################################################################

class DataVendorBitfinex(DataVendor):
    """Class for reading in data from various web sources into findatapy
    library including
    """

    # Data limit = 1000

    def __init__(self):
        super(DataVendorBitfinex, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, md_request):
        logger = LoggerManager().getLogger(__name__)
        md_request_vendor = self.construct_vendor_md_request(
            md_request)

        logger.info("Request data from Bitfinex.")

        import time
        bitfinex_url = 'https://api.bitfinex.com/v2/candles/trade:{}:t{}/hist?start={}&end={}&limit=1000&sort=1'
        if md_request_vendor.freq == 'intraday':
            period = '1m'
        if md_request_vendor.freq == 'daily':
            period = '1D'

        data_frame = pandas.DataFrame(columns=[0, 1, 2, 3, 4, 5])
        start_time = int(
            md_request_vendor.start_date.timestamp() * 1000)
        finish_time = int(
            md_request_vendor.finish_date.timestamp() * 1000)
        stop_flag = 0
        while stop_flag == 0:
            if stop_flag == 1:
                break
            json_url = bitfinex_url.format(period,
                                           md_request_vendor.tickers[
                                               0], start_time, finish_time)
            data_read = pandas.read_json(json_url)
            if (len(data_read) < 1000):
                if ((len(data_read) == 0) & (len(data_frame) == 0)):
                    break
                else:
                    stop_flag = 1
            data_frame = data_frame.append(data_read)
            start_time = int(data_frame[0].tail(1))
            time_library.sleep(2)

        if len(data_frame) == 0:
            logger.warning(
                "Warning: No data. Please change the start_date and finish_date.")

        #    return data_frame

        data_frame.columns = ['mts', 'open', 'close', 'high', 'low', 'volume']
        data_frame = data_frame.set_index('mts')

        data_frame = data_frame[~data_frame.index.duplicated(keep='first')]

        data_frame.index.name = 'Date'
        data_frame.index = pandas.to_datetime(data_frame.index, unit='ms')
        data_frame.columns = [md_request.tickers[0] + '.open',
                              md_request.tickers[0] + '.close',
                              md_request.tickers[0] + '.high',
                              md_request.tickers[0] + '.low',
                              md_request.tickers[0] + '.volume']

        field_selected = []
        for i in range(0, len(md_request_vendor.fields)):
            field_selected.append(0)
            field_selected[-1] = md_request.tickers[0] + '.' + \
                                 md_request_vendor.fields[i]

        logger.info("Completed request from Bitfinex.")

        return data_frame[field_selected]


###############################################################################

class DataVendorGdax(DataVendor):
    """Class for reading in data from various web sources into findatapy
    library including
    """

    # Data limit = 350

    def __init__(self):
        super(DataVendorGdax, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, md_request):
        logger = LoggerManager().getLogger(__name__)

        md_request_vendor = self.construct_vendor_md_request(
            md_request)

        logger.info("Request data from Gdax.")

        gdax_url = 'https://api.gdax.com/products/{}/candles?start={}&end={}&granularity={}'
        start_time = md_request_vendor.start_date
        end_time = md_request_vendor.finish_date
        if md_request_vendor.freq == 'intraday':
            # 1 minute data
            period = '60'
            dt = timedelta(minutes=1)
        if md_request_vendor.freq == 'daily':
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
            json_url = gdax_url.format(md_request_vendor.tickers[0],
                                       start_time.isoformat(),
                                       data_end_time.isoformat(), period)
            data_read = pandas.read_json(json_url)
            data_frame = data_frame.append(data_read)
            if (len(data_read) == 0):
                start_time = data_end_time
            else:
                start_time = pandas.to_datetime(int(data_read[0].head(1)),
                                                unit='s')
            time_library.sleep(2)

        if len(data_frame) == 0:
            logger.warning(
                "Warning: No data. Please change the start_date and finish_date.")

        data_frame.columns = ['time', 'low', 'high', 'open', 'close', 'volume']
        data_frame = data_frame.set_index('time')
        data_frame.index = pandas.to_datetime(data_frame.index, unit='s')
        data_frame.index.name = 'Date'
        data_frame = data_frame[~data_frame.index.duplicated(keep='first')]
        data_frame = data_frame.sort_index(ascending=True)
        data_frame.columns = [md_request.tickers[0] + '.low',
                              md_request.tickers[0] + '.high',
                              md_request.tickers[0] + '.open',
                              md_request.tickers[0] + '.close',
                              md_request.tickers[0] + '.volume']

        field_selected = []
        for i in range(0, len(md_request_vendor.fields)):
            field_selected.append(0)
            field_selected[-1] = md_request.tickers[0] + '.' + \
                                 md_request_vendor.fields[i]

        logger.info("Completed request from Gdax.")

        return data_frame[field_selected]


###############################################################################

class DataVendorKraken(DataVendor):
    """Class for reading in data from various web sources into findatapy
    library including
    """

    # Data limit : can only get the most recent 720 rows for klines
    # Collect data from all trades data

    def __init__(self):
        super(DataVendorKraken, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, md_request):
        logger = LoggerManager().getLogger(__name__)

        md_request_vendor = self.construct_vendor_md_request(
            md_request)

        logger.info("Request data from Kraken.")

        # kraken_url = 'https://api.kraken.com/0/public/OHLC?pair={}&interval={}&since=0'
        # if md_request_vendor.freq == 'intraday':
        #    period = 1
        # if md_request_vendor.freq == 'daily':
        #    period = 1440
        start_time = int(
            md_request_vendor.start_date.timestamp() * 1e9)
        end_time = int(
            md_request_vendor.finish_date.timestamp() * 1e9)

        kraken_url = 'https://api.kraken.com/0/public/Trades?pair={}&since={}'
        data_frame = pandas.DataFrame(
            columns=['close', 'volume', 'time', 'buy-sell', 'market-limit',
                     'miscellaneous'])
        stop_flag = 0

        while stop_flag == 0:
            if stop_flag == 1:
                break

            json_url = kraken_url.format(md_request_vendor.tickers[0],
                                         start_time)
            data_read = json.loads(requests.get(json_url).text)
            if (len(list(data_read)) == 1):
                time_library.sleep(10)
                data_read = json.loads(requests.get(json_url).text)

            data_list = list(data_read['result'])[0]
            data_read = data_read['result'][data_list]
            df = pandas.DataFrame(data_read,
                                  columns=['close', 'volume', 'time',
                                           'buy-sell', 'market-limit',
                                           'miscellaneous'])
            start_time = int(df['time'].tail(1) * 1e9)
            if (start_time > end_time):
                stop_flag = 1
            if (end_time < int(df['time'].head(1) * 1e9)):
                stop_flag = 1
            data_frame = data_frame.append(df)
            time_library.sleep(5)

        data_frame = data_frame.set_index('time')
        data_frame.index = pandas.to_datetime(data_frame.index, unit='s')
        data_frame.index.name = 'Date'
        data_frame = data_frame.drop(['miscellaneous'], axis=1)
        data_frame.replace(['b', 's', 'm', 'l'], [1, -1, 1, -1], inplace=True)
        data_frame = data_frame[
            (data_frame.index >= md_request_vendor.start_date) & (
                    data_frame.index <= md_request_vendor.finish_date)]
        if len(data_frame) == 0:
            logger.warning(
                "Warning: No data. Please change the start_date and finish_date.")

        data_frame.columns = [md_request.tickers[0] + '.close',
                              md_request.tickers[0] + '.volume',
                              md_request.tickers[0] + '.buy-sell',
                              md_request.tickers[0] + '.market-limit']

        field_selected = []
        for i in range(0, len(md_request_vendor.fields)):
            field_selected.append(0)
            field_selected[-1] = md_request.tickers[0] + '.' + \
                                 md_request_vendor.fields[i]

        logger.info("Completed request from Kraken.")

        return data_frame[field_selected]


###############################################################################

class DataVendorBitmex(DataVendor):
    """Class for reading in data from various web sources into findatapy
    library including
    """

    # Data limit = 500,  150 calls / 5 minutes

    def __init__(self):
        super(DataVendorBitmex, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, md_request):
        logger = LoggerManager().getLogger(__name__)
        md_request_vendor = self.construct_vendor_md_request(
            md_request)

        logger.info("Request data from Bitmex.")

        bitMEX_url = 'https://www.bitmex.com/api/v1/quote?symbol={}&count=500&reverse=false&startTime={}&endTime={}'
        data_frame = pandas.DataFrame(
            columns=['askPrice', 'askSize', 'bidPrice', 'bidSize', 'symbol',
                     'timestamp'])
        start_time = md_request_vendor.start_date.timestamp()
        finish_time = md_request_vendor.finish_date.timestamp()
        symbol = md_request_vendor.tickers[0]

        stop_flag = 0
        while stop_flag == 0:
            if stop_flag == 1:
                break
            json_url = bitMEX_url.format(symbol, start_time.isoformat(),
                                         finish_time.isoformat())
            data_read = pandas.read_json(json_url)
            if (len(data_read) < 500):
                stop_flag = 1
            data_frame = data_frame.append(data_read)
            start_time = data_read['timestamp'][data_frame.index[-1]]
            time_library.sleep(2)

        if (len(data_frame) == 0):
            logger.warning(
                "Warning: No data. Please change the start_date and finish_date.")

        data_frame = data_frame.drop(columns=['symbol'])
        col = ['ask-price', 'ask-size', 'bid-price', 'bid-size', 'timestamp']
        data_frame.columns = col
        data_frame = data_frame.set_index('timestamp')
        data_frame.index = pandas.to_datetime(data_frame.index, unit='ms')
        data_frame = data_frame[~data_frame.index.duplicated(keep='first')]
        data_frame.columns = [md_request.tickers[0] + '.ask-price',
                              md_request.tickers[0] + '.ask-size',
                              md_request.tickers[0] + '.bid-price',
                              md_request.tickers[0] + '.bid-size']

        field_selected = []
        for i in range(0, len(md_request_vendor.fields)):
            field_selected.append(0)
            field_selected[-1] = md_request.tickers[0] + '.' + \
                                 md_request_vendor.fields[i]

        logger.info("Completed request from Bitfinex.")

        return data_frame[field_selected]


class DataVendorHuobi(DataVendor):
    """Class for reading in data from various web sources into findatapy
    library including
    """

    # Data limit = 500,  150 calls / 5 minutes

    def __init__(self):
        super(DataVendorHuobi, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, md_request):
        logger = LoggerManager().getLogger(__name__)

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

        md_request_vendor = self.construct_vendor_md_request(
            md_request)

        logger.info("Request data from Huobi.")

        request_size, period = _calc_period_size(
            md_request_vendor.freq,
            md_request_vendor.start_date,
            md_request_vendor.finish_date)

        if request_size > 2000:
            raise ValueError(
                "Requested data too old for candle-stick frequency of '{}'".
                format(
                    md_request_vendor.freq))

        url = "https://api.huobi.pro/market/history/kline?period={period}&size={size}&symbol={symbol}".format(
            period=period,
            size=request_size,
            symbol=md_request_vendor.tickers[0]
        )

        response = requests.get(url, headers=header)
        raw_data = json.loads(response.text)
        df = pandas.DataFrame(raw_data["data"])
        df["timestamp"] = pandas.to_datetime(df["id"], unit="s")

        df = df.set_index("timestamp").sort_index(ascending=True)
        df = df[~df.index.duplicated(keep='first')]

        df.drop(["id"], axis=1, inplace=True)

        if df.empty:
            logger.info(
                "Warning: No data. Please change the start_date "
                "and finish_date.")

        df.columns = ["{}.{}".format(md_request.tickers[0], col) for
                      col in df.columns]

        field_selected = []
        for i in range(0, len(md_request_vendor.fields)):
            field_selected.append(0)
            field_selected[-1] = md_request.tickers[0] + '.' + \
                                 md_request_vendor.fields[i]

        logger.info("Completed request from Huobi.")

        df = df[field_selected]
        return df