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

import copy

import datetime

import pandas as pd

from findatapy.market.ioengine import IOEngine
from findatapy.market.marketdatarequest import MarketDataRequest
from findatapy.timeseries import Filter, Calculations
from findatapy.util import DataConstants, LoggerManager, ConfigManager, \
    SwimPool

constants = DataConstants()


class MarketDataGenerator(object):
    """Returns market data time series by directly calling market data sources.

    At present it supports Bloomberg (bloomberg), Yahoo (yahoo), 
    Quandl (quandl), FRED (fred) etc. which are implemented in subclasses of 
    DataVendor class. This provides a common wrapper for all these 
    data sources.
    """

    def __init__(self, data_vendor_dict={}):
        self._config = ConfigManager().get_instance()
        self._filter = Filter()
        self._calculations = Calculations()
        self._io_engine = IOEngine()
        self._intraday_code = -1
        self._days_expired_intraday_contract_download = -1
        self._data_vendor_dict = data_vendor_dict

        return

    def set_intraday_code(self, code):
        self._intraday_code = code

    def get_data_vendor(self, md_request):
        """Loads appropriate data vendor class

        Parameters
        ----------
        md_request : MarketDataRequest
            the data_source to use "bloomberg", "quandl", "yahoo", "google",
            "fred" etc. we can also have forms like "bloomberg-boe" separated
            by hyphens

        Returns
        -------
        DataVendor
        """
        logger = LoggerManager().getLogger(__name__)

        data_source = md_request.data_source
        data_engine = md_request.data_engine

        # Special case for files (csv, h5, parquet or zip)
        if ".csv" in str(data_source) or ".h5" in str(data_source) or \
                ".parquet" in str(data_source) or ".zip" in str(data_source) \
                or (data_engine is not None
                    and md_request.category is not None
                    and "internet_load" not in md_request.cache_algo):
            from findatapy.market.datavendorweb import DataVendorFlatFile
            data_vendor = DataVendorFlatFile()
        else:
            try:
                data_source = data_source.split("-")[0]
            except:
                logger.error("Was data data_source specified?")

                return None

            if data_source == "bloomberg":
                try:
                    from findatapy.market.datavendorbbg import \
                        DataVendorBBGOpen
                    data_vendor = DataVendorBBGOpen()
                except:
                    logger.warn("Bloomberg needs to be installed")

            elif data_source == "quandl":
                from findatapy.market.datavendorweb import DataVendorQuandl
                data_vendor = DataVendorQuandl()

            elif data_source == "eikon":
                from findatapy.market.datavendorweb import DataVendorEikon
                data_vendor = DataVendorEikon()

            elif data_source == "ons":
                from findatapy.market.datavendorweb import DataVendorONS
                data_vendor = DataVendorONS()

            elif data_source == "boe":
                from findatapy.market.datavendorweb import DataVendorBOE
                data_vendor = DataVendorBOE()

            elif data_source == "dukascopy":
                from findatapy.market.datavendorweb import DataVendorDukasCopy
                data_vendor = DataVendorDukasCopy()

            elif data_source == "fxcm":
                from findatapy.market.datavendorweb import DataVendorFXCM
                data_vendor = DataVendorFXCM()

            elif data_source == "alfred":
                from findatapy.market.datavendorweb import DataVendorALFRED
                data_vendor = DataVendorALFRED()

            elif data_source == "yahoo":
                from findatapy.market.datavendorweb import DataVendorYahoo
                data_vendor = DataVendorYahoo()

            elif data_source in ["google", "fred", "oecd", "eurostat",
                                 "edgar-index"]:
                from findatapy.market.datavendorweb import DataVendorPandasWeb
                data_vendor = DataVendorPandasWeb()

            elif data_source == "bitcoincharts":
                from findatapy.market.datavendorweb import \
                    DataVendorBitcoincharts
                data_vendor = DataVendorBitcoincharts()
            elif data_source == "poloniex":
                from findatapy.market.datavendorweb import DataVendorPoloniex
                data_vendor = DataVendorPoloniex()
            elif data_source == "binance":
                from findatapy.market.datavendorweb import DataVendorBinance
                data_vendor = DataVendorBinance()
            elif data_source == "bitfinex":
                from findatapy.market.datavendorweb import DataVendorBitfinex
                data_vendor = DataVendorBitfinex()
            elif data_source == "gdax":
                from findatapy.market.datavendorweb import DataVendorGdax
                data_vendor = DataVendorGdax()
            elif data_source == "kraken":
                from findatapy.market.datavendorweb import DataVendorKraken
                data_vendor = DataVendorKraken()
            elif data_source == "bitmex":
                from findatapy.market.datavendorweb import DataVendorBitmex
                data_vendor = DataVendorBitmex()
            elif data_source == "alphavantage":
                from findatapy.market.datavendorweb import \
                    DataVendorAlphaVantage
                data_vendor = DataVendorAlphaVantage()
            elif data_source == "huobi":
                from findatapy.market.datavendorweb import DataVendorHuobi
                data_vendor = DataVendorHuobi()
            elif data_source in self._data_vendor_dict:
                data_vendor = self._data_vendor_dict[data_source]
            elif data_source in md_request.data_vendor_custom:
                data_vendor = md_request.data_vendor_custom[data_source]
            else:
                logger.warn(str(data_source) +
                            " is an unrecognized data source")

        return data_vendor

    def fetch_market_data(self, md_request):
        """Loads time series from specified data provider

        Parameters
        ----------
        md_request : MarketDataRequest
            contains various properties describing time series to fetched, 
            including ticker, start & finish date etc.

        Returns
        -------
        pandas.DataFrame
        """
        logger = LoggerManager().getLogger(__name__)

        # data_vendor = self.get_data_vendor(md_request.data_source)

        # Check if tickers have been specified (if not load all of them for a 
        # category)
        # also handle single tickers/list tickers
        create_tickers = False

        if md_request.vendor_tickers is not None \
                and md_request.tickers is None:
            md_request.tickers = md_request.vendor_tickers

        tickers = md_request.tickers

        if tickers is None:
            create_tickers = True
        elif isinstance(tickers, str):
            if tickers == "": create_tickers = True
        elif isinstance(tickers, list):
            if tickers == []: create_tickers = True

        if create_tickers:
            md_request.tickers = ConfigManager().get_instance()\
                .get_tickers_list_for_category(
                    md_request.category, md_request.data_source,
                    md_request.freq, md_request.cut)

            if md_request.pretransformation is not None:
                df_tickers = ConfigManager().get_instance()\
                    .get_dataframe_tickers()

                df_tickers = df_tickers[
                    (df_tickers["category"] == md_request.category) &
                    (df_tickers["data_source"] == md_request.data_source) &
                    (df_tickers["freq"] == md_request.freq) &
                    (df_tickers["cut"] == md_request.cut)]

                if "pretransformation" in df_tickers.columns:
                    md_request.pretransformation = \
                        df_tickers["pretransformation"].tolist()

        # intraday or tick: only one ticker per cache file
        if md_request.freq in ["intraday", "tick", "second", "hour",
                                "minute"]:
            df_agg = self.download_intraday_tick(md_request)

        # Daily: multiple tickers per cache file - assume we make one API call 
        # to vendor library
        else:
            df_agg = self.download_daily(md_request)

        if "internet_load" in md_request.cache_algo:
            logger.debug("Internet loading.. ")

        if md_request.cache_algo == "cache_algo":
            logger.debug(
                "Only caching data in memory, do not return any time series.")
            
            return

        # Only return time series if specified in the algo
        if "return" in md_request.cache_algo:
            # Special case for events/events-dt which is not indexed like other 
            # tables (also same for downloading futures contracts dates)
            if md_request.category is not None:
                if "events" in md_request.category:
                    return df_agg

            # Pad columns a second time (is this necessary to do here again?)
            # TODO only do this for not daily data?
            try:
                if df_agg is not None:
                    filter_by_column_names = True

                    for col in df_agg.columns:
                        if "all-vintages" in col:
                            filter_by_column_names = False

                    df_agg = self._filter.filter_time_series(
                        md_request, df_agg, pad_columns=True,
                        filter_by_column_names=filter_by_column_names)

                    df_agg = df_agg.dropna(how="all")

                    # Resample data using pandas if specified in the 
                    # MarketDataRequest
                    if md_request.resample is not None:
                        if "last" in md_request.resample_how:
                            df_agg = df_agg.resample(
                                md_request.resample).last()
                        elif "first" in md_request.resample_how:
                            df_agg = df_agg.resample(
                                md_request.resample).first()

                        if "dropna" in md_request.resample_how:
                            df_agg = df_agg.dropna(how="all")
                else:
                    logger.warn("No data returned for " + str(
                        md_request.tickers))

                return df_agg
            except Exception as e:
                
                if df_agg is not None:
                    return df_agg

                import traceback

                logger.warn(
                    "No data returned for " 
                    + str(md_request.tickers) + ", " + str(e))

                return None

    def create_time_series_hash_key(self, md_request, ticker=None):
        """Creates a hash key for retrieving the time series

        Parameters
        ----------
        md_request : MarketDataRequest
            contains various properties describing time series to fetched, 
            including ticker, start & finish date etc.

        Returns
        -------
        str
        """

        if (isinstance(ticker, list)):
            ticker = ticker[0]

        return self.create_cache_file_name(
            MarketDataRequest().create_category_key(
                md_request=md_request, ticker=ticker))

    def download_intraday_tick(self, md_request):
        """Loads intraday time series from specified data provider

        Parameters
        ----------
        md_request : MarketDataRequest
            contains various properties describing time series to fetched, 
            including ticker, start & finish date etc.

        Returns
        -------
        pandas.DataFrame
        """

        df_agg = None
        calcuations = Calculations()

        ticker_cycle = 0

        df_group = []

        # Single threaded version
        # handle intraday ticker calls separately one by one
        if len(md_request.tickers) == 1 or constants.market_thread_no[
            "other"] == 1:
            for ticker in md_request.tickers:
                md_request_single = copy.copy(md_request)
                md_request_single.tickers = ticker

                if md_request.vendor_tickers is not None:
                    md_request_single.vendor_tickers = [
                        md_request.vendor_tickers[ticker_cycle]]
                    ticker_cycle = ticker_cycle + 1

                df_single = self.fetch_single_time_series(
                    md_request)

                # If the vendor doesn"t provide any data, don"t attempt to append
                if df_single is not None:
                    if df_single.empty == False:
                        df_single.index.name = "Date"
                        df_single = df_single.astype("float32")

                        df_group.append(df_single)

            # If you call for returning multiple tickers, be careful with 
            # memory considerations!
            if df_group is not None:
                df_agg = calcuations.join(df_group, how="outer")

            return df_agg

        else:
            md_request_list = []

            # Create a list of MarketDataRequests
            for ticker in md_request.tickers:
                md_request_single = copy.copy(md_request)
                md_request_single.tickers = ticker

                if md_request.vendor_tickers is not None:
                    md_request_single.vendor_tickers = [
                        md_request.vendor_tickers[ticker_cycle]]
                    ticker_cycle = ticker_cycle + 1

                md_request_list.append(md_request_single)

            return self.fetch_group_time_series(md_request_list)

    def fetch_single_time_series(self, md_request):
        
        md_request = MarketDataRequest(md_request=md_request)

        # Only includes those tickers have not expired yet!
        start_date = pd.Timestamp(md_request.start_date).date()

        current_date = pd.Timestamp(datetime.datetime.utcnow().date())

        tickers = md_request.tickers
        vendor_tickers = md_request.vendor_tickers

        expiry_date = pd.Timestamp(md_request.expiry_date)

        config = ConfigManager().get_instance()

        # In many cases no expiry is defined so skip them
        for i in range(0, len(tickers)):
            try:
                expiry_date = config.get_expiry_for_ticker(
                    md_request.data_source, tickers[i])
            except:
                pass

            if expiry_date is not None:
                expiry_date = pd.Timestamp(expiry_date)

                if not (pd.isna(expiry_date)):
                    # Use pandas Timestamp, a bit more robust with weird dates 
                    # (can fail if comparing date vs datetime)
                    # if the expiry is before the start date of our download 
                    # don"t bother downloading this ticker
                    if expiry_date < start_date:
                        tickers[i] = None

                    # Special case for futures-contracts which are intraday
                    # avoid downloading if the expiry date is very far in the 
                    # past
                    # (we need this before there might be odd situations where 
                    # we run on an expiry date, but still want to get
                    # data right till expiry time)
                    if md_request.category == "futures-contracts" \
                            and md_request.freq == "intraday" \
                            and self._days_expired_intraday_contract_download \
                                > 0:

                        if expiry_date + pd.Timedelta(
                                days=
                                self._days_expired_intraday_contract_download) \
                                < current_date:
                            tickers[i] = None

                    if vendor_tickers is not None and tickers[i] is None:
                        vendor_tickers[i] = None

        md_request.tickers = [e for e in tickers if e != None]

        if vendor_tickers is not None:
            md_request.vendor_tickers = [e for e in vendor_tickers if
                                                  e != None]

        df_single = None

        if len(md_request.tickers) > 0:
            df_single = self.get_data_vendor(
                md_request).load_ticker(md_request)

        if df_single is not None:
            if df_single.empty == False:
                df_single.index.name = "Date"

                # Will fail for DataFrames which includes dates/strings 
                # eg. futures contract names
                df_single = Calculations().convert_to_numeric_dataframe(
                    df_single)

                if md_request.freq == "second":
                    df_single = df_single.resample("1s")

        return df_single

    def fetch_group_time_series(self, market_data_request_list):

        logger = LoggerManager().getLogger(__name__)

        df_agg = None

        thread_no = constants.market_thread_no["other"]

        if market_data_request_list[
            0].data_source in constants.market_thread_no:
            thread_no = constants.market_thread_no[
                market_data_request_list[0].data_source]

        if thread_no > 0:
            pool = SwimPool().create_pool(
                thread_technique=constants.market_thread_technique,
                thread_no=thread_no)

            # Open the market data downloads in their own threads and return 
            # the results
            result = pool.map_async(self.fetch_single_time_series,
                                    market_data_request_list)
            df_group = result.get()

            pool.close()
            pool.join()
        else:
            df_group = []

            for md_request in market_data_request_list:
                df_group.append(
                    self.fetch_single_time_series(md_request))

        # Collect together all the time series
        if df_group is not None:
            df_group = [i for i in df_group if i is not None]

            if df_group is not None:
                try:
                    df_agg = self._calculations.join(df_group,
                                                             how="outer")

                    # Force ordering to be the same!
                    # df_agg = df_agg[columns]
                except Exception as e:
                    logger.warning(
                        "Possible overlap of columns? Have you specifed same "
                        "ticker several times: " + str(e))

        return df_agg

    def download_daily(self, md_request):
        """Loads daily time series from specified data provider

        Parameters
        ----------
        md_request : MarketDataRequest
            contains various properties describing time series to fetched, 
            including ticker, start & finish date etc.

        Returns
        -------
        pandas.DataFrame
        """

        key = MarketDataRequest().create_category_key(
            md_request=md_request)

        is_key_overriden = False

        for k in constants.override_multi_threading_for_categories:
            if k in key:
                is_key_overriden = True
                break

        # By default use other
        thread_no = constants.market_thread_no["other"]

        if str(md_request.data_source) in constants.market_thread_no:
            thread_no = constants.market_thread_no[
                md_request.data_source]

        # Daily data does not include ticker in the key, as multiple tickers 
        # in the same file
        if thread_no == 1 or ".csv" in str(md_request.data_source) or \
                ".h5" in str(
            md_request.data_source) or ".parquet" in str(
            md_request.data_source) \
                or ".zip" in str(
            md_request.data_source) or md_request.data_engine is not None:
            # df_agg = data_vendor.load_ticker(md_request)
            df_agg = self.fetch_single_time_series(md_request)
        else:
            md_request_list = []

            # When trying your example "equitiesdata_example" I had a -1 result 
            # so it went out of the comming loop and I had errors in execution
            group_size = max(
                int(len(md_request.tickers) / thread_no - 1), 0)

            if group_size == 0: group_size = 1

            # Split up tickers into groups related to number of threads to call
            for i in range(0, len(md_request.tickers), group_size):
                md_request_single = copy.copy(md_request)
                md_request_single.tickers = \
                    md_request.tickers[i:i + group_size]

                if md_request.vendor_tickers is not None:
                    md_request_single.vendor_tickers = \
                        md_request.vendor_tickers[i:i + group_size]

                if md_request.pretransformation is not None:
                    md_request_single.pretransformation = \
                        md_request.pretransformation[i:i + group_size]

                md_request_list.append(md_request_single)

            # Special case where we make smaller calls one after the other
            if is_key_overriden:

                df_list = []

                for md in md_request_list:
                    df_list.append(self.fetch_single_time_series(md))

                df_agg = self._calculations.join(df_list,
                                                         how="outer")
            else:
                df_agg = self.fetch_group_time_series(
                    md_request_list)

        return df_agg

    def refine_expiry_date(self, market_data_request):

        # Expiry date
        if market_data_request.expiry_date is None:
            ConfigManager().get_instance().get_expiry_for_ticker(
                market_data_request.data_source, market_data_request.ticker)

        return market_data_request

    def create_cache_file_name(self, filename):
        return constants.folder_time_series_data + "/" + filename
