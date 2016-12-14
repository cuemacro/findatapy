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
Market

Market object is higher level object which fetches market data using underlying classes such as MarketDataGenerator. Also
contains several other classes, which are for asset specific instances.

"""

from findatapy.util import DataConstants
# from deco import *

class Market(object):

    def __init__(self, market_data_generator = None, md_request = None):
        if market_data_generator is None:
            if DataConstants().default_market_data_generator == "marketdatagenerator":
                from findatapy.market import MarketDataGenerator
                market_data_generator = MarketDataGenerator()
            elif DataConstants().default_market_data_generator == 'cachedmarketdatagenerator':
                # NOT CURRENTLY IMPLEMENTED FOR FUTURE USE
                from finexpy.market import CachedMarketDataGenerator
                market_data_generator = CachedMarketDataGenerator()

        self.market_data_generator = market_data_generator
        self.md_request = md_request

    def fetch_market(self, md_request = None):
        if self.md_request is not None:
            md_request = self.md_request

        # special cases when a predefined category has been asked
        if md_request.category is not None:

            if (md_request.category == 'fx-spot-volume' and md_request.data_source == 'quandl'):
                # NOT CURRENTLY IMPLEMENTED FOR FUTURE USE
                from findatapy.market.fxclsvolume import FXCLSVolume
                fxcls = FXCLSVolume(market_data_generator=self.market_data_generator)

                return fxcls.get_fx_volume(md_request.start_date, md_request.finish_date, md_request.tickers, cut="LOC", source="quandl",
                       cache_algo=md_request.cache_algo)

            if (md_request.category == 'fx' or md_request.category == 'fx-tot') and md_request.tickers is not None:
                fxcf = FXCrossFactory(market_data_generator=self.market_data_generator)

                if md_request.category == 'fx':
                    type = 'spot'
                elif md_request.category == 'fx-tot':
                    type = 'tot'

                if (md_request.freq != 'tick' and md_request.fields == ['close']) or (md_request.freq == 'tick' and md_request.data_source == 'dukascopy'):
                    return fxcf.get_fx_cross(md_request.start_date, md_request.finish_date,
                                             md_request.tickers,
                     cut = md_request.cut, source = md_request.data_source, freq = md_request.freq, cache_algo=md_request.cache_algo, type = type,
                     environment = md_request.environment, fields = md_request.fields)
            if (md_request.category == 'fx-implied-vol'):
                if md_request.tickers is not None and md_request.freq == 'daily':
                    df = []

                    fxvf = FXVolFactory(market_data_generator=self.market_data_generator)

                    for t in md_request.tickers:
                        if len(t) == 6:
                            df.append(fxvf.get_fx_implied_vol(md_request.start_date, md_request.finish_date, t, fxvf.tenor,
                                                              cut=md_request.cut, source=md_request.data_source, part=fxvf.part,
                               cache_algo_return=md_request.cache_algo))

                    if df != []:
                        return Calculations().pandas_outer_join(df)

            if(md_request.category == 'fx-vol-market'):
                if md_request.tickers is not None:
                    df = []

                    fxcf = FXCrossFactory(market_data_generator=self.market_data_generator)
                    fxvf = FXVolFactory(market_data_generator=self.market_data_generator)
                    rates = RatesFactory(market_data_generator=self.market_data_generator)

                    for t in md_request.tickers:
                        if len(t) == 6:
                            df.append(fxcf.get_fx_cross(start=md_request.start_date, end=md_request.finish_date, cross=t,
                                                        cut=md_request.cut, source=md_request.data_source, freq=md_request.freq,
                                                        cache_algo=md_request.cache_algo, type='spot', environment=md_request.environment,
                                                        fields=['close']))

                            df.append(fxvf.get_fx_implied_vol(md_request.start_date, md_request.finish_date, t, fxvf.tenor,
                                                              cut=md_request.cut, source=md_request.data_source,
                                                              part=fxvf.part,
                                                              cache_algo=md_request.cache_algo))

                            df.append(rates.get_fx_forward_points(md_request.start_date, md_request.finish_date, t, fxvf.tenor,
                                                              cut=md_request.cut, source=md_request.data_source,
                                                              cache_algo=md_request.cache_algo))

                    df.append(rates.get_base_depos(md_request.start_date, md_request.finish_date, ["USD", "EUR", "CHF", "GBP"], fxvf.tenor,
                                                   cut=md_request.cut, source=md_request.data_source,
                                                   cache_algo=md_request.cache_algo
                                                   ))

                    if df != []:
                        return Calculations().pandas_outer_join(df)


            # TODO add more special examples here for different asset classes
            # the idea is that we do all the market data downloading here, rather than elsewhere

        # by default: pass the market data request to MarketDataGenerator
        return self.market_data_generator.fetch_market_data(md_request)

########################################################################################################################

"""
FXCrossFactory

Class generates FX spot time series and FX total return time series (assuming we already have
total return indices available from xxxUSD form) from underlying series.

"""

from findatapy.market.marketdatarequest import MarketDataRequest
from findatapy.timeseries import Calculations

from findatapy.util.fxconv import FXConv
from findatapy.util.loggermanager import LoggerManager

class FXCrossFactory(object):

    def __init__(self, market_data_generator = None):
        self.logger = LoggerManager().getLogger(__name__)
        self.fxconv = FXConv()

        self.cache = {}

        self.calculations = Calculations()
        self.market_data_generator = market_data_generator

        return

    def flush_cache(self):
        self.cache = {}

    # cross is a ticker[] in original properties
    def get_fx_cross_tick(self, start, end, cross,
                     cut = "NYC", source = "dukascopy", cache_algo = 'internet_load_return', type = 'spot',
                     environment = 'backtest', fields = ['bid', 'ask']):

        if isinstance(cross, str):
            cross = [cross]

        market_data_request = MarketDataRequest(
            gran_freq="tick",
            freq_mult = 1,
            freq = 'tick',
            cut = cut,
            fields = fields,
            cache_algo=cache_algo,
            environment = environment,
            start_date = start,
            finish_date = end,
            data_source = source,
            category = 'fx'
        )

        market_data_generator = self.market_data_generator
        data_frame_agg = None


        for cr in cross:

            if (type == 'spot'):
                market_data_request.tickers = cr

                cross_vals = market_data_generator.fetch_market_data(market_data_request)

                # if user only wants 'close' calculate that from the bid/ask fields
                if fields == ['close']:
                    cross_vals = cross_vals[[cr + '.bid', cr + '.ask']].mean(axis=1)
                    cross_vals.columns = [cr + '.close']
                else:
                    filter = Filter()

                    filter_columns = [cr + '.' + f for f in fields]
                    cross_vals = filter.filter_time_series_by_columns(cross_vals, filter_columns)

            if data_frame_agg is None:
                data_frame_agg = cross_vals
            else:
                data_frame_agg = data_frame_agg.join(cross_vals, how='outer')

        # strip the nan elements
        data_frame_agg = data_frame_agg.dropna()
        return data_frame_agg

    # cross is a ticker[] in original properties
    def get_fx_cross(self, start, end, cross,
                     cut = "NYC", source = "bloomberg", freq = "intraday", cache_algo='internet_load_return', type = 'spot',
                     environment = 'backtest', fields = ['close']):

        if source == "gain" or source == 'dukascopy' or freq == 'tick':
            return self.get_fx_cross_tick(start, end, cross,
                     cut = cut, source = source, cache_algo = cache_algo, type = 'spot', fields = fields)

        if isinstance(cross, str):
            cross = [cross]

        market_data_request_list = []
        freq_list = []
        type_list = []

        for cr in cross:
            market_data_request = MarketDataRequest(freq_mult=1,
                                                cut=cut,
                                                fields=['close'],
                                                freq=freq,
                                                cache_algo=cache_algo,
                                                start_date=start,
                                                finish_date=end,
                                                data_source=source,
                                                environment=environment)

            market_data_request.type = type
            market_data_request.cross = cr

            if freq == 'intraday':
                market_data_request.gran_freq = "minute"                # intraday

            elif freq == 'daily':
                market_data_request.gran_freq = "daily"                 # daily

            market_data_request_list.append(market_data_request)

        data_frame_agg = []

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

        # fudge, issue with multithreading and accessing HDF5 files
        # if self.market_data_generator.__class__.__name__ == 'CachedMarketDataGenerator':
        #    thread_no = 0

        if (thread_no > 0):
            pool = Pool(thread_no)

            # open the market data downloads in their own threads and return the results
            result = pool.map_async(self._get_individual_fx_cross, market_data_request_list)
            data_frame_agg = self.calculations.iterative_outer_join(result.get())

            # data_frame_agg = self.calculations.pandas_outer_join(result.get())

            # pool would have already been closed earlier
            # try:
            #    pool.close()
            #    pool.join()
            # except: pass
        else:
            for md_request in market_data_request_list:
                data_frame_agg.append(self._get_individual_fx_cross(md_request))

            data_frame_agg = self.calculations.pandas_outer_join(data_frame_agg)

        # strip the nan elements
        data_frame_agg = data_frame_agg.dropna()

        return data_frame_agg

    def _get_individual_fx_cross(self, market_data_request):
        cr = market_data_request.cross
        type = market_data_request.type
        freq = market_data_request.freq

        base = cr[0:3]
        terms = cr[3:6]

        if (type == 'spot'):
            # non-USD crosses
            if base != 'USD' and terms != 'USD':
                base_USD = self.fxconv.correct_notation('USD' + base)
                terms_USD = self.fxconv.correct_notation('USD' + terms)

                # TODO check if the cross exists in the database

                # download base USD cross
                market_data_request.tickers = base_USD
                market_data_request.category = 'fx'

                if base_USD + '.close' in self.cache:
                    base_vals = self.cache[base_USD + '.close']
                else:
                    base_vals = self.market_data_generator.fetch_market_data(market_data_request)
                    self.cache[base_USD + '.close'] = base_vals

                # download terms USD cross
                market_data_request.tickers = terms_USD
                market_data_request.category = 'fx'

                if terms_USD + '.close' in self.cache:
                    terms_vals = self.cache[terms_USD + '.close']
                else:
                    terms_vals = self.market_data_generator.fetch_market_data(market_data_request)
                    self.cache[terms_USD + '.close'] = terms_vals

                # if quoted USD/base flip to get USD terms
                if (base_USD[0:3] == 'USD'):
                    if 'USD' + base in '.close' in self.cache:
                        base_vals = self.cache['USD' + base + '.close']
                    else:
                        base_vals = 1 / base_vals
                        self.cache['USD' + base + '.close'] = base_vals

                # if quoted USD/terms flip to get USD terms
                if (terms_USD[0:3] == 'USD'):
                    if 'USD' + terms in '.close' in self.cache:
                        terms_vals = self.cache['USD' + terms + '.close']
                    else:
                        terms_vals = 1 / terms_vals
                        self.cache['USD' + terms + '.close'] = base_vals

                base_vals.columns = ['temp'];
                terms_vals.columns = ['temp']

                cross_vals = base_vals.div(terms_vals, axis='index')
                cross_vals.columns = [cr + '.close']

                base_vals.columns = [base_USD + '.close']
                terms_vals.columns = [terms_USD + '.close']
            else:
                # if base == 'USD': non_USD = terms
                # if terms == 'USD': non_USD = base

                correct_cr = self.fxconv.correct_notation(cr)

                market_data_request.tickers = correct_cr
                market_data_request.category = 'fx'

                if correct_cr + '.close' in self.cache:
                    cross_vals = self.cache[correct_cr + '.close']
                else:
                    cross_vals = self.market_data_generator.fetch_market_data(market_data_request)

                    # flip if not convention
                    if (correct_cr != cr):
                        if cr + '.close' in self.cache:
                            cross_vals = self.cache[cr + '.close']
                        else:
                            cross_vals = 1 / cross_vals
                            self.cache[cr + '.close'] = cross_vals

                    self.cache[correct_cr + '.close'] = cross_vals

                # cross_vals = self.market_data_generator.harvest_time_series(market_data_request)
                cross_vals.columns.names = [cr + '.close']

        elif type[0:3] == "tot":
            if freq == 'daily':
                # download base USD cross
                market_data_request.tickers = base + 'USD'
                market_data_request.category = 'fx-tot'

                if type == "tot":
                    base_vals = self.market_data_generator.fetch_market_data(market_data_request)
                else:
                    x = 0

                # download terms USD cross
                market_data_request.tickers = terms + 'USD'
                market_data_request.category = 'fx-tot'

                if type == "tot":
                    terms_vals = self.market_data_generator.fetch_market_data(market_data_request)
                else:
                    pass

                base_rets = self.calculations.calculate_returns(base_vals)
                terms_rets = self.calculations.calculate_returns(terms_vals)

                cross_rets = base_rets.sub(terms_rets.iloc[:, 0], axis=0)

                # first returns of a time series will by NaN, given we don't know previous point
                cross_rets.iloc[0] = 0

                cross_vals = self.calculations.create_mult_index(cross_rets)
                cross_vals.columns = [cr + '-tot.close']

            elif freq == 'intraday':
                self.logger.info('Total calculated returns for intraday not implemented yet')
                return None

        return cross_vals

#######################################################################################################################

"""
FXVolFactory

Class generates FX implied volatility time series and surfaces (using very simple interpolation!).

"""

import pandas

from findatapy.market.marketdatarequest import MarketDataRequest
from findatapy.util import LoggerManager
from findatapy.timeseries import Calculations, Filter, Timezone

class FXVolFactory(object):
    # types of quotation on vol surface
    # ATM, 25d riskies, 10d riskies, 25d strangles, 10d strangles
    part = ["V", "25R", "10R", "25B", "10B"]

    # all the tenors on our vol surface
    tenor = ["ON", "1W", "2W", "3W", "1M", "2M", "3M", "6M", "9M", "1Y", "2Y", "3Y", "5Y"]

    def __init__(self, market_data_generator=None):
        self.logger = LoggerManager().getLogger(__name__)

        self.market_data_generator = market_data_generator

        self.calculations = Calculations()
        self.filter = Filter()
        self.timezone = Timezone()

        self.rates = RatesFactory()

        return

    def get_fx_implied_vol(self, start, end, cross, tenor, cut="BGN", source="bloomberg", part="V",
                           cache_algo="internet_load_return"):
        """ get_implied_vol = get implied vol for specified cross, tenor and part of surface

        :param start: start date
        :param end: end date
        :param cross: asset to be calculated
        :param tenor: tenor to calculate
        :param cut: closing time of data
        :param source: source of data eg. bloomberg
        :param part: part of vol surface eg. V for ATM implied vol, 25R 25 delta risk reversal

        :return: realised volatility
        """

        market_data_generator = self.market_data_generator

        if isinstance(cross, str): cross = [cross]
        if isinstance(tenor, str): tenor = [tenor]
        if isinstance(part, str): part = [part]

        tickers = []

        for cr in cross:
            for tn in tenor:
                for pt in part:
                    tickers.append(cr + pt + tn)

        market_data_request = MarketDataRequest(
            start_date=start, finish_date=end,
            data_source=source,
            category='fx-implied-vol',
            freq='daily',
            cut=cut,
            tickers=tickers,
            fields=['close'],
            cache_algo=cache_algo,
            environment='backtest'
        )
        data_frame = market_data_generator.fetch_market_data(market_data_request)
        data_frame.index.name = 'Date'

        return data_frame

    def extract_vol_surface_for_date(self, df, cross, date_index):

        # assume we have a matrix of the form
        # eg. EURUSDVON.close ...

        # types of quotation on vol surface
        # self.part = ["V", "25R", "10R", "25B", "10B"]

        # all the tenors on our vol surface
        # self.tenor = ["ON", "1W", "2W", "3W", "1M", "2M", "3M", "6M", "9M", "1Y", "2Y", "5Y"]

        strikes = ["10DP",
                   "25DP",
                   "ATM",
                   "25DC",
                   "10DC"]

        tenor = self.tenor

        df_surf = pandas.DataFrame(index=strikes, columns=tenor)

        for ten in tenor:
            df_surf.ix["10DP", ten] = df.ix[date_index, cross + "V" + ten + ".close"] \
                                      - (df.ix[date_index, cross + "10R" + ten + ".close"] / 2.0) \
                                      + (df.ix[date_index, cross + "10B" + ten + ".close"])

            df_surf.ix["10DC", ten] = df.ix[date_index, cross + "V" + ten + ".close"] \
                                      + (df.ix[date_index, cross + "10R" + ten + ".close"] / 2.0) \
                                      + (df.ix[date_index, cross + "10B" + ten + ".close"])

            df_surf.ix["25DP", ten] = df.ix[date_index, cross + "V" + ten + ".close"] \
                                      - (df.ix[date_index, cross + "25R" + ten + ".close"] / 2.0) \
                                      + (df.ix[date_index, cross + "25B" + ten + ".close"])

            df_surf.ix["25DC", ten] = df.ix[date_index, cross + "V" + ten + ".close"] \
                                      + (df.ix[date_index, cross + "25R" + ten + ".close"] / 2.0) \
                                      + (df.ix[date_index, cross + "25B" + ten + ".close"])

            df_surf.ix["ATM", ten] = df.ix[date_index, cross + "V" + ten + ".close"]

        return df_surf

#######################################################################################################################

class RatesFactory(object):

    def __init__(self, market_data_generator=None):
        self.logger = LoggerManager().getLogger(__name__)

        self.cache = {}

        self.calculations = Calculations()
        self.market_data_generator = market_data_generator

        return

    # all the tenors on our forwards
    # forwards_tenor = ["ON", "1W", "2W", "3W", "1M", "2M", "3M", "6M", "9M", "1Y", "2Y", "3Y", "5Y"]

    def get_base_depos(self, start, end, currencies, tenor, cut="NYC", source="bloomberg",
                              cache_algo="internet_load_return"):
        """ get_forward_points = get forward points for specified cross, tenor and part of surface

        :param start: start date
        :param end: end date
        :param cross: asset to be calculated
        :param tenor: tenor to calculate
        :param cut: closing time of data
        :param source: source of data eg. bloomberg

        :return: forward points
        """

        market_data_generator = self.market_data_generator

        if isinstance(currencies, str): currencies = [currencies]
        if isinstance(tenor, str): tenor = [tenor]

        tickers = []

        for cr in currencies:
            for tn in tenor:
                tickers.append(cr + tn)

        market_data_request = MarketDataRequest(
            start_date=start, finish_date=end,
            data_source=source,
            category='base-depos',
            freq='daily',
            cut=cut,
            tickers=tickers,
            fields=['close'],
            cache_algo=cache_algo,
            environment='backtest'
        )

        data_frame = market_data_generator.fetch_market_data(market_data_request)
        data_frame.index.name = 'Date'

        return data_frame

    def get_fx_forward_points(self, start, end, cross, tenor, cut="BGN", source="bloomberg",
                           cache_algo="internet_load_return"):
        """ get_forward_points = get forward points for specified cross, tenor and part of surface

        :param start: start date
        :param end: end date
        :param cross: asset to be calculated
        :param tenor: tenor to calculate
        :param cut: closing time of data
        :param source: source of data eg. bloomberg

        :return: forward points
        """

        market_data_request = MarketDataRequest()
        market_data_generator = self.market_data_generator

        market_data_request.data_source = source  # use bbg as a source
        market_data_request.start_date = start  # start_date
        market_data_request.finish_date = end  # finish_date

        if isinstance(cross, str): cross = [cross]
        if isinstance(tenor, str): tenor = [tenor]

        tenor = [x.replace('1Y', '12M') for x in tenor]

        tickers = []

        for cr in cross:
            for tn in tenor:
                tickers.append(cr + tn)

        market_data_request = MarketDataRequest(
            start_date = start, finish_date = end,
            data_source = source,
            category = 'fx-forwards',
            freq = 'daily',
            cut = cut,
            tickers=tickers,
            fields = ['close'],
            cache_algo = cache_algo,
            environment = 'backtest'
        )

        data_frame = market_data_generator.fetch_market_data(market_data_request)
        data_frame.columns = [x.replace('12M', '1Y') for x in data_frame.columns]
        data_frame.index.name = 'Date'

        return data_frame

