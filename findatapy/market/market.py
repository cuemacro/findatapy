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
from findatapy.util import ConfigManager
from findatapy.util import DataConstants
from findatapy.market.ioengine import SpeedCache

import concurrent.futures

import json

constants = DataConstants()


# from deco import *

class Market(object):
    """Higher level class which fetches market data using underlying classes
    such as MarketDataGenerator.

    Also contains several other classes, which are for asset specific
    instances, for example for generating FX spot time series
    or FX volatility surfaces.
    """

    def __init__(self, market_data_generator=None, md_request=None):
        if market_data_generator is None:
            if constants.default_market_data_generator\
                    == "marketdatagenerator":
                from findatapy.market import MarketDataGenerator
                market_data_generator = MarketDataGenerator()
            elif constants.default_market_data_generator \
                    == 'cachedmarketdatagenerator':
                # NOT CURRENTLY IMPLEMENTED FOR FUTURE USE
                from finaddpy.market import CachedMarketDataGenerator
                market_data_generator = CachedMarketDataGenerator()
            else:
                from findatapy.market import MarketDataGenerator
                market_data_generator = MarketDataGenerator()

        self.speed_cache = SpeedCache()
        self._market_data_generator = market_data_generator
        self._filter = Filter()
        self._calculations = Calculations()
        self.md_request = md_request

    def fetch_market(self, md_request=None, md_request_df=None,
                     md_request_str=None, md_request_dict=None, tickers=None,
                     start_date=None, finish_date=None, best_match_only=False,
                     **kwargs):
        """Fetches market data for specific tickers

        The user does not need to know to the low level API for each data
        provider works. The MarketDataRequest needs to supply parameters that
        define each data request. It has details which include:
            ticker eg. EURUSD
            field eg. close
            category eg. fx
            data_source eg. bloomberg
            start_date eg. 01 Jan 2015
            finish_date eg. 01 Jan 2017

        It can also have many optional attributes, such as
            vendor_ticker eg. EURUSD Curncy
            vendor_field eg. PX_LAST

        We can also create MarketDataRequest objects, using strings eg. fx.quandl.daily.NYC.EURUSD (category.data_source.freq.cut.ticker),
        as Python dict, as DataFrame with various properties (like ticker, category etc.)

        Parameters
        ----------
        md_request : MarketDataRequest (or shorthand str/DataFrame/dict)
            Describing what market data to fetch

        md_request_df : DataFrame
            Another way to specify some of the data request properties

        md_request_str : str
            We can specify part of the MarketDataRequest as a string, which gets converted later (or a JSON object)

        Returns
        -------
        pd.DataFrame
            Contains the requested market data

        """

        if self.md_request is not None:
            md_request = self.md_request

        if isinstance(md_request, str):
            md_request_str = md_request
            md_request = MarketDataRequest()

        if isinstance(md_request, dict):
            md_request_dict = md_request
            md_request = MarketDataRequest()

        if isinstance(md_request, pd.DataFrame):
            md_request_df = md_request
            md_request = MarketDataRequest()

        # Any kwargs are assumed to be to set MarketDataRequest attributes
        if kwargs != {}:
            md_request = self._kwargs_to_md_request(kwargs, md_request)

        # When we have specified a string
        if md_request_str is not None:
            md_request = self.create_md_request_from_str(
                md_request_str, md_request=md_request,
                start_date=start_date, finish_date=finish_date,
                best_match_only=best_match_only, **kwargs)

            return self.fetch_market(md_request)

        # When we have specified predefined tickers
        if tickers is not None:
            md_request = self.create_md_request_from_tickers(
                tickers, md_request=md_request, start_date=start_date,
                finish_date=finish_date, best_match_only=best_match_only,
                **kwargs)

            return self.fetch_market(md_request)

        # When we have specified a DataFrame with tickers
        if md_request_df is not None:
            md_request = self.create_md_request_from_dataframe(
                md_request_df, md_request=md_request,
                start_date=start_date, finish_date=finish_date,
                best_match_only=best_match_only, **kwargs)

            return self.fetch_market(md_request)

        # Or directly as a string
        if md_request_str is not None:
            md_request = self.create_md_request_from_str(
                md_request, 
                start_date=start_date, finish_date=finish_date,
                best_match_only=best_match_only, **kwargs)

            return self.fetch_market(md_request)

        # Or directly as a dict
        if md_request_dict is not None:
            md_request = self.create_md_request_from_dict(
                md_request_dict, md_request=md_request,
                start_date=start_date, finish_date=finish_date)

            return self.fetch_market(md_request)

        # Or directly as a DataFrame
        if md_request_df is not None:
            md_request = self.create_md_request_from_dataframe(
                md_request_df, md_request=md_request,
                start_date=start_date, finish_date=finish_date,
                **kwargs)

            return self.fetch_market(md_request)

        data_frame = None

        if isinstance(md_request, list):
            md_request = self.flatten_list_of_lists(md_request)

            if len(md_request) == 1:
                md_request = md_request[0]

        # If we've got a list MarketDataRequest objects, use threading to 
        # independently call them
        if isinstance(md_request, list):
            if len(md_request) > 0:

                with concurrent.futures.ThreadPoolExecutor(
                        max_workers=md_request[0].list_threads) as executor:
                    df_list = list(executor.map(self.fetch_market, md_request))

                df_filtered_list = []

                for md, df in zip(md_request, df_list):

                    columns = []

                    for tick in md.tickers:
                        for fiel in md.fields:
                            columns.append(tick + "." + fiel)

                    if df is not None:
                        if df.empty:
                            df = pd.DataFrame(columns=columns)
                    else:
                        df = pd.DataFrame(columns=columns)

                    df_filtered_list.append(df)

                return self._calculations.join(df_filtered_list)
            else:
                return None

        key = md_request.generate_key()

        # If internet_load has been specified don't bother going to cache,
        # might end up calling lower level cache though through 
        # MarketDataGenerator)
        if 'cache_algo' in md_request.cache_algo and md_request.push_to_cache:
            data_frame = self.speed_cache.get_dataframe(key)

        if data_frame is not None:
            return data_frame

        if md_request.split_request_chunks > 0:
            md_request_list = []

            if md_request.vendor_tickers is not None:

                tickers = copy.copy(md_request.tickers)
                vendor_tickers = copy.copy(md_request.vendor_tickers)

                for t, v in zip(tickers, vendor_tickers):
                    md = MarketDataRequest(md_request=md_request)
                    md.tickers = t
                    md.vendor_tickers = v
                    md.split_request_chunks = 0

                    md_request_list.append(md)

            return self.fetch_market(md_request_list)

        # Special cases when a predefined category has been asked
        if md_request.category is not None:

            if (
                    md_request.category == 'fx-spot-volume' and 
                    md_request.data_source == 'quandl'):
                # NOT CURRENTLY IMPLEMENTED FOR FUTURE USE
                from findatapy.market.fxclsvolume import FXCLSVolume
                fxcls = FXCLSVolume(
                    market_data_generator=self._market_data_generator)

                data_frame = fxcls.get_fx_volume(
                    md_request.start_date, md_request.finish_date,
                    md_request.tickers, cut="LOC",
                    data_source="quandl",
                    cache_algo=md_request.cache_algo)

            # For FX we have special methods for returning cross rates or total 
            # returns
            if md_request.category in ['fx', 'fx-tot',
                                        'fx-tot-forwards'] \
                    and md_request.tickers is not None and \
                    md_request.abstract_curve is None:
                fxcf = FXCrossFactory(
                    market_data_generator=self._market_data_generator)

                if md_request.category == 'fx':
                    type = 'spot'
                elif md_request.category == 'fx-tot':
                    type = 'tot'

                elif md_request.category == 'fx-tot-forwards':
                    type = 'tot-forwards'

                if md_request.freq != 'tick' and (md_request.fields == [
                    'close'] or md_request.fields == ['open']) or \
                        (md_request.freq == 'tick'
                         and md_request.data_source in ['dukascopy', 'fxcm']):
                    data_frame = fxcf.get_fx_cross(
                        md_request.start_date,
                        md_request.finish_date,
                        md_request.tickers,
                        cut=md_request.cut,
                        data_source=md_request.data_source,
                        freq=md_request.freq,
                        cache_algo=md_request.cache_algo,
                        type=type,
                        environment=md_request.environment,
                        fields=md_request.fields,
                        data_engine=md_request.data_engine)

            # For FX implied volatility we can return the full surface
            if md_request.category == 'fx-implied-vol':
                if md_request.tickers is not None \
                        and md_request.freq == 'daily':
                    df = []

                    fxvf = FXVolFactory(
                        market_data_generator=self._market_data_generator)

                    for t in md_request.tickers:
                        if len(t) == 6:
                            df.append(
                                fxvf.get_fx_implied_vol(
                                    md_request.start_date,
                                    md_request.finish_date,
                                    t,
                                    md_request.fx_vol_tenor,
                                    cut=md_request.cut,
                                    data_source=md_request.data_source,
                                    part=md_request.fx_vol_part,
                                    cache_algo=md_request.cache_algo,
                                    environment=md_request.environment,
                                    field=md_request.fields,
                                    data_engine=md_request.data_engine))

                    if df != []:
                        data_frame = Calculations().join(df, how='outer')

            # For FX vol market return all the market data necessary for 
            # pricing options which includes FX spot, volatility surface, 
            # forward points, deposit rates
            if (md_request.category == 'fx-vol-market'):
                if md_request.tickers is not None:
                    df = []

                    fxcf = FXCrossFactory(
                        market_data_generator=self._market_data_generator)
                    fxvf = FXVolFactory(
                        market_data_generator=self._market_data_generator)
                    rates = RatesFactory(
                        market_data_generator=self._market_data_generator)

                    # For each FX cross fetch the spot, vol and forward points
                    for t in md_request.tickers:
                        if len(t) == 6:
                            # Spot
                            df.append(
                                fxcf.get_fx_cross(
                                    start=md_request.start_date,
                                    end=md_request.finish_date,
                                    cross=t,
                                    cut=md_request.cut,
                                    data_source=md_request.data_source,
                                    freq=md_request.freq,
                                    cache_algo=md_request.cache_algo,
                                    type='spot',
                                    environment=md_request.environment,
                                    fields=md_request.fields,
                                    data_engine=md_request.data_engine))

                            # Entire FX vol surface
                            df.append(
                                fxvf.get_fx_implied_vol(
                                    md_request.start_date,
                                    md_request.finish_date,
                                    t,
                                    md_request.fx_vol_tenor,
                                    cut=md_request.cut,
                                    data_source=md_request.data_source,
                                    part=md_request.fx_vol_part,
                                    cache_algo=md_request.cache_algo,
                                    environment=md_request.environment,
                                    field=md_request.fields,
                                    data_engine=md_request.data_engine))

                            # FX forward points for every point on curve
                            df.append(rates.get_fx_forward_points(
                                md_request.start_date, md_request.finish_date,
                                t,
                                md_request.fx_forwards_tenor,
                                cut=md_request.cut,
                                data_source=md_request.data_source,
                                environment=md_request.environment,
                                cache_algo=md_request.cache_algo,
                                field=md_request.fields,
                                data_engine=md_request.data_engine))

                    # Lastly fetch the base depos
                    df.append(rates.get_base_depos(
                        md_request.start_date,
                        md_request.finish_date,
                        self._get_base_depo_currencies(
                           md_request.tickers),
                        md_request.base_depos_tenor,
                        environment=md_request.environment,
                        cut=md_request.cut,
                        data_source=md_request.data_source,
                        cache_algo=md_request.cache_algo,
                        field=md_request.fields,
                        data_engine=md_request.data_engine))

                    if df != []:
                        data_frame = Calculations().join(df, how='outer')

            if md_request.category == 'fx-forwards-market':
                if md_request.tickers is not None:
                    df = []

                    fxcf = FXCrossFactory(
                        market_data_generator=self._market_data_generator)
                    rates = RatesFactory(
                        market_data_generator=self._market_data_generator)

                    # For each FX cross fetch the spot and forward points
                    for t in md_request.tickers:
                        if len(t) == 6:
                            # Spot
                            df.append(
                                fxcf.get_fx_cross(
                                    start=md_request.start_date,
                                    end=md_request.finish_date,
                                    cross=t,
                                    cut=md_request.cut,
                                    data_source=md_request.data_source,
                                    freq=md_request.freq,
                                    cache_algo=md_request.cache_algo,
                                    type='spot',
                                    environment=md_request.environment,
                                    fields=md_request.fields,
                                    data_engine=md_request.data_engine))

                            # FX forward points for every point on curve
                            df.append(rates.get_fx_forward_points(
                                md_request.start_date, md_request.finish_date,
                                t,
                                md_request.fx_forwards_tenor,
                                cut=md_request.cut,
                                data_source=md_request.data_source,
                                environment=md_request.environment,
                                cache_algo=md_request.cache_algo,
                                field=md_request.fields,
                                data_engine=md_request.data_engine))

                    # Lastly fetch the base depos
                    df.append(rates.get_base_depos(
                        md_request.start_date,
                        md_request.finish_date,
                        self._get_base_depo_currencies(
                           md_request.tickers),
                        md_request.base_depos_tenor,
                        cut=md_request.cut,
                        data_source=md_request.data_source,
                        cache_algo=md_request.cache_algo,
                        environment=md_request.environment,
                        field=md_request.fields,
                        data_engine=md_request.data_engine))

                    if df:
                        data_frame = Calculations().join(df, how='outer')

            # eg. for calculating total return indices from first principles (
            # rather than downloading them from a data vendor)
            if md_request.abstract_curve is not None:
                data_frame = md_request.abstract_curve.fetch_continuous_time_series \
                    (md_request, self._market_data_generator)

            if md_request.category == 'crypto':
                # Add more features later
                data_frame = self._market_data_generator.fetch_market_data(
                    md_request)

            # TODO add more special examples here for different asset classes
            # the idea is that we do all the market data downloading here,
            # rather than elsewhere

        # By default: pass the market data request to MarketDataGenerator
        # if data_frame is not None:
        #    data_frame = None

        if data_frame is None:
            data_frame = self._market_data_generator.fetch_market_data(
                md_request)

        # Special case where we can sometimes have duplicated data times
        if md_request.freq == 'intraday' and md_request.cut == 'BSTP':
            data_frame = self._filter.remove_duplicate_indices(data_frame)

        # Push into cache
        if md_request.push_to_cache:
            if data_frame is not None:
                self.speed_cache.put_dataframe(key, data_frame)

        return data_frame

    def create_md_request_from_dataframe(self, md_request_df, md_request=None,
                                         start_date=None, finish_date=None,
                                         smart_group=True,
                                         keep_initial_md_request_att_cols=[
                                             'tickers', 'fields', 'freq'],
                                         **kwargs):

        md_list = []

        # Aggregate/shrink dataframe grouping it by common attributes
        # tickers, vendor_tickers, fields, vendor_fields
        if smart_group:
            md_request_df = ConfigManager().get_instance()\
                .smart_group_dataframe_tickers(
                md_request_df, ret_fields=md_request_df.columns.tolist())

        # Now populate MarketDataRequests based on the DataFrame
        for index, row in md_request_df.iterrows():
            if md_request is None:
                md_request_copy = MarketDataRequest()
            else:
                md_request_copy = MarketDataRequest(md_request=md_request)

            for col, val in row.items():

                try:
                    if ',' in val:
                        val = val.split(',')
                except:
                    pass

                # Only override the initial setting in md_request if they are 
                # None, or the user has specified this
                if getattr(md_request_copy,
                           col) is None or col not \
                        in keep_initial_md_request_att_cols:
                    try:
                        if isinstance(val, list):
                            val = self.flatten_list_of_lists(val)

                        getattr(type(md_request_copy), col).fset(
                            md_request_copy, val)
                    except:
                        pass

            if start_date is not None: 
                md_request_copy.start_date = start_date
                
            if finish_date is not None: 
                md_request_copy.finish_date = finish_date

            md_request_copy = self._kwargs_to_md_request(kwargs,
                                                         md_request_copy)

            md_list.append(md_request_copy)

        return md_list

    def create_md_request_from_dict(self, md_request_dict, md_request=None,
                                    start_date=None, finish_date=None,
                                    **kwargs):

        if md_request is None:
            md_request = MarketDataRequest()

        for k in md_request_dict.keys():
            getattr(type(md_request), k).fset(md_request, md_request_dict[k])

        if start_date is not None: md_request.start_date = start_date
        if finish_date is not None: md_request.finish_date = finish_date

        md_request = self._kwargs_to_md_request(kwargs, md_request)

        return md_request

    def create_md_request_from_tickers(self, tickers, md_request=None,
                                       start_date=None, finish_date=None,
                                       best_match_only=False, smart_group=True,
                                       **kwargs):
        md_request_list = []

        if isinstance(tickers, str):
            tickers = [tickers]

        for t in tickers:
            md_request_copy = self.create_md_request_from_str(
                '_.' + t, md_request=md_request, start_date=start_date,
                finish_date=finish_date, best_match_only=best_match_only,
                smart_group=smart_group)

            md_request_copy = self._kwargs_to_md_request(kwargs,
                                                         md_request_copy)
            md_request_list.append(md_request_copy)

        return self.flatten_list_of_lists(md_request_list)

    def _kwargs_to_md_request(self, kw, md_request):

        if not (isinstance(md_request, list)):
            # Any kwargs are assumed to be to set MarketDataRequest attributes
            if kw != {}:

                if md_request is None:
                    md_request = MarketDataRequest()

                for k in kw.keys():
                    setattr(md_request, k, kw[k])

            return md_request

        # Any kwargs are assumed to be to set MarketDataRequest attributes
        if kw != {}:
            md_request_mod_list = []

            for md in md_request:
                if md is None:
                    md = MarketDataRequest()

                for k in kw.keys():
                    setattr(md, k, kw[k])

                md_request_mod_list.append(md)

            md_request = md_request_mod_list

        return md_request

    def create_md_request_from_str(self, md_request_str, md_request=None,
                                   start_date=None, finish_date=None,
                                   best_match_only=False,
                                   smart_group=True, **kwargs):

        json_md_request = None

        # Try to parse str as JSON if that fails, then try as a str
        try:
            json_md_request = json.loads(md_request_str)

            if md_request is None:
                md_request = MarketDataRequest()

            for k in json_md_request.keys():
                getattr(type(md_request), k).fset(md_request,
                                                  json_md_request[k])

            if start_date is not None: md_request.start_date = start_date
            if finish_date is not None: md_request.finish_date = finish_date

            return md_request
        except:
            pass

        # If we failed to parse as a JSON, let's try as string
        if json_md_request is None:

            # Split up ticker string
            md_request_params = ConfigManager.split_ticker_string(
                md_request_str)

            environment = md_request_params[0]

            # The user can omit the environment
            if environment in constants.possible_data_environment:
                i = 0

            # Otherwise, what if the user wants to specify each 
            # property manually?
            elif environment == 'raw' or environment == 'r':
                # Here the user can specify any tickers/fields etc. they want, 
                # they don't have to be predefined
                # eg. raw.data_source.bloomberg.tickers.EURUSD.vendor_tickers.EURUSD Curncy
                if md_request is None:
                    md_request = MarketDataRequest()

                freeform_md_request = {}

                for c in range(1, len(md_request_params), 2):
                    f = md_request_params[c + 1]

                    if ',' in f:
                        f = f.split(',')

                    freeform_md_request[md_request_params[c]] = f

                md_request.freeform_md_request = freeform_md_request

                if start_date is not None: 
                    md_request.start_date = start_date
                    
                if finish_date is not None: 
                    md_request.finish_date = finish_date

                md_request = self.create_md_request_from_freeform(md_request)
                md_request = self._kwargs_to_md_request(kwargs, md_request)

                return md_request

            # Otherwise we do a partial match of predefined tickers
            elif environment == "_":
                # Try a heuristic/approximate match eg. _.quandl.fx
                md_request_df = ConfigManager().get_instance()\
                    .free_form_tickers_query(
                    md_request_params[1:],
                    best_match_only=best_match_only,
                    smart_group=smart_group)

                md_request = self.create_md_request_from_dataframe(
                    md_request_df,
                    md_request=md_request, start_date=start_date,
                    finish_date=finish_date)

                md_request = self._kwargs_to_md_request(kwargs,
                                                           md_request)

                # if best_match_only:
                return md_request


            else:
                i = -1
                environment = None

            # Otherwise the user has specified the MarketDataRequest str in 
            # the form
            # category.data_source.freq.cut.tickers.field = fx.bloomberg.daily.NYC.EURUSD.close
            category = md_request_params[i + 1]
            data_source = md_request_params[i + 2]

            cut = None
            freq = None
            tickers = None
            fields = None

            # The freq, cut, tickers, fields are optional (in which case 
            # defaults will be used)
            try:
                freq = md_request_params[i + 3]
            except:
                pass

            try:
                cut = md_request_params[i + 4]
            except:
                pass

            # We can have multiple tickers and fields separately by a ticker
            try:
                tickers = md_request_params[i + 5]

                if ',' in tickers:
                    tickers = tickers.split(',')
            except:
                pass

            try:
                fields = md_request_params[i + 6]

                if ',' in fields:
                    fields = fields.split(',')
            except:
                pass

            if md_request is None:
                md_request = MarketDataRequest(category=category,
                                               data_source=data_source)
            else:
                md_request.category = category
                md_request.data_source = data_source

            if environment is not None: md_request.environment = environment
            if start_date is not None: md_request.start_date = start_date
            if finish_date is not None: md_request.finish_date = finish_date
            if freq is not None: md_request.freq = freq
            if cut is not None: md_request.cut = cut
            if tickers is not None: md_request.tickers = tickers
            if fields is not None: md_request.fields = fields

        return md_request

    def create_md_request_from_freeform(self, md_request,
                                        freeform_md_request=None,
                                        return_df=False, **kwargs):

        if freeform_md_request is None:
            freeform_md_request = md_request.freeform_md_request

        if freeform_md_request is None:
            return md_request

        if isinstance(freeform_md_request, pd.DataFrame):
            pass
        else:
            if not (isinstance(freeform_md_request, list)):
                freeform_md_request = [freeform_md_request]

            df = pd.DataFrame.from_dict(freeform_md_request)

        group_columns = [x for x in df.columns if
                         x not in ['tickers', 'vendor_tickers']]

        # Group by everything but the tickers, vendor_tickers (which are 
        # concatenated) and make sure we remove any duplicated tickers
        if group_columns:
            if 'tickers' in df.columns and 'vendor_tickers' not in df.columns:
                df = df.groupby(group_columns, as_index=False).agg(
                    {'tickers': list})

                for i in range(0, len(df.index)):
                    df.at[
                        i, 'tickers'] = self.remove_duplicates_and_flatten_list(
                        df.iloc[i]['tickers'])

            elif 'tickers' in df.columns and 'vendor_tickers' in df.columns:
                df = df.groupby(group_columns, as_index=False).agg(
                    {'tickers': list, 'vendor_tickers': list})

                for i in range(0, len(df.index)):
                    df.at[
                        i, 'tickers'] = self.remove_duplicates_and_flatten_list(
                        df.iloc[i]['tickers'])
                    df.at[
                        i, 'vendor_tickers'] = self.remove_duplicates_and_flatten_list(
                        df.iloc[i]['vendor_tickers'])

            freeform_md_request = df.to_dict('records')
        else:
            if 'tickers' in df.columns and 'vendor_tickers' not in df.columns:
                df['tickers'] = self.remove_duplicates_and_flatten_list(
                    df['tickers'].values.tolist())
                freeform_md_request = [
                    {'tickers': df['tickers'].values.tolist()}]
            elif 'tickers' in df.columns and 'vendor_tickers' in df.columns:
                df['tickers'] = self.remove_duplicates_and_flatten_list(
                    df['tickers'].values.tolist())
                df['vendor_tickers'] = self.remove_duplicates_and_flatten_list(
                    df['vendor_tickers'].values.tolist())
                freeform_md_request = [
                    {'tickers': df['tickers'].values.tolist(),
                     'vendor_tickers': df['vendor_tickers'].values.tolist()}]
        

        md_request_copy = MarketDataRequest(md_request=md_request)
        md_request_copy.freeform_md_request = None

        md_request_list = []

        for f in freeform_md_request:
            md_request_temp = MarketDataRequest(md_request=md_request_copy)

            for k in f.keys():
                lst = f[k]

                if isinstance(f[k], list):
                    # Make sure we don't have duplicated tickers requested (
                    # or indeed any other values)
                    lst = self.remove_duplicates_and_flatten_list(f[k])

                getattr(type(md_request_temp), k).fset(md_request_temp, lst)

            md_request_temp = self._kwargs_to_md_request(kwargs,
                                                         md_request_temp)

            md_request_list.append(md_request_temp)

        if len(md_request_list) == 1:
            md_request_list = md_request_list[0]

        if return_df:
            return md_request_list, df

        return md_request_list

    def remove_list_duplicates(self, lst):
        return list(dict.fromkeys(lst))

    def remove_duplicates_and_flatten_list(self, lst):
        return list(dict.fromkeys(self.flatten_list_of_lists(lst)))

    def flatten_list_of_lists(self, list_of_lists):
        """Flattens lists of obj, into a single list of strings (rather than 
        characters, which is default behavior).

        Parameters
        ----------
        list_of_lists : obj (list)
            List to be flattened

        Returns
        -------
        str (list)
        """

        if isinstance(list_of_lists, list):
            rt = []
            for i in list_of_lists:
                if isinstance(i, list):
                    rt.extend(self.flatten_list_of_lists(i))
                else:
                    rt.append(i)

            return rt

        return list_of_lists

    def _get_base_depo_currencies(self, cross):

        if not (isinstance(cross, list)):
            cross = [cross]

        base_depo_currencies = []

        for c in cross:
            base = c[0:3];
            terms = c[3:6]

            if base in constants.base_depos_currencies:
                base_depo_currencies.append(base)

            if terms in constants.base_depos_currencies:
                base_depo_currencies.append(terms)

        base_depo_currencies = list(set(base_depo_currencies))

        return base_depo_currencies


###############################################################################

from findatapy.util.fxconv import FXConv


class FXCrossFactory(object):
    """Generates FX spot time series and FX total return time series (assuming 
    we already have total return indices available from xxxUSD form) from 
    underlying series. Can also produce cross rates from the USD crosses.

    """

    def __init__(self, market_data_generator=None):
        self._fxconv = FXConv()

        self.cache = {}

        self._calculations = Calculations()
        self._market_data_generator = market_data_generator

        return

    def get_fx_cross_tick(self, start, end, cross,
                          cut="NYC", data_source="dukascopy",
                          cache_algo='internet_load_return', type='spot',
                          environment=constants.default_data_environment,
                          fields=['bid', 'ask'],
                          data_engine=constants.default_data_engine):

        if isinstance(cross, str):
            cross = [cross]

        market_data_request = MarketDataRequest(
            gran_freq="tick",
            freq_mult=1,
            freq='tick',
            cut=cut,
            fields=['bid', 'ask', 'bidv', 'askv'],
            cache_algo=cache_algo,
            environment=environment,
            start_date=start,
            finish_date=end,
            data_source=data_source,
            category='fx',
            data_engine=data_engine
        )

        market_data_generator = self._market_data_generator
        data_frame_agg = None

        for cr in cross:

            if (type == 'spot'):
                market_data_request.tickers = cr

                cross_vals = market_data_generator.fetch_market_data(
                    market_data_request)

                if cross_vals is not None:

                    # If user only wants 'close' calculate that from the
                    # bid/ask fields
                    if fields == ['close']:
                        cross_vals = pd.DataFrame(
                            cross_vals[[cr + '.bid', cr + '.ask']].mean(
                                axis=1))
                        cross_vals.columns = [cr + '.close']
                    else:
                        filter = Filter()

                        filter_columns = [cr + '.' + f for f in fields]
                        cross_vals = filter.filter_time_series_by_columns(
                            filter_columns, cross_vals)

            if data_frame_agg is None:
                data_frame_agg = cross_vals
            else:
                data_frame_agg = data_frame_agg.join(cross_vals, how='outer')

        if data_frame_agg is not None:
            # Strip the nan elements
            data_frame_agg = data_frame_agg.dropna()

        return data_frame_agg

    def get_fx_cross(self, start, end, cross,
                     cut="NYC", data_source="bloomberg", freq="intraday",
                     cache_algo='internet_load_return',
                     type='spot',
                     environment=constants.default_data_environment,
                     fields=['close'],
                     data_engine=constants.default_data_engine):

        if data_source == "gain" or data_source == 'dukascopy' \
                or freq == 'tick':
            return self.get_fx_cross_tick(start, end, cross,
                                          cut=cut, data_source=data_source,
                                          cache_algo=cache_algo, type='spot',
                                          environment=environment,
                                          fields=fields,
                                          data_engine=data_engine)

        if isinstance(cross, str):
            cross = [cross]

        market_data_request_list = []
        freq_list = []
        type_list = []

        for cr in cross:
            market_data_request = MarketDataRequest(freq_mult=1,
                                                    cut=cut,
                                                    fields=fields,
                                                    freq=freq,
                                                    cache_algo=cache_algo,
                                                    start_date=start,
                                                    finish_date=end,
                                                    data_source=data_source,
                                                    environment=environment,
                                                    data_engine=data_engine)

            market_data_request.type = type
            market_data_request.cross = cr

            if freq == 'intraday':
                market_data_request.gran_freq = "minute"  # intraday

            elif freq == 'daily':
                market_data_request.gran_freq = "daily"  # daily

            market_data_request_list.append(market_data_request)

        data_frame_agg = []

        # Depends on the nature of operation as to whether we should use
        # threading or multiprocessing library
        if constants.market_thread_technique == "thread":
            from multiprocessing.dummy import Pool
        else:
            # Most of the time is spend waiting for Bloomberg to return, so can
            # use threads rather than multiprocessing must use the multiprocess
            # library otherwise can't pickle objects correctly
            # note: currently not very stable
            from multiprocess import Pool

        # For time being do not use multithreading
        # thread_no = constants.market_thread_no['other']
        #
        # if market_data_request_list[
        #     0].data_source in constants.market_thread_no:
        #     thread_no = constants.market_thread_no[
        #         market_data_request_list[0].data_source]

        thread_no = 0

        if thread_no > 0:
            pool = Pool(thread_no)

            # Open the market data downloads in their own threads and return
            # the results
            df_list = pool.map_async(self._get_individual_fx_cross,
                                     market_data_request_list).get()

            data_frame_agg = self._calculations.join(df_list, how='outer')

            # data_frame_agg = self._calculations.pandas_outer_join(result.get())

            try:
                pool.close()
                pool.join()
            except:
                pass
        else:
            for md_request in market_data_request_list:
                data_frame_agg.append(
                    self._get_individual_fx_cross(md_request))

            data_frame_agg = self._calculations.join(data_frame_agg,
                                                     how='outer')

        # Strip the nan elements
        data_frame_agg = data_frame_agg.dropna(how='all')

        # self.speed_cache.put_dataframe(key, data_frame_agg)

        return data_frame_agg

    def _get_individual_fx_cross(self, market_data_request):
        cr = market_data_request.cross
        type = market_data_request.type
        freq = market_data_request.freq

        base = cr[0:3]
        terms = cr[3:6]

        if (type == 'spot'):
            # Non-USD crosses
            if base != 'USD' and terms != 'USD':
                base_USD = self._fxconv.correct_notation('USD' + base)
                terms_USD = self._fxconv.correct_notation('USD' + terms)

                # TODO check if the cross exists in the database

                # Download base USD cross
                market_data_request.tickers = base_USD
                market_data_request.category = 'fx'

                base_vals = self._market_data_generator.fetch_market_data(
                    market_data_request)

                # Download terms USD cross
                market_data_request.tickers = terms_USD
                market_data_request.category = 'fx'

                terms_vals = self._market_data_generator.fetch_market_data(
                    market_data_request)

                # If quoted USD/base flip to get USD terms
                if base_USD[0:3] == 'USD':
                    base_vals = 1 / base_vals

                # If quoted USD/terms flip to get USD terms
                if terms_USD[0:3] == 'USD':
                    terms_vals = 1 / terms_vals

                base_vals.columns = ['temp'];
                terms_vals.columns = ['temp']

                cross_vals = base_vals.div(terms_vals, axis='index')
                cross_vals.columns = [cr + '.' + market_data_request.fields[0]]

                base_vals.columns = [
                    base_USD + '.' + market_data_request.fields[0]]
                terms_vals.columns = [
                    terms_USD + '.' + market_data_request.fields[0]]
            else:
                # if base == 'USD': non_USD = terms
                # if terms == 'USD': non_USD = base

                correct_cr = self._fxconv.correct_notation(cr)

                market_data_request.tickers = correct_cr
                market_data_request.category = 'fx'

                cross_vals = self._market_data_generator.fetch_market_data(
                    market_data_request)

                # Special case for USDUSD!
                if base + terms == 'USDUSD':
                    if freq == 'daily':
                        cross_vals = pd.DataFrame(1, index=cross_vals.index,
                                                  columns=cross_vals.columns)
                        filter = Filter()
                        cross_vals = filter.filter_time_series_by_holidays(
                            cross_vals, cal='WEEKDAY')
                else:
                    # Flip if not convention (eg. JPYUSD)
                    if (correct_cr != cr):
                        cross_vals = 1 / cross_vals

                cross_vals.columns = [cr + '.' + market_data_request.fields[0]]

        elif type[0:3] == "tot":
            if freq == 'daily':
                # Download base USD cross
                market_data_request.tickers = base + 'USD'
                market_data_request.category = 'fx-' + type

                if type[0:3] == "tot":
                    base_vals = self._market_data_generator.fetch_market_data(
                        market_data_request)

                # Download terms USD cross
                market_data_request.tickers = terms + 'USD'
                market_data_request.category = 'fx-' + type

                if type[0:3] == "tot":
                    terms_vals = self._market_data_generator.fetch_market_data(
                        market_data_request)

                # base_rets = self._calculations.calculate_returns(base_vals)
                # terms_rets = self._calculations.calculate_returns(terms_vals)

                # Special case for USDUSD case (and if base or terms USD are
                # USDUSD
                if base + terms == 'USDUSD':
                    base_rets = self._calculations.calculate_returns(base_vals)
                    cross_rets = pd.DataFrame(0, index=base_rets.index,
                                              columns=base_rets.columns)
                elif base + 'USD' == 'USDUSD':
                    cross_rets = -self._calculations.calculate_returns(
                        terms_vals)
                elif terms + 'USD' == 'USDUSD':
                    cross_rets = self._calculations.calculate_returns(
                        base_vals)
                else:
                    base_rets = self._calculations.calculate_returns(base_vals)
                    terms_rets = self._calculations.calculate_returns(
                        terms_vals)

                    cross_rets = base_rets.sub(terms_rets.iloc[:, 0], axis=0)

                # First returns of a time series will by NaN, given we don't
                # know previous point
                cross_rets.iloc[0] = 0

                cross_vals = self._calculations.create_mult_index(cross_rets)
                cross_vals.columns = [
                    cr + '-' + type + '.' + market_data_request.fields[0]]

            elif freq == 'intraday':
                LoggerManager().getLogger(__name__).info(
                    'Total calculated returns for intraday not implemented yet')
                return None

        return cross_vals


###############################################################################

import pandas as pd

from findatapy.market.marketdatarequest import MarketDataRequest
from findatapy.util import LoggerManager
from findatapy.timeseries import Calculations, Filter, Timezone


class FXVolFactory(object):
    """Generates FX implied volatility time series and surfaces (using very
    simple interpolation!) and only in delta space.

    """

    def __init__(self, market_data_generator=None):

        self._market_data_generator = market_data_generator

        self._calculations = Calculations()
        self._filter = Filter()
        self._timezone = Timezone()

        self._rates = RatesFactory()

        return

    def get_fx_implied_vol(self, start, end, cross, tenor, cut="BGN",
                           data_source="bloomberg", part="V",
                           cache_algo="internet_load_return",
                           environment=constants.default_data_environment,
                           field='close',
                           data_engine=constants.default_data_engine):
        """Get implied vol for specified cross, tenor and part of surface.
        By default we use Bloomberg, but we could use any data provider for
        which we have vol tickers.

        Note, that for Bloomberg not every point will be quoted for each
        dataset (typically, BGN will have more points than for example LDN)

        Parameters
        ----------
        start : datetime
            start date of request
        end : datetime
            end date of request
        cross : str
            FX cross
        tenor : str
            tenor of implied vol
        cut : str
            closing time of data
        data_source : str
            data_source of market data eg. bloomberg
        part : str
            part of vol surface eg. V for ATM implied vol, 25R 25 delta
            risk reversal

        Return
        ------
        pd.DataFrame
        """

        market_data_generator = self._market_data_generator

        if tenor is None:
            tenor = constants.fx_vol_tenor

        if part is None:
            part = constants.fx_vol_part

        tickers = self.get_labels(cross, part, tenor)

        market_data_request = MarketDataRequest(
            start_date=start, finish_date=end,
            data_source=data_source,
            category='fx-implied-vol',
            freq='daily',
            cut=cut,
            tickers=tickers,
            fields=field,
            cache_algo=cache_algo,
            environment=environment,
            data_engine=data_engine
        )

        data_frame = market_data_generator.fetch_market_data(
            market_data_request)
        # data_frame.index.name = 'Date'

        # Special case for 10AM NYC cut
        # - get some historical 10AM NYC data (only available on BBG for a
        # few years, before 2007)
        # - fill the rest with a weighted average of TOK/LDN closes
        if cut == "10AM":
            # Where we have actual 10am NY data use that & overwrite earlier
            # estimated data (next)
            vol_data_10am = data_frame

            # As for most dates we probably won't have 10am data, so drop rows
            # where there's no data at all
            # Can have the situation where some data won't be there (eg. longer
            # dated illiquid tenors)
            if vol_data_10am is not None:
                vol_data_10am = vol_data_10am.dropna(
                    how='all')  # Only have limited ON 10am cut data

            # Now get LDN and TOK vol data to fill any gaps
            vol_data_LDN = self.get_fx_implied_vol(start=start, end=end,
                                                   cross=cross, tenor=tenor,
                                                   data_source=data_source,
                                                   cut='LDN', part=part,
                                                   cache_algo=cache_algo,
                                                   field=field,
                                                   data_engine=data_engine)

            vol_data_TOK = self.get_fx_implied_vol(start=start, end=end,
                                                   cross=cross, tenor=tenor,
                                                   data_source=data_source,
                                                   cut='TOK', part=part,
                                                   cache_algo=cache_algo,
                                                   field=field,
                                                   data_engine=data_engine)

            # vol_data_LDN.index = pandas.DatetimeIndex(vol_data_LDN.index)
            # vol_data_TOK.index = pandas.DatetimeIndex(vol_data_TOK.index)

            old_cols = vol_data_LDN.columns

            vol_data_LDN.columns = vol_data_LDN.columns.values + "LDN"
            vol_data_TOK.columns = vol_data_TOK.columns.values + "TOK"

            data_frame = vol_data_LDN.join(vol_data_TOK, how='outer')

            # Create very naive average of LDN and TOK to estimate 10am NY
            # value because we often don't have this data
            # Note, this isn't perfect, particularly on days where you have
            # payrolls data, and we're looking at ON data
            # You might choose to create your own approximation for 10am NY
            for col in old_cols:
                data_frame[col] = (1 * data_frame[col + "LDN"] + 3 *
                                   data_frame[col + "TOK"]) / 4
                # data_frame[col] = data_frame[col + "LDN"]
                data_frame.pop(col + "LDN")
                data_frame.pop(col + "TOK")

            # Get TOK/LDN vol data before 10am and after 10am (10am data is
            # only available for a few years)
            # If we have no original 10am data don't bother
            if vol_data_10am is not None:
                if not (vol_data_10am.empty):
                    pre_vol_data = data_frame[
                        data_frame.index < vol_data_10am.index[0]]
                    post_vol_data = data_frame[
                        data_frame.index > vol_data_10am.index[-1]]

                    data_frame = (pre_vol_data.append(vol_data_10am)).append(
                        post_vol_data)

            # data_frame.index = pandas.to_datetime(data_frame.index)

        return data_frame

    def get_labels(self, cross, part, tenor):
        if isinstance(cross, str): cross = [cross]
        if isinstance(tenor, str): tenor = [tenor]
        if isinstance(part, str): part = [part]

        tickers = []

        for cr in cross:
            for tn in tenor:
                for pt in part:
                    tickers.append(cr + pt + tn)

        return tickers

    def extract_vol_surface_for_date(self, df, cross, date_index,
                                     delta=constants.fx_vol_delta,
                                     tenor=constants.fx_vol_tenor,
                                     field='close'):
        """Get's the vol surface in delta space without any interpolation

        Parameters
        ----------
        df : DataFrame
            With vol data
        cross : str
            Currency pair
        date_index : int
            Which date to extract
        delta : list(int)
            Deltas which are quoted in order of out-of-money -> in-the-money
            (eg. [10, 25])
        tenor : list(str)
            Tenors which are quoted (eg. ["ON", "1W"...]

        Returns
        -------
        DataFrame
        """

        # Assume we have a matrix of the form
        # eg. EURUSDVON.close ...

        # types of quotation on vol surface
        # self.part = ["V", "25R", "10R", "25B", "10B"]

        # all the tenors on our vol surface
        # self.tenor = ["ON", "1W", "2W", "3W", "1M", "2M", "3M", "6M", "9M",
        # "1Y", "2Y", "5Y"]

        strikes = []

        for d in delta:
            strikes.append(str(d) + "DP")

        strikes.append('ATM')

        for d in delta:
            strikes.append(str(d) + "DC")

        df_surf = pd.DataFrame(index=strikes, columns=tenor)

        for ten in tenor:
            for d in delta:
                df_surf[ten][str(d) + "DP"] = \
                df[cross + "V" + ten + "." + field][date_index] \
                - (df[cross + str(d) + "R" + ten + "." + field][
                       date_index] / 2.0) \
                + (df[cross + str(d) + "B" + ten + "." + field][date_index])

                df_surf[ten][str(d) + "DC"] = \
                df[cross + "V" + ten + "." + field][date_index] \
                + (df[cross + str(d) + "R" + ten + "." + field][
                       date_index] / 2.0) \
                + (df[cross + str(d) + "B" + ten + "." + field][date_index])

            df_surf[ten]["ATM"] = df[cross + "V" + ten + "." + field][
                date_index]

        return df_surf


###############################################################################

class RatesFactory(object):
    """Gets the deposit rates for a particular currency (or forwards for a
    currency pair)

    """

    def __init__(self, market_data_generator=None):

        self.cache = {}

        self._calculations = Calculations()
        self._market_data_generator = market_data_generator

        return

    def get_base_depos(self, start, end, currencies, tenor, cut="NYC",
                       data_source="bloomberg",
                       cache_algo="internet_load_return", field='close',
                       environment=constants.default_data_environment,
                       data_engine=constants.default_data_engine):
        """Gets the deposit rates for a particular tenor and part of surface

        Parameter
        ---------
        start : DateTime
            Start date
        end : DateTime
            End data
        currencies : str
            Currencies for which we want to download deposit rates
        tenor : str
            Tenor of deposit rate
        cut : str
            Closing time of the market data
        data_source : str
            data_source of the market data eg. bloomberg
        cache_algo : str
            Caching scheme for the data

        Returns
        -------
        pd.DataFrame
            Contains deposit rates
        """

        market_data_generator = self._market_data_generator

        if tenor is None:
            tenor = constants.base_depos_tenor

        if isinstance(currencies, str): currencies = [currencies]
        if isinstance(tenor, str): tenor = [tenor]

        tickers = []

        for cr in currencies:

            for tn in tenor:
                tickers.append(cr + tn)

        # Special case for Fed Funds Effective Rate which we add in all
        # instances
        if 'USDFedEffectiveRate' not in tickers:
            tickers.append("USDFedEffectiveRate")

        # For depos there usually isn't a 10AM NYC cut available, so just use
        # TOK data
        # Also no BGN tends to available for deposits, so use NYC
        if cut == '10AM':
            cut = 'TOK'
        elif cut == 'BGN':
            cut = 'NYC'

        market_data_request = MarketDataRequest(
            start_date=start, finish_date=end,
            data_source=data_source,
            category='base-depos',
            freq='daily',
            cut=cut,
            tickers=tickers,
            fields=field,
            cache_algo=cache_algo,
            environment=environment,
            data_engine=data_engine
        )

        data_frame = market_data_generator.fetch_market_data(
            market_data_request)
        data_frame.index.name = 'Date'

        return data_frame

    def get_fx_forward_points(self, start, end, cross, tenor, cut="BGN",
                              data_source="bloomberg",
                              cache_algo="internet_load_return", field='close',
                              environment=constants.default_data_environment,
                              data_engine=constants.default_data_engine):
        """Gets the forward points for a particular tenor and currency

        Parameter
        ---------
        start : Datetime
            Start date
        end : Datetime
            End data
        cross : str
            FX crosses for which we want to download forward points
        tenor : str
            Tenor of deposit rate
        cut : str
            Closing time of the market data
        data_source : str
            data_source of the market data eg. bloomberg
        cache_algo : str
            Caching scheme for the data

        Returns
        -------
        pd.DataFrame
        Contains deposit rates
        """

        # md_request = MarketDataRequest()
        market_data_generator = self._market_data_generator

        # md_request.data_source = data_source  # use bbg as a data_source
        # md_request.start_date = start  # start_date
        # md_request.finish_date = end  # finish_date

        if tenor is None:
            tenor = constants.fx_forwards_tenor

        if isinstance(cross, str): cross = [cross]
        if isinstance(tenor, str): tenor = [tenor]

        # Tickers are often different on Bloomberg for forwards/depos vs vol,
        # so want consistency so 12M is always 1Y
        tenor = [x.replace('1Y', '12M') for x in tenor]

        tickers = []

        for cr in cross:
            for tn in tenor:
                tickers.append(cr + tn)

        market_data_request = MarketDataRequest(
            start_date=start, finish_date=end,
            data_source=data_source,
            category='fx-forwards',
            freq='daily',
            cut=cut,
            tickers=tickers,
            fields=field,
            cache_algo=cache_algo,
            environment=environment,
            data_engine=data_engine
        )

        data_frame = market_data_generator.fetch_market_data(
            market_data_request)
        data_frame.columns = [x.replace('12M', '1Y') for x in
                              data_frame.columns]
        data_frame.index.name = 'Date'

        return data_frame
