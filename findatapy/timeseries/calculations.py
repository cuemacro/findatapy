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

import datetime
import functools
import math

import numpy
import pandas as pd
import pandas.tseries.offsets

try:
    from pandas.stats.api import ols
except:
    # temporary fix to get compilation, need to rewrite regression code to get
    # this to work
    # later versions of pandas no longer support OLS
    #
    # fails with SciPy 1.3.0 unless we have the very latest version of
    # StatsModels pip install statsmodels==0.10.0rc2 --pre
    #
    try:
        from statsmodels.formula.api import ols
    except:
        pass

from findatapy.timeseries import Filter, Calendar, Timezone
from findatapy.util.dataconstants import DataConstants
from findatapy.util.loggermanager import LoggerManager

from pandas import compat
import numpy as np

import copy
from datetime import timedelta

constants = DataConstants()


class Calculations(object):
    """Calculations on time series, such as calculating strategy returns and
    various wrappers on pd for rolling sums etc.

    """

    ##### calculate

    def calculate_signal_tc(self, signal_data_frame, tc, period_shift=1):
        """Calculates the transaction costs for a particular signal

        Parameters
        ----------
        signal_data_frame : DataFrame
            contains trading signals
        tc : float
            transaction costs
        period_shift : int
            number of periods to shift signal

        Returns
        -------
        DataFrame
        """
        return (signal_data_frame.shift(
            period_shift) - signal_data_frame).abs().multiply(tc)

    def calculate_entry_tc(self, entry_data_frame, tc, period_shift=1):
        """Calculates the transaction costs for defined trading points

        Parameters
        ----------
        entry_data_frame : DataFrame
            contains points where we enter/exit trades
        tc : float
            transaction costs
        period_shift : int
            number of periods to shift signal

        Returns
        -------
        DataFrame
        """
        return entry_data_frame.abs().multiply(tc)

    def calculate_signal_returns(self, signal_data_frame, returns_data_frame,
                                 period_shift=1):
        """Calculates the trading startegy returns for given signal and asset

        Parameters
        ----------
        signal_data_frame : DataFrame
            trading signals
        returns_data_frame: DataFrame
            returns of asset to be traded
        period_shift : int
            number of periods to shift signal

        Returns
        -------
        DataFrame
        """

        # can cause issues, if the names of the columns are not identical
        return signal_data_frame.shift(period_shift) * returns_data_frame

    def calculate_signal_returns_as_matrix(self, signal_data_frame,
                                           returns_data_frame, period_shift=1):

        return pd.DataFrame(
            signal_data_frame.shift(
                period_shift).values * returns_data_frame.values,
            index=returns_data_frame.index,
            columns=returns_data_frame.columns)

    def calculate_individual_trade_gains(self, signal_data_frame,
                                         strategy_returns_data_frame):
        """Calculates profits on every trade (experimental code)

        Parameters
        ----------
        signal_data_frame : DataFrame
            trading signals
        strategy_returns_data_frame: DataFrame
            returns of strategy to be tested

        Returns
        -------
        DataFrame contains the P&L for every trade
        """

        # signal need to be aligned to NEXT period for returns
        # signal_data_frame_pushed = signal_data_frame.shift(1)

        # find all the trade points
        trade_points = ((signal_data_frame - signal_data_frame.shift(1)).abs())
        cumulative = self.create_mult_index(strategy_returns_data_frame)

        indices = trade_points > 0
        indices.columns = cumulative.columns

        # get P&L for every trade (from the end point - start point)
        trade_returns = numpy.nan * cumulative
        trade_points_cumulative = cumulative[indices]

        # for each set of signals/returns, calculate the trade returns - where there isn't a trade
        # assign a NaN
        # TODO do in one vectorised step without for loop
        for col_name in trade_points_cumulative:
            col = trade_points_cumulative[col_name]
            col = col.dropna()
            col = col / col.shift(1) - 1

            # TODO experiment with quicker ways of writing below?
            # for val in col.index:
            # trade_returns.set_value(val, col_name, col[val])
            # trade_returns[col_name][val] = col[val]

            date_indices = trade_returns.index.searchsorted(col.index)
            trade_returns[col_name][date_indices] = col

        return trade_returns

    def calculate_cum_rets_trades(self, signal_data_frame,
                                  strategy_returns_data_frame):
        """Calculates cumulative returns resetting at each new trade

        Parameters
        ----------
        signal_data_frame : DataFrame
            trading signals
        strategy_returns_data_frame: DataFrame
            returns of strategy to be tested
        period_shift : int
            number of periods to shift signal

        Returns
        -------
        DataFrame
        """

        # Signal need to be aligned to NEXT period for returns
        signal_data_frame_pushed = signal_data_frame.shift(1)

        # Find all the trade points
        reset_points = ((
                                    signal_data_frame_pushed - signal_data_frame_pushed.shift(
                                1)).abs())

        reset_points = reset_points.cumsum()

        # Make sure they have the same column names (otherwise issues around
        # pd calc - assume same ordering for cols)
        old_cols = strategy_returns_data_frame.columns
        strategy_returns_data_frame.columns = signal_data_frame_pushed.columns

        for c in reset_points.columns:
            strategy_returns_data_frame[c + 'cumsum'] = reset_points[c]
            strategy_returns_data_frame[c] = \
            strategy_returns_data_frame.groupby([c + 'cumsum'])[c].cumsum()
            strategy_returns_data_frame = strategy_returns_data_frame.drop(
                [c + 'cumsum'], axis=1)

        strategy_returns_data_frame.columns = old_cols

        return strategy_returns_data_frame

    def calculate_trade_no(self, signal_data_frame):

        ####### how many trades have there been (ignore size of the trades)
        trades = abs(signal_data_frame - signal_data_frame.shift(-1))
        trades = trades[trades > 0].count()

        signal_data_frame = pd.DataFrame(index=trades.index,
                                         columns=['Trades'], data=trades)

        return signal_data_frame

    def calculate_trade_duration(self, signal_data_frame):
        """Calculates cumulative trade durations

        Parameters
        ----------
        signal_data_frame : DataFrame
            trading signals

        Returns
        -------
        DataFrame
        """

        # TODO
        # # signal need to be aligned to NEXT period for returns
        # signal_data_frame_pushed = signal_data_frame.shift(1)
        #
        # # find all the trade points
        # reset_points = ((signal_data_frame_pushed - signal_data_frame_pushed.shift(1)).abs())
        #
        # reset_points = reset_points.cumsum()
        #
        # time_data_frame = pd.DataFrame(index = signal_data_frame.index, columns = signal_data_frame.columns,
        #                                    data=numpy.ones([len(signal_data_frame.index), len(signal_data_frame.columns)]))
        #
        # # make sure they have the same column names (otherwise issues around pd calc - assume same ordering for cols)
        # old_cols = time_data_frame.columns
        # time_data_frame.columns = signal_data_frame_pushed.columns
        #
        # for c in reset_points.columns:
        #     time_data_frame[c + 'cumperiods'] = reset_points[c]
        #     time_data_frame[c] = time_data_frame.groupby([c + 'cumperiods'])[c].cumsum()
        #     time_data_frame = time_data_frame.drop([c + 'cumperiods'], axis=1)
        #
        # time_data_frame.columns = old_cols
        #
        # return time_data_frame

    def calculate_final_trade_duration(self, signal_data_frame):
        """Calculates cumulative trade durations

        Parameters
        ----------
        signal_data_frame : DataFrame
            trading signals

        Returns
        -------
        DataFrame
        """

        # Signal need to be aligned to NEXT period for returns
        signal_data_frame_pushed = signal_data_frame.shift(1)

        # Find all the trade points
        reset_points = ((
                                    signal_data_frame_pushed - signal_data_frame_pushed.shift(
                                1)).abs())

        reset_points = reset_points.cumsum()

        time_data_frame = pd.DataFrame(index=signal_data_frame.index,
                                       columns=signal_data_frame.columns,
                                       data=numpy.ones(
                                           [len(signal_data_frame.index),
                                            len(signal_data_frame.columns)]))

        # Make sure they have the same column names (otherwise issues around pd calc - assume same ordering for cols)
        old_cols = time_data_frame.columns
        time_data_frame.columns = signal_data_frame_pushed.columns

        for c in reset_points.columns:
            time_data_frame[c + 'cumperiods'] = reset_points[c]
            time_data_frame[c] = time_data_frame.groupby([c + 'cumperiods'])[
                c].cumsum()
            time_data_frame = time_data_frame.drop([c + 'cumperiods'], axis=1)

        time_data_frame.columns = old_cols

        return time_data_frame

    def calculate_risk_stop_signals(self, signal_data_frame, cum_rets_trades,
                                    stop_loss, take_profit):
        """

        Parameters
        ----------
        signal_data_frame : DataFrame
            Contains all the trade signals (typically mix of 0, +1 and +1

        cum_rets_trades : DataFrame
            Cumulative returns of strategy reset at every new trade

        stop_loss : float (or DataFrame)
            Stop loss level eg. -0.02

        take_profit : float (or DataFrame)
            Take profit level eg. +0.03

        Returns
        -------
        DataFrame containing amended signals that take into account stops and take profits

        """

        signal_data_frame_pushed = signal_data_frame  # signal_data_frame.shift(1)
        reset_points = ((
                                    signal_data_frame_pushed - signal_data_frame_pushed.shift(
                                1)).abs())

        ind = (cum_rets_trades > take_profit) | (cum_rets_trades < stop_loss)

        # to allow indexing, need to match column names
        ind.columns = signal_data_frame.columns
        signal_data_frame[ind] = 0

        reset_points[ind] = 1

        signal_data_frame[reset_points == 0] = numpy.nan
        signal_data_frame = signal_data_frame.ffill()
        # signal_data_frame = signal_data_frame.shift(-1)

        return signal_data_frame

    def calculate_risk_stop_dynamic_signals(self, signal_data_frame,
                                            asset_data_frame, stop_loss_df,
                                            take_profit_df):
        """

        Parameters
        ----------
        signal_data_frame : DataFrame
            Contains all the trade signals (typically mix of 0, +1 and +1

        stop_loss_df : DataFrame
            Continuous stop losses in the asset (in price amounts eg +2, +2.5, +2.6 USD - as opposed to percentages)

        take_profit_df : DataFrame
            Continuous take profits in the asset (in price amounts eg -2, -2.1, -2.5 USD - as opposed to percentages)

        Returns
        -------
        DataFrame containing amended signals that take into account stops and take profits

        """

        signal_data_frame_pushed = signal_data_frame  # signal_data_frame.shift(1)
        reset_points = ((
                                    signal_data_frame_pushed - signal_data_frame_pushed.shift(
                                1)).abs())

        # ensure all the inputs are pd DataFrames (rather than mixture of Series and DataFrames)
        asset_data_frame = pd.DataFrame(asset_data_frame)
        signal_data_frame = pd.DataFrame(signal_data_frame)
        stop_loss_df = pd.DataFrame(stop_loss_df)
        take_profit_df = pd.DataFrame(take_profit_df)

        non_trades = reset_points == 0

        # need to temporarily change column names to allow indexing (ASSUMES: columns in same order!)
        non_trades.columns = take_profit_df.columns

        # where we don't have a trade fill with NaNs
        take_profit_df[non_trades] = numpy.nan

        non_trades.columns = stop_loss_df.columns
        stop_loss_df[non_trades] = numpy.nan

        asset_df_copy = asset_data_frame.copy(deep=True)

        non_trades.columns = asset_df_copy.columns
        asset_df_copy[non_trades] = numpy.nan

        take_profit_df = take_profit_df.ffill()
        stop_loss_df = stop_loss_df.ffill()
        asset_df_copy = asset_df_copy.ffill()

        # take profit for buys
        ind1 = (asset_data_frame.values > (
                    asset_df_copy.values + take_profit_df.values)) & (
                       signal_data_frame.values > 0)

        # take profit for sells
        ind2 = (asset_data_frame.values < (
                    asset_df_copy.values - take_profit_df.values)) & (
                       signal_data_frame.values < 0)

        # stop loss for buys
        ind3 = (asset_data_frame.values < (
                    asset_df_copy.values + stop_loss_df.values)) & (
                           signal_data_frame.values > 0)

        # stop loss for sells
        ind4 = (asset_data_frame.values > (
                    asset_df_copy.values - stop_loss_df.values)) & (
                           signal_data_frame.values < 0)

        # when has there been a stop loss or take profit? assign those as being flat points
        ind = ind1 | ind2 | ind3 | ind4

        ind = pd.DataFrame(data=ind, columns=signal_data_frame.columns,
                           index=signal_data_frame.index)

        # for debugging
        # sum_ind = (ind == True).sum(); print(sum_ind)

        signal_data_frame[ind] = 0

        # those places where we have been stopped out/taken profit are additional trade "reset points", which we need to define
        # (already have ordinary buy/sell trades defined)
        reset_points[ind] = 1

        # where we don't have trade make these NaN and then fill down
        signal_data_frame[reset_points == 0] = numpy.nan
        signal_data_frame = signal_data_frame.ffill()

        return signal_data_frame

    # TODO
    def calculate_risk_stop_defined_signals(self, signal_data_frame,
                                            stops_data_frame):
        """

        Parameters
        ----------
        signal_data_frame : DataFrame
            Contains all the trade signals (typically mix of 0, +1 and +1

        stops_data_frame : DataFrame
            Contains 1/-1 to indicate where trades would be stopped out

        Returns
        -------
        DataFrame containing amended signals that take into account stops and take profits

        """

        signal_data_frame_pushed = signal_data_frame  # signal_data_frame.shift(1)
        reset_points = ((
                                    signal_data_frame_pushed - signal_data_frame_pushed.shift(
                                1)).abs())

        stops_data_frame = stops_data_frame.abs()
        ind = stops_data_frame >= 1

        ind.columns = signal_data_frame.columns
        signal_data_frame[ind] = 0

        reset_points[ind] = 1

        signal_data_frame[reset_points == 0] = numpy.nan
        signal_data_frame = signal_data_frame.ffill()

        return signal_data_frame

    def calculate_signal_returns_matrix(self, signal_data_frame,
                                        returns_data_frame, period_shift=1):
        """Calculates the trading strategy returns for given signal and asset
        as a matrix multiplication

        Parameters
        ----------
        signal_data_frame : DataFrame
            trading signals

        returns_data_frame: DataFrame
            returns of asset to be traded

        period_shift : int
            number of periods to shift signal

        Returns
        -------
        DataFrame
        """
        return pd.DataFrame(
            signal_data_frame.shift(
                period_shift).values * returns_data_frame.values,
            index=returns_data_frame.index)

    def calculate_signal_returns_with_tc(self, signal_data_frame,
                                         returns_data_frame, tc,
                                         period_shift=1):
        """Calculates the trading startegy returns for given signal and asset returns including
        transaction costs

        Parameters
        ----------
        signal_data_frame : DataFrame
            trading signals

        returns_data_frame: DataFrame
            returns of asset to be traded

        tc : float
            transaction costs

        period_shift : int
            number of periods to shift signal

        Returns
        -------
        DataFrame
        """

        tc_costs = self.calculate_signal_tc(signal_data_frame, tc,
                                            period_shift)

        return signal_data_frame.shift(
            period_shift) * returns_data_frame - tc_costs

    def calculate_signal_returns_with_tc_from_prices(self, signal_data_frame,
                                                     prices_data_frame, tc,
                                                     period_shift=1):
        """Calculates the trading startegy returns for given signal and asset prices including
        transaction costs (and roll costs)

        Parameters
        ----------
        signal_data_frame : DataFrame
            trading signals
        price_data_frame: DataFrame
            price of asset to be traded
        tc : float
            transaction costs
        period_shift : int
            number of periods to shift signal
        is_returns : bool (default: True)
            is the series of returns (as opposed to prices)

        Returns
        -------
        DataFrame
        """

        tc_costs = self.calculate_signal_tc(signal_data_frame, tc,
                                            period_shift)

        return signal_data_frame.shift(period_shift) * self.calculate_returns(
            prices_data_frame) - tc_costs

    def calculate_signal_returns_with_tc_matrix(self, signal_data_frame,
                                                returns_data_frame, tc,
                                                rc=None, period_shift=1):
        """Calculates the trading startegy returns for given signal and asset with transaction costs with matrix multiplication

        Parameters
        ----------
        signal_data_frame : DataFrame
            trading signals

        returns_data_frame: DataFrame
            returns of asset to be traded

        tc : float
            transaction costs

        rc : float
            roll costs

        period_shift : int
            number of periods to shift signal

        Returns
        -------
        DataFrame
        """

        # for transaction costs which vary by asset name

        # TODO add transaction costs which vary by size (maybe using a market impact model?)
        if isinstance(tc, dict):

            tc_ind = []

            for k in returns_data_frame.columns:
                try:
                    tc_ind.append(tc[k.split('.')[0]])
                except:
                    tc_ind.append(tc['default'])

            tc_ind = numpy.array(tc_ind)

            tc_costs = (numpy.abs(signal_data_frame.shift(
                period_shift).values - signal_data_frame.values) * tc_ind)
        elif isinstance(tc, pd.DataFrame):
            tc_ind = []

            # Get indices related to the returns
            for k in returns_data_frame.columns:
                try:
                    tc_ind.append(k.split('.')[0] + ".spread")
                except:
                    tc_ind.append('default.spread')

            # Don't include transaction costs at a portfolio level (TODO - weight it according to the assets)
            tc['Portfolio.spread'] = 0

            # Get associated transaction costs time series
            tc_costs = tc[tc_ind]

            # Make sure transaction costs are aligned to the signals
            signal_data_frame, tc_costs = signal_data_frame.align(tc_costs,
                                                                  join='left',
                                                                  axis='index')

            tc_costs = tc_costs.fillna(method='ffill')

            # Calculate the transaction costs by multiplying by trades
            tc_costs = (numpy.abs(signal_data_frame.shift(
                period_shift).values - signal_data_frame.values) * tc_costs.values)

        else:
            tc_costs = (numpy.abs(signal_data_frame.shift(
                period_shift).values - signal_data_frame.values) * tc)

        # Now handle roll costs
        if rc is not None:
            if isinstance(rc, dict):

                rc_ind = []

                for k in returns_data_frame.columns:
                    try:
                        rc_ind.append(rc[k.split('.')[0]])
                    except:
                        rc_ind.append(rc['default'])

                rc_ind = numpy.array(rc_ind)

                rc_costs = (numpy.abs(
                    signal_data_frame.shift(period_shift).values) * rc_ind)
            elif isinstance(rc, pd.DataFrame):
                rc_ind = []

                # Get indices related to the returns
                for k in returns_data_frame.columns:
                    try:
                        rc_ind.append(k.split('.')[0] + ".rc")
                    except:
                        rc_ind.append('default.rc')

                # Don't include roll costs at a portfolio level (TODO - weight it according to the assets)
                rc['Portfolio.rc'] = 0

                # Get associated roll costs time series
                rc_costs = rc[rc_ind]

                # Make sure transaction costs are aligned to the signals
                signal_data_frame, rc_costs = signal_data_frame.align(rc_costs,
                                                                      join='left',
                                                                      axis='index')

                rc_costs = rc_costs.fillna(0)

                # Calculate the roll costs by multiplying by our position (eg. if position is zero, then no roll cost)
                rc_costs = (numpy.abs(signal_data_frame.shift(
                    period_shift).values) * rc_costs.values)

            else:
                rc_costs = (numpy.abs(
                    signal_data_frame.shift(period_shift).values) * rc)
        else:
            rc_costs = 0

        return pd.DataFrame(
            signal_data_frame.shift(
                period_shift).values * returns_data_frame.values - tc_costs - rc_costs,
            index=returns_data_frame.index)

    def calculate_returns(self, data_frame, period_shift=1):
        """Calculates the simple returns for an asset

        Parameters
        ----------
        data_frame : DataFrame
            asset price
        period_shift : int
            number of periods to shift signal

        Returns
        -------
        DataFrame
        """
        return data_frame / data_frame.shift(period_shift) - 1

    def calculate_diff_returns(self, data_frame, period_shift=1):
        """Calculates the differences for an asset

        Parameters
        ----------
        data_frame : DataFrame
            asset price
        period_shift : int
            number of periods to shift signal

        Returns
        -------
        DataFrame
        """
        return data_frame - data_frame.shift(period_shift)

    def calculate_log_returns(self, data_frame, period_shift=1):
        """Calculates the log returns for an asset

        Parameters
        ----------
        data_frame : DataFrame
            asset price
        period_shift : int
            number of periods to shift signal

        Returns
        -------
        DataFrame
        """
        return math.log(data_frame / data_frame.shift(period_shift))

    def create_mult_index(self, df_rets):
        """ Calculates a multiplicative index for a time series of returns starting from 100

        Parameters
        ----------
        df_rets : DataFrame
            asset price returns

        Returns
        -------
        DataFrame
        """

        df = 100.0 * (1.0 + df_rets).cumprod()

        # get the first non-nan values for rets and then start index
        # one before that (otherwise will ignore first rets point)
        # first_date_indices = df_rets.apply(lambda series: series.first_valid_index())
        # first_ord_indices = list()
        #
        # for i in first_date_indices:
        #     try:
        #         ind = df.index.searchsorted(i)
        #     except:
        #         ind = 0
        #
        #     if ind > 0: ind = ind - 1
        #
        #     first_ord_indices.append(ind)
        #
        # for i in range(0, len(df.columns)):
        #     df.iloc[first_ord_indices[i],i] = 100

        if isinstance(df, pd.Series):
            df = pd.DataFrame(df)

        # Probably a quicker way to do this, maybe using group by?
        for c in df.columns:
            df.loc[df[c].first_valid_index(), c] = 100

        return df

    def create_mult_index_from_prices(self, data_frame):
        """Calculates a multiplicative index for a time series of prices

        Parameters
        ----------
        df_rets : DataFrame
            asset price

        Returns
        -------
        DataFrame
        """
        return self.create_mult_index(self.calculate_returns(data_frame))

    def create_add_index(self, df_rets):
        """ Calculates a additive index for a time series of returns starting from 1

        Parameters
        ----------
        df_rets : DataFrame
            asset price returns

        Returns
        -------
        DataFrame
        """

        df = 1.0 + df_rets.cumsum()

        # get the first non-nan values for rets and then start index
        # one before that (otherwise will ignore first rets point)
        # first_date_indices = df_rets.apply(lambda series: series.first_valid_index())
        # first_ord_indices = list()
        #
        # for i in first_date_indices:
        #     try:
        #         ind = df.index.searchsorted(i)
        #     except:
        #         ind = 0
        #
        #     if ind > 0: ind = ind - 1
        #
        #     first_ord_indices.append(ind)
        #
        # for i in range(0, len(df.columns)):
        #     df.iloc[first_ord_indices[i],i] = 100

        if isinstance(df, pd.Series):
            df = pd.DataFrame(df)

        # Probably a quicker way to do this, maybe using group by?
        for c in df.columns:
            df.loc[df[c].first_valid_index(), c] = 1

        return df

    def create_add_index_from_prices(self, data_frame):
        """Calculates a additive index for a time series of prices

        Parameters
        ----------
        df_rets : DataFrame
            asset price

        Returns
        -------
        DataFrame
        """
        return self.create_add_index(self.calculate_returns(data_frame))

    def rolling_z_score(self, data_frame, periods):
        """Calculates the rolling z score for a time series

        Parameters
        ----------
        data_frame : DataFrame
            asset prices
        periods : int
            rolling window for z score computation

        Returns
        -------
        DataFrame
        """
        return (data_frame - data_frame.rolling(center=False,
                                                window=periods).mean()) / data_frame.rolling(
            center=False,
            window=periods).std()

    def expanding_z_score(self, data_frame, min_periods):
        """Calculates the exp z score for a time series

        Parameters
        ----------
        data_frame : DataFrame
            asset prices
        min_periods : int
            minimum window for z score computation

        Returns
        -------
        DataFrame
        """
        return (data_frame - data_frame.expanding(center=False,
                                                  min_periods=min_periods).mean()) / data_frame.expanding(
            center=False,
            min_periods=min_periods).std()

    def rolling_volatility(self, data_frame, periods, obs_in_year=252):
        """
        rolling_volatility - Calculates the annualised rolling volatility

        Parameters
        ----------
        data_frame : DataFrame
            contains returns time series
        obs_in_year : int
            number of observation in the year

        Returns
        -------
        DataFrame
        """

        # return pd.rolling_std(data_frame, periods) * math.sqrt(obs_in_year)
        return data_frame.rolling(window=periods,
                                  center=False).std() * math.sqrt(obs_in_year)

    def rolling_mean(self, data_frame, periods):
        return self.rolling_average(data_frame, periods)

    def rolling_average(self, data_frame, periods):
        """Calculates the rolling moving average

        Parameters
        ----------
        data_frame : DataFrame
            contains time series
        periods : int
            periods in the average

        Returns
        -------
        DataFrame
        """
        return data_frame.rolling(periods).mean()

    def rolling_sparse_average(self, data_frame, periods):
        """Calculates the rolling moving average of a sparse time series

        Parameters
        ----------
        data_frame : DataFrame
            contains time series
        periods : int
            number of periods in the rolling sparse average

        Returns
        -------
        DataFrame
        """

        # 1. calculate rolling sum (ignore NaNs)
        # 2. count number of non-NaNs
        # 3. average of non-NaNs
        foo = lambda z: z[pd.notnull(z)].sum()

        # rolling_sum = pd.rolling_apply(data_frame, periods, foo, min_periods=1)
        # rolling_non_nans = pd.stats.moments.rolling_count(data_frame, periods, freq=None, center=False, how=None) \
        rolling_sum = data_frame.rolling(center=False, window=periods,
                                         min_periods=1).apply(func=foo)
        rolling_non_nans = data_frame.rolling(window=periods,
                                              center=False).count()

        # For pd 0.18 onwards (TODO)
        # rolling_non_nans = data_frame.rolling(span=periods, freq=None, center=False, how=None).count()

        return rolling_sum / rolling_non_nans

    def rolling_sparse_sum(self, data_frame, periods):
        """Calculates the rolling moving sum of a sparse time series

        Parameters
        ----------
        data_frame : DataFrame
            contains time series
        periods : int
            period for sparse rolling sum

        Returns
        -------
        DataFrame
        """

        # 1. calculate rolling sum (ignore NaNs)
        # 2. count number of non-NaNs
        # 3. average of non-NaNs
        foo = lambda z: z[pd.notnull(z)].sum()

        rolling_sum = pd.rolling_apply(data_frame, periods, foo, min_periods=1)

        return rolling_sum

    def rolling_median(self, data_frame, periods):
        """Calculates the rolling moving average

        Parameters
        ----------
        data_frame : DataFrame
            contains time series
        periods : int
            number of periods in the median

        Returns
        -------
        DataFrame
        """
        return data_frame.rolling(periods).median()

    def rolling_sum(self, data_frame, periods):
        """Calculates the rolling sum

        Parameters
        ----------
        data_frame : DataFrame
            contains time series
        periods : int
            period for rolling sum

        Returns
        -------
        DataFrame
        """
        return data_frame.rolling(periods).sum()

    def cum_sum(self, data_frame):
        """Calculates the cumulative sum

        Parameters
        ----------
        data_frame : DataFrame
            contains time series

        Returns
        -------
        DataFrame
        """
        return data_frame.cumsum()

    def rolling_ewma(self, data_frame, periods):
        """Calculates exponentially weighted moving average

        Parameters
        ----------
        data_frame : DataFrame
            contains time series
        periods : int
            periods in the EWMA

        Returns
        -------
        DataFrame
        """

        # span = 2 / (1 + periods)

        return pd.ewma(data_frame, span=periods)

    ##### correlation methods
    def rolling_corr(self, data_frame1, periods, data_frame2=None,
                     pairwise=False, flatten_labels=True):
        """Calculates rolling correlation wrapping around pd functions

        Parameters
        ----------
        data_frame1 : DataFrame
            contains time series to run correlations on
        periods : int
            period of rolling correlations
        data_frame2 : DataFrame (optional)
            contains times series to run correlation against
        pairwise : boolean
            should we do pairwise correlations only?

        Returns
        -------
        DataFrame
        """

        # this is the new bit of code here
        if pd.__version__ < '0.17':
            if pairwise:
                panel = pd.rolling_corr_pairwise(data_frame1.join(data_frame2),
                                                 periods)
            else:
                panel = pd.rolling_corr(data_frame1, data_frame2, periods)
        else:
            # panel = pd.rolling_corr(data_frame1, data_frame2, periods, pairwise = pairwise)
            panel = data_frame1.rolling(window=periods).corr(other=data_frame2,
                                                             pairwise=True)

        try:
            df = panel.to_frame(filter_observations=False).transpose()

        except:
            df = panel

        if flatten_labels:
            if pairwise:
                series1 = df.columns.get_level_values(0)
                series2 = df.columns.get_level_values(1)
                new_labels = []

                for i in range(len(series1)):
                    new_labels.append(series1[i] + " v " + series2[i])

            else:
                new_labels = []

                try:
                    series1 = data_frame1.columns
                except:
                    series1 = [data_frame1.name]

                series2 = data_frame2.columns

                for i in range(len(series1)):
                    for j in range(len(series2)):
                        new_labels.append(series1[i] + " v " + series2[j])

            df.columns = new_labels

        return df

    def rolling_autocorr(self, data_frame, period, lag):
        """Calculates rolling auto-correlation wrapping around pd functions by column

        Parameters
        ----------
        data_frame : DataFrame
            contains time series to run correlations on
        period : int
            period of autocorrelation
        lag : int
            lag of the autocorrelation

        Returns
        -------
        DataFrame
        """
        return (data_frame.rolling(window=period).corr(data_frame.shift(lag)))

    def calculate_column_matrix_signal_override(self, override_df, signal_df):
        length_cols = len(signal_df.columns)
        override_matrix = numpy.repeat(
            override_df.values.flatten()[numpy.newaxis, :], length_cols, 0)

        # final portfolio signals (including signal & override matrix)
        return pd.DataFrame(
            data=numpy.multiply(numpy.transpose(override_matrix),
                                signal_df.values),
            index=signal_df.index, columns=signal_df.columns)

    def join_left_fill_right(self, df_left, df_right):

        # say our right series is a signal
        # say our left series is an asset to be traded

        # first do an outer join then fill down our right signal
        df_left_1, df_right = df_left.align(df_right, join='outer', axis=0)
        df_right = df_right.fillna(method='ffill')

        # now realign back to days when we trade
        # df_left.to_csv('left.csv'); df_right.to_csv('right.csv')

        df_left, df_right = df_left.align(df_right, join='left', axis=0)

        return df_left, df_right

    ####################################################################################################################

    # DEPRECATED: several types of outer join, use "join" function later in class
    def pandas_outer_join(self, df_list):
        if df_list is None: return None

        if not (isinstance(df_list, list)):
            df_list = [df_list]

        # remove any None elements (which can't be joined!)
        df_list = [i for i in df_list if i is not None]

        if len(df_list) == 0:
            return None
        elif len(df_list) == 1:
            return df_list[0]

        # df_list = [dd.from_pd(df) for df in df_list]

        return df_list[0].join(df_list[1:], how="outer")

    def functional_outer_join(self, df_list):
        def join_dfs(ldf, rdf):
            return ldf.join(rdf, how='outer')

        return functools.reduce(join_dfs, df_list)

    # experimental!
    # splits dataframe list into halves
    def iterative_outer_join_second(self, df_list):
        if df_list is None: return None

        # remove any None elements (which can't be joined!)
        df_list = [i for i in df_list if i is not None]

        if len(df_list) == 0:
            return None

        elif len(df_list) == 1:
            return df_list[0]

        while (True):
            length = len(df_list)

            if length == 1: break

            df_list_out = []

            for i in range(0, length, 2):
                df_list_out.append(self.join_aux(i, df_list))

            df_list = df_list_out

        return df_list[0]

    def iterative_outer_join(self, df_list, pool=None):

        if not (isinstance(df_list, list)):
            return df_list

        if pool is None:
            from multiprocessing.dummy import Pool
            pool = Pool(4)

        if (len(df_list) < 3):
            return self.join(df_list, how='outer')

        while (True):
            # split into two
            length = len(df_list)

            if length == 1: break

            job_args = [(item_a, df_list) for i, item_a in
                        enumerate(range(0, length, 2))]
            df_list = pool.map_async(self.join_aux_helper, job_args).get()

        pool.close()
        pool.join()

        return df_list[0]

    def join_aux_helper(self, args):
        return self.join_aux(*args)

    def join_aux(self, i, df_list):
        if i == len(df_list) - 1: return df_list[i]

        return df_list[i].join(df_list[i + 1], how="outer")

    ####################################################################################################################

    def concat_dataframe_list(self, df_list, sort=True):
        """Concatenates a list of DataFrames into a single DataFrame and sorts them. Removes any empty or None elements
        from the list and optionally sorts them)

        Parameters
        ----------
        df_list : DataFrame (list)
            DataFrames to be concatenated

        sort : bool (default: True)
            Sorts final concatenated DataFrame by index

        Returns
        -------
        DataFrame
        """
        if df_list is not None:

            # remove and empty dataframes from the list
            if isinstance(df_list, list):
                df_list = [x for x in df_list if x is not None]
                df_list = [x for x in df_list if not x.empty]
            else:
                return df_list

            # only concatenate if any non-empty dataframes are left
            if len(df_list) > 0:
                # careful: concatenating DataFrames can change the order, so insist on arranging by old cols
                old_cols = df_list[0].columns

                if len(df_list) == 1:
                    df_list = df_list[0]

                    if sort:
                        df_list = df_list.sort_index()

                else:
                    df_list = pd.concat(df_list, sort=sort)

                return df_list[old_cols]

        return None

    ## Numba/Pandas based implementations ##############################################################################

    # Note: Numba implementations not finished yet
    def join(self, df_list, engine='pandas', how='outer'):
        if df_list is None: return None

        if not (isinstance(df_list, list)):
            df_list = [df_list]

        # remove any None elements (which can't be joined!)
        df_list = [i for i in df_list if i is not None]

        if len(df_list) == 0:
            return None
        elif len(df_list) == 1:
            return df_list[0]

        # df_list = [dd.from_pd(df) for df in df_list]

        if engine == 'pandas':
            df_new_list = []

            # Convert any Series to DataFrames
            for d in df_list:
                if isinstance(d, pd.Series):
                    d = pd.DataFrame(d)

                df_new_list.append(d)

            df_list = df_new_list

            return df_list[0].join(df_list[1:], how=how)

        elif engine == 'numba':
            raise Exception('Numba join not implemented')

    def align(self, df_left, df_right, engine='pandas', join='outer', axis=0):
        if engine == 'pandas':
            return df_left.align(df_right, join=join, axis=axis)

        elif engine == 'numba':
            raise Exception('Numba align not implemented')

    # Note: Numba implementations not finished yet
    def join_intraday_daily(self, df_intraday_list, df_daily_list,
                            engine='pandas', intraday_how='outer',
                            daily_how='outer', daily_time_of_day='10:00',
                            daily_time_zone='Americas/New_York'):
        """Join together intraday and daily dataframes. Any columns in the intraday dataframe will overwrite those of the
        data dataframe.

        Parameters
        ----------
        df_intraday_list : list(pd.DataFrame)
            A number of intraday dataframes

        df_daily_list : list(pd.DataFrame)
            A number of daily dataframes

        engine : str
            'pandas' - default
            'numba'

        intraday_how : str
            how to join intraday dataframe

        daily_how : str
            how to join daily dataframe

        daily_time_of_day : str
            time of day to apply to daily dataframes
            '10:00' - default

        daily_time_zone : str
            timezone of the daily dataframe to apply

        Returns
        -------
        pd.DataFrame
        """
        if df_daily_list is None: return None
        if df_intraday_list is None: return None

        df_intraday = self.join(df_intraday_list, engine=engine,
                                how=intraday_how)

        if not (isinstance(df_daily_list, list)):
            df_daily_list = [df_daily_list]

        # remove any None elements (which can't be joined!)
        df_list = [i for i in df_daily_list if i is not None]

        if len(df_list) == 0:
            return None

        # df_list = [dd.from_pd(df) for df in df_list]

        if engine == 'pandas':
            if len(df_list) == 1:
                df_daily = df_list[0]
            else:
                df_daily = df_list[0].join(df_list[1:], how=daily_how)

            common_cols = [x for x in df_intraday.columns if
                           x in df_daily.columns]

            if len(common_cols) > 0:
                df_daily = df_daily.drop(common_cols, axis=1)

            tz = Timezone()

            if not (df_daily.empty) and not (df_intraday.empty):
                try:
                    df_daily = tz.localize_index_as_UTC(df_daily)
                except:
                    pass

                df_daily.index = pd.to_datetime(df_daily.index)
                df_daily = tz.convert_index_aware_to_alt(df_daily,
                                                         daily_time_zone)
                df_daily.index = df_daily.index + pd.Timedelta(
                    hours=int(daily_time_of_day[0:2]),
                    minutes=int(daily_time_of_day[3:4]))
                df_daily = tz.convert_index_aware_to_alt(df_daily,
                                                         df_intraday.index.tz)

                return df_intraday.join(df_daily, how='left')
            elif df_daily.empty and not (df_intraday.empty):
                return df_intraday
            elif not (df_daily.empty) and df_intraday.empty:
                return df_daily
            else:
                return None

        elif engine == 'numba':
            raise Exception('Numba join not implemented')

    ####################################################################################################################

    def amalgamate_intraday_daily_data(self, intraday_df_dict, daily_df_dict):
        pass

    def linear_regression(self, df_y, df_x):
        return pd.stats.api.ols(y=df_y, x=df_x)

    def linear_regression_single_vars(self, df_y, df_x, y_vars, x_vars,
                                      use_stats_models=True):
        """Do a linear regression of a number of y and x variable pairs in different dataframes, report back the coefficients.

        Parameters
        ----------
        df_y : DataFrame
            y variables to regress
        df_x : DataFrame
            x variables to regress
        y_vars : str (list)
            Which y variables should we regress
        x_vars : str (list)
            Which x variables should we regress
        use_stats_models : bool (default: True)
            Should we use statsmodels library directly or pandas.stats.api.ols wrapper (warning: deprecated)

        Returns
        -------
        List of regression statistics

        """

        stats = []

        for i in range(0, len(y_vars)):
            y = df_y[y_vars[i]]
            x = df_x[x_vars[i]]

            try:
                if pd.__version__ < '0.17' or not (use_stats_models):
                    out = pd.stats.api.ols(y=y, x=x)
                else:
                    # pandas.stats.api is now being depreciated, recommended replacement package
                    # http://www.statsmodels.org/stable/regression.html

                    # we follow the example from there - Fit and summarize OLS model

                    import statsmodels.api as sm
                    import statsmodels

                    # to remove NaN values (otherwise regression is undefined)
                    (y, x, a, b, c, d) = self._filter_data(y, x)

                    # assumes we have a constant (remove add_constant wrapper to have no intercept reported)
                    mod = sm.OLS(y.get_values(),
                                 statsmodels.tools.add_constant(
                                     x.get_values()))
                    out = mod.fit()
            except:
                out = None

            stats.append(out)

        return stats

    def strip_linear_regression_output(self, indices, ols_list, var):

        # TODO deal with output from statsmodel as opposed to pandas.stats.ols
        if not (isinstance(var, list)):
            var = [var]

        df = pd.DataFrame(index=indices, columns=var)

        for v in var:
            list_o = []

            for o in ols_list:
                if o is None:
                    list_o.append(numpy.nan)
                else:
                    if v == 't_stat':
                        list_o.append(o.t_stat.x)
                    elif v == 't_stat_intercept':
                        list_o.append(o.t_stat.intercept)
                    elif v == 'beta':
                        list_o.append(o.beta.x)
                    elif v == 'beta_intercept':
                        list_o.append(o.beta.intercept)
                    elif v == 'r2':
                        list_o.append(o.r2)
                    elif v == 'r2_adj':
                        list_o.append(o.r2_adj)
                    else:
                        return None

            df[v] = list_o

        return df

    ##### Various methods for averaging time series by hours, mins and days (or specific columns) to create summary time series
    def average_by_columns_list(self, data_frame, columns):
        return data_frame. \
            groupby(columns).mean()

    def average_by_hour_min_of_day(self, data_frame):
        # Older pd
        try:
            return data_frame. \
                groupby([data_frame.index.hour.rename('hour'),
                         data_frame.index.minute.rename('minute')]).mean()
        except:
            return data_frame. \
                groupby(
                [data_frame.index.hour, data_frame.index.minute]).mean()

    def average_by_hour_min_of_day_pretty_output(self, data_frame):
        # Older pd
        try:
            data_frame = data_frame. \
                groupby([data_frame.index.hour.rename('hour'),
                         data_frame.index.minute.rename('minute')]).mean()
        except:
            data_frame = data_frame. \
                groupby(
                [data_frame.index.hour, data_frame.index.minute]).mean()

        data_frame.index = data_frame.index.map(lambda t: datetime.time(*t))

        return data_frame

    def average_by_hour_min_sec_of_day_pretty_output(self, data_frame):
        # Older pd
        try:
            data_frame = data_frame. \
                groupby([data_frame.index.hour.rename('hour'),
                         data_frame.index.minute.rename('minute'),
                         data_frame.index.second.rename('second')]).mean()
        except:
            data_frame = data_frame. \
                groupby([data_frame.index.hour, data_frame.index.minute,
                         data_frame.index.second]).mean()

        data_frame.index = data_frame.index.map(lambda t: datetime.time(*t))

        return data_frame

    def all_by_hour_min_of_day_pretty_output(self, data_frame):

        df_new = []

        for group in data_frame.groupby(data_frame.index.date):
            df_temp = group[1]
            df_temp.index = df_temp.index.time
            df_temp.columns = [group[0]]
            df_new.append(df_temp)

        return pd.concat(df_new, axis=1)

    def average_by_year_hour_min_of_day_pretty_output(self, data_frame):
        # years = range(data_frame.index[0].year, data_frame.index[-1].year)
        #
        # time_of_day = []
        #
        # for year in years:
        #     temp = data_frame[data_frame.index.year == year]
        #     time_of_day.append(temp.groupby(temp.index.time).mean())
        #
        # data_frame = pd.concat(time_of_day, axis=1, keys = years)

        # Older pd
        try:
            data_frame = data_frame. \
                groupby([data_frame.index.year.rename('year'),
                         data_frame.index.hour.rename('hour'),
                         data_frame.index.minute.rename('minute')]).mean()
        except:
            data_frame = data_frame. \
                groupby([data_frame.index.year, data_frame.index.hour,
                         data_frame.index.minute]).mean()

        data_frame = data_frame.unstack(0)

        data_frame.index = data_frame.index.map(lambda t: datetime.time(*t))

        return data_frame

    def average_by_day_of_week_hour_min_of_day_pretty_output(self, data_frame):
        # years = range(data_frame.index[0].year, data_frame.index[-1].year)
        #
        # time_of_day = []
        #
        # for year in years:
        #     temp = data_frame[data_frame.index.year == year]
        #     time_of_day.append(temp.groupby(temp.index.time).mean())
        #
        # data_frame = pd.concat(time_of_day, axis=1, keys = years)

        # Older pd
        try:
            data_frame = data_frame. \
                groupby([data_frame.index.dayofweek.rename('dayofweek'),
                         data_frame.index.hour.rename('hour'),
                         data_frame.index.minute.rename('minute')]).mean()
        except:
            data_frame = data_frame. \
                groupby([data_frame.index.dayofweek, data_frame.index.hour,
                         data_frame.index.minute]).mean()

        data_frame = data_frame.unstack(0)

        data_frame.index = data_frame.index.map(lambda t: datetime.time(*t))

        return data_frame

    def average_by_annualised_year(self, data_frame, obs_in_year=252):
        data_frame = data_frame. \
                         groupby([data_frame.index.year]).mean() * obs_in_year

        return data_frame

    def average_by_month(self, data_frame):
        data_frame = data_frame. \
            groupby([data_frame.index.month]).mean()

        return data_frame

    def average_by_bus_day(self, data_frame, cal="FX"):
        date_index = data_frame.index

        return data_frame. \
            groupby([Calendar().get_bus_day_of_month(date_index, cal)]).mean()

    def average_by_cal_day(self, data_frame):

        return data_frame. \
            groupby(data_frame.index.day).mean()

    def average_by_month_day_hour_min_by_bus_day(self, data_frame, cal="FX"):
        date_index = data_frame.index

        # Older pd
        try:
            return data_frame. \
                groupby([date_index.month.rename('month'),
                         Calendar().get_bus_day_of_month(date_index, cal,
                                                         tz=data_frame.index.tz).rename(
                             'day'),
                         date_index.hour.rename('hour'),
                         date_index.minute.rename('minute')]).mean()
        except:
            return data_frame. \
                groupby([date_index.month,
                         Calendar().get_bus_day_of_month(date_index, cal,
                                                         tz=data_frame.index.tz),
                         date_index.hour, date_index.minute]).mean()

    def average_by_month_day_by_bus_day(self, data_frame, cal="FX"):
        date_index = data_frame.index

        # Older pd
        try:
            return data_frame. \
                groupby([date_index.month.rename('month'),
                         Calendar().get_bus_day_of_month(date_index, cal,
                                                         tz=data_frame.index.tz).rename(
                             'day')]).mean()
        except:
            return data_frame. \
                groupby([date_index.month,
                         Calendar().get_bus_day_of_month(date_index, cal,
                                                         tz=data_frame.index.tz)]).mean()

    def average_by_month_day_by_day(self, data_frame):
        date_index = data_frame.index

        return data_frame. \
            groupby([date_index.month, date_index.day]).mean()

    def group_by_year(self, data_frame):
        date_index = data_frame.index

        return data_frame. \
            groupby([date_index.year])

    def average_by_day_hour_min_by_bus_day(self, data_frame):
        date_index = data_frame.index

        # Older pd
        try:
            return data_frame. \
                groupby(
                [Calendar().get_bus_day_of_month(date_index).rename('day'),
                 date_index.hour.rename('hour'),
                 date_index.minute.rename('minute')]).mean()
        except:
            return data_frame. \
                groupby([Calendar().get_bus_day_of_month(date_index),
                         date_index.hour, date_index.minute]).mean()

    def remove_NaN_rows(self, data_frame):
        return data_frame.dropna()

    def get_top_valued_sorted(self, df, order_column, n=20):
        df_sorted = df.sort(columns=order_column)
        df_sorted = df_sorted.tail(n=n)

        return df_sorted

    def get_bottom_valued_sorted(self, df, order_column, n=20):
        df_sorted = df.sort(columns=order_column)
        df_sorted = df_sorted.head(n=n)

        return df_sorted

    def convert_month_day_to_date_time(self, df, year=1970):
        new_index = []

        # TODO use map?
        for i in range(0, len(df.index)):
            x = df.index[i]
            new_index.append(datetime.date(year, x[0], int(x[1])))

        df.index = pd.DatetimeIndex(new_index)

        return df

    ###### preparing data for OLS statsmodels ######
    ###### these methods are originally from pandas.stats.ols
    ###### which is being deprecated
    def _filter_data(self, lhs, rhs, weights=None):
        """
        Cleans the input for single OLS.
        Parameters
        ----------
        lhs : Series
            Dependent variable in the regression.
        rhs : dict, whose values are Series, DataFrame, or dict
            Explanatory variables of the regression.
        weights : array-like, optional
            1d array of weights.  If None, equivalent to an unweighted OLS.
        Returns
        -------
        Series, DataFrame
            Cleaned lhs and rhs
        """

        if not isinstance(lhs, pd.Series):
            if len(lhs) != len(rhs):
                raise AssertionError("length of lhs must equal length of rhs")
            lhs = pd.Series(lhs, index=rhs.index)

        rhs = self._combine_rhs(rhs)
        lhs = pd.DataFrame({'__y__': lhs}, dtype=float)
        pre_filt_rhs = rhs.dropna(how='any')

        combined = rhs.join(lhs, how='outer')

        if weights is not None:
            combined['__weights__'] = weights

        valid = (combined.count(1) == len(combined.columns)).values
        index = combined.index
        combined = combined[valid]

        if weights is not None:
            filt_weights = combined.pop('__weights__')
        else:
            filt_weights = None

        filt_lhs = combined.pop('__y__')
        filt_rhs = combined

        if hasattr(filt_weights, 'to_dense'):
            filt_weights = filt_weights.to_dense()

        return (filt_lhs.to_dense(), filt_rhs.to_dense(), filt_weights,
                pre_filt_rhs.to_dense(), index, valid)

    def _safe_update(self, d, other):
        """
        Combine dictionaries with non-overlapping keys
        """
        for k, v in compat.iteritems(other):
            if k in d:
                raise Exception('Duplicate regressor: %s' % k)

            d[k] = v

    def _combine_rhs(self, rhs):
        """
        Glue input X variables together while checking for potential
        duplicates
        """
        series = {}

        if isinstance(rhs, pd.Series):
            series['x'] = rhs
        elif isinstance(rhs, pd.DataFrame):
            series = rhs.copy()
        elif isinstance(rhs, dict):
            for name, value in pd.compat.iteritems(rhs):
                if isinstance(value, pd.Series):
                    self._safe_update(series, {name: value})
                elif isinstance(value, (dict, pd.DataFrame)):
                    self._safe_update(series, value)
                else:  # pragma: no cover
                    raise Exception('Invalid RHS data type: %s' % type(value))
        else:  # pragma: no cover
            raise Exception('Invalid RHS type: %s' % type(rhs))

        if not isinstance(series, pd.DataFrame):
            series = pd.DataFrame(series, dtype=float)

        return series

    def insert_sparse_time_series(self, df_sparse_time_series, pre_window_size,
                                  post_window_size, unit):
        """  Given a sparse time series dataframe, return inserted dataframe with given unit/window
        e.g   for a given sparse time series df, df[30] = 4.0
              pre and post window sizes are 5
              then the function will insert 4.0 to df[26:30] and df[30:35]

        *Note - may have chaotic results if df is not sparse enough (since the windows may overlap)

        Parameters
        ----------
        df_sparse_time_series : DateTime
            time series to be inserted
        pre_window_size :  int
            e.g. pre_window_size = 5
        post_window_size:  int
            e.g. post_window_size = 5
        unit: minutes, hours, days
            the unit shoould be same as used in the index of dataframe

        Returns
        -------
        DataFrame
        """

        from datetime import timedelta
        df = df_sparse_time_series
        col = list(df)
        for i in col:
            non_empty_list = df[i].loc[df[i] != 0].index
            ### unit options can be added if necessary
            if unit == 'minutes':
                post_window_list = non_empty_list + timedelta(
                    minutes=post_window_size)
                pre_window_list = non_empty_list - timedelta(
                    minutes=pre_window_size)
                backward_fill_bound = pre_window_list[0] - timedelta(minutes=1)
            if unit == 'hours':
                post_window_list = non_empty_list + timedelta(
                    hours=post_window_size)
                pre_window_list = non_empty_list - timedelta(
                    hours=pre_window_size)
                backward_fill_bound = pre_window_list[0] - timedelta(hours=1)
            if unit == 'days':
                post_window_list = non_empty_list + timedelta(
                    days=post_window_size)
                pre_window_list = non_empty_list - timedelta(
                    days=pre_window_size)
                backward_fill_bound = pre_window_list[0] - timedelta(days=1)

            # now the given df should be [0 0 0 0 0 0 x 0 0 0 0 0 0]
            # x is the non zero value
            # specify the cases as window sizes can be 0
            if post_window_size > 0:
                narray = numpy.where(df[i].index.isin(post_window_list), 2, 0)
                df[i] = df[[i]] + pd.DataFrame(index=df.index, columns=[i],
                                               data=narray)
            if pre_window_size > 0:
                narray = numpy.where(df[i].index.isin(pre_window_list), 3, 0)
                df[i] = df[[i]] + pd.DataFrame(index=df.index, columns=[i],
                                               data=narray)

            # now df should become [0 0 0 3 0 0 x 0 0 0 2 0 0 ]
            # to make sure the final backward filling won't replace all elements to the first one
            # we give a value at the backward_fill_bound (which is one unit before the pre window)
            df[i].at[backward_fill_bound] = 4

            # now df should become [0 0 4 3 0 0 x 0 0 0 2 0 0]

            df[i].replace(to_replace=0, method='ffill', inplace=True)

            # now df should become [0 0 4 3 3 3 x x x 2 2 2]

            df[i].replace(to_replace=3, value=0, inplace=True)

            # now df should become [0 0 4 0 0 0 x x x 2 2 2]

            df[i].replace(to_replace=0, method='bfill', inplace=True)

            # now df should become [4 4 4 x x x x x x 2 2 2]

            df[i].replace(to_replace=2, value=0, inplace=True)

            # now df should become [4 4 4 x x x x x x 0 0 0]

            df[i].replace(to_replace=4, value=0, inplace=True)

            # now df should become [0 0 0 x x x x x x 0 0 0]

        return df

    def floor_tick_of_date(self, date, add_day=False):
        """For a particular date, floor the time to 0

        Parameters
        ----------
        date : datetime
            Date to be amended

        Returns
        -------
        datetime
        """
        date = copy.copy(date)
        date = date.replace(hour=0)
        date = date.replace(minute=0)
        date = date.replace(second=0)
        date = date.replace(microsecond=0)

        if add_day:
            date = date + timedelta(days=1)

        return date

    def resample_tick_data_ohlc(self, df, asset, freq='1min',
                                avg_fields=['bid', 'ask']):

        if not (isinstance(asset, list)):
            asset = [asset]

        if avg_fields is not None:
            for a in asset:
                df[a] = df[[a + "." + f for f in avg_fields]].mean(axis=1)

        df = df[asset]
        df = df.resample(freq).ohlc().dropna()

        new_fields = []

        for a in asset:
            for x in ['open', 'high', 'low', 'close']:
                new_fields.append(a + "." + x)

        df.columns = new_fields

        return df

    def convert_to_numeric_dataframe(self, data_frame,
                                     numeric_columns=constants.always_numeric_column,
                                     date_columns=constants.always_date_columns):

        logger = LoggerManager().getLogger(__name__)

        failed_conversion_cols = []

        for c in data_frame.columns:
            is_date = False

            # If it's a date column don't append to convert to a float
            for d in date_columns:
                if d in c or 'release-dt' in c:
                    is_date = True
                    break

            if is_date:
                try:
                    data_frame[c] = pd.to_datetime(data_frame[c],
                                                   errors='coerce')
                except:
                    pass
            else:
                try:
                    data_frame[c] = data_frame[c].astype('float32')
                except:
                    if '.' in c:
                        if c.split('.')[1] in numeric_columns:
                            data_frame[c] = data_frame[c].astype('float32',
                                                                 errors='coerce')
                        else:
                            failed_conversion_cols.append(c)
                    elif c in numeric_columns:
                        data_frame[c] = data_frame[c].astype('float32',
                                                             errors='coerce')
                    else:
                        failed_conversion_cols.append(c)

                try:
                    data_frame[c] = data_frame[c].fillna(value=np.nan)
                except:
                    pass

        if failed_conversion_cols != []:
            logger.warning('Could not convert to float for ' + str(
                failed_conversion_cols))

        return data_frame


if __name__ == '__main__':
    # test functions
    calc = Calculations()
    tsf = Filter()

    # test rolling ewma
    date_range = pd.bdate_range('2014-01-01', '2014-02-28')

    print(calc.get_bus_day_of_month(date_range))

    foo = pd.DataFrame(numpy.arange(0.0, 13.0))
    print(calc.rolling_ewma(foo, span=3))
