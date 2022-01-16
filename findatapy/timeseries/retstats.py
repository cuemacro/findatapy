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

import math
import pandas
import collections

from findatapy.timeseries.calculations import Calculations


class RetStats(object):
    """Calculating return statistics of a time series

    """

    def __init__(self, returns_df=None, ann_factor=None, resample_freq=None):
        self._returns_df = returns_df
        self._ann_factor = ann_factor
        self._resample_freq = resample_freq

        self._rets = None
        self._vol = None
        self._inforatio = None
        self._kurtosis = None
        self._dd = None
        self._yoy_rets = None

    def split_into_dict(self):
        """If we have multiple columns in our returns, we can opt to split up
        the RetStats object into a dictionary of smaller RetStats object, one
        for each asset return

        Returns
        -------
        dict
            Dictionary of RetStats objects
        """

        ret_stats_dict = collections.OrderedDict()

        for d in self._returns_df.columns:

            returns_df = pandas.DataFrame(self._returns_df[d])

            # if column is of the form asset / signal, just keep the asset part
            try:
                d = d.split(' / ')[0]
            except:
                pass

            returns_df.columns = [d]

            ret_stats_dict[d] = RetStats(returns_df, self._ann_factor)

        return ret_stats_dict

    def calculate_ret_stats_from_prices(self, prices_df, ann_factor):
        """Calculates return statistics for an asset's price

        Parameters
        ----------
        prices_df : DataFrame
            asset prices
        ann_factor : int
            annualisation factor to use on return statistics

        Returns
        -------
        DataFrame
        """
        calculations = Calculations()

        self.calculate_ret_stats(calculations.calculate_returns(prices_df),
                                 ann_factor)

    def calculate_ret_stats(self, returns_df=None, ann_factor=None):
        """Calculates return statistics for an asset's returns including IR,
        vol, ret and drawdowns

        Parameters
        ----------
        returns_df : DataFrame
            asset returns
        ann_factor : int
            annualisation factor to use on return statistics

        Returns
        -------
        DataFrame
        """

        if returns_df is None: returns_df = self._returns_df
        if ann_factor is None: ann_factor = self._ann_factor

        if self._resample_freq is not None:
            returns_df = returns_df.resample(self._resample_freq).sum()

        # TODO work on optimizing this method
        self._rets = returns_df.mean(axis=0) * ann_factor
        self._vol = returns_df.std(axis=0) * math.sqrt(ann_factor)
        self._inforatio = self._rets / self._vol
        self._kurtosis = returns_df.kurtosis(axis=0) / math.sqrt(ann_factor)

        index_df = (1.0 + returns_df).cumprod()

        if pandas.__version__ < '0.17':
            max2here = pandas.expanding_max(index_df)
        else:
            max2here = index_df.expanding(min_periods=1).max()

        dd2here = index_df / max2here - 1

        self._dd = dd2here.min()
        self._yoy_rets = index_df.resample('Y').last().pct_change()

        return self

    def ann_returns(self):
        """Gets annualised returns

        Returns
        -------
        float
        """

        if self._rets is None: self.calculate_ret_stats()

        return self._rets

    def ann_vol(self):
        """Gets annualised volatility

        Returns
        -------
        float
        """
        if self._vol is None: self.calculate_ret_stats()

        return self._vol

    def inforatio(self):
        """Gets information ratio

        Returns
        -------
        float
        """

        if self._inforatio is None: self.calculate_ret_stats()

        return self._inforatio

    def drawdowns(self):
        """Gets drawdowns for an asset or strategy

        Returns
        -------
        float
        """
        if self._dd is None: self.calculate_ret_stats()

        return self._dd

    def kurtosis(self):
        """Gets kurtosis for an asset or strategy

        Returns
        -------
        float
        """
        if self._kurtosis is None: self.calculate_ret_stats()

        return self._kurtosis

    def yoy_rets(self):
        """Calculates the yoy rets

        Returns
        -------
        float
        """
        if self._yoy_rets is None: self.calculate_ret_stats()

        return self._yoy_rets

    def summary(self):
        """Gets summary string contains various return statistics

        Returns
        -------
        str
        """

        if self._rets is None: self.calculate_ret_stats()

        stat_list = []

        for i in range(0, len(self._rets.index)):
            stat_list.append(self._rets.index[i] + " Ret = " + str(
                round(self._rets[i] * 100, 1))
                             + "% Vol = " + str(round(self._vol[i] * 100, 1))
                             + "% IR = " + str(round(self._inforatio[i], 2))
                             + " Dr = " + str(round(self._dd[i] * 100, 1))
                             + "%")  # Kurt = " + str(round(self._kurtosis[i], 2)))

        return stat_list
