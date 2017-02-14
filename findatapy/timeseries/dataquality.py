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
#the License for the specific language governing permissions and limitations under the License.
#

import datetime
import functools
import math

import numpy
import pandas
import pandas.tseries.offsets
from pandas.stats.api import ols

from findatapy.timeseries.filter import Filter
from findatapy.timeseries.filter import Calendar

from pandas import compat

class DataQuality(object):
    """Checks the data quality of a DataFrame, reporting statistics such as the percentage of NaN values

    """

    def percentage_nan(self, df):
        nan = float(df.isnull().sum())
        valid = float(df.count())
        total = nan + valid

        if total == 0: return 0

        return round(100.0 * (nan / total), 1)

    def percentage_nan_between_start_finish_dates(self, df, df_properties, asset_field, start_date_field, finish_date_field):
        percentage_nan = {}

        df_properties = df_properties.sort_values(asset_field)

        df_dates = pandas.DataFrame(index=df_properties[asset_field].values, data=df_properties[[start_date_field, finish_date_field]].values,
                                    columns=[start_date_field, finish_date_field])

        c_new = [x.split(".")[0] for x in df.columns]

        index = df_dates.index.searchsorted(c_new)
        start_date = df_dates.ix[index, start_date_field]
        finish_date = df_dates.ix[index, finish_date_field]

        for i in range(0, len(df.columns)):
            df_sub = df[df.columns[i]]

            percentage_nan[df.columns[i]] = self.percentage_nan(df_sub[start_date[i]:finish_date[i]])

        return percentage_nan

        pass