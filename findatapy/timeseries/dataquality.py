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
import pandas
import pandas.tseries.offsets

from findatapy.timeseries.filter import Filter, Calendar

from pandas import compat


class DataQuality(object):
    """Checks the data quality of a DataFrame, reporting statistics such as the
    percentage of NaN values by column and through the whole DataFrame. Can
    also check by column between specific dates.

    """

    def percentage_nan(self, df, start_date=None):
        """Calculates the percentage of NaN values in a DataFrame.

        Parameters
        ----------
        df : DataFrame
            The data to be checked for data integrity
        start_date : str
            Filters time series by start date

        Returns
        -------
        float
            Between 0 and 100 representing the number of NaN values in a
            DataFrame (0 if the dataframe is None)
        """

        if df is None:
            return 100.0

        if start_date is not None:
            df = df[df.index >= start_date]

        nan = float(df.isnull().sum().sum())

        valid = float(df.count().sum())
        total = nan + valid

        if total == 0: return 0

        return round(100.0 * (nan / total), 1)

    def percentage_nan_by_columns(self, df, start_date=None):
        """Calculates the percentage of NaN values in a DataFrame and reports
        results by column. Likely, can do this
        for whole dataframe in one operation.

        Parameters
        ----------
        df : DataFrame
            The data to be checked for data integrity
        start_date : str
            Filters time series by start date

        Returns
        -------
        dict
            Dictionary of column names and percentage of NaN etween 0 and 100
            representing the number of NaN values in each column
        """

        if start_date is not None:
            df = df[df.index >= start_date]

        nan_dict = {}

        for c in df.columns:
            nan_dict = self.percentage_nan(df[c])

        return nan_dict

    def percentage_nan_between_start_finish_dates(
            self, df, df_properties, asset_field, start_date_field,
            finish_date_field):
        """Calculates the percentage of NaN in a DataFrame in a customisable
        way. For each column it will only check the NaNs between specific start
        and finish dates.

        Parameters
        ----------
        df : DataFrame
            Data to be checked for integrity
        df_properties : DataFrame
            Record of each column and the start/finish dates that will be
            used for NaNs
        asset_field : str
            The column in df_properties which contains the column names
        start_date_field : str
            The column in df_properties which contains the start date
        finish_date_field : str
            The column in df properties which contains the finish date

        Returns
        -------
        dict
            Contains column names and the associated percentage of NaNs
        """
        percentage_nan = {}

        df_properties = df_properties.sort_values(asset_field)

        df_dates = pandas.DataFrame(
            index=df_properties[asset_field].values,
            data=df_properties[[start_date_field, finish_date_field]].values,
            columns=[start_date_field, finish_date_field])

        c_new = [x.split(".")[0] for x in df.columns]

        index = df_dates.index.searchsorted(c_new)
        start_date = df_dates[start_date_field][index]
        finish_date = df_dates[finish_date_field][index]

        for i in range(0, len(df.columns)):
            df_sub = df[df.columns[i]]

            percentage_nan[df.columns[i]] = self.percentage_nan(
                df_sub[start_date[i]:finish_date[i]])

        return percentage_nan

    def strip_dataframe_before_large_nan_section(self, df, freq='daily',
                                                 max_nan_gap=20):
        """For each column in a dataframe, where there is a large gap (eg.
        20 working days), all values before that will be filled with NaN.
        This can for example be useful if we are constructing futures
        continuous time series and we need a continual array of futures
        values to be available to back adjust.

        Parameters
        ----------
        df : DataFrame
            Data to be assessed
        freq : str
            'daily' or 'intraday'
        max_nan_gap : int
            Number of days observations for missing threshold

        Returns
        -------
        DataFrame
            Overwritten earlier values with NaN, if they are before a large
            NaN section
        """

        if freq == 'daily':
            # resample by business days
            df_re = df.resample('B').mean()

            # calculate the rolling valid business days
            df_re_nan_count = df_re.rolling(window=max_nan_gap).count()

            # if 20 business days in a row are invalid, then flag this
            strip = df_re_nan_count[df_re_nan_count == 0]

            # For each column get the make everything before the last nan
            # section, as equal to nan
            # eg. if 2003 is all nan, but pre 2003 is populated by real values,
            # we'll overwrite the pre 2003 values with nan, can cause issues
            # with backtesting if we have large nan sections
            for c in df.columns:
                strip_index = strip[c].last_valid_index()

                # Only overwrite, if we have poor quality sections (if the data
                # quality is good we won't have this issue)
                if strip_index is not None:
                    df.ix[:strip_index, c] = numpy.nan

        elif freq == 'intraday':
            pass

        return df

    def count_repeated_dates(self, df):
        """Counts number of duplicated dates in a DataFrame and returns these

        Parameters
        ----------
        df : DataFrame
            Data to be checked

        Returns
        -------
        int, DateTimeIndex
            Number of duplicated entries, the duplicated entries themselves
        """

        duplicated = df.index.duplicated()

        return len(duplicated), df.index[duplicated]
