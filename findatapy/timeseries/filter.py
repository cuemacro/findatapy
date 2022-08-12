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

import numpy as np
import pandas as pd
import pytz

import datetime
from datetime import timedelta

from findatapy.timeseries.calendar import Calendar

from findatapy.util.dataconstants import DataConstants
from findatapy.util.loggermanager import LoggerManager

constants = DataConstants()


class Filter(object):
    """Functions for filtering time series by dates and columns.

    This class is used extensively in both findatapy and finmarketpy.

    Market holidays are collected from web sources such as 
    https://www.timeanddate.com/holidays/ and also individual
    exchange websites, and is manually updated from time to time to take 
    into account newly instituted holidays, and stored
    in conf/holidays_table.parquet - if you need to add your own holidays.

    """

    _time_series_cache = {}  # shared across all instances of object!

    def __init__(self):
        self._calendar = Calendar()

    def filter_time_series(self, md_request, data_frame,
                           pad_columns=False, filter_by_column_names=True):
        """Filters a time series given a set of criteria (like start/finish 
        date and tickers)

        Parameters
        ----------
        md_request : MarketDataRequest
            defining time series filtering
        data_frame : DataFrame
            time series to be filtered
        pad_columns : boolean
            true, non-existant columns with nan

        Returns
        -------
        DataFrame
        """
        start_date = md_request.start_date
        finish_date = md_request.finish_date

        data_frame = self.filter_time_series_by_date(start_date, finish_date,
                                                     data_frame)

        # Filter by ticker.field combinations requested
        columns = self.create_tickers_fields_list(md_request)

        if filter_by_column_names:
            if pad_columns:
                data_frame = self.pad_time_series_columns(columns, data_frame)
            else:
                data_frame = self.filter_time_series_by_columns(columns,
                                                                data_frame)

        return data_frame

    def filter_time_series_by_holidays(self, data_frame, cal='FX',
                                       holidays_list=[]):
        """Removes holidays from a given time series

        Parameters
        ----------
        data_frame : DataFrame
            data frame to be filtered
        cal : str
            business calendar to use

        Returns
        -------
        DataFrame
        """

        # Optimal case for weekdays: remove Saturday and Sunday
        if (cal == 'WEEKDAY' or cal == 'WKY'):
            return data_frame[data_frame.index.dayofweek <= 4]

        # Select only those holidays in the sample
        holidays_start = self._calendar.get_holidays(
            data_frame.index[0],
            data_frame.index[-1], cal,
            holidays_list=holidays_list)

        if holidays_start.size == 0:
            return data_frame

        holidays_end = holidays_start + np.timedelta64(1, 'D')

        # floored_dates = data_frame.index.normalize()
        #
        # filter_by_index_start = floored_dates.searchsorted(holidays_start)
        # filter_by_index_end = floored_dates.searchsorted(holidays_end)
        #
        # indices_to_keep = []
        #
        # if filter_by_index_end[0] == 0:
        #     counter = filter_by_index_end[0] + 1
        #     start_index = 1
        # else:
        #     counter = 0
        #     start_index = 0
        #
        # for i in range(start_index, len(holidays_start)):
        #     indices = list(range(counter, filter_by_index_start[i] - 1))
        #     indices_to_keep = indices_to_keep + indices
        #
        #     counter = filter_by_index_end[i] + 1
        #
        # indices = list(range(counter, len(floored_dates)))
        # indices_to_keep = indices_to_keep + indices
        #
        # data_frame_filtered = data_frame[indices_to_keep]

        if data_frame.index.tz is None:
            holidays_start = holidays_start.tz_localize(None)
            holidays_end = holidays_end.tz_localize(None)

        data_frame_left = data_frame
        data_frame_filtered = []

        for i in range(0, len(holidays_start)):
            data_frame_temp = data_frame_left[
                data_frame_left.index < holidays_start[i]]
            data_frame_left = data_frame_left[
                data_frame_left.index >= holidays_end[i]]

            data_frame_filtered.append(data_frame_temp)

        data_frame_filtered.append(data_frame_left)

        return pd.concat(data_frame_filtered)

    def filter_time_series_by_date(self, start_date, finish_date, data_frame):
        """Filter time series by start/finish dates

        Parameters
        ----------
        start_date : DateTime
            start date of calendar
        finish_date : DataTime
            finish date of calendar
        data_frame : DataFrame
            data frame to be filtered

        Returns
        -------
        DataFrame
        """
        offset = 0  # inclusive

        return self.filter_time_series_by_date_offset(start_date, finish_date,
                                                      data_frame, offset,
                                                      exclude_start_end=False)

    def filter_time_series_by_days(self, days, data_frame):
        """Filter time series by start/finish dates

        Parameters
        ----------
        start_date : DateTime
            start date of calendar
        finish_date : DataTime
            finish date of calendar
        data_frame : DataFrame
            data frame to be filtered

        Returns
        -------
        DataFrame
        """
        offset = 0  # inclusive

        finish_date = datetime.datetime.utcnow()
        start_date = finish_date - timedelta(days=days)

        return self.filter_time_series_by_date_offset(start_date, finish_date,
                                                      data_frame, offset)

    def filter_time_series_by_date_exc(self, start_date, finish_date,
                                       data_frame):
        """Filter time series by start/finish dates (exclude start &
        finish dates)

        Parameters
        ----------
        start_date : DateTime
            start date of calendar
        finish_date : DataTime
            finish date of calendar
        data_frame : DataFrame
            data frame to be filtered

        Returns
        -------
        DataFrame
        """
        offset = 1  # exclusive of start finish date

        return self.filter_time_series_by_date_offset(start_date, finish_date,
                                                      data_frame, offset,
                                                      exclude_start_end=True)

        # try:
        #     # filter by dates for intraday data
        #     if(start_date is not None):
        #         data_frame = data_frame.loc[start_date <= data_frame.index]
        #
        #     if(finish_date is not None):
        #         # filter by start_date and finish_date
        #         data_frame = data_frame.loc[data_frame.index <= finish_date]
        # except:
        #     # filter by dates for daily data
        #     if(start_date is not None):
        #         data_frame = data_frame.loc[start_date.date() <= data_frame.index]
        #
        #     if(finish_date is not None):
        #         # filter by start_date and finish_date
        #         data_frame = data_frame.loc[data_frame.index <= finish_date.date()]
        #
        # return data_frame

    def filter_time_series_by_date_offset(self, start_date, finish_date,
                                          data_frame, offset,
                                          exclude_start_end=False):
        """Filter time series by start/finish dates (and an offset)

        Parameters
        ----------
        start_date : DateTime
            start date of calendar
        finish_date : DataTime
            finish date of calendar
        data_frame : DataFrame
            data frame to be filtered
        offset : int
            offset to be applied

        Returns
        -------
        DataFrame
        """

        if hasattr(data_frame.index, 'tz'):
            if data_frame.index.tz is not None:

                # If the start/finish dates are timezone naive, overwrite with
                # the DataFrame timezone
                if not (isinstance(start_date, str)):
                    start_date = start_date.replace(tzinfo=data_frame.index.tz)

                if not (isinstance(finish_date, str)):
                    finish_date = finish_date.replace(
                        tzinfo=data_frame.index.tz)
            else:
                # Otherwise remove timezone from start_date/finish_date
                if not (isinstance(start_date, str)):
                    try:
                        start_date = start_date.replace(tzinfo=None)
                    except:
                        pass

                if not (isinstance(finish_date, str)):
                    try:
                        finish_date = finish_date.replace(tzinfo=None)
                    except:
                        pass

        if 'int' in str(data_frame.index.dtype):
            return data_frame

        try:
            data_frame = self.filter_time_series_aux(start_date, finish_date,
                                                     data_frame, offset)
        except:
            # start_date = start_date.date()
            # finish_date = finish_date.date()
            # if isinstance(start_date, str):
            #     # format expected 'Jun 1 2005 01:33', '%b %d %Y %H:%M'
            #     try:
            #         start_date = datetime.datetime.strptime(start_date,
            #         '%b %d %Y %H:%M')
            #     except:
            #         i = 0
            #
            # if isinstance(finish_date, str):
            #     # format expected 'Jun 1 2005 01:33', '%b %d %Y %H:%M'
            #     try:
            #         finish_date = datetime.datetime.strptime(finish_date,
            #         '%b %d %Y %H:%M')
            #     except:
            #         i = 0

            # try:
            #     start_date = start_date.date()
            # except: pass
            #
            # try:
            #     finish_date = finish_date.date()
            # except: pass

            # if we have dates stored as opposed to TimeStamps (ie. daily data),
            # we use a simple (slower) method
            # for filtering daily data
            if start_date is not None:
                if exclude_start_end:
                    data_frame = data_frame.loc[start_date < data_frame.index]
                else:
                    data_frame = data_frame.loc[start_date <= data_frame.index]

            if finish_date is not None:
                if exclude_start_end:
                    data_frame = data_frame.loc[data_frame.index < finish_date]
                else:
                    # filter by start_date and finish_date
                    data_frame = data_frame.loc[
                        data_frame.index <= finish_date]

        return data_frame

    def filter_time_series_aux(self, start_date, finish_date, data_frame,
                               offset):
        """Filter time series by start/finish dates (and an offset)

        Parameters
        ----------
        start_date : DateTime
            start date of calendar

        finish_date : DataTime
            finish date of calendar

        data_frame : DataFrame
            data frame to be filtered

        offset : int (not implemented!)
            offset to be applied

        Returns
        -------
        DataFrame
        """

        # start_index = 0
        # finish_index = len(data_frame.index) - offset

        # filter by dates for intraday data
        # if(start_date is not None):
        #     start_index = data_frame.index.searchsorted(start_date)
        #
        #     if (0 <= start_index + offset < len(data_frame.index)):
        #         start_index = start_index + offset
        #
        #         # data_frame = data_frame[start_date < data_frame.index]
        #
        # if(finish_date is not None):
        #     finish_index = data_frame.index.searchsorted(finish_date)
        #
        #     if (0 <= finish_index - offset < len(data_frame.index)):
        #         finish_index = finish_index - offset
        # CAREFUL: need + 1 otherwise will only return 1 less than usual
        # return data_frame.iloc[start_date:finish_date]

        # Just use pandas, quicker and simpler code!
        if data_frame is None:
            return None

        # Slower method..
        # return data_frame.loc[start_date:finish_date]

        # Much faster, start and finish dates are inclusive
        return data_frame[(data_frame.index >= start_date) & (
                    data_frame.index <= finish_date)]

    def filter_time_series_by_time_of_day_timezone(self, hour, minute,
                                                   data_frame,
                                                   timezone_of_snap='UTC'):

        old_tz = data_frame.index.tz
        data_frame = data_frame.tz_convert(pytz.timezone(timezone_of_snap))

        data_frame = data_frame[data_frame.index.minute == minute]
        data_frame = data_frame[data_frame.index.hour == hour]

        data_frame = data_frame.tz_convert(old_tz)

        return data_frame

    def filter_time_series_by_time_of_day(self, hour, minute, data_frame,
                                          in_tz=None, out_tz=None):
        """Filter time series by time of day

        Parameters
        ----------
        hour : int
            hour of day
        minute : int
            minute of day
        data_frame : DataFrame
            data frame to be filtered
        in_tz : str (optional)
            time zone of input data frame
        out_tz : str (optional)
            time zone of output data frame

        Returns
        -------
        DataFrame
        """
        if out_tz is not None:
            try:
                if in_tz is not None:
                    data_frame = data_frame.tz_localize(pytz.timezone(in_tz))
            except:
                data_frame = data_frame.tz_convert(pytz.timezone(in_tz))

            data_frame = data_frame.tz_convert(pytz.timezone(out_tz))

            # change internal representation of time
            data_frame.index = pd.DatetimeIndex(data_frame.index.values)

        data_frame = data_frame[data_frame.index.minute == minute]
        data_frame = data_frame[data_frame.index.hour == hour]

        return data_frame

    def filter_time_series_by_minute_of_hour(self, minute, data_frame,
                                             in_tz=None, out_tz=None):
        """Filter time series by minute of hour

        Parameters
        ----------
        minute : int
            minute of hour
        data_frame : DataFrame
            data frame to be filtered
        in_tz : str (optional)
            time zone of input data frame
        out_tz : str (optional)
            time zone of output data frame

        Returns
        -------
        DataFrame
        """
        if out_tz is not None:
            if in_tz is not None:
                data_frame = data_frame.tz_localize(pytz.timezone(in_tz))

            data_frame = data_frame.tz_convert(pytz.timezone(out_tz))

            # change internal representation of time
            data_frame.index = pd.DatetimeIndex(data_frame.index.values)

        data_frame = data_frame[data_frame.index.minute == minute]

        return data_frame

    def filter_time_series_between_hours(self, start_hour, finish_hour,
                                         data_frame):
        """Filter time series between hours of the day

        Parameters
        ----------
        start_hour : int
            start of hour filter
        finish_hour : int
            finish of hour filter
        data_frame : DataFrame
            data frame to be filtered

        Returns
        -------
        DataFrame
        """

        data_frame = data_frame[data_frame.index.hour <= finish_hour]
        data_frame = data_frame[data_frame.index.hour >= start_hour]

        return data_frame

    def filter_time_series_by_columns(self, columns, data_frame):
        """Filter time series by certain columns

        Parameters
        ----------
        columns : list(str)
            start of hour filter
        data_frame : DataFrame
            data frame to be filtered

        Returns
        -------
        DataFrame
        """
        if data_frame is not None and columns is not None:
            return data_frame[columns]

        return None

    def pad_time_series_columns(self, columns, data_frame):
        """Selects time series from a dataframe and if necessary creates
        empty columns

        Parameters
        ----------
        columns : str
            columns to be included with this keyword
        data_frame : DataFrame
            data frame to be filtered

        Returns
        -------
        DataFrame
        """
        old_columns = data_frame.columns.tolist()

        common_columns = [val for val in columns if val in old_columns]
        uncommon_columns = [val for val in columns if val not in old_columns]
        uncommon_columns = [str(x) for x in uncommon_columns]

        data_frame = data_frame[common_columns]

        if len(uncommon_columns) > 0:
            logger = LoggerManager().getLogger(__name__)

            logger.info(
                "Padding missing columns...")  # " + str(uncommon_columns))

            new_data_frame = pd.DataFrame(index=data_frame.index,
                                          columns=uncommon_columns)

            data_frame = pd.concat([data_frame, new_data_frame], axis=1)

            # Force new columns to float NaNs (not objects which causes
            # problems with newer pandas versions)
            # or to NaT if they are date columns
            for u in uncommon_columns:
                is_date = False

                for c in constants.always_date_columns:
                    if c in u:
                        is_date = True

                if is_date:
                    data_frame[u] = np.datetime64('NaT')
                else:
                    data_frame[u] = np.nan

            # SLOW method below
            # for x in uncommon_columns: data_frame.loc[:,x] = np.nan

        # Get columns in same order again
        data_frame = data_frame[columns]

        return data_frame

    def filter_time_series_by_excluded_keyword(self, keyword, data_frame):
        """Filter time series to exclude columns which contain keyword

        Parameters
        ----------
        keyword : str
            columns to be excluded with this keyword

        data_frame : DataFrame
            data frame to be filtered

        Returns
        -------
        DataFrame
        """

        if not (isinstance(keyword, list)):
            keyword = [keyword]

        columns = []

        for k in keyword:
            columns.append(
                [elem for elem in data_frame.columns if k not in elem])

        columns = self._calendar.flatten_list_of_lists(columns)

        return self.filter_time_series_by_columns(columns, data_frame)

    def filter_time_series_by_included_keyword(self, keyword, data_frame,
                                               ignore_case=False):
        """Filter time series to include columns which contain keyword

        Parameters
        ----------
        keyword : str
            columns to be included with this keyword

        data_frame : DataFrame
            data frame to be filtered

        Returns
        -------
        DataFrame
        """

        if not (isinstance(keyword, list)):
            keyword = [keyword]

        columns = []

        if ignore_case:
            for k in keyword:
                columns.append([elem for elem in data_frame.columns if
                                k.lower() in elem.lower()])
        else:
            for k in keyword:
                columns.append(
                    [elem for elem in data_frame.columns if k in elem])

        columns = self._calendar.flatten_list_of_lists(columns)

        return self.filter_time_series_by_columns(columns, data_frame)

    def filter_time_series_by_minute_freq(self, freq, data_frame):
        """Filter time series where minutes correspond to certain minute filter

        Parameters
        ----------
        freq : int
            minute frequency to be filtered

        data_frame : DataFrame
            data frame to be filtered

        Returns
        -------
        DataFrame
        """
        return data_frame.loc[data_frame.index.minute % freq == 0]

    def create_tickers_fields_list(self, md_request):
        """Creates a list of tickers concatenated with fields from a
        MarketDataRequest

        Parameters
        ----------
        md_request : MarketDataRequest
            request to be expanded

        Returns
        -------
        list(str)
        """
        tickers = md_request.tickers
        fields = md_request.fields

        if isinstance(tickers, str): tickers = [tickers]
        if isinstance(fields, str): fields = [fields]

        tickers_fields_list = []

        # Create ticker.field combination for series we wish to return
        for f in fields:
            for t in tickers:
                tickers_fields_list.append(t + '.' + f)

        return tickers_fields_list

    def resample_time_series(self, data_frame, freq):
        return data_frame.asfreq(freq, method='pad')

    def resample_time_series_frequency(self, data_frame, data_resample_freq,
                                       data_resample_type='mean',
                                       fill_empties=False):

        # Should we take the mean, first, last in our resample
        if data_resample_type == 'mean':
            data_frame_r = data_frame.resample(data_resample_freq).mean()
        elif data_resample_type == 'first':
            data_frame_r = data_frame.resample(data_resample_freq).first()
        elif data_resample_type == 'last':
            data_frame_r = data_frame.resample(data_resample_freq).last()
        else:
            # TODO implement other types
            return

        if fill_empties == True:
            data_frame, data_frame_r = data_frame.align(data_frame_r,
                                                        join='left', axis=0)
            data_frame_r = data_frame_r.fillna(method='ffill')

        return data_frame_r

    def make_FX_1_min_working_days(self, data_frame):
        data_frame = data_frame.resample('1min').mean()
        data_frame = self.filter_time_series_by_holidays(data_frame, 'FX')
        data_frame = data_frame.fillna(method='ffill')
        data_frame = self.remove_out_FX_out_of_hours(data_frame)

        return data_frame

    def remove_out_FX_out_of_hours(self, data_frame):
        """Filtered a time series for FX hours (ie. excludes
        22h GMT Fri - 19h GMT Sun and New Year's Day)

        Parameters
        ----------
        data_frame : DataFrame
            data frame with FX prices

        Returns
        -------
        list(str)
        """
        # assume data_frame is in GMT time
        # remove Fri after 22:00 GMT
        # remove Sat
        # remove Sun before 19:00 GMT

        # Monday = 0, ..., Sunday = 6
        data_frame = data_frame[~((data_frame.index.dayofweek == 4) & (
                    data_frame.index.hour > 22))]
        data_frame = data_frame[~((data_frame.index.dayofweek == 5))]
        data_frame = data_frame[~((data_frame.index.dayofweek == 6) & (
                    data_frame.index.hour < 19))]
        data_frame = data_frame[
            ~((data_frame.index.day == 1) & (data_frame.index.month == 1))]

        return data_frame

    def remove_duplicate_indices(self, df):
        return df[~df.index.duplicated(keep='first')]

    def mask_time_series_by_time(self, df, time_list, time_zone):
        """ Mask a time series by time of day and time zone specified
        e.g. given a time series minutes data
             want to keep data at specific time periods every day with a
             considered time zone

        Parameters
        ----------
        df : DateTime
            time series needed to be masked
        time_list : list of tuples
            deciding the time periods which we want to keep the data on each day
            e.g. time_list =
                [('01:08', '03:02'),('12:24','12:55'),('17:31','19:24')]
            * Note: assume no overlapping of these tuples
        time_zone: str
            e.g. 'Europe/London'

        Returns
        -------
        DataFrame  (which the time zone is 'UTC')
        """

        # Change the time zone from 'UTC' to a given one
        df.index = df.index.tz_convert(time_zone)
        df_mask = pd.DataFrame(0, index=df.index, columns=['mask'])

        # Mask data with each given tuple
        for i in range(0, len(time_list)):
            start_hour = int(time_list[i][0].split(':')[0])
            start_minute = int(time_list[i][0].split(':')[1])
            end_hour = int(time_list[i][1].split(':')[0])
            end_minute = int(time_list[i][1].split(':')[1])

            # E.g. if tuple is ('01:08', '03:02'),
            # take hours in target - take values in [01:00,04:00]
            narray = np.where(
                df.index.hour.isin(range(start_hour, end_hour + 1)), 1, 0)
            df_mask_temp = pd.DataFrame(index=df.index,
                                        columns=df_mask.columns.tolist(),
                                        data=narray)

            # Remove minutes not in target -
            # remove values in [01:00,01:07], [03:03,03:59]
            narray = np.where(((df.index.hour == start_hour) & (
                        df.index.minute < start_minute)), 0, 1)
            df_mask_temp = df_mask_temp * pd.DataFrame(
                index=df.index,
                columns=df_mask.columns.tolist(),
                data=narray)
            narray = np.where(
                (df.index.hour == end_hour) & (df.index.minute > end_minute),
                0, 1)
            df_mask_temp = df_mask_temp * pd.DataFrame(
                index=df.index,
                columns=df_mask.columns.tolist(),
                data=narray)

            # Collect all the periods we want to keep the data
            df_mask = df_mask + df_mask_temp

        narray = np.where(df_mask == 1, df, 0)
        df = pd.DataFrame(index=df.index, columns=df.columns.tolist(),
                          data=narray)
        df.index = df.index.tz_convert('UTC')  # change the time zone to 'UTC'

        return df
