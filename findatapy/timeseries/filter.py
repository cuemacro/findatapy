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

from findatapy.util.loggermanager import LoggerManager

from pandas.tseries.offsets import CustomBusinessDay

import numpy as np
import pandas
import pytz

class Filter(object):
    """Functions for filtering time series by dates and columns.

    This class is used extensively in both findatapy and finmarketpy.

    """

    _time_series_cache = {} # shared across all instances of object!

    def __init__(self):
        # self.config = ConfigManager()
        self.logger = LoggerManager().getLogger(__name__)
        return

    def filter_time_series(self, market_data_request, data_frame, pad_columns = False):
        """Filters a time series given a set of criteria (like start/finish date and tickers)

        Parameters
        ----------
        market_data_request : MarketDataRequest
            defining time series filtering
        data_frame : DataFrame
            time series to be filtered
        pad_columns : boolean
            true, non-existant columns with nan

        Returns
        -------
        DataFrame
        """
        start_date = market_data_request.start_date
        finish_date = market_data_request.finish_date

        data_frame = self.filter_time_series_by_date(start_date, finish_date, data_frame)

        # filter by ticker.field combinations requested
        columns = self.create_tickers_fields_list(market_data_request)

        if (pad_columns):
            data_frame = self.pad_time_series_columns(columns, data_frame)
        else:
            data_frame = self.filter_time_series_by_columns(columns, data_frame)

        return data_frame

    def create_calendar_bus_days(self, start_date, end_date, cal = 'FX'):
        """Creates a calendar of business days

        Parameters
        ----------
        start_date : DateTime
            start date of calendar
        end_date : DataFrame
            finish date of calendar
        cal : str
            business calendar to use

        Returns
        -------
        list
        """
        hols = self.get_holidays(start_date, end_date, cal)
        index = pandas.bdate_range(start=start_date, end=end_date, freq='D')

        return [x for x in index if x not in hols]

    def get_holidays(self, start_date, end_date, cal = 'FX'):
        """Gets the holidays for a given calendar

        Parameters
        ----------
        start_date : DateTime
            start date of calendar
        end_date : DataFrame
            finish date of calendar
        cal : str
            business calendar to use

        Returns
        -------
        list
        """

        # TODO use Pandas CustomBusinessDays to get more calendars
        holidays_list = []

        if cal == 'FX':
            # filter for Christmas & New Year's Day
            for i in range(1970, 2020):
                holidays_list.append(str(i) + "-12-25")
                holidays_list.append(str(i) + "-01-01")

        if cal == 'WEEKDAY':
            bday = CustomBusinessDay(weekmask='Sat Sun')

            holidays_list = pandas.date_range(start_date, end_date, freq=bday)

        # holidays_list = pandas.to_datetime(holidays_list).order()
        holidays_list = pandas.to_datetime(holidays_list).sort_values()

        # floor start date
        start = np.datetime64(start_date) - np.timedelta64(1, 'D')

        # ceiling end date
        end = np.datetime64(end_date) + np.timedelta64(1, 'D')

        holidays_list = [x for x in holidays_list if x >= start and x <= end]

        return pandas.to_datetime(holidays_list)

    def filter_time_series_by_holidays(self, data_frame, cal = 'FX'):
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

        # optimal case for weekdays: remove Saturday and Sunday
        if (cal == 'WEEKDAY'):
            return data_frame.ix[data_frame.index.dayofweek <= 4]

        # select only those holidays in the sample
        holidays_start = self.get_holidays(data_frame.index[0], data_frame.index[-1], cal)

        if(holidays_start.size == 0):
            return data_frame

        holidays_end = holidays_start + np.timedelta64(1,'D')

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
        # data_frame_filtered = data_frame.ix[indices_to_keep]

        data_frame_left = data_frame
        data_frame_filtered = []

        for i in range(0, len(holidays_start)):
            data_frame_temp = data_frame_left.ix[data_frame_left.index < holidays_start[i]]
            data_frame_left = data_frame_left.ix[data_frame_left.index >= holidays_end[i]]

            data_frame_filtered.append(data_frame_temp)

        data_frame_filtered.append(data_frame_left)

        return pandas.concat(data_frame_filtered)

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
        offset = 0 # inclusive

        return self.filter_time_series_by_date_offset(start_date, finish_date, data_frame, offset, exclude_start_end = False)

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
        offset = 0 # inclusive

        finish_date = datetime.datetime.utcnow()
        start_date = finish_date - timedelta(days=days)
        return self.filter_time_series_by_date_offset(start_date, finish_date, data_frame, offset)

    def filter_time_series_by_date_exc(self, start_date, finish_date, data_frame):
        """Filter time series by start/finish dates (exclude start & finish dates)

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
        offset = 1 # exclusive of start finish date

        return self.filter_time_series_by_date_offset(start_date, finish_date, data_frame, offset, exclude_start_end = True)

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

    def filter_time_series_by_date_offset(self, start_date, finish_date, data_frame, offset, exclude_start_end = False):
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
        try:
            data_frame = self.filter_time_series_aux(start_date, finish_date, data_frame, offset)
        except:
            # start_date = start_date.date()
            # finish_date = finish_date.date()
            # if isinstance(start_date, str):
            #     # format expected 'Jun 1 2005 01:33', '%b %d %Y %H:%M'
            #     try:
            #         start_date = datetime.datetime.strptime(start_date, '%b %d %Y %H:%M')
            #     except:
            #         i = 0
            #
            # if isinstance(finish_date, str):
            #     # format expected 'Jun 1 2005 01:33', '%b %d %Y %H:%M'
            #     try:
            #         finish_date = datetime.datetime.strptime(finish_date, '%b %d %Y %H:%M')
            #     except:
            #         i = 0

            # try:
            #     start_date = start_date.date()
            # except: pass
            #
            # try:
            #     finish_date = finish_date.date()
            # except: pass

            # if we have dates stored as opposed to TimeStamps (ie. daily data), we use a simple (slower) method
            # for filtering daily data
            if(start_date is not None):
                if exclude_start_end:
                    data_frame = data_frame.loc[start_date < data_frame.index]
                else:
                    data_frame = data_frame.loc[start_date <= data_frame.index]

            if(finish_date is not None):
                if exclude_start_end:
                    data_frame = data_frame.loc[data_frame.index < finish_date]
                else:
                    # filter by start_date and finish_date
                    data_frame = data_frame.loc[data_frame.index <= finish_date]

        return data_frame

    def filter_time_series_aux(self, start_date, finish_date, data_frame, offset):
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
        #         # data_frame = data_frame.ix[start_date < data_frame.index]
        #
        # if(finish_date is not None):
        #     finish_index = data_frame.index.searchsorted(finish_date)
        #
        #     if (0 <= finish_index - offset < len(data_frame.index)):
        #         finish_index = finish_index - offset
        # CAREFUL: need + 1 otherwise will only return 1 less than usual
        # return data_frame.iloc[start_date:finish_date]

        # just use pandas, quicker and simpler code!
        if data_frame is None:
            return None

        return data_frame.loc[start_date:finish_date]


    def filter_time_series_by_time_of_day(self, hour, minute, data_frame, in_tz = None, out_tz = None):
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
            if in_tz is not None:
                data_frame = data_frame.tz_localize(pytz.timezone(in_tz))

            data_frame = data_frame.tz_convert(pytz.timezone(out_tz))

            # change internal representation of time
            data_frame.index = pandas.DatetimeIndex(data_frame.index.values)

        data_frame = data_frame[data_frame.index.minute == minute]
        data_frame = data_frame[data_frame.index.hour == hour]

        return data_frame

    def filter_time_series_by_minute_of_hour(self, minute, data_frame, in_tz = None, out_tz = None):
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
            data_frame.index = pandas.DatetimeIndex(data_frame.index.values)

        data_frame = data_frame[data_frame.index.minute == minute]

        return data_frame

    def filter_time_series_between_hours(self, start_hour, finish_hour, data_frame):
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
        return data_frame[columns]

    def pad_time_series_columns(self, columns, data_frame):
        """Selects time series from a dataframe and if necessary creates empty columns

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
        old_columns = data_frame.columns

        common_columns = [val for val in columns if val in old_columns]
        uncommon_columns = [val for val in columns if val not in old_columns]
        uncommon_columns = [str(x) for x in uncommon_columns]

        data_frame = data_frame[common_columns]

        if len(uncommon_columns) > 0:
            self.logger.info("Padding missing columns...") # " + str(uncommon_columns))

            new_data_frame = pandas.DataFrame(index=data_frame.index, columns=uncommon_columns)

            data_frame = pandas.concat([data_frame, new_data_frame], axis=1)

            # SLOW method below
            # for x in uncommon_columns: data_frame.loc[:,x] = np.nan

        # get columns in same order again
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
        columns = [elem for elem in data_frame.columns if keyword not in elem]

        return self.filter_time_series_by_columns(columns, data_frame)

    def filter_time_series_by_included_keyword(self, keyword, data_frame):
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
        columns = [elem for elem in data_frame.columns if keyword in elem]

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

    def create_tickers_fields_list(self, market_data_request):
        """Creates a list of tickers concatenated with fields from a MarketDataRequest

        Parameters
        ----------
        market_data_request : MarketDataRequest
            request to be expanded

        Returns
        -------
        list(str)
        """
        tickers = market_data_request.tickers
        fields = market_data_request.fields

        if isinstance(tickers, str): tickers = [tickers]
        if isinstance(fields, str): fields = [fields]

        tickers_fields_list = []

        # create ticker.field combination for series we wish to return
        for f in fields:
            for t in tickers:
                tickers_fields_list.append(t + '.' + f)

        return tickers_fields_list

    def resample_time_series(self, data_frame, freq):
        return data_frame.asfreq(freq, method = 'pad')

    def resample_time_series_frequency(self, data_frame, data_resample_freq,
                                       data_resample_type = 'mean', fill_empties = False):
        # should we take the mean, first, last in our resample
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
            data_frame, data_frame_r = data_frame.align(data_frame_r, join='left', axis=0)
            data_frame_r = data_frame_r.fillna(method='ffill')

        return data_frame_r

    def make_FX_1_min_working_days(self, data_frame):
        data_frame = data_frame.resample('1min').mean()
        data_frame = self.filter_time_series_by_holidays(data_frame, 'FX')
        data_frame = data_frame.fillna(method='ffill')
        data_frame = self.remove_out_FX_out_of_hours(data_frame)

        return data_frame

    def remove_out_FX_out_of_hours(self, data_frame):
        """Filtered a time series for FX hours (ie. excludes 22h GMT Fri - 19h GMT Sun)

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
        data_frame = data_frame.ix[~((data_frame.index.dayofweek == 4) & (data_frame.index.hour > 22))]
        data_frame = data_frame.ix[~((data_frame.index.dayofweek == 5))]
        data_frame = data_frame.ix[~((data_frame.index.dayofweek == 6)& (data_frame.index.hour < 19))]

        return data_frame

#######################################################################################################################

import datetime
from datetime import timedelta

import numpy
import pandas
import pandas.tseries.offsets

from findatapy.timeseries.timezone import Timezone

from pandas.tseries.offsets import BDay
from pandas.tseries.offsets import CustomBusinessDay

from findatapy.timeseries.filter import Filter

class Calendar(object):
    """Provides calendar based functions for working out options expiries. Note, that in practice, we would often take
    into account market holidays.

    """

    def get_business_days_tenor(self, tenor):
        if tenor == '1W':
            return 5
        elif tenor == 'ON':
            return 1
        elif tenor == '1M':
            return 20
        elif tenor == '3M':
            return 60
        elif tenor == '6M':
            return 120
        elif tenor == '1Y':
            return 252

    def get_dates_from_tenors(self, start, end, calendar, tenor):
        freq = str(self.get_business_days_tenor(tenor)) + "B"
        return pandas.DataFrame(index=pandas.bdate_range(start, end, freq=freq))

    def get_expiries_from_dates(self, date_time_index, calendar, tenor):
        freq = self.get_business_days_tenor(tenor)

        return pandas.DatetimeIndex(date_time_index + BDay(freq))

    def align_to_NY_cut_in_UTC(self, date_time):

        tstz = Timezone()
        date_time = tstz.localise_index_as_new_york_time(date_time)
        date_time.index = date_time.index + timedelta(hours=10)

        return tstz.convert_index_aware_to_UTC_time(date_time)

    def floor_date(self, data_frame):
        data_frame.index = data_frame.index.normalize()

        return data_frame

    def create_bus_day(self, start, end):
        return pandas.date_range(start, end, freq='B')

    def get_bus_day_of_month(self, date, cal = 'FX'):
        """ get_bus_day_of_month(date = list of dates, cal = calendar name)

            returns the business day of the month (ie. 3rd Jan, on a Monday,
            would be the 1st business day of the month
        """
        filter = Filter()

        try:
            date = date.normalize() # strip times off the dates - for business dates just want dates!
        except: pass

        start = pandas.to_datetime(datetime.datetime(date.year[0], date.month[0], 1))
        end = datetime.datetime.today()#pandas.to_datetime(datetime.datetime(date.year[-1], date.month[-1], date.day[-1]))

        holidays = filter.get_holidays(start, end, cal)

        bday = CustomBusinessDay(holidays=holidays, weekmask='Mon Tue Wed Thu Fri')

        bus_dates = pandas.date_range(start, end, freq=bday)

        month = bus_dates.month

        work_day_index = numpy.zeros(len(bus_dates))
        work_day_index[0] = 1

        for i in range(1, len(bus_dates)):
            if month[i] == month[i-1]:
                work_day_index[i] = work_day_index[i-1] + 1
            else:
                work_day_index[i] = 1

        bus_day_of_month = work_day_index[bus_dates.searchsorted(date)]

        return bus_day_of_month

# functions to test class
if __name__ == '__main__':

    logger = LoggerManager.getLogger(__name__)

    tsf = Filter()

    if False:
        start = pandas.to_datetime('2000-01-01')
        end = pandas.to_datetime('2020-01-01')

        logger.info('Get FX holidays')
        hols = tsf.get_holidays(start, end, cal='FX')
        print(hols)

        logger.info('Get business days, excluding holidays')
        bus_days = tsf.create_calendar_bus_days(start, end, cal='FX')
        print(bus_days)

    if False:
        logger.info('Remove out of hours')

        rng = pandas.date_range('01 Jan 2014', '05 Jan 2014', freq='1min')
        intraday_vals = pandas.DataFrame(data=pandas.np.random.randn(len(rng)), index=rng)

        intraday_vals = tsf.resample_time_series(intraday_vals, '60min')
        intraday_vals = tsf.remove_out_FX_out_of_hours(intraday_vals)

        print(intraday_vals)

    if True:
        logger.info('Remove holiday days')

        rng = pandas.date_range('01 Jan 2007', '05 Jan 2014', freq='1min')
        intraday_vals = pandas.DataFrame(data=pandas.np.random.randn(len(rng)), index=rng)

        import cProfile

        cProfile.run("intraday_vals = tsf.filter_time_series_by_holidays(intraday_vals, 'FX')")

        print(intraday_vals)

