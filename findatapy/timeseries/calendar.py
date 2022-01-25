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

import re

import numpy as np
import pandas as pd

import datetime
from datetime import timedelta

from pandas.tseries.offsets import BDay, CustomBusinessDay, Day, \
    CustomBusinessMonthEnd, DateOffset
from findatapy.timeseries.timezone import Timezone

from findatapy.util.dataconstants import DataConstants
from findatapy.util.loggermanager import LoggerManager

constants = DataConstants()

# To speed up CustomBusinessDay
# https://stackoverflow.com/questions/31523302/performance-of-pandas-custom-business-day-offset

class Calendar(object):
    """Provides calendar based functions for working out options expiries,
    holidays etc. Note, that at present, the expiry _calculations are
    approximate.

    """

    # Approximate mapping from tenor to number of business days
    _tenor_bus_day_dict = {'ON': 1,
                           'TN': 2,
                           '1W': 5,
                           '2W': 10,
                           '3W': 15,
                           '1M': 20,
                           '2M': 40,
                           '3M': 60,
                           '4M': 80,
                           '6M': 120,
                           '9M': 180,
                           '1Y': 252,
                           '2Y': 252 * 2,
                           '3Y': 252 * 3,
                           '5Y': 252 * 5
                           }

    def __init__(self):
        self._holiday_df = pd.read_parquet(constants.holidays_parquet_table)

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

    def _get_full_cal(self, cal):
        holidays_list = []

        # Calendars which have been hardcoded in the parquet file (which users
        # may also edit)
        if len(cal) == 6:
            # Eg. EURUSD (load EUR and USD calendars and combine the holidays)
            holidays_list.append(
                [self._get_full_cal(cal[0:3]), self._get_full_cal(cal[3:6])])
        elif len(cal) == 9:
            holidays_list.append(
                [self._get_full_cal(cal[0:3]), self._get_full_cal(cal[3:6]),
                 self._get_full_cal(cal[6:9])])
        else:
            if cal == 'FX' or cal == 'NYX':
                # Filter for Christmas & New Year's Day
                for i in range(1999, 2025):
                    holidays_list.append(pd.Timestamp(str(i) + "-12-25"))
                    holidays_list.append(pd.Timestamp(str(i) + "-01-01"))

            elif cal == 'NYD' or cal == 'NEWYEARSDAY':
                # Filter for New Year's Day
                for i in range(1999, 2025):
                    holidays_list.append(pd.Timestamp(str(i) + "-01-01"))

            elif cal == 'WDY' or cal == 'WEEKDAY':
                bday = CustomBusinessDay(weekmask='Sat Sun')

                holidays_list.append([x for x in pd.date_range('01 Jan 1999',
                                                               '31 Dec 2025',
                                                               freq=bday)])
            elif cal == 'WKD':  #
                pass
                # holidays_list.append()

            else:
                label = cal + ".holiday-dates"

                try:
                    holidays_list = self._holiday_df[label].dropna().tolist()
                except:
                    logger = LoggerManager().getLogger(__name__)
                    logger.warning(cal + " holiday calendar not found.")

        return holidays_list

    def create_calendar_bus_days(self, start_date, end_date, cal='FX'):
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
        hols = self.get_holidays(start_date=start_date, end_date=end_date,
                                 cal=cal)

        return pd.bdate_range(start=start_date, end=end_date, freq='D',
                              holidays=hols)

    def get_holidays(self, start_date=None, end_date=None, cal='FX',
                     holidays_list=[]):
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
        # holidays_list ,  = []

        # TODO use Pandas CustomBusinessDays to get more calendars
        holidays_list = self._get_full_cal(cal)
        # .append(lst)

        # Use 'set' so we don't have duplicate dates if we are incorporating
        # multiple calendars
        holidays_list = np.array(
            list(set(self.flatten_list_of_lists(holidays_list))))
        holidays_list = pd.to_datetime(
            holidays_list).sort_values().tz_localize('UTC')

        # Floor start date
        if start_date is not None:
            start_date = pd.Timestamp(start_date).floor('D')

            try:
                start_date = start_date.tz_localize('UTC')
            except:
                pass

            holidays_list = holidays_list[(holidays_list >= start_date)]

        if end_date is not None:
            # Ceiling end date
            end_date = pd.Timestamp(end_date).ceil('D')

            try:
                end_date = end_date.tz_localize('UTC')
            except:
                pass

            holidays_list = holidays_list[(holidays_list <= end_date)]

        # Remove all weekends unless it is WEEKDAY calendar
        if cal != 'WEEKDAY' or cal != 'WKY':
            holidays_list = holidays_list[holidays_list.dayofweek <= 4]

        return holidays_list

    def get_business_days_tenor(self, tenor):
        if tenor in self._tenor_bus_day_dict.keys():
            return self._tenor_bus_day_dict[tenor]

        return None

    def get_dates_from_tenors(self, start, end, tenor, cal=None):
        freq = str(self.get_business_days_tenor(tenor)) + "B"
        return pd.DataFrame(index=pd.bdate_range(start, end, freq=freq))

    def get_delta_between_dates(self, date1, date2, unit='days'):
        if unit == 'days':
            return (date2 - date1).days

    def get_delivery_date_from_horizon_date(self, horizon_date, tenor,
                                            cal=None, asset_class='fx'):
        if 'fx' in asset_class:
            tenor_unit = ''.join(re.compile(r'\D+').findall(tenor))
            asset_holidays = self.get_holidays(cal=cal)

            if tenor_unit == 'ON':
                return horizon_date + CustomBusinessDay(n=1,
                                                        holidays=asset_holidays)
            elif tenor_unit == 'TN':
                return horizon_date + CustomBusinessDay(n=2,
                                                        holidays=asset_holidays)
            elif tenor_unit == 'SP':
                pass
            elif tenor_unit == 'SN':
                tenor_unit = 'D'
                tenor_digit = 1
            else:
                tenor_digit = int(''.join(re.compile(r'\d+').findall(tenor)))

            horizon_date = self.get_spot_date_from_horizon_date(
                horizon_date, cal, asset_holidays=asset_holidays)

            if 'SP' in tenor_unit:
                return horizon_date
            elif tenor_unit == 'D':
                return horizon_date + CustomBusinessDay(
                    n=tenor_digit, holidays=asset_holidays)
            elif tenor_unit == 'W':
                return horizon_date + Day(
                    n=tenor_digit * 7) + CustomBusinessDay(
                    n=0, holidays=asset_holidays)
            else:
                if tenor_unit == 'Y':
                    tenor_digit = tenor_digit * 12

                horizon_period_end = horizon_date + CustomBusinessMonthEnd(
                    tenor_digit + 1)
                horizon_floating = horizon_date + DateOffset(
                    months=tenor_digit)

                cbd = CustomBusinessDay(n=1, holidays=asset_holidays)

                delivery_date = []

                if isinstance(horizon_period_end, pd.Timestamp):
                    horizon_period_end = [horizon_period_end]

                if isinstance(horizon_floating, pd.Timestamp):
                    horizon_floating = [horizon_floating]

                for period_end, floating in zip(horizon_period_end,
                                                horizon_floating):
                    if floating < period_end:
                        delivery_date.append(floating - cbd + cbd)
                    else:
                        delivery_date.append(period_end)

                return pd.DatetimeIndex(delivery_date)

    def get_expiry_date_from_horizon_date(self, horizon_date, tenor, cal=None,
                                          asset_class='fx-vol'):
        """Calculates the expiry date of FX options, based on the horizon date,
        the tenor and the holiday calendar associated with the asset.

        Uses expiry rules from Iain Clark's FX option pricing book

        Parameters
        ----------
        horizon_date : pd.Timestamp (collection)
            Horizon date of contract

        tenor : str
            Tenor of the contract

        cal : str
            Holiday calendar (usually related to the asset)

        asset_class : str
            'fx-vol' - FX options (default)

        Returns
        -------
        pd.Timestamp (collection)
        """
        if asset_class == 'fx-vol':

            tenor_unit = ''.join(re.compile(r'\D+').findall(tenor))

            asset_holidays = self.get_holidays(cal=cal)

            if tenor_unit == 'ON':
                tenor_digit = 1;
                tenor_unit = 'D'
            else:
                tenor_digit = int(''.join(re.compile(r'\d+').findall(tenor)))

            if tenor_unit == 'D':
                return horizon_date + CustomBusinessDay(
                    n=tenor_digit, holidays=asset_holidays)
            elif tenor_unit == 'W':
                return horizon_date + Day(
                    n=tenor_digit * 7) + CustomBusinessDay(
                    n=0, holidays=asset_holidays)
            else:
                horizon_date = self.get_spot_date_from_horizon_date(
                    horizon_date, cal, asset_holidays=asset_holidays)

                if tenor_unit == 'M':
                    pass
                elif tenor_unit == 'Y':
                    tenor_digit = tenor_digit * 12

                cbd = CustomBusinessDay(n=1, holidays=asset_holidays)

                horizon_period_end = horizon_date + CustomBusinessMonthEnd(
                    tenor_digit + 1)
                horizon_floating = horizon_date + DateOffset(
                    months=tenor_digit)

                delivery_date = []

                if isinstance(horizon_period_end, pd.Timestamp):
                    horizon_period_end = [horizon_period_end]

                if isinstance(horizon_floating, pd.Timestamp):
                    horizon_floating = [horizon_floating]

                # TODO: double check this!
                for period_end, floating in zip(horizon_period_end,
                                                horizon_floating):
                    if floating < period_end:
                        delivery_date.append(floating - cbd + cbd)
                    else:
                        delivery_date.append(period_end)

                delivery_date = pd.DatetimeIndex(delivery_date)

                return self.get_expiry_date_from_delivery_date(delivery_date,
                                                               cal)

    def _get_settlement_T(self, asset):
        base = asset[0:3]
        terms = asset[3:6]

        if base in ['CAD', 'TRY', 'RUB'] or terms in ['CAD', 'TRY', 'RUB']:
            return 1

        return 2

    def get_spot_date_from_horizon_date(self, horizon_date, asset,
                                        asset_holidays=None):
        base = asset[0:3]
        terms = asset[3:6]

        settlement_T = self._get_settlement_T(asset)

        if asset_holidays is None:
            asset_holidays = self.get_holidays(cal=asset)

        # First adjustment step
        if settlement_T == 2:
            if base in ['MXN', 'ARS', 'CLP'] or terms in ['MXN', 'ARS', 'CLP']:
                horizon_date = horizon_date + BDay(1)
            else:
                if base == 'USD':
                    horizon_date = horizon_date + CustomBusinessDay(
                        holidays=self.get_holidays(cal=terms))
                elif terms == 'USD':
                    horizon_date = horizon_date + CustomBusinessDay(
                        holidays=self.get_holidays(cal=base))
                else:
                    horizon_date = horizon_date + CustomBusinessDay(
                        holidays=asset_holidays)

        if 'USD' not in asset:
            asset_holidays = self.get_holidays(cal='USD' + asset)

        # Second adjustment step - move forward if horizon_date isn't a good
        # business day in base, terms or USD
        if settlement_T <= 2:
            horizon_date = horizon_date + CustomBusinessDay(
                holidays=asset_holidays)

        return horizon_date

    def get_delivery_date_from_spot_date(self, spot_date, cal):
        pass

    def get_expiry_date_from_delivery_date(self, delivery_date, cal):
        base = cal[0:3]
        terms = cal[3:6]

        if base == 'USD':
            cal = terms
        elif terms == 'USD':
            cal = base

        hols = self.get_holidays(cal=cal + 'NYD')

        # cbd = CustomBusinessDay(1, holidays=self.get_holidays(cal=cal))

        return delivery_date - CustomBusinessDay(self._get_settlement_T(cal),
                                                 holidays=hols)  # - cbd + cbd

    def align_to_NY_cut_in_UTC(self, date_time, hour_of_day=10):

        tstz = Timezone()
        date_time = tstz.localize_index_as_new_york_time(date_time)
        date_time.index = date_time.index + timedelta(hours=hour_of_day)

        return tstz.convert_index_aware_to_UTC_time(date_time)

    def floor_date(self, data_frame):
        data_frame.index = data_frame.index.normalize()

        return data_frame

    def create_bus_day(self, start, end, cal=None):

        if cal is None:
            return pd.bdate_range(start, end)

        return pd.date_range(start, end,
                             hols=self.get_holidays(start_date=start,
                                                    end_date=end, cal=cal))

    def get_bus_day_of_month(self, date, cal='FX', tz=None):
        """ Returns the business day of the month (ie. 3rd Jan, on a Monday,
        would be the 1st business day of the month)
        """

        try:
            # Strip times off the dates - for business dates just want dates!
            date = date.normalize()
        except:
            pass

        start = pd.to_datetime(
            datetime.datetime(date.year[0], date.month[0], 1))
        end = pd.Timestamp(
            datetime.datetime.today())
        # pd.to_datetime(datetime.datetime(date.year[-1], date.month[-1], date.day[-1]))

        holidays = self.get_holidays(start_date=start, end_date=end, cal=cal)

        # bday = CustomBusinessDay(holidays=holidays, weekmask='Mon Tue Wed Thu Fri')

        holidays = holidays.tz_localize(None).date

        bus_dates = pd.bdate_range(start, end)

        # Not most efficient way...
        bus_dates = pd.to_datetime([x for x in bus_dates if x not in holidays])

        month = bus_dates.month

        work_day_index = np.zeros(len(bus_dates))
        work_day_index[0] = 1

        for i in range(1, len(bus_dates)):
            if month[i] == month[i - 1]:
                work_day_index[i] = work_day_index[i - 1] + 1
            else:
                work_day_index[i] = 1

        bus_day_of_month = work_day_index[bus_dates.searchsorted(date)]

        return bus_day_of_month

    def set_market_holidays(self, holiday_df):
        self._holiday_df = holiday_df
