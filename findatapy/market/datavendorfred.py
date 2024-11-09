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

from urllib.request import urlopen
from urllib.parse import quote_plus
from urllib.parse import urlencode
from urllib.error import HTTPError

import xml.etree.ElementTree as ET

import pandas as pd

from findatapy.timeseries import Filter, Calculations

from findatapy.market.datavendor import DataVendor
from findatapy.util import LoggerManager

"""Contains implementations of DataVendor for

ALFRED/FRED (free datasource) - DataVendorALFRED

"""

class DataVendorALFRED(DataVendor):
    """Class for reading in data from ALFRED (and FRED) into findatapy library
    (based upon fredapi from https://github.com/mortada/fredapi)

    """

    def __init__(self):
        super(DataVendorALFRED, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, md_request):
        logger = LoggerManager().getLogger(__name__)

        md_request_vendor = self.construct_vendor_md_request(
            md_request)

        logger.info("Request ALFRED/FRED data")

        data_frame = self.download_daily(md_request_vendor)

        if data_frame is None or data_frame.index is []: return None

        # convert from vendor to findatapy tickers/fields
        if data_frame is not None:
            returned_tickers = data_frame.columns

        if data_frame is not None:
            # tidy up tickers into a format that is more easily translatable
            # we can often get multiple fields returned (even if we don't ask
            # for them!)
            # convert to lower case
            returned_fields = [(x.split('.')[1]) for x in returned_tickers]
            returned_tickers = [(x.split('.')[0]) for x in returned_tickers]

            try:
                fields = self.translate_from_vendor_field(returned_fields,
                                                          md_request)
                tickers = self.translate_from_vendor_ticker(returned_tickers,
                                                            md_request)
            except:
                print('error')

            ticker_combined = []

            for i in range(0, len(fields)):
                ticker_combined.append(tickers[i] + "." + fields[i])

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

        logger.info(
            "Completed request from ALFRED/FRED for " + str(ticker_combined))

        return data_frame

    def download_daily(self, md_request):
        logger = LoggerManager().getLogger(__name__)

        trials = 0

        data_frame_list = []
        data_frame_release = []

        # TODO refactor this code, a bit messy at the moment!
        for i in range(0, len(md_request.tickers)):
            while (trials < 5):
                try:
                    fred = Fred(api_key=md_request.fred_api_key)

                    # acceptable fields: close, actual-release,
                    # release-date-time-full
                    if 'close' in md_request.fields and \
                            'release-date-time-full' in md_request.fields:
                        data_frame = fred.get_series_all_releases(
                            md_request.tickers[i],
                            observation_start=md_request.start_date,
                            observation_end=md_request.finish_date)

                        data_frame = data_frame.rename(
                            columns={
                                "realtime_start":
                                    md_request.tickers[
                                                      i]
                                    + '.release-date-time-full',
                                "date": "Date",
                                "value": md_request.tickers[
                                             i] + '.close'})

                        data_frame = data_frame.sort_values(
                            by=['Date', md_request.tickers[
                                i] + '.release-date-time-full'])
                        data_frame = data_frame.drop_duplicates(
                            subset=['Date'], keep='last')
                        data_frame = data_frame.set_index(['Date'])

                        filter = Filter()
                        data_frame = filter.filter_time_series_by_date(
                            md_request.start_date,
                            md_request.finish_date, data_frame)

                        data_frame_list.append(data_frame)
                    elif 'close' in md_request.fields:

                        data_frame = fred.get_series(
                            series_id=md_request.tickers[i],
                            observation_start=md_request.start_date,
                            observation_end=md_request.finish_date)

                        data_frame = data_frame.to_frame(
                            name=md_request.tickers[i] + '.close')
                        data_frame.index.name = 'Date'

                        data_frame_list.append(data_frame)

                    if 'first-revision' in md_request.fields:
                        data_frame = fred.get_series_first_revision(
                            md_request.tickers[i],
                            observation_start=md_request.start_date,
                            observation_end=md_request.finish_date)

                        data_frame = data_frame.to_frame(
                            name=md_request.tickers[
                                     i] + '.first-revision')
                        data_frame.index.name = 'Date'

                        filter = Filter()
                        data_frame = filter.filter_time_series_by_date(
                            md_request.start_date,
                            md_request.finish_date, data_frame)

                        data_frame_list.append(data_frame)

                    if 'actual-release' in md_request.fields and \
                            'release-date-time-full' in md_request.fields:
                        data_frame = fred.get_series_all_releases(
                            md_request.tickers[i],
                            observation_start=md_request.start_date,
                            observation_end=md_request.finish_date)

                        data_frame = data_frame.rename(
                            columns={
                                "realtime_start": md_request.tickers[
                                                      i]
                                                  + '.release-date-time-full',
                                "date": "Date",
                                "value": md_request.tickers[
                                             i] + '.actual-release'})

                        data_frame = data_frame.sort_values(
                            by=['Date', md_request.tickers[
                                i] + '.release-date-time-full'])
                        data_frame = data_frame.drop_duplicates(
                            subset=['Date'], keep='first')
                        data_frame = data_frame.set_index(['Date'])

                        filter = Filter()
                        data_frame = filter.filter_time_series_by_date(
                            md_request.start_date,
                            md_request.finish_date, data_frame)

                        data_frame_list.append(data_frame)

                    elif 'actual-release' in md_request.fields:
                        data_frame = fred.get_series_first_release(
                            md_request.tickers[i],
                            observation_start=md_request.start_date,
                            observation_end=md_request.finish_date)

                        data_frame = data_frame.to_frame(
                            name=md_request.tickers[
                                     i] + '.actual-release')
                        data_frame.index.name = 'Date'
                        # data_frame = data_frame.rename(columns={"value" :
                        # md_request.tickers[i] + '.actual-release'})

                        filter = Filter()
                        data_frame = filter.filter_time_series_by_date(
                            md_request.start_date,
                            md_request.finish_date, data_frame)

                        data_frame_list.append(data_frame)

                    elif 'release-date-time-full' in md_request.fields:
                        data_frame = fred.get_series_all_releases(
                            md_request.tickers[i],
                            observation_start=md_request.start_date,
                            observation_end=md_request.finish_date)

                        data_frame = data_frame['realtime_start']

                        data_frame = data_frame.to_frame(
                            name=md_request.tickers[
                                     i] + '.release-date-time-full')

                        data_frame.index = data_frame[
                            md_request.tickers[
                                i] + '.release-date-time-full']
                        data_frame = data_frame.sort_index()
                        data_frame = data_frame.drop_duplicates()

                        filter = Filter()
                        data_frame_release.append(
                            filter.filter_time_series_by_date(
                                md_request.start_date,
                                md_request.finish_date,
                                data_frame))

                    break
                except Exception as e:
                    trials = trials + 1
                    logger.info("Attempting... " + str(
                        trials) + " request to download from ALFRED/FRED"
                                + str(e))

            if trials == 5:
                logger.error(
                    "Couldn't download from ALFRED/FRED after several "
                    "attempts!")

        calc = Calculations()

        data_frame1 = calc.join(data_frame_list, how='outer')
        data_frame2 = calc.join(data_frame_release, how='outer')

        data_frame = pd.concat([data_frame1, data_frame2], axis=1)

        return data_frame

class Fred(object):
    """Auxillary class for getting access to ALFRED/FRED directly.

    Based on https://github.com/mortada/fredapi (with minor edits)

    """
    earliest_realtime_start = '1776-07-04'
    latest_realtime_end = '9999-12-31'
    nan_char = '.'
    max_results_per_request = 1000

    def __init__(self,
                 api_key=None,
                 api_key_file=None):
        """Initialize the Fred class that provides useful functions to query the Fred dataset. You need to specify a valid
        API key in one of 3 ways: pass the string via api_key, or set api_key_file to a file with the api key in the
        first line, or set the environment variable 'FRED_API_KEY' to the value of your api key. You can sign up for a
        free api key on the Fred website at http://research.stlouisfed.org/fred2/
        """
        self.api_key = None
        if api_key is not None:
            self.api_key = api_key
        elif api_key_file is not None:
            f = open(api_key_file, 'r')
            self.api_key = f.readline().strip()
            f.close()
        else:
            self.api_key = os.environ.get('FRED_API_KEY')
        self.root_url = 'https://api.stlouisfed.org/fred'

        if self.api_key is None:
            import textwrap

            raise ValueError(textwrap.dedent("""\
                    You need to set a valid API key. You can set it in 3 ways:
                    pass the string with api_key, or set api_key_file to a
                    file with the api key in the first line, or set the
                    environment variable 'FRED_API_KEY' to the value of your
                    api key. You can sign up for a free api key on the Fred
                    website at http://research.stlouisfed.org/fred2/"""))

    def __fetch_data(self, url):
        """Helper function for fetching data given a request URL
        """
        try:
            response = urlopen(url)
            root = ET.fromstring(response.read())
        except HTTPError as exc:
            root = ET.fromstring(exc.read())
            raise ValueError(root.get('message'))
        return root

    def _parse(self, date_str, format='%Y-%m-%d'):
        """Helper function for parsing FRED date string into datetime

        """
        from pandas import to_datetime
        rv = to_datetime(date_str, format=format)
        if hasattr(rv, 'to_datetime'):
            rv = rv.to_pydatetime()  # to_datetime is depreciated
        return rv

    def get_series_info(self, series_id):
        """Get information about a series such as its title, frequency, observation start/end dates, units, notes, etc.

        Parameters
        ----------
        series_id : str
            Fred series id such as 'CPIAUCSL'

        Returns
        -------
        info : Series
            a pandas Series containing information about the Fred series
        """
        url = "%s/series?series_id=%s&api_key=%s" % (self.root_url, series_id,
                                                     self.api_key)
        root = self.__fetch_data(url)
        if root is None:
            raise ValueError('No info exists for series id: ' + series_id)
        from pandas import Series
        info = Series(root.getchildren()[0].attrib)
        return info

    def get_series(self, series_id, observation_start=None,
                   observation_end=None, **kwargs):
        """Get data for a Fred series id. This fetches the latest known data, and is equivalent to get_series_latest_release()

        Parameters
        ----------
        series_id : str
            Fred series id such as 'CPIAUCSL'
        observation_start : datetime or datetime-like str such as '7/1/2014', optional
            earliest observation date
        observation_end : datetime or datetime-like str such as '7/1/2014', optional
            latest observation date
        kwargs : additional parameters
            Any additional parameters supported by FRED. You can see https://api.stlouisfed.org/docs/fred/series_observations.html for the full list

        Returns
        -------
        data : Series
            a Series where each index is the observation date and the value is the data for the Fred series
        """
        url = "%s/series/observations?series_id=%s&api_key=%s" % (
        self.root_url,
        series_id,
        self.api_key)
        from pandas import to_datetime, Series

        if observation_start is not None:
            observation_start = to_datetime(observation_start, errors='raise')
            url += '&observation_start=' + observation_start.strftime(
                '%Y-%m-%d')
        if observation_end is not None:
            observation_end = to_datetime(observation_end, errors='raise')
            url += '&observation_end=' + observation_end.strftime('%Y-%m-%d')

        if kwargs is not None:
            url += '&' + urlencode(kwargs)

        root = self.__fetch_data(url)
        if root is None:
            raise ValueError('No data exists for series id: ' + series_id)
        data = {}
        for child in root.iter():
            val = child.get('value')
            if val is None:
                float('NaN')
            elif val == self.nan_char:
                val = float('NaN')
            else:
                val = float(val)
            data[self._parse(child.get('date'))] = val
        return Series(data)

    def get_series_latest_release(self, series_id, observation_start=None,
                                  observation_end=None):
        """Get data for a Fred series id. This fetches the latest known data, and is equivalent to get_series()

        Parameters
        ----------
        series_id : str
            Fred series id such as 'CPIAUCSL'

        Returns
        -------
        info : Series
            a Series where each index is the observation date and the value is the data for the Fred series
        """
        return self.get_series(series_id, observation_start=observation_start,
                               observation_end=observation_end)

    def get_series_first_release(self, series_id, observation_start=None,
                                 observation_end=None):
        """Get first-release data for a Fred series id.

        This ignores any revision to the data series. For instance,
        The US GDP for Q1 2014 was first released to be 17149.6, and then later revised to 17101.3, and 17016.0.
        This will ignore revisions after the first release.

        Parameters
        ----------
        series_id : str
            Fred series id such as 'GDP'

        Returns
        -------
        data : Series
            a Series where each index is the observation date and the value is the data for the Fred series
        """
        df = self.get_series_all_releases(series_id,
                                          observation_start=observation_start,
                                          observation_end=observation_end)
        first_release = df.groupby('date').head(1)
        data = first_release.set_index('date')['value']
        return data

    def get_series_first_revision(self, series_id, observation_start=None,
                                  observation_end=None):
        """Get first-revision data for a Fred series id.

        This will give the first revision to the data series. For instance,
        The US GDP for Q1 2014 was first released to be 17149.6, and then later revised to 17101.3, and 17016.0.
        This will take the first revision ie. 17101.3 in this case.

        Parameters
        ----------
        series_id : str
            Fred series id such as 'GDP'

        Returns
        -------
        data : Series
            a Series where each index is the observation date and the value is the data for the Fred series
        """
        df = self.get_series_all_releases(series_id,
                                          observation_start=observation_start,
                                          observation_end=observation_end)
        first_revision = df.groupby('date').head(2)
        data = first_revision.set_index('date')['value']
        data = data[~data.index.duplicated(keep='last')]

        return data

    def get_series_as_of_date(self, series_id, as_of_date):
        """Get latest data for a Fred series id as known on a particular date.

        This includes any revision to the data series
        before or on as_of_date, but ignores any revision on dates after as_of_date.

        Parameters
        ----------
        series_id : str
            Fred series id such as 'GDP'
        as_of_date : datetime, or datetime-like str such as '10/25/2014'
            Include data revisions on or before this date, and ignore revisions afterwards

        Returns
        -------
        data : Series
            a Series where each index is the observation date and the value is the data for the Fred series
        """
        from pandas import to_datetime
        as_of_date = to_datetime(as_of_date)
        df = self.get_series_all_releases(series_id)
        data = df[df['realtime_start'] <= as_of_date]
        return data

    def get_series_all_releases(self, series_id, observation_start=None,
                                observation_end=None):
        """Get all data for a Fred series id including first releases and all revisions.

        This returns a DataFrame
        with three columns: 'date', 'realtime_start', and 'value'. For instance, the US GDP for Q4 2013 was first released
        to be 17102.5 on 2014-01-30, and then revised to 17080.7 on 2014-02-28, and then revised to 17089.6 on
        2014-03-27. You will therefore get three rows with the same 'date' (observation date) of 2013-10-01 but three
        different 'realtime_start' of 2014-01-30, 2014-02-28, and 2014-03-27 with corresponding 'value' of 17102.5, 17080.7
        and 17089.6

        Parameters
        ----------
        series_id : str
            Fred series id such as 'GDP'

        Returns
        -------
        data : DataFrame
            a DataFrame with columns 'date', 'realtime_start' and 'value' where 'date' is the observation period and 'realtime_start'
            is when the corresponding value (either first release or revision) is reported.
        """
        url = "%s/series/observations?series_id=%s&api_key=%s&realtime_start=%s&realtime_end=%s" % (
        self.root_url,
        series_id,
        self.api_key,
        self.earliest_realtime_start,
        self.latest_realtime_end)

        from pandas import to_datetime

        if observation_start is not None:
            observation_start = to_datetime(observation_start, errors='raise')
            url += '&observation_start=' + observation_start.strftime(
                '%Y-%m-%d')
        if observation_end is not None:
            observation_end = to_datetime(observation_end, errors='raise')
            url += '&observation_end=' + observation_end.strftime('%Y-%m-%d')

        root = self.__fetch_data(url)
        if root is None:
            raise ValueError('No data exists for series id: ' + series_id)
        data = {}
        i = 0
        for child in root.iter():
            val = child.get('value')
            if val is None:
                val = float("NaN")
            elif val == self.nan_char:
                val = float('NaN')
            else:
                val = float(val)
            realtime_start = self._parse(child.get('realtime_start'))
            # realtime_end = self._parse(child.get('realtime_end'))
            date = self._parse(child.get('date'))

            data[i] = {'realtime_start': realtime_start,
                       # 'realtime_end': realtime_end,
                       'date': date,
                       'value': val}
            i += 1
        from pandas import DataFrame
        data = DataFrame(data).T
        return data

    def get_series_vintage_dates(self, series_id):
        """Get a list of vintage dates for a series.

        Vintage dates are the dates in history when a series' data values were revised or new data values were released.

        Parameters
        ----------
        series_id : str
            Fred series id such as 'CPIAUCSL'

        Returns
        -------
        dates : list
            list of vintage dates
        """
        url = "%s/series/vintagedates?series_id=%s&api_key=%s" % (
        self.root_url,
        series_id,
        self.api_key)
        root = self.__fetch_data(url)
        if root is None:
            raise ValueError(
                'No vintage date exists for series id: ' + series_id)
        dates = []
        for child in root.getchildren():
            dates.append(self._parse(child.text))
        return dates

    def __do_series_search(self, url):
        """Helper function for making one HTTP request for data, and parsing the returned results into a DataFrame
        """
        root = self.__fetch_data(url)

        series_ids = []
        data = {}

        num_results_returned = 0  # number of results returned in this HTTP request
        num_results_total = int(
            root.get(
                'count'))  # total number of results, this can be larger than number of results returned
        for child in root.getchildren():
            num_results_returned += 1
            series_id = child.get('id')
            series_ids.append(series_id)
            data[series_id] = {"id": series_id}
            fields = ["realtime_start", "realtime_end", "title",
                      "observation_start", "observation_end",
                      "frequency", "frequency_short", "units", "units_short",
                      "seasonal_adjustment",
                      "seasonal_adjustment_short", "last_updated",
                      "popularity", "notes"]
            for field in fields:
                data[series_id][field] = child.get(field)

        if num_results_returned > 0:
            from pandas import DataFrame
            data = DataFrame(data, columns=series_ids).T
            # parse datetime columns
            for field in ["realtime_start", "realtime_end",
                          "observation_start", "observation_end",
                          "last_updated"]:
                data[field] = data[field].apply(self._parse, format=None)
            # set index name
            data.index.name = 'series id'
        else:
            data = None
        return data, num_results_total

    def __get_search_results(self, url, limit, order_by, sort_order):
        """Helper function for getting search results up to specified limit on the number of results. The Fred HTTP API
        truncates to 1000 results per request, so this may issue multiple HTTP requests to obtain more available data.
        """

        order_by_options = ['search_rank', 'series_id', 'title', 'units',
                            'frequency',
                            'seasonal_adjustment', 'realtime_start',
                            'realtime_end', 'last_updated',
                            'observation_start', 'observation_end',
                            'popularity']
        if order_by is not None:
            if order_by in order_by_options:
                url = url + '&order_by=' + order_by
            else:
                raise ValueError(
                    '%s is not in the valid list of order_by options: %s' % (
                    order_by, str(order_by_options)))

        sort_order_options = ['asc', 'desc']
        if sort_order is not None:
            if sort_order in sort_order_options:
                url = url + '&sort_order=' + sort_order
            else:
                raise ValueError(
                    '%s is not in the valid list of sort_order options: %s' % (
                    sort_order, str(sort_order_options)))

        data, num_results_total = self.__do_series_search(url)
        if data is None:
            return data

        if limit == 0:
            max_results_needed = num_results_total
        else:
            max_results_needed = limit

        if max_results_needed > self.max_results_per_request:
            for i in range(1,
                           max_results_needed // self.max_results_per_request + 1):
                offset = i * self.max_results_per_request
                next_data, _ = self.__do_series_search(
                    url + '&offset=' + str(offset))
                data = data.append(next_data)
        return data.head(max_results_needed)

    def search(self, text, limit=1000, order_by=None, sort_order=None):
        """Do a fulltext search for series in the Fred dataset.

        Returns information about matching series in a DataFrame.

        Parameters
        ----------
        text : str
            text to do fulltext search on, e.g., 'Real GDP'
        limit : int, optional
            limit the number of results to this value. If limit is 0, it means fetching all results without limit.
        order_by : str, optional
            order the results by a criterion. Valid options are 'search_rank', 'series_id', 'title', 'units', 'frequency',
            'seasonal_adjustment', 'realtime_start', 'realtime_end', 'last_updated', 'observation_start', 'observation_end',
            'popularity'
        sort_order : str, optional
            sort the results by ascending or descending order. Valid options are 'asc' or 'desc'

        Returns
        -------
        info : DataFrame
            a DataFrame containing information about the matching Fred series
        """
        url = "%s/series/search?search_text=%s&api_key=%s" % (self.root_url,
                                                              quote_plus(text),
                                                              self.api_key)
        info = self.__get_search_results(url, limit, order_by, sort_order)
        return info

    def search_by_release(self, release_id, limit=0, order_by=None,
                          sort_order=None):
        """Search for series that belongs to a release id.

        Returns information about matching series in a DataFrame.

        Parameters
        ----------
        release_id : int
            release id, e.g., 151
        limit : int, optional
            limit the number of results to this value. If limit is 0, it means fetching all results without limit.
        order_by : str, optional
            order the results by a criterion. Valid options are 'search_rank', 'series_id', 'title', 'units', 'frequency',
            'seasonal_adjustment', 'realtime_start', 'realtime_end', 'last_updated', 'observation_start', 'observation_end',
            'popularity'
        sort_order : str, optional
            sort the results by ascending or descending order. Valid options are 'asc' or 'desc'

        Returns
        -------
        info : DataFrame
            a DataFrame containing information about the matching Fred series
        """
        url = "%s/release/series?release_id=%d&&api_key=%s" % (self.root_url,
                                                               release_id,
                                                               self.api_key)
        info = self.__get_search_results(url, limit, order_by, sort_order)
        if info is None:
            raise ValueError(
                'No series exists for release id: ' + str(release_id))
        return info

    def search_by_category(self, category_id, limit=0, order_by=None,
                           sort_order=None):
        """Search for series that belongs to a category id.

        Returns information about matching series in a DataFrame.

        Parameters
        ----------
        category_id : int
            category id, e.g., 32145
        limit : int, optional
            limit the number of results to this value. If limit is 0, it means fetching all results without limit.
        order_by : str, optional
            order the results by a criterion. Valid options are 'search_rank', 'series_id', 'title', 'units', 'frequency',
            'seasonal_adjustment', 'realtime_start', 'realtime_end', 'last_updated', 'observation_start', 'observation_end',
            'popularity'
        sort_order : str, optional
            sort the results by ascending or descending order. Valid options are 'asc' or 'desc'

        Returns
        -------
        info : DataFrame
            a DataFrame containing information about the matching Fred series
        """
        url = "%s/category/series?category_id=%d&api_key=%s" % (self.root_url,
                                                                category_id,
                                                                self.api_key)
        info = self.__get_search_results(url, limit, order_by, sort_order)
        if info is None:
            raise ValueError(
                'No series exists for category id: ' + str(category_id))
        return info
