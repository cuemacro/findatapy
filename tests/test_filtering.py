__author__ = "saeedamen"  # Saeed Amen

#
# Copyright 2022 Cuemacro
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

import pytest
import pandas as pd

from findatapy.timeseries import Filter


def test_filtering_by_dates():

    filter = Filter()

    # filter S&P500 between specific working days
    start_date = '01 Oct 2008'
    finish_date = '29 Oct 2008'

    # read CSV from disk, and make sure to parse dates
    df = pd.read_csv("S&P500.csv", parse_dates=['Date'], index_col=['Date'])
    df = filter.filter_time_series_by_date(start_date=start_date,
                                           finish_date=finish_date,
                                           data_frame=df)

    assert df.index[0] == pd.to_datetime(start_date)
    assert df.index[-1]== pd.to_datetime(finish_date)


if __name__ == '__main__':
    pytest.main()
