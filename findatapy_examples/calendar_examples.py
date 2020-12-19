__author__ = 'saeedamen'  # Saeed Amen

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


if __name__ == '__main__':
    ###### below line CRUCIAL when running Windows, otherwise multiprocessing doesn't work! (not necessary on Linux)
    from findatapy.util import SwimPool; SwimPool()

    from findatapy.timeseries import Filter, Calendar

    import pandas as pd
    import numpy as np

    filter = Filter()
    calendar = Calendar()

    # choose run_example = 0 for everything
    # run_example = 1 - get holidays for FX, EUR and EURUSD, as well as listing weekends
    # run_example = 2 - get FX delivery dates and FX option expiries for various tenors

    run_example = 0

    if run_example == 1 or run_example == 0:

        # Get the holidays (which aren't weekends)
        print(filter.get_holidays(start_date='01 Jan 1999 00:50', end_date='31 Dec 1999', cal='FX'))
        print(filter.get_holidays(start_date='01 Jan 2000 00:10', end_date='31 Dec 2000', cal='EUR'))
        print(filter.get_holidays(start_date='01 Jan 2000 00:10', end_date='31 Dec 2000', cal='EURUSD'))

        # Get the holidays (which are weekends)
        print(filter.get_holidays(start_date='01 Jan 1999 00:50', end_date='31 Dec 1999', cal='WKY'))

    if run_example == 2 or run_example == 0:

        # Get delivery dates for these horizon dates - typically would use to get forward maturities
        print(calendar.get_delivery_date_from_horizon_date(
            pd.to_datetime([pd.Timestamp('02 Nov 2020')]), 'ON', cal='EURUSD'))

        print(calendar.get_delivery_date_from_horizon_date(
            pd.to_datetime([pd.Timestamp('02 Nov 2020')]), '1W', cal='EURUSD'))

        print(calendar.get_delivery_date_from_horizon_date(
            pd.to_datetime([pd.Timestamp('27 Nov 2020')]), '1W', cal='EURUSD'))

        # Get 1M expires for these horizon dates - typically would use to get option expiries
        print(calendar.get_expiry_date_from_horizon_date(
            pd.to_datetime([pd.Timestamp('26 Oct 2020')]), '1M', cal='EURUSD'))


