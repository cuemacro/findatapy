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

if __name__ == "__main__":
    ###### below line CRUCIAL when running Windows, otherwise multiprocessing
    # doesn"t work! (not necessary on Linux)
    from findatapy.util import SwimPool;

    SwimPool()

    from findatapy.timeseries import Filter, Calendar, Calculations

    import pandas as pd

    calculations = Calculations()
    calendar = Calendar()
    filter = Filter()

    # choose run_example = 0 for everything
    # run_example = 1 - combine intraday dataframe with daily data dataframe

    run_example = 0

    if run_example == 1 or run_example == 0:
        df_intraday = pd.DataFrame(
            index=pd.date_range(start="01 Jan 2020", end="10 Jan 2020",
                                freq="1min"),
            columns=["ones"])
        df_intraday["ones"] = 1

        df_intraday = df_intraday.tz_localize("utc")

        df_daily = pd.DataFrame(
            index=pd.date_range(start="01 Jan 2020", end="10 Jan 2020",
                                freq="B"),
            columns=["ones", "twos"])

        df_daily["ones"] = 1
        df_daily["twos"] = 2

        df_joined = calculations.join_intraday_daily(
            df_intraday, df_daily, daily_time_of_day="10:00",
            daily_time_zone="Europe/London")

        print(df_joined)
