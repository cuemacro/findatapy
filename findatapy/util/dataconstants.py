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

"""
DataConstants

Has various constants required for the findatapy project. These have been defined as static variables.

"""

import os

class DataConstants(object):

    ###### SHOULD AUTODETECT FOLDER
    root_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\', '/') + "/"
    temp_folder = root_folder + "temp"

    ###### FOR FUTURE VERSIONS (which include caching)
    # folders for holding market data
    folder_historic_CSV = "x"
    folder_time_series_data = "x"

    ###### FOR ALIAS TICKERS
    # config file for time series categories
    time_series_categories_fields = \
        root_folder + "conf/time_series_categories_fields.csv"

    # we can have multiple tickers files (separated by ";")
    time_series_tickers_list = root_folder + "conf/time_series_tickers_list.csv;" + \
                               root_folder + "conf/fx_vol_tickers.csv"

    time_series_fields_list = root_folder + "conf/time_series_fields_list.csv"

    # config file for long term econ data
    all_econ_tickers = root_folder + "conf/all_econ_tickers.csv"
    econ_country_codes = root_folder + "conf/econ_country_codes.csv"
    econ_country_groups = root_folder + "conf/econ_country_groups.csv"

    # for events filtering
    events_category = 'events'
    events_category_dt = 'events_dt'

    ###### FOR CURRENT VERSION

    # which marketdatagenerator type to use?
    # note - marketdatagenerator currently implemented
    #        cachedmarketdatagenerator is only for proprietary version at present
    default_market_data_generator = "marketdatagenerator"

    # in Python threading does not offer true parallisation, but can be useful when downloading data, because
    # a lot of the time is spend waiting on data, multiprocessing library addresses this problem by spawning new Python
    # instances, but this has greater overhead (maybe more advisable when downloading very long time series)

    # "thread" or "multiprocessing" (experimental!) library to use when downloading data
    time_series_factory_thread_technique = "thread"

    # how many threads to use for loading external data (don't do too many on slow machines!)
    # also some data sources will complain if you start too many parallel threads to call data!
    # for some data providers might get better performance from 1 thread only!
    time_series_factory_thread_no = {'quandl'      : 4,
                                     'bloomberg'   : 8,
                                     'yahoo'       : 8,
                                     'other'       : 4}

    # log config file
    logging_conf = root_folder + "conf/logging.conf"

    # Bloomberg settings
    bbg_server = "localhost"       # needs changing if you use Bloomberg Server API
    bbg_server_port = 8194

    # Dukascopy settings
    dukascopy_base_url = "http://www.dukascopy.com/datafeed/"
    dukascopy_write_temp_tick_disk = False

    # Quandl settings
    quandl_api_key = "x"

    # Twitter settings (you need to set these up on Twitter)
    TWITTER_APP_KEY             = "x"
    TWITTER_APP_SECRET          = "x"
    TWITTER_OAUTH_TOKEN	     = "x"
    TWITTER_OAUTH_TOKEN_SECRET	 = "x"


    # or we can store credentials (or anything else) in a file "datacred.py" in the same folder, which will overwrite the above
    try:
        from findatapy.util.datacred import DataCred

        cred = DataCred()

        folder_historic_CSV = cred.folder_historic_CSV
        folder_time_series_data = cred.folder_time_series_data

        default_market_data_generator = cred.default_market_data_generator

        TWITTER_APP_KEY = cred.TWITTER_APP_KEY
        TWITTER_APP_SECRET = cred.TWITTER_APP_SECRET
        TWITTER_OAUTH_TOKEN = cred.TWITTER_OAUTH_TOKEN
        TWITTER_OAUTH_TOKEN_SECRET = cred.TWITTER_OAUTH_TOKEN_SECRET

        quandl_api_key = cred.quandl_api_key

    except:
        pass
