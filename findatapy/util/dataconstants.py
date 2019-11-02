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
import keyring

def key_store(service_name):
    key = keyring.get_password(service_name, os.getlogin())

    if key is None:
        key = input("Please enter the %s API key: " % service_name)

        keyring.set_password(service_name, os.getlogin(), key)

    return key


class DataConstants(object):

    ###### SHOULD AUTODETECT FOLDER
    root_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\', '/') + "/"
    temp_folder = root_folder + "temp"

    ###### FOR FUTURE VERSIONS (which include caching)
    # folders for holding market data
    folder_historic_CSV = "x:/"
    folder_time_series_data = "x:/"

    ###### FOR DATABASE
    db_server = '127.0.0.1'
    db_port = '27017'

    ###### FOR TEMPORARY IN-MEMRORY CACHE (Redis)
    db_cache_server = '127.0.0.1'
    db_cache_port = '6379'
    write_cache_engine = 'redis'  # 'redis' or 'no_cache' means we don't use cache

    use_cache_compression = True

    ###### FOR ALIAS TICKERS
    # config file for time series categories
    config_root_folder = root_folder

    time_series_categories_fields = \
        root_folder + "conf/time_series_categories_fields.csv"

    # we can have multiple tickers files (separated by ";")
    time_series_tickers_list = root_folder + "conf/time_series_tickers_list.csv;" + \
                               root_folder + "conf/fx_vol_tickers.csv;" + \
                               root_folder + "conf/fx_forwards_tickers.csv;" + \
                               root_folder + "conf/base_depos_tickers_list.csv"


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
    market_thread_technique = "thread"

    multiprocessing_library = 'multiprocess' # 'multiprocessing_on_dill' or 'multiprocess' or 'multiprocessing'

    # how many threads to use for loading external data (don't do too many on slow machines!)
    # also some data sources will complain if you start too many parallel threads to call data!
    # for some data providers might get better performance from 1 thread only!
    market_thread_no = {             'quandl'      : 4,
                                     'bloomberg'   : 4,
                                     'yahoo'       : 1, # yfinance already threads requests, so don't do it twice!
                                     'other'       : 4,
                                     'dukascopy'   : 8,
                                     'fxcm'        : 4}

    # we can override the thread count and drop back to single thread for certain market data downloads, as can have issues with
    # quite large daily datasets from Bloomberg (and other data vendors) when doing multi-threading, so can override and use
    # single threading on these (and also split into several chunks)
    #
    override_multi_threading_for_categories = []

    # log config file
    logging_conf = root_folder + "conf/logging.conf"

    # Bloomberg settings
    bbg_server = "localhost"       # needs changing if you use Bloomberg Server API
    bbg_server_port = 8194

    # Dukascopy settings
    dukascopy_base_url = "http://www.dukascopy.com/datafeed/"
    dukascopy_write_temp_tick_disk = False

    # FXCM settings
    fxcm_base_url = 'https://tickdata.fxcorporate.com/'
    fxcm_write_temp_tick_disk = False

    # Quandl settings
    quandl_api_key = key_store("Quandl")

    # Alpha Vantage settings
    alpha_vantage_api_key = key_store("AlphaVantage")

    # FXCM API (contact FXCM to get this)
    fxcm_api_key = "x"

    # Twitter settings (you need to set these up on Twitter)
    TWITTER_APP_KEY             = key_store("Twitter App Key")
    TWITTER_APP_SECRET          = key_store("Twitter App Secret")
    TWITTER_OAUTH_TOKEN	        = key_store("Twitter OAUTH token")
    TWITTER_OAUTH_TOKEN_SECRET	= key_store("Twitter OAUTH token Secret")

    # FRED (Federal Reserve of St Louis data) settings
    fred_api_key = key_store("FRED")

    # overwrite field variables with those listed in DataCred
    def __init__(self):
        try:
            from findatapy.util.datacred import DataCred
            cred_keys = DataCred.__dict__.keys()

            for k in DataConstants.__dict__.keys():
                if k in cred_keys and '__' not in k:
                    setattr(DataConstants, k, getattr(DataCred, k))
        except:
            pass

    @staticmethod
    def reset_api_key(service_name, api_key):
        keyring.set_password(service_name, os.getlogin(), api_key)
