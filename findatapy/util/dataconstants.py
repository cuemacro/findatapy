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


def path_join(folder, file):
    if 's3://' in folder:
        folder = folder.replace("s3://", "")
        folder = folder + "/" + file

        folder = folder.replace("//", "/")

        folder = "s3://" + folder

    else:
        if file[0] == '/':
            file = file[1::]

        folder = os.path.join(folder, file)

    folder = folder.replace("\\\\", "/")
    folder = folder.replace("\\", "/")

    return folder

def key_store(service_name):
    key = None

    # this will fail on some cloud notebook platforms so put in try/except loop
    try:
        key = keyring.get_password(service_name, os.getlogin())
    except:
        pass

    # set the keys by running set_api_keys.py file!

    # if key is None:
    #    key = input("Please enter the %s API key: " % service_name)
    #
    #    keyring.set_password(service_name, os.getlogin(), key)

    return key

class DataConstants(object):

    ###### SHOULD AUTODETECT FOLDER
    root_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\', '/')
    temp_folder = root_folder + "temp"

    ###### FOR FUTURE VERSIONS (which include caching)
    # Folders for holding market data
    folder_historic_CSV = "x:/"
    folder_time_series_data = "x:/"

    # Usually the data folder where we want to store market data (eg. '.../test/*.parquet')
    # or 'arctic'
    default_data_engine = None

    ###### FOR DATABASE (Arctic/MongoDB)
    db_server = '127.0.0.1'
    db_port = '27017'
    db_username = None
    db_password = None

    ###### FOR TEMPORARY IN-MEMORY CACHE (Redis)
    db_cache_server = '127.0.0.1'
    db_cache_port = '6379'
    write_cache_engine = 'redis'  # 'redis' or 'no_cache' means we don't use cache

    use_cache_compression = True

    parquet_compression = 'gzip' # 'gzip' or 'snappy'

    # Note for AWS you can set these globally without having to specify here with AWS CLI
    cloud_credentials = {'aws_anon' : False}

    ## eg. {
    # {
    #    "aws_anon" : False,
    #    "aws_access_key": "asdfksesdf",
    #    "aws_secret_key": "asfsdf",
    #    "aws_access_token": "adsfsdf",
    # },

    ###### FOR ALIAS TICKERS
    # Config file for time series categories
    config_root_folder = path_join(root_folder, "conf")

    time_series_categories_fields = \
        path_join(config_root_folder, "time_series_categories_fields.csv")

    # We can have multiple tickers files (separated by ";")
    time_series_tickers_list = path_join(config_root_folder, "time_series_tickers_list.csv") +";" + \
                               path_join(config_root_folder, "fx_vol_tickers.csv")+";" + \
                               path_join(config_root_folder, "fx_forwards_tickers.csv")+";" + \
                               path_join(config_root_folder, "base_depos_tickers_list.csv")+";"

    time_series_fields_list = path_join(config_root_folder, "time_series_fields_list.csv")

    # Config file for long term econ data
    all_econ_tickers = path_join(config_root_folder, "all_econ_tickers.csv")
    econ_country_codes = path_join(config_root_folder, "econ_country_codes.csv")
    econ_country_groups = path_join(config_root_folder, "econ_country_groups.csv")

    holidays_parquet_table = path_join(config_root_folder, "holidays_table.parquet")

    # For events filtering
    events_category = 'events'
    events_category_dt = 'events_dt'

    # Ignore these columns when doing smart grouping
    drop_cols_smart_tickers_grouping = ['level_0']

    ###### FOR CURRENT VERSION

    # which marketdatagenerator type to use?
    # note - marketdatagenerator currently implemented
    #        cachedmarketdatagenerator is only for proprietary version at present
    default_market_data_generator = "marketdatagenerator"

    # In Python threading does not offer true parallisation, but can be useful when downloading data, because
    # a lot of the time is spend waiting on data, multiprocessing library addresses this problem by spawning new Python
    # instances, but this has greater overhead (maybe more advisable when downloading very long time series)

    # "thread" or "multiprocessing" (experimental!) library to use when downloading data
    market_thread_technique = "thread"

    multiprocessing_library = 'multiprocess' # 'multiprocessing_on_dill' or 'multiprocess' or 'multiprocessing'

    # How many threads to use for loading external data (don't do too many on slow machines!)
    # also some data sources will complain if you start too many parallel threads to call data!
    # for some data providers might get better performance from 1 thread only!
    market_thread_no = {             'quandl'      : 4,
                                     'bloomberg'   : 4,
                                     'yahoo'       : 1, # yfinance already threads requests, so don't do it twice!
                                     'other'       : 4,
                                     'dukascopy'   : 8,
                                     'fxcm'        : 4}

    # Seconds for timeout
    timeout_downloader = {'dukascopy' : 120}

    # Dukascopy specific settings
    dukascopy_retries = 20
    dukascopy_mini_timeout_seconds = 10
    dukascopy_multithreading = True # Can get rejected connections when threading with Dukascopy
    dukascopy_try_time = 0 # Usually values of 0-1/8-1/4-1 are reasonable
    # smaller values => quicker retry, but don't want to poll server too much

    # We can override the thread count and drop back to single thread for certain market data downloads, as can have issues with
    # quite large daily datasets from Bloomberg (and other data vendors) when doing multi-threading, so can override and use
    # single threading on these (and also split into several chunks)
    #
    override_multi_threading_for_categories = []

    # These fields should always be converted to numbers (for every data vendor in MarketDataGenerator)
    always_numeric_column = ['close', 'open', 'high', 'low', 'tot']

    # These fields will be forcibly be converted to datetime64 (only for Bloomberg)
    always_date_columns = ['release-date-time-full', 'last-tradeable-day',
                          'futures-chain-last-trade-dates', 'first-notice-date', 'first-tradeable-day',
                          'cal-non-settle-dates', 'first-revision-date', 'release-dt']

    default_time_units = 'us' # 'ns' or 'ms' too

    # These are string/object fields which do not need to be converted
    always_str_fields = ['futures-chain-tickers']

    # Dataframe chunk size
    chunk_size_mb = 500

    # Log config file
    logging_conf = path_join(config_root_folder, "logging.conf")

    ####### Bloomberg settings
    bbg_server = "localhost"       # needs changing if you use Bloomberg Server API
    bbg_server_port = 8194

    # These fields are BDS style fields to be downloaded using Bloomberg's Reference Data interface
    # You may need to add to this list
    bbg_ref_fields = {'release-date-time-full' : 'ECO_FUTURE_RELEASE_DATE_LIST',
                          'last-tradeable-day' : 'LAST_TRADEABLE_DT',
                          'futures-chain-tickers' :  'FUT_CHAIN',
                          'futures-chain-last-trade-dates' :'FUT_CHAIN_LAST_TRADE_DATES',
                          'first-notice-date' : 'FUT_NOTICE_FIRST',
                          'first-tradeable-day' : 'FUT_FIRST_TRADE_DT',
                          'cal-non-settle-dates': 'CALENDAR_NON_SETTLEMENT_DATES'
    }

    # Depending on the ticker field inclusion of specific keywords,
    # apply a particular BBG override (make sure all lowercase)
    bbg_keyword_dict_override = {
        'RELEASE_STAGE_OVERRIDE' : {'A': ['gdp', 'advance'],
                                    'F': ['gdp', 'final'],
                                    'P': ['gdp', 'preliminary'],
                                    'F': ['cpi', 'final'],
                                    'P': ['cpi', 'preliminary']
                                    }
    }

    #######  Dukascopy settings
    dukascopy_base_url = "https://www.dukascopy.com/datafeed/"
    dukascopy_write_temp_tick_disk = False

    #######  FXCM settings
    fxcm_base_url = 'https://tickdata.fxcorporate.com/'
    fxcm_write_temp_tick_disk = False

    #######  Quandl settings
    quandl_api_key = key_store("Quandl")

    #######  Alpha Vantage settings
    alpha_vantage_api_key = key_store("AlphaVantage")

    #######  FXCM API (contact FXCM to get this)
    fxcm_api_key = "x"

    #######  Eikon settings
    eikon_api_key = key_store("Eikon")

    #######  Twitter settings (you need to set these up on Twitter)
    TWITTER_APP_KEY             = key_store("Twitter App Key")
    TWITTER_APP_SECRET          = key_store("Twitter App Secret")
    TWITTER_OAUTH_TOKEN	        = key_store("Twitter OAUTH token")
    TWITTER_OAUTH_TOKEN_SECRET	= key_store("Twitter OAUTH token Secret")

    ####### FRED (Federal Reserve of St Louis data) settings
    fred_api_key = key_store("FRED")

    ####### FX vol fields
    # Default download for FX vol surfaces etc.
    # types of quotation on vol surface
    # ATM, 25d riskies, 10d riskies, 25d strangles/butterflies, 10d strangles/butterflies
    fx_vol_part = ["V", "25R", "10R", "25B", "10B"]

    # Deltas quoted, eg 10d and 25d
    fx_vol_delta = [10, 25]

    # All the tenors on our vol surface
    fx_vol_tenor = ["ON", "1W", "2W", "3W", "1M", "2M", "3M", "4M", "6M", "9M", "1Y", "2Y", "3Y", "5Y"]

    # Which base depo currencies are available?
    base_depos_currencies = ['EUR', 'GBP', 'AUD', 'NZD', 'USD', 'CAD', 'CHF', 'NOK', 'SEK', 'JPY']

    # Tenors available for base depos
    base_depos_tenor = ["ON", "TN", "SN", "1W", "2W", "3W", "1M", "2M", "3M", "4M", "6M", "9M", "1Y", "2Y", "3Y", "5Y"]

    ### FX forwards total return index construction
    # All the tenors on our forwards
    fx_forwards_tenor = ["ON", "TN", "SN", "1W", "2W", "3W", "1M", "2M", "3M", "4M", "6M", "9M", "1Y", "2Y", "3Y", "5Y"]

    override_fields = {}

    ### What data environments are there
    default_data_environment = 'backtest'
    possible_data_environment = ['backtest', 'prod']

    data_vendor_custom = {}

    # Overwrite field variables with those listed in DataCred or user provided dictionary override_fields
    def __init__(self, override_fields={}):
        try:
            from findatapy.util.datacred import DataCred
            cred_keys = DataCred.__dict__.keys()

            for k in DataConstants.__dict__.keys():
                if k in cred_keys and '__' not in k:
                    setattr(DataConstants, k, getattr(DataCred, k))
        except:
            pass

        # Store overrided fields
        if override_fields == {}:
            override_fields = DataConstants.override_fields
        else:
            DataConstants.override_fields = override_fields

        for k in override_fields.keys():
            if '__' not in k:
                setattr(DataConstants, k, override_fields[k])

    @staticmethod
    def reset_api_key(service_name, api_key):
        keyring.set_password(service_name, os.getlogin(), api_key)
