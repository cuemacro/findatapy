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

import os

import csv
import pandas as pd
from typing import Optional, Union, List, Any

from findatapy.timeseries import Calculations
from findatapy.util.dataconstants import DataConstants
from findatapy.util.singleton import Singleton
from findatapy.util.loggermanager import LoggerManager

from dateutil.parser import parse

import re

import threading


class ConfigManager(object):
    """Functions for converting between vendor tickers and findatapy tickers 
    (and vice-versa).

    """
    __metaclass__ = Singleton

    # Tickers and fields
    _dict_time_series_tickers_list_library_to_vendor = {}
    _dict_time_series_tickers_list_vendor_to_library = {}
    _dict_time_series_fields_list_vendor_to_library = {}
    _dict_time_series_fields_list_library_to_vendor = {}

    # Store expiry date
    _dict_time_series_ticker_expiry_date_library_to_library = {}

    # Store categories -> fields
    _dict_time_series_category_fields_library_to_library = {}
    _dict_time_series_category_startdate_library_to_library = {}
    _dict_time_series_category_revision_periods_library_to_library = {}
    _dict_time_series_category_tickers_library_to_library = {}

    # category, source, freq, ticker, cut, fields, sourceticker, local-close, 
    # expiry, ldn_clo
    _data_frame_time_series_tickers = None

    # Store categories ->
    _dict_time_series_tickers_list_library = {}

    __lock = threading.Lock()

    __instance = None

    def __init__(self, *args, **kwargs):
        pass

    def get_instance(cls, data_constants=None):
        if not ConfigManager.__instance:
            with ConfigManager.__lock:
                if not ConfigManager.__instance:
                    ConfigManager.__instance = super(ConfigManager,
                                                     cls).__new__(
                        ConfigManager)

                    if data_constants is None:
                        data_constants = DataConstants()

                    ConfigManager.__instance.populate_time_series_dictionaries(
                        data_constants=data_constants)

        return ConfigManager.__instance

    ### time series ticker manipulators
    @staticmethod
    def populate_time_series_dictionaries(data_constants: Optional[DataConstants] = None) -> None:
        if data_constants is None:
            data_constants = DataConstants()

        # There are several CSV files which contain data on the tickers

        # time_series_tickers_list - contains every tickers (findatapy tickers => vendor tickers)
        # category,	data_source,	freq, tickers, cut, fields, vendor_tickers (from your data provider)
        # eg. fx / bloomberg / daily / EURUSD / TOK / close,open,high,low / EURUSD CMPT Curncy

        # time_series_fields_list - translate findatapy fields name to vendor fields names
        # findatapy fields => vendor fields
        # data_source, fields, vendor_fields

        # time_series_categories_fields - for each category specific generic properties
        # category,	freq, data_source, fields, startdate
        # eg. fx / daily / bloomberg / close,high,low,open / 01-Jan-70

        # eg. bloomberg / close / PX_LAST

        ## Populate tickers list (allow for multiple files)
        df_tickers = ConfigManager._extract_metadata_files_dataframes(
            file_paths=data_constants.time_series_tickers_list,
            data_frames=data_constants.time_series_tickers_data_frames_list)

        ConfigManager._populate_tickers(df_tickers)

        try:
            df_tickers = df_tickers.sort_values(
                by=["category", "data_source", "freq", "cut"])
        except:
            pass

        try:
            df_tickers = df_tickers.reset_index()
        except:
            pass

        try:
            df_tickers = df_tickers.drop("level_0", axis=1).reset_index()
        except:
            pass

        ConfigManager._data_frame_time_series_tickers = df_tickers

        ## Populate fields conversions
        df_fields = ConfigManager._extract_metadata_files_dataframes(
            file_paths=data_constants.time_series_fields_list,
            data_frames=data_constants.time_series_fields_data_frames_list
        )

        ConfigManager._populate_fields_mappings(df_fields)

        ## Populate categories fields list
        df_categories = ConfigManager._extract_metadata_files_dataframes(
            file_paths=data_constants.time_series_categories_fields,
            data_frames=data_constants.time_series_categories_fields_data_frames_list
        )

        ConfigManager._populate_categories_field_mappings(df_categories)

    @staticmethod
    def _extract_metadata_files_dataframes(file_paths: Optional[Union[str, List[str]]] = None,
                                           data_frames: Optional[Union[pd.DataFrame, List[pd.DataFrame]]] = None) -> pd.DataFrame:
        if isinstance(file_paths, str):
            file_paths = file_paths.split(";")

        df_list = []

        for file in file_paths:
            if os.path.isfile(file):
                # reader = csv.DictReader(open(tickers_list_file))
                if ".parquet" in file:
                    df = pd.read_parquet(file)
                else:
                    df = pd.read_csv(file)
                df = df.dropna(how="all")

                df_list.append(df)

        if data_frames is not None:
            if not isinstance(data_frames, list):
                data_frames = [data_frames]

            for df in data_frames:
                df = df.dropna(how="all")
                df_list.append(df)

        return pd.concat(df_list)

    @staticmethod
    def _populate_fields_mappings(df: Optional[pd.DataFrame] = None) -> None:

        ConfigManager.has_columns(df, ["data_source", "fields", "vendor_fields"],
                                  "Fields mappings")

        for index, line in df.iterrows():
            data_source = line["data_source"]
            fields = line["fields"]
            vendor_fields = line["vendor_fields"]

            # Conversion from vendor vendor_fields to library fields
            ConfigManager._dict_time_series_fields_list_vendor_to_library[
                data_source + "." + vendor_fields] = fields

            # Conversion from library tickers to vendor vendor_fields
            ConfigManager._dict_time_series_fields_list_library_to_vendor[
                data_source + "." + fields] = vendor_fields

    @staticmethod
    def _populate_categories_field_mappings(df: Optional[pd.DataFrame] = None) -> None:

        ConfigManager.has_columns(
            df,
            ["category", "data_source", "freq", "cut", "fields",
             "revision_periods"],
            "Categories fields mappings")

        for index, line in df.iterrows():
            category = line["category"]
            data_source = line["data_source"]
            freq = line["freq"]
            cut = line["cut"]
            fields = line["fields"].split(",")

            startdate = line["startdate"]
            revision_periods = line["revision_periods"]

            if category != "":
                # Conversion from library category to library fields list
                ConfigManager._dict_time_series_category_fields_library_to_library[
                    category + "." + data_source + "." + freq + "." + cut] = fields

                # Conversion from library category to library startdate
                ConfigManager._dict_time_series_category_startdate_library_to_library[
                    category + "." + data_source + "." + freq + "." + cut] = parse(
                    startdate).date()

                # Conversion from library category to library revision periods
                ConfigManager._dict_time_series_category_revision_periods_library_to_library[
                    category + "." + data_source + "." + freq + "." + cut] = revision_periods

    @staticmethod
    def _populate_tickers(df: Optional[pd.DataFrame] = None) -> None:

        ConfigManager.has_columns(
            df,
            ["category", "data_source", "freq", "tickers", "vendor_tickers", "cut"],
            "Tickers mappings")

        # Pre-process dataframe for vectorized operations
        # Convert vendor_tickers to string and strip whitespace
        df_work = df.copy()
        df_work['vendor_tickers'] = df_work['vendor_tickers'].astype(str).str.strip()
        df_work['vendor_tickers_lower'] = df_work['vendor_tickers'].str.lower()

        # Filter out invalid vendor_tickers (keeping only valid rows)
        # Note: original condition has OR which means it keeps almost everything except edge cases
        valid_mask = (df_work['vendor_tickers_lower'] != 'nan') | \
                     (df_work['vendor_tickers'] != '') | \
                     (df_work['vendor_tickers_lower'] != 'none')
        df_work = df_work[valid_mask & (df_work['category'] != '')]

        if len(df_work) == 0:
            return

        # Handle expiry column if it exists
        has_expiry = 'expiry' in df_work.columns
        if has_expiry:
            def safe_parse_expiry(val):
                try:
                    if pd.notna(val) and str(val).strip() != "":
                        return parse(str(val))
                except:
                    pass
                return None
            df_work['expiry_parsed'] = df_work['expiry'].apply(safe_parse_expiry)
        else:
            df_work['expiry_parsed'] = None

        # Explode freq column to handle comma-separated values
        df_work['freq_list'] = df_work['freq'].str.split(',')
        df_exploded = df_work.explode('freq_list').reset_index(drop=True)
        df_exploded['freq'] = df_exploded['freq_list']

        # Create all the key combinations using vectorized string operations
        df_exploded['key_base'] = (df_exploded['category'] + '.' +
                                    df_exploded['data_source'] + '.' +
                                    df_exploded['freq'] + '.' +
                                    df_exploded['cut'])

        df_exploded['library_to_vendor_key'] = (df_exploded['key_base'] + '.' +
                                                 df_exploded['tickers'])

        df_exploded['vendor_to_library_key'] = (df_exploded['key_base'] + '.' +
                                                 df_exploded['vendor_tickers'])

        df_exploded['expiry_key'] = (df_exploded['data_source'] + '.' +
                                      df_exploded['tickers'])

        # Populate dictionaries using vectorized operations
        # Conversion from library tickers to vendor tickers
        library_to_vendor_dict = dict(zip(df_exploded['library_to_vendor_key'],
                                          df_exploded['vendor_tickers']))
        ConfigManager._dict_time_series_tickers_list_library_to_vendor.update(library_to_vendor_dict)

        # Conversion from library tickers to library expiry date
        expiry_dict = dict(zip(df_exploded['expiry_key'],
                               df_exploded['expiry_parsed']))
        ConfigManager._dict_time_series_ticker_expiry_date_library_to_library.update(expiry_dict)

        # Conversion from vendor tickers to library tickers (with error handling)
        # Bulk update should work for all valid entries
        vendor_to_library_dict = dict(zip(df_exploded['vendor_to_library_key'],
                                          df_exploded['tickers']))

        # Check for any problematic keys before updating
        # (In practice, dict update with string keys rarely fails unless keys are malformed)
        try:
            ConfigManager._dict_time_series_tickers_list_vendor_to_library.update(vendor_to_library_dict)
        except Exception as e:
            # If bulk update fails, fall back to row-by-row with error handling
            LoggerManager.getLogger(__name__).warning(f"Bulk update failed: {e}. Falling back to row-by-row processing.")
            for vendor_key, ticker in vendor_to_library_dict.items():
                try:
                    ConfigManager._dict_time_series_tickers_list_vendor_to_library[vendor_key] = ticker
                except:
                    # Extract components for error message
                    key_idx = df_exploded[df_exploded['vendor_to_library_key'] == vendor_key].index[0]
                    key_base = df_exploded.loc[key_idx, 'key_base']
                    vendor_ticker = df_exploded.loc[key_idx, 'vendor_tickers']
                    LoggerManager.getLogger(__name__).warning(
                        "Ticker not specified correctly (is some of this missing?) " +
                        str(key_base) + "." + str(vendor_ticker))

        # Build category tickers dictionary
        grouped = df_exploded.groupby('key_base')['tickers'].apply(list).to_dict()
        for key, ticker_list in grouped.items():
            if key in ConfigManager._dict_time_series_category_tickers_library_to_library:
                ConfigManager._dict_time_series_category_tickers_library_to_library[key].extend(ticker_list)
            else:
                ConfigManager._dict_time_series_category_tickers_library_to_library[key] = ticker_list

    def free_form_tickers_regex_query(self, category=None, data_source=None,
                                      freq=None, cut=None, tickers=None,
                                      dict_filter={},
                                      ret_fields=["category", "data_source",
                                                  "freq", "cut"],
                                      smart_group=False):

        df = ConfigManager._data_frame_time_series_tickers

        if category is not None and not df.empty:
            df = df[df["category"].str.match(category) == True]

        if data_source is not None and not df.empty:
            df = df[df["data_source"].str.match(data_source) == True]

        if freq is not None and not df.empty:
            df = df[df["freq"].str.match(freq) == True]

        if cut is not None and not df.empty:
            df = df[df["cut"].str.match(cut) == True]

        if tickers is not None and not df.empty:
            df = df[df["tickers"].str.match(tickers) == True]

        if cut is not None and not df.empty:
            df = df[df["cut"].str.match(cut) == True]

        for k in dict_filter.keys():
            if k is not None and not df.empty:
                df = df[df[k].str.match(dict_filter[k]) == True]

        if ret_fields is not None and not (df.empty):
            df = df[ret_fields]

        df = df.drop_duplicates()

        # Group any tickers/vendor_tickers together
        if smart_group:
            df = ConfigManager.smart_group_dataframe_tickers(
                df, ret_fields=ret_fields)

        return df

    def free_form_tickers_query(self, free_form_query, best_match_only=False,
                                list_query=False,
                                ret_fields=["category", "data_source", "freq",
                                            "cut", "tickers", "vendor_tickers",
                                            "fields"],
                                smart_group=True):
        """From a string or list of properties for predefined tickers, we 
        create a DataFrame that can be used to populate a MarketDataRequest. 
        We search through all the predefined tickers, and "guess" any matches 
        to our query, without having to use the standard query format which 
        consists of category.data_source.freq.cut.ticker such as this example
        fx.bloomberg.daily.NYC.EURUSD.close

        eg. quandl.fx will match all tickers which are from "quandl" and 
        have a "category" fx

        We must be careful to make sure that categories, data_sources  etc. 
        are unique and do not overlap with other properties like tickers

        Parameters
        ----------
        free_form_query : str
            A query that can be used to generate a MarketDataRequest

            eg. quandl.fx

        best_match_only : bool
            Only return at most 1 row of a DataFrame (default: False)

        list_query : bool
            Is this a list of tickers?

        ret_fields : str(list)
            Which properties of a MarketDataRequest to return

        smart_group : bool
            Smart group tickers of a particular category in a specific row

        Returns
        -------
        DataFrame
        """
        logger = LoggerManager().getLogger(__name__)

        logger.info(
            "Finding ticker combination which matches " + str(free_form_query))

        df = ConfigManager._data_frame_time_series_tickers

        if list_query and isinstance(free_form_query, list):
            free_form_query = free_form_query
        elif "," in free_form_query:
            free_form_query = free_form_query.split(",")
        else:
            free_form_query = [free_form_query]

        df_joined_list = []

        for key in free_form_query:
            df_joined = df

            key = ConfigManager.split_ticker_string(key)

            # Search through all the keywords, and see if matches with any 
            # columns of our predefined tickers
            try:
                for k in key:
                    for c in df.columns:
                        try:
                            df_temp = df_joined[df_joined[c] == k]
                        except:
                            df_temp = pd.DataFrame()

                        if not (df_temp.empty):
                            df_joined = df_temp
                            break

                df_joined_list.append(df_joined)
            except Exception as e:
                pass

        # Drop any duplicated tickers
        df = pd.concat(df_joined_list).drop_duplicates()

        if len(df.index) > 1:
            logger.info(
                "Found multiple matches for ticker combination, first "
                "trying smart group...")

            if smart_group:
                df = self.smart_group_dataframe_tickers(
                    df, ret_fields=ret_fields)

            if best_match_only:
                logger.info("Taking only top match...")
                df = pd.DataFrame(df.head(1))

        if ret_fields is not None and not (df.empty):
            df = df[ret_fields]

        return df

    @staticmethod
    def split_ticker_string(md_request_str: Union[str, List[str]]) -> Union[str, List[str]]:

        if isinstance(md_request_str, str):
            split_lst = []

            word = ""
            ignore_dot = False

            for c in md_request_str:
                if c == "{":
                    ignore_dot = True

                elif c == "}":
                    ignore_dot = False

                elif c == "." and not (ignore_dot):
                    split_lst.append(word)
                    word = ""
                    ignore_dot = False
                else:
                    word = word + c

            split_lst.append(word)

            return split_lst

        return md_request_str

    @staticmethod
    def smart_group_dataframe_tickers(df: pd.DataFrame,
                                      ret_fields: Optional[Union[str, List[str]]] = None,
                                      data_constants: Optional[DataConstants] = None) -> pd.DataFrame:
        """Groups together a DataFrame of metadata associated with assets,
        which can be used to create MarketDataRequest
        objects
        """
        if data_constants is None:
            data_constants = DataConstants()

        if ret_fields is None:
            ret_fields = df.columns.to_list()
        elif isinstance(ret_fields, str):
            if ret_fields == "all":
                ret_fields = df.columns.to_list()
        elif isinstance(ret_fields, list):
            if ret_fields == []:
                ret_fields = df.columns.to_list()

        if set(["category", "data_source", "freq", "cut"]).issubset(
                ret_fields):
            group_fields = ret_fields.copy()

            agg_dict = {}

            if "tickers" in ret_fields:
                df["tickers"] = [[x] for x in df["tickers"].tolist()]
                agg_dict["tickers"] = "sum"
                group_fields.remove("tickers")

            if "vendor_tickers" in ret_fields:
                df["vendor_tickers"] = [[x] for x in
                                        df["vendor_tickers"].tolist()]
                agg_dict["vendor_tickers"] = "sum"
                group_fields.remove("vendor_tickers")

            if agg_dict != {}:

                try:
                    df = df.drop(
                        data_constants.drop_cols_smart_tickers_grouping,
                        axis=1)
                except:
                    pass

                df_temp = df.groupby(group_fields).agg(agg_dict)

                # If grouping fails (when there aren"t multiple elements to group!)
                if df_temp.empty:
                    pass
                else:
                    for i, g in enumerate(group_fields):
                        df_temp[g] = df_temp.index.get_level_values(i)

                    df = df_temp.reset_index(drop=True)

        return df

    @staticmethod
    def get_dataframe_tickers() -> Optional[pd.DataFrame]:
        return ConfigManager._data_frame_time_series_tickers

    @staticmethod
    def get_categories_from_fields() -> Any:
        return ConfigManager.\
            _dict_time_series_category_fields_library_to_library.keys()

    @staticmethod
    def get_categories_from_tickers() -> Any:
        return ConfigManager.\
            _dict_time_series_category_tickers_library_to_library.keys()

    @staticmethod
    def get_categories_from_tickers_selective_filter(filter):
        initial_list = ConfigManager.\
            _dict_time_series_category_tickers_library_to_library.keys()
        filtered_list = []

        for category_desc in initial_list:
            split_cat = category_desc.split(".")

            category = split_cat[0]
            # data_source = split_cat[1]
            # freq = split_cat[2]
            # cut = split_cat[3]

            if filter in category:
                filtered_list.append(category_desc)

        return filtered_list

    @staticmethod
    def get_potential_caches_from_tickers():
        all_categories = ConfigManager.\
            _dict_time_series_category_tickers_library_to_library.keys()

        expanded_category_list = []

        for sing in all_categories:
            split_sing = sing.split(".")
            category = split_sing[0]
            data_source = split_sing[1]
            freq = split_sing[2]
            cut = split_sing[3]

            if (freq == "intraday"):
                intraday_tickers = ConfigManager.get_tickers_list_for_category(
                    category, data_source, freq, cut)

                for intraday in intraday_tickers:
                    expanded_category_list.append(
                        category + "." + data_source + "." + freq +
                        "." + cut + "." + intraday)
            else:
                expanded_category_list.append(
                    category + "." + data_source + "." + freq +
                    "." + cut)

        return expanded_category_list

    @staticmethod
    def get_fields_list_for_category(category, data_source, freq, cut):
        return \
        ConfigManager._dict_time_series_category_fields_library_to_library[
            category + "." + data_source + "." + freq + "." + cut]

    @staticmethod
    def get_fields_list_for_category_str(category):
        return \
        ConfigManager._dict_time_series_category_fields_library_to_library[
            category]

    @staticmethod
    def get_startdate_for_category(category, source, freq, cut):
        return \
        ConfigManager._dict_time_series_category_startdate_library_to_library[
            category + "." + source + "." + freq + "." + cut]

    @staticmethod
    def get_revision_periods_for_category(category, source, freq, cut):
        return \
        ConfigManager._dict_time_series_category_revision_periods_library_to_library[
            category + "." + source + "." + freq + "." + cut]

    @staticmethod
    def get_expiry_for_ticker(data_source, ticker):
        return \
        ConfigManager._dict_time_series_ticker_expiry_date_library_to_library[
            data_source + "." + ticker]

    @staticmethod
    def get_filtered_tickers_list_for_category(category, data_source, freq,
                                               cut, filter):
        tickers = \
        ConfigManager._dict_time_series_category_tickers_library_to_library[
            category + "." + data_source + "." + freq + "." + cut]

        filtered_tickers = []

        for tick in tickers:
            if re.search(filter, tick):
                filtered_tickers.append(tick)

        return filtered_tickers

    @staticmethod
    def get_tickers_list_for_category(category, data_source, freq, cut):
        return \
        ConfigManager._dict_time_series_category_tickers_library_to_library[
            category + "." + data_source + "." + freq + "." + cut]

    @staticmethod
    def get_vendor_tickers_list_for_category(category, data_source, freq, cut):
        category_source_freq_cut = category + "." + data_source + "." + freq + "." + cut

        return ConfigManager.get_vendor_tickers_list_for_category_str(
            category_source_freq_cut)

    @staticmethod
    def get_tickers_list_for_category_str(category_data_source_freq_cut):
        return \
        ConfigManager._dict_time_series_category_tickers_library_to_library[
            category_data_source_freq_cut]

    @staticmethod
    def get_vendor_tickers_list_for_category_str(
            category_data_source_freq_cut):
        tickers = \
        ConfigManager._dict_time_series_category_tickers_library_to_library[
            category_data_source_freq_cut]

        vendor_tickers = []

        for t in tickers:
            vendor_tickers.append(
                ConfigManager.convert_library_to_vendor_ticker_str(
                    category_data_source_freq_cut + "." + t))

        return ConfigManager.flatten_list_of_lists(vendor_tickers)

    @staticmethod
    def convert_library_to_vendor_ticker(category, data_source, freq, cut,
                                         ticker):
        return ConfigManager._dict_time_series_tickers_list_library_to_vendor[
            category + "." + data_source + "." + freq + "." + cut + "." + ticker]

    @staticmethod
    def convert_library_to_vendor_ticker_str(
            category_data_source_freq_cut_ticker):
        return ConfigManager._dict_time_series_tickers_list_library_to_vendor[
            category_data_source_freq_cut_ticker]

    @staticmethod
    def convert_vendor_to_library_ticker(category, data_source, freq, cut,
                                         vendor_tickers):
        return ConfigManager._dict_time_series_tickers_list_vendor_to_library[
            category + "." + data_source + "." + freq + "." + cut + "." + vendor_tickers]

    @staticmethod
    def convert_vendor_to_library_field(data_source, vendor_fields):
        return ConfigManager._dict_time_series_fields_list_vendor_to_library[
            data_source + "." + vendor_fields]

    @staticmethod
    def convert_library_to_vendor_field(data_source, fields):
        return ConfigManager._dict_time_series_fields_list_library_to_vendor[
            data_source + "." + fields]

    @staticmethod
    def remove_duplicates_and_flatten_list(lst: List[Any]) -> List[Any]:
        return list(dict.fromkeys(ConfigManager.flatten_list_of_lists(lst)))

    @staticmethod
    def has_columns(df: Optional[pd.DataFrame], columns: Union[str, List[str]], description: Optional[str] = None) -> bool:
        """Check if a DataFrame has specific columns.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to check
        columns : str or list
            Column name(s) to check for. Can be a single column name (str) or
            a list of column names.
        description : str, optional
            User-defined description to include in log messages for context

        Returns
        -------
        bool
            True if all specified columns exist in the DataFrame, False otherwise
        """
        logger = LoggerManager.getLogger(__name__)

        prefix = f"{description}: " if description else ""

        if df is None or df.empty:
            logger.warning(f"{prefix}DataFrame is None or empty")
            return False

        if isinstance(columns, str):
            columns = [columns]

        missing_columns = [col for col in columns if col not in df.columns]

        if missing_columns:
            logger.warning(f"{prefix}Missing columns: {missing_columns}. Available columns: {list(df.columns)}")
            return False

        return True

    @staticmethod
    def flatten_list_of_lists(list_of_lists: Union[List[Any], Any]) -> Union[List[Any], Any]:
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
                    rt.extend(ConfigManager.flatten_list_of_lists(i))
                else:
                    rt.append(i)

            return rt

        return list_of_lists
