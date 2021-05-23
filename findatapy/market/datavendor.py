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

import abc
import copy

from findatapy.market.marketdatarequest import MarketDataRequest
from findatapy.util import ConfigManager, LoggerManager

class DataVendor(object):
    """Abstract class for various data source loaders.

    """
    def __init__(self):
        self.config = ConfigManager().get_instance()
        # self.config = None
        return

    @abc.abstractmethod
    def load_ticker(self, market_data_request):
        """Retrieves market data from external data source

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains all the various parameters detailing time series start and finish, tickers etc

        Returns
        -------
        DataFrame
        """

        return

        # to be implemented by subclasses

    @abc.abstractmethod
    def kill_session(self):
        return

    def construct_vendor_market_data_request(self, market_data_request):
        """Creates a MarketDataRequest with the vendor tickers

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains all the various parameters detailing time series start and finish, tickers etc

        Returns
        -------
        MarketDataRequest
        """

        symbols_vendor = self.translate_to_vendor_ticker(market_data_request)
        fields_vendor = self.translate_to_vendor_field(market_data_request)

        market_data_request_vendor = MarketDataRequest(md_request=market_data_request)

        market_data_request_vendor.tickers = symbols_vendor
        market_data_request_vendor.fields = fields_vendor

        return market_data_request_vendor

    def translate_to_vendor_field(self, market_data_request):
        """Converts all the fields from findatapy fields to vendor fields

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains all the various parameters detailing time series start and finish, tickers etc

        Returns
        -------
        List of Strings
        """

        if market_data_request.vendor_fields is not None:
            return market_data_request.vendor_fields

        source = market_data_request.data_source
        fields_list = market_data_request.fields

        if isinstance(fields_list, str):
            fields_list = [fields_list]

        if self.config is None: return fields_list

        fields_converted = []

        for field in fields_list:
            try:
                f = self.config.convert_library_to_vendor_field(source, field)
            except:
                logger = LoggerManager().getLogger(__name__)
                logger.warn("Couldn't find field conversion, did you type it correctly: " + field)

                return

            fields_converted.append(f)

        return fields_converted

    # Translate findatapy ticker to vendor ticker
    def translate_to_vendor_ticker(self, market_data_request):
        """Converts all the tickers from findatapy tickers to vendor tickers

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains all the various parameters detailing time series start and finish, tickers etc

        Returns
        -------
        List of Strings
        """

        if market_data_request.vendor_tickers is not None:
            return market_data_request.vendor_tickers

        category = market_data_request.category
        source = market_data_request.data_source
        freq = market_data_request.freq
        cut = market_data_request.cut
        tickers_list = market_data_request.tickers

        if isinstance(tickers_list, str):
            tickers_list = [tickers_list]

        if self.config is None: return tickers_list

        tickers_list_converted = []

        for ticker in tickers_list:
            try:
                t = self.config.convert_library_to_vendor_ticker(category, source, freq, cut, ticker)
            except:
                logger = LoggerManager().getLogger(__name__)
                logger.error("Couldn't find ticker conversion, did you type it correctly: " + ticker)

                return

            tickers_list_converted.append(t)

        return tickers_list_converted

    def translate_from_vendor_field(self, vendor_fields_list, market_data_request):
        """Converts all the fields from vendors fields to findatapy fields

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains all the various parameters detailing time series start and finish, tickers etc

        Returns
        -------
        List of Strings
        """

        data_source = market_data_request.data_source

        if isinstance(vendor_fields_list, str):
            vendor_fields_list = [vendor_fields_list]

        # if self.config is None: return vendor_fields_list

        fields_converted = []

        # If we haven't set the configuration files for automatic configuration
        if market_data_request.vendor_fields is not None:

            dictionary = dict(zip(self.get_lower_case_list(market_data_request.vendor_fields), market_data_request.fields))

            for vendor_field in vendor_fields_list:
                try:
                    fields_converted.append(dictionary[vendor_field.lower()])
                except:
                    fields_converted.append(vendor_field)

        # Otherwise used stored configuration files (every field needs to be defined!)
        else:
            for vendor_field in vendor_fields_list:
                try:
                    v = self.config.convert_vendor_to_library_field(data_source, vendor_field)
                except:
                    logger = LoggerManager().getLogger(__name__)
                    logger.error("Couldn't find field conversion, did you type it correctly: " + vendor_field + ", using 'close' as default.")

                    v = 'close'

                fields_converted.append(v)

        return fields_converted

    # Translate findatapy ticker to vendor ticker
    def translate_from_vendor_ticker(self, vendor_tickers_list, md_request):
        """Converts all the fields from vendor tickers to findatapy tickers

        Parameters
        ----------
        md_request : MarketDataRequest
            contains all the various parameters detailing time series start and finish, tickers etc

        Returns
        -------
        List of Strings
        """

        if md_request.vendor_tickers is not None:

            dictionary = dict(zip(self.get_lower_case_list(md_request.vendor_tickers), md_request.tickers))

            tickers_stuff = []

            for vendor_ticker in vendor_tickers_list:
                tickers_stuff.append(dictionary[vendor_ticker.lower()])

            return tickers_stuff # [item for sublist in tickers_stuff for item in sublist]


        # tickers_list = md_request.tickers

        if isinstance(vendor_tickers_list, str):
            vendor_tickers_list = [vendor_tickers_list]

        if self.config is None: return vendor_tickers_list

        tickers_converted = []

        for vendor_ticker in vendor_tickers_list:
            try:
                v = self.config.convert_vendor_to_library_ticker(
                    md_request.category, md_request.data_source,
                    md_request.freq, md_request.cut, vendor_ticker)
            except:
                logger = LoggerManager().getLogger(__name__)
                logger.error("Couldn't find ticker conversion, did you type it correctly: " + vendor_ticker)

                return

            tickers_converted.append(v)

        return tickers_converted

    def get_lower_case_list(self, lst):
        return [k.lower() for k in lst]


