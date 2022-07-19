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
import abc
from operator import itemgetter

import pandas as pd
import numpy as np

from findatapy.market.datavendor import DataVendor
from findatapy.market.marketdatarequest import MarketDataRequest
from findatapy.timeseries import Calculations


class DataVendorBBG(DataVendor):
    """Abstract class for download of Bloomberg daily, intraday data and 
    reference data.

    Implemented by:
        DataVendorBBGOpen - Adapted version of new Bloomberg Open API for 
        Python which is recommended. Note that this requires compilation, 
        via installed C++ compiler. For Python 3.5, this is Microsoft Visual 
        Studio 2015.

        Or it is easier to install blpapi via conda, which deals with 
        installing the DLL, adding the environment path etc.

        Note: no longer supports COM API, which is slower and only 32 bit

    """

    def __init__(self):
        super(DataVendorBBG, self).__init__()

    # implement method in abstract superclass
    def load_ticker(self, md_request):
        """Retrieves market data from external data source (in this case 
        Bloomberg)

        Parameters
        ----------
        md_request : MarketDataRequest
            contains all the various parameters detailing time series start 
            and finish, tickers etc

        Returns
        -------
        DataFrame
        """
        constants = DataConstants()

        md_request = MarketDataRequest(md_request=md_request)
        md_request_vendor = self.construct_vendor_md_request(
            md_request)

        data_frame = None

        logger = LoggerManager().getLogger(__name__)
        logger.info("Request Bloomberg data")

        # Do we need daily or intraday data?
        if (md_request.freq in ['daily', 'weekly', 'monthly',
                                         'quarterly', 'yearly']):

            # Work out the fields which need to be downloaded via Bloomberg ref request (BDP) and
            # those that can be downloaded via Historical request (BDH)
            ref_fields = []
            ref_vendor_fields = []

            # Get user defined list of BBG fields/vendor fields which need to
            # be downloaded by BDP
            bbg_ref_fields = list(constants.bbg_ref_fields.keys())
            bbg_ref_vendor_fields = list(constants.bbg_ref_fields.values())

            for i in range(0, len(md_request.fields)):
                if md_request.fields[i] in bbg_ref_fields \
                        or md_request_vendor.fields[
                    i] in bbg_ref_vendor_fields:
                    ref_fields.append(md_request.fields[i])
                    ref_vendor_fields.append(
                        md_request_vendor.fields[i])

            non_ref_fields = []
            non_ref_vendor_fields = []

            for i in range(0, len(md_request.fields)):
                if md_request.fields[i] not in bbg_ref_fields \
                        and md_request_vendor.fields[
                    i] not in bbg_ref_vendor_fields:
                    non_ref_fields.append(md_request.fields[i])
                    non_ref_vendor_fields.append(
                        md_request_vendor.fields[i])

            # For certain cases, need to use ReferenceDataRequest
            # eg. for events times/dates, last tradeable date fields (when specified)
            if len(ref_fields) > 0:

                # Careful: make sure you copy the market data request object
                # (when threading, altering that can
                # cause concurrency issues!)
                old_fields = copy.deepcopy(md_request.fields)
                old_vendor_fields = copy.deepcopy(
                    md_request_vendor.fields)

                # md_request = MarketDataRequest(md_request=md_request_copy)

                md_request.fields = ref_fields
                md_request.vendor_fields = ref_vendor_fields
                md_request_vendor = self.construct_vendor_md_request(
                    md_request)

                # Just select those reference fields to download via reference
                datetime_data_frame = self.get_reference_data(
                    md_request_vendor, md_request)

                # Download all the other event or non-ref fields
                # (uses HistoricalDataRequest to Bloomberg)
                # concatenate with date time fields
                if len(non_ref_fields) > 0:

                    md_request.fields = non_ref_fields
                    md_request.vendor_fields = non_ref_vendor_fields
                    md_request_vendor = self.construct_vendor_md_request(
                        md_request)

                    events_data_frame = self.get_daily_data(
                        md_request, md_request_vendor)

                    col = events_data_frame.index.name
                    events_data_frame = events_data_frame.reset_index(
                        drop=False)

                    data_frame = pd.concat(
                        [events_data_frame, datetime_data_frame], axis=1)
                    temp = data_frame[col]
                    del data_frame[col]
                    data_frame.index = temp
                else:
                    data_frame = datetime_data_frame

                md_request.fields = copy.deepcopy(old_fields)
                md_request_vendor.fields = copy.deepcopy(
                    old_vendor_fields)

            # For all other daily/monthly/quarter data, we can use
            # HistoricalDataRequest to Bloomberg
            else:
                data_frame = self.get_daily_data(md_request,
                                                 md_request_vendor)

                # if data_frame is not None:
                #     # Convert fields with release-dt to dates (special case!) and assume everything else numerical
                #     for c in data_frame.columns:
                #         try:
                #             if 'release-dt' in c:
                #                 data_frame[c] = (data_frame[c]).astype('int').astype(str).apply(
                #                         lambda x: pd.to_datetime(x, format='%Y%m%d'))
                #             else:
                #                 data_frame[c] = pd.to_numeric(data_frame[c])
                #         except:
                #             pass

        # Assume one ticker only for intraday data and use IntradayDataRequest
        # to Bloomberg
        if (md_request.freq in ['tick', 'intraday', 'second',
                                         'minute', 'hourly']):
            md_request_vendor.tickers = \
            md_request_vendor.tickers[0]

            if md_request.freq in ['tick', 'second']:
                data_frame = self.download_tick(md_request_vendor)
            else:
                data_frame = self.download_intraday(md_request_vendor)

            if data_frame is not None:
                if data_frame.empty:
                    try:
                        logger.info(
                            "No tickers returned for: "
                            + md_request_vendor.tickers)
                    except:
                        pass

                    return None

                cols = data_frame.columns.values

                import pytz

                try:
                    data_frame = data_frame.tz_localize(pytz.utc)
                except:
                    data_frame = data_frame.tz_convert(pytz.utc)

                cols = md_request.tickers[0] + "." + cols
                data_frame.columns = cols

        logger.info("Completed request from Bloomberg.")

        return data_frame

    def get_daily_data(self, md_request, md_request_vendor):
        logger = LoggerManager().getLogger(__name__)

        data_frame = self.download_daily(md_request_vendor)

        # Convert from vendor to findatapy tickers/fields
        if data_frame is not None:
            if data_frame.empty:
                logger.info("No tickers returned for...")

                try:
                    logger.info(str(md_request_vendor.tickers))
                except:
                    pass

                return None

            returned_fields = data_frame.columns.get_level_values(0)
            returned_tickers = data_frame.columns.get_level_values(1)

            # TODO if empty try downloading again a year later
            try:
                fields = self.translate_from_vendor_field(returned_fields,
                                                          md_request)
            except:
                print('Problem translating vendor field')

            tickers = self.translate_from_vendor_ticker(returned_tickers,
                                                        md_request)

            ticker_combined = []

            for i in range(0, len(fields)):
                ticker_combined.append(tickers[i] + "." + fields[i])

            # Convert numerical columns to floats and dates to dates (avoids
            # having object columns which can cause issues with later Pandas)
            data_frame = self.force_type_conversion(data_frame)

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

            # Force sorting of index
            try:
                data_frame = data_frame.sort_index()
            except:
                pass

        return data_frame

    def get_reference_data(self, md_request_vendor,
                           md_request):
        logger = LoggerManager().getLogger(__name__)

        constants = DataConstants()

        end = datetime.utcnow()

        from datetime import timedelta
        end = end + timedelta(
            days=365)  # because very often we may with to download data about
        # future calendar events
        #  end.replace(year = end.year + 1)

        md_request_vendor.finish_date = end

        logger.debug(
            "Requesting ref for " + md_request_vendor.tickers[
                0] + " etc.")

        data_frame = self.download_ref(md_request_vendor)

        logger.debug("Waiting for ref...")

        # Convert from vendor to findatapy tickers/fields
        if data_frame is not None:
            if data_frame.empty:
                return None

            returned_fields = data_frame.columns.get_level_values(0)
            returned_tickers = data_frame.columns.get_level_values(1)

        if data_frame is not None:
            # TODO if empty try downloading again a year later
            fields = self.translate_from_vendor_field(returned_fields,
                                                      md_request)
            tickers = self.translate_from_vendor_ticker(returned_tickers,
                                                        md_request)

            ticker_combined = []

            for i in range(0, len(fields)):
                ticker_combined.append(tickers[i] + "." + fields[i])

            data_frame.columns = ticker_combined

            # Need to convert numerical and datetime columns separately post
            # pandas 0.23
            data_frame = self.force_type_conversion(data_frame)

            # data_frame = data_frame.apply(pd.to_datetime, errors='ignore')
            # data_frame = data_frame.apply(pd.to_numeric, errors='ignore')

            # TODO coerce will be deprecated from pandas 0.23.0 onwards) so
            #  remove!
            # data_frame = data_frame.convert_objects(convert_dates = 'coerce',
            # convert_numeric= 'coerce')

        return data_frame

    def force_type_conversion(self, data_frame):
        constants = DataConstants()

        logger = LoggerManager().getLogger(__name__)

        if data_frame is not None:
            if not (data_frame.empty):
                # Need to convert numerical and datetime columns separately
                # post pandas 0.23
                for c in data_frame.columns:
                    is_date = False

                    # Special case for ECO_RELEASE_DT / FIRST_REVISION_DATE
                    if 'ECO_RELEASE_DT' in c or 'FIRST_REVISION_DATE' in c:
                        try:
                            temp_col = []  # data_frame[c].values

                            for i in range(0, len(data_frame[c].values)):
                                try:
                                    temp_col.append(pd.to_datetime(
                                        str(int(data_frame[c].values[i])),
                                        format='%Y%m%d'))
                                except:
                                    temp_col.append(np.datetime64('NaT'))

                            data_frame[c] = temp_col
                        except Exception as e:
                            logger.warning("Couldn't convert " + str(
                                c) + " to date.. was this column empty? " + str(
                                e))

                    else:
                        # Only convert those Bloomberg reference fields to
                        # dates which have been listed explicitly
                        for d in constants.always_date_columns:
                            if d in c:
                                try:
                                    data_frame[c] = pd.to_datetime(
                                        data_frame[c], errors='coerce')

                                    is_date = True
                                    break
                                except:
                                    pass

                        # Otherwise this is not a date field so attempt to
                        # convert into numbers
                        if not (is_date):
                            try:
                                data_frame[c] = pd.to_numeric(data_frame[c],
                                                              errors='ignore')
                            except:
                                pass

        logger.debug("Returning converted dataframe...")

        return data_frame

    # implement method in abstract superclass
    @abc.abstractmethod
    def kill_session(self):
        return

    @abc.abstractmethod
    def download_tick(self, md_request):
        return

    @abc.abstractmethod
    def download_intraday(self, md_request):
        return

    @abc.abstractmethod
    def download_daily(self, md_request, md_request_vendor):
        return

    @abc.abstractmethod
    def download_ref(self, md_request):
        return


###############################################################################

import abc
import copy
import collections
import datetime
import re

try:
    import blpapi  # obtainable from Bloomberg website
except:
    pass

from findatapy.util.dataconstants import DataConstants
from findatapy.market.datavendorbbg import DataVendorBBG

from collections import defaultdict


class DataVendorBBGOpen(DataVendorBBG):
    """Calls the Bloomberg Open API to download market data: daily, intraday
    and reference data (needs blpapi).

    """

    def __init__(self):
        super(DataVendorBBGOpen, self).__init__()

    def download_tick(self, md_request):
        # Bloomberg OpenAPI implementation
        low_level_loader = BBGLowLevelTick()

        # by default we download all available fields!
        data_frame = low_level_loader.load_time_series(md_request)

        # self.kill_session() # need to forcibly kill_session since can't
        # always reopen

        return data_frame

    def download_intraday(self, md_request):
        # Bloomberg OpenAPI implementation
        low_level_loader = BBGLowLevelIntraday()

        # by default we download all available fields!
        data_frame = low_level_loader.load_time_series(md_request)

        # self.kill_session() # need to forcibly kill_session since can't
        # always reopen

        return data_frame

    def download_daily(self, md_request):
        # Bloomberg Open API implementation
        low_level_loader = BBGLowLevelDaily()

        # By default we download all available fields!
        data_frame = low_level_loader.load_time_series(md_request)

        # self.kill_session() # need to forcibly kill_session since can't
        # always reopen

        return data_frame

    def download_ref(self, md_request):
        # Bloomberg Open API implementation
        low_level_loader = BBGLowLevelRef()

        md_request_vendor_selective = copy.copy(md_request)

        # special case for future date releases
        # if 'release-date-time-full' in md_request.fields:
        #     md_request_vendor_selective.fields = ['ECO_FUTURE_RELEASE_DATE_LIST']
        #
        # if 'last-tradeable-day' in md_request.fields:
        #     md_request_vendor_selective.fields = ['LAST_TRADEABLE_DT']

        data_frame = low_level_loader.load_time_series(
            md_request_vendor_selective)

        # self.kill_session() # need to forcibly kill_session since can't
        # always reopen

        return data_frame

    def kill_session(self):
        # TODO not really needed, because we automatically kill sessions
        BBGLowLevelDaily().kill_session(None)
        BBGLowLevelRef().kill_session(None)
        BBGLowLevelIntraday().kill_session(None)


###############################################################################
#### Lower level code to interact with Bloomberg Open API

class BBGLowLevelTemplate:  # in order that the init function works in
    # child classes

    convert_override_fields = {
        'settlement-calendar-code': 'SETTLEMENT_CALENDAR_CODE'}

    _session = None

    def __init__(self):
        self.RESPONSE_ERROR = blpapi.Name("responseError")
        self.SESSION_TERMINATED = blpapi.Name("SessionTerminated")
        self.CATEGORY = blpapi.Name("category")
        self.MESSAGE = blpapi.Name("message")

        return

    def load_time_series(self, md_request):

        # if(BBGLowLevelTemplate._session is None):
        logger = LoggerManager().getLogger(__name__)

        session = self.start_bloomberg_session()

        # else:
        #    session = BBGLowLevelTemplate._session

        def download_data_frame(sess, eventQ, opt, ci):
            if opt.security is not None:
                self.send_bar_request(sess, eventQ, opt, ci)

                logger.info("Waiting for data to be returned...")

                return self.event_loop(sess)
            else:
                logger.warn("No ticker or field specified!")

                return None

        try:
            # if can't open the session, kill existing one
            # then try reopen (up to 5 times...)
            i = 0

            while i < 5:
                if session is not None:
                    if not session.openService("//blp/refdata"):
                        logger.info(
                            "Try reopening Bloomberg session... try " +
                            str(i))
                        self.kill_session(
                            session)  # need to forcibly kill_session since
                        # can't always reopen
                        session = self.start_bloomberg_session()

                        if session is not None:
                            if session.openService("//blp/refdata"): i = 6
                else:
                    logger.info(
                        "Try opening Bloomberg session... try " + str(i))
                    session = self.start_bloomberg_session()

                i = i + 1

            # Give error if still doesn't work after several tries..
            if not session.openService("//blp/refdata"):
                logger.error("Failed to open //blp/refdata")

                return

            logger.info("Creating request...")

            eventQueue = blpapi.EventQueue()
            # eventQueue = None

            # Create a request
            from blpapi import CorrelationId

            options = self.fill_options(md_request)

            # In some instances we might split the options if need to have
            # different overrides
            if isinstance(options, list):
                data_frame_list = []

                for op in options:
                    cid = CorrelationId()
                    data_frame_list.append(
                        download_data_frame(session, eventQueue, op, cid))

                data_frame = Calculations().join(data_frame_list)
            else:
                cid = CorrelationId()
                data_frame = download_data_frame(session, eventQueue, options,
                                                 cid)
        finally:
            # stop the session (will fail if NoneType)
            try:
                session.stop()
            except:
                pass

        return data_frame

    def event_loop(self, session):
        not_done = True

        data_frame_slice = None

        data_frame_list = []
        data_frame_cols = []

        while not_done:
            # nextEvent() method can be called with timeout to let
            # the program catch Ctrl-C between arrivals of new events
            event = session.nextEvent()  # removed time out
            # event = eventQueue.nextEvent()

            # Bloomberg will send us responses in chunks
            if event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                # logger.info("Processing Bloomberg Partial Response")
                data_frame_slice = self.process_response_event(event)
            elif event.eventType() == blpapi.Event.RESPONSE:
                # logger.info("Processing Bloomberg Full Response")
                data_frame_slice = self.process_response_event(event)
                not_done = False
            else:
                for msg in event:
                    if event.eventType() == blpapi.Event.SESSION_STATUS:

                        if msg.messageType() == self.SESSION_TERMINATED:
                            not_done = False

            # Append DataFrame only if not empty
            if data_frame_slice is not None:
                data_frame_list.append(
                    self.combine_slices(data_frame_cols, data_frame_slice))

                # Keep list of columns we've already found (occasionally BBG
                # seems to return columns more than once?)
                # (this will fail for intraday time series)
                try:
                    data_frame_cols.append(list(
                        data_frame_slice.columns.get_level_values(1).values)[
                                               0])
                except:
                    pass

        if data_frame_cols == [] and data_frame_list != []:  # intraday case
            data_frame = pd.concat(data_frame_list)
        else:  # daily frequencies
            data_frame = Calculations().join(data_frame_list, how='outer')

        # make sure we do not have any duplicates in the time series
        if data_frame is not None:
            # if data_frame.empty == False:
            data_frame.drop_duplicates(keep='last')

        return data_frame

    # Process raw message returned by Bloomberg
    def process_response_event(self, event):
        data_frame_list = []

        logger = LoggerManager().getLogger(__name__)

        for msg in event:
            # Generates a lot of output - so don't use unless for
            # debugging purposes
            # logger.info(msg)

            if msg.hasElement(self.RESPONSE_ERROR):
                logger.error("REQUEST FAILED: " + str(
                    msg.getElement(self.RESPONSE_ERROR)))
                continue

            data_frame_slice = self.process_message(msg)

            if (data_frame_slice is not None):
                data_frame_list.append(data_frame_slice)

        if data_frame_list == []:
            logger.warn("No elements for ticker.")
            return None
        else:
            return pd.concat(data_frame_list)

    def get_previous_trading_date(self):
        tradedOn = datetime.date.today()

        while True:
            try:
                tradedOn -= datetime.timedelta(days=1)
            except OverflowError:
                return None

            if tradedOn.weekday() not in [5, 6]:
                return tradedOn

    # Create a session for Bloomberg with appropriate server & port
    def start_bloomberg_session(self):

        constants = DataConstants()
        tries = 0

        session = None

        logger = LoggerManager().getLogger(__name__)

        # Try up to 5 times to start a session
        while (tries < 5):
            try:
                # fill SessionOptions
                sessionOptions = blpapi.SessionOptions()
                sessionOptions.setServerHost(constants.bbg_server)
                sessionOptions.setServerPort(constants.bbg_server_port)

                logger.info("Starting Bloomberg session...")

                # create a Session
                session = blpapi.Session(sessionOptions)

                # start a Session
                if not session.start():
                    logger.error("Failed to start session.")
                    return

                logger.info("Returning session...")

                tries = 5
            except:
                tries = tries + 1

        # BBGLowLevelTemplate._session = session

        if session is None:
            logger.error("Failed to start session.")
            return

        return session

    def add_override(self, request, field, value):
        overrides = request.getElement("overrides")
        override1 = overrides.appendElement()
        override1.setElement("fieldId", field)
        override1.setElement("value", value)

    def add_override_dict(self, request, options):
        if options.overrides != {}:
            for k in options.overrides.keys():
                new_k = k

                # Is there a pretty name for this?
                if k in self.convert_override_fields:
                    new_k = self.convert_override_fields[k]

                self.add_override(request, new_k, options.overrides[k])

    @abc.abstractmethod
    def fill_options(self, md_request):
        pass

    @abc.abstractmethod
    def process_message(self, msg):
        # To be implemented by subclass
        return

    # Create request for data
    @abc.abstractmethod
    def send_bar_request(self, session, eventQueue, options, cid):
        # To be implemented by subclass
        return

    # create request for data
    @abc.abstractmethod
    def combine_slices(self, data_frame_cols, data_frame_slice):
        # To be implemented by subclass
        return

    def kill_session(self, session):
        logger = LoggerManager().getLogger(__name__)
        if (session is not None):
            try:
                session.stop()

                logger.info("Stopping session...")
            finally:
                logger.info("Finally stopping session...")

            session = None


class BBGLowLevelDaily(BBGLowLevelTemplate):

    def __init__(self):
        super(BBGLowLevelDaily, self).__init__()

    def combine_slices(self, data_frame_cols, data_frame_slice):
        # data
        try:
            if (data_frame_slice.columns.get_level_values(1).values[0]
                    not in data_frame_cols):
                # return data_frame.join(data_frame_slice, how="outer")
                return data_frame_slice
        except Exception as e:

            LoggerManager().getLogger(__name__).warn(
                'Data slice empty ' + str(e))

            return None

        return None

    # Populate options for Bloomberg request for asset daily request
    def fill_options(self, md_request):
        constants = DataConstants()

        options = OptionsBBG()

        options.security = None  # md_request.tickers
        options.startDateTime = md_request.start_date
        options.endDateTime = md_request.finish_date
        options.fields = md_request.fields

        options.overrides = md_request.overrides

        options_list = []

        override_dict = {}

        if md_request.old_tickers is not None:

            ticker_list = []
            # curr_options = OptionsBBG(options_bbg=options)

            ## Special case for GDP where the advance, final and preliminary
            # releases (but can define more in DataConstants)
            # have the same ticker but different overrides
            bbg_keyword_dict_override = constants.bbg_keyword_dict_override

            for tick, old_tick in zip(md_request.tickers,
                                      md_request.old_tickers):
                if old_tick is not None:

                    t = old_tick.lower()

                    # eg. RELEASE_STAGE_OVERRIDE
                    for bbg_override in bbg_keyword_dict_override.keys():

                        keyword_dict = bbg_keyword_dict_override[bbg_override]

                        for bbg_keyword in keyword_dict.keys():

                            # eg. ['gdp', 'advance']
                            keyword = keyword_dict[bbg_keyword]

                            # if this matches a case, we have override
                            if all(k.lower() in t for k in keyword):

                                # In case we have multiple overrides for
                                # this ticker
                                if tick in override_dict:
                                    override_dict[tick][
                                        bbg_override] = bbg_keyword
                                else:
                                    override_dict[tick] = {
                                        bbg_override: bbg_keyword}

                    ## Add other special cases
                    if tick not in override_dict:
                        override_dict[tick] = {}

            # if ticker_list != []:
            #    curr_options.security = ticker_list

            last_override = {}

            def add_new_options(tick_):
                curr_options = OptionsBBG(options_bbg=options)
                curr_options.security = [tick_]

                if override != {}:
                    curr_options.overrides = override

                options_list.append(curr_options)

            # Combine the securities into a list of options (each with common
            # overrides)
            for tick, override in override_dict.items():
                if override == last_override:

                    if len(options_list) > 0:
                        options_list[-1].security.append(tick)
                    else:
                        add_new_options(tick)
                else:
                    add_new_options(tick)

                last_override = override

            # print('stop')
            # options_list.append(curr_options)
        else:
            options.security = md_request.tickers

            return options

        if len(options_list) == 1:
            return options_list[0]

        return options_list

    def process_message(self, msg):

        constants = DataConstants()
        # Process received events

        # SLOW loop (careful, not all the fields will be returned every time
        # hence need to include the field name in the tuple)
        # perhaps try to run in parallel?
        logger = LoggerManager().getLogger(__name__)


        ticker = msg.getElement('securityData').getElement(
            'security').getValue()
        fieldData = msg.getElement('securityData').getElement('fieldData')

        data = defaultdict(dict)

        # FASTER avoid calling getValue/getElement methods in blpapi,
        # very slow, better to cache variables
        for i in range(fieldData.numValues()):
            mini_field_data = fieldData.getValue(i)
            date = mini_field_data.getElement(0).getValue()

            for j in range(1, mini_field_data.numElements()):
                field_value = mini_field_data.getElement(j)

                data[(str(field_value.name()), ticker)][
                    date] = field_value.getValue()

        # ORIGINAL repeated calling getValue/getElement much slower
        # for i in range(fieldData.numValues()):
        #     for j in range(1, fieldData.getValue(i).numElements()):
        #         data[(str(fieldData.getValue(i).getElement(j).name()),
        #         ticker)][fieldData.getValue(i).getElement(0).getValue()] \
        #             = fieldData.getValue(i).getElement(j).getValue()

        data_frame = pd.DataFrame(data)

        # If obsolete ticker could return no values
        if data_frame.empty:
            return None
        else:
            # data_frame.columns = pd.MultiIndex.from_tuples(data,
            # names=['field', 'ticker'])
            data_frame.index = pd.to_datetime(data_frame.index)
            logger.info("Read: " + ticker + ' ' + str(
                data_frame.index.min()) + ' - ' + str(data_frame.index.max()))

        return data_frame

    # Create request for data
    def send_bar_request(self, session, eventQueue, options, cid):
        logger = LoggerManager().getLogger(__name__)

        refDataService = session.getService("//blp/refdata")
        request = refDataService.createRequest("HistoricalDataRequest")

        request.set("startDate", options.startDateTime.strftime('%Y%m%d'))
        request.set("endDate", options.endDateTime.strftime('%Y%m%d'))

        # Only one security/eventType per request
        for field in options.fields:
            request.getElement("fields").appendValue(field)

        for security in options.security:
            request.getElement("securities").appendValue(security)

        # Add user defined overrides for BBG request
        self.add_override_dict(request, options)

        logger.info("Sending Bloomberg Daily Request:" + str(request))
        session.sendRequest(request=request, correlationId=cid)


class BBGLowLevelRef(BBGLowLevelTemplate):

    def __init__(self):
        super(BBGLowLevelRef, self).__init__()

    # Populate options for Bloomberg request for reference request
    def fill_options(self, md_request):
        options = OptionsBBG()

        options.security = md_request.tickers
        options.startDateTime = md_request.start_date
        options.endDateTime = md_request.finish_date
        options.fields = md_request.fields

        options.overrides = md_request.overrides

        return options

    def process_message(self, msg):
        logger = LoggerManager().getLogger(__name__)
        data = collections.defaultdict(dict)

        # process received events
        securityDataArray = msg.getElement('securityData')

        index = 0
        single = False

        for securityData in list(securityDataArray.values()):

            ticker = securityData.getElementAsString("security")
            fieldData = securityData.getElement("fieldData")

            for field in fieldData.elements():
                if not field.isValid():
                    field_name = "%s" % field.name()

                    logger.error(field_name + " is NULL")
                elif field.isArray():
                    # iterate over complex data returns.
                    field_name = "%s" % field.name()

                    for i, row in enumerate(field.values()):
                        try:
                            field_val = re.findall(r'"(.*?)"', "%s" % row)[0]
                        except:
                            e = row.getElement(0)
                            # k = str(e.name())
                            field_val = e.getValue()

                        data[(field_name, ticker)][index] = field_val

                        index = index + 1
                else:
                    field_name = "%s" % field.name()
                    data[(field_name, ticker)][0] = field.getValueAsString()

                    index = index + 1
                    single = True  # no need to create multi-index late,
                    # because just row!! CAREFUL!! needed for futures expiries

            fieldExceptionArray = securityData.getElement("fieldExceptions")

            for fieldException in list(fieldExceptionArray.values()):
                errorInfo = fieldException.getElement("errorInfo")

                print(errorInfo.getElementAsString("category"), ":", \
                      fieldException.getElementAsString("fieldId"))
                print("stop")

        # Explicitly state from_dict (buggy if create pd.DataFrame(data)
        data_frame = pd.DataFrame.from_dict(data)

        # If obsolete ticker could return no values
        if data_frame.empty:
            return None
        else:
            logger.info("Reading: " + ticker + ' ' + str(
                data_frame.index[0]) + ' - ' + str(data_frame.index[-1]))

        return data_frame

    def combine_slices(self, data_frame_cols, data_frame_slice):
        if (data_frame_slice.columns.get_level_values(1).values[
            0] not in data_frame_cols):
            # return data_frame.join(data_frame_slice, how="outer")
            return data_frame_slice

        return None

    # Create request for data
    def send_bar_request(self, session, eventQueue, options, cid):
        logger = LoggerManager().getLogger(__name__)

        refDataService = session.getService("//blp/refdata")
        request = refDataService.createRequest('ReferenceDataRequest')

        self.add_override(request, 'TIME_ZONE_OVERRIDE', 23)  # force GMT time
        self.add_override(request, 'INCLUDE_EXPIRED_CONTRACTS',
                          "Y")  # include expired contracts
        self.add_override(request, 'START_DT',
                          options.startDateTime.strftime('%Y%m%d'))
        self.add_override(request, 'END_DT',
                          options.endDateTime.strftime('%Y%m%d'))

        # Only one security/eventType per request
        for field in options.fields:
            request.getElement("fields").appendValue(field)

        for security in options.security:
            request.getElement("securities").appendValue(security)

        # Add user defined overrides for BBG request
        self.add_override_dict(request, options)

        logger.info("Sending Bloomberg Ref Request:" + str(request))
        session.sendRequest(request=request, correlationId=cid)


class BBGLowLevelIntraday(BBGLowLevelTemplate):

    def __init__(self):
        super(BBGLowLevelIntraday, self).__init__()

        # constants
        self.BAR_DATA = blpapi.Name("barData")
        self.BAR_TICK_DATA = blpapi.Name("barTickData")
        self.OPEN = blpapi.Name("open")
        self.HIGH = blpapi.Name("high")
        self.LOW = blpapi.Name("low")
        self.CLOSE = blpapi.Name("close")
        self.VOLUME = blpapi.Name("volume")
        self.NUM_EVENTS = blpapi.Name("numEvents")
        self.TIME = blpapi.Name("time")

    def combine_slices(self, data_frame_cols, data_frame_slice):
        # return data_frame.append(data_frame_slice)
        return data_frame_slice

    # Populate options for Bloomberg request for asset intraday request
    def fill_options(self, md_request):
        options = OptionsBBG()

        options.security = md_request.tickers[
            0]  # get 1st ticker only!
        options.event = md_request.trade_side.upper()
        options.barInterval = md_request.freq_mult
        options.startDateTime = md_request.start_date
        options.endDateTime = md_request.finish_date
        options.gapFillInitialBar = False
        options.overrides = md_request.overrides

        if hasattr(options.startDateTime, 'microsecond'):
            options.startDateTime = options.startDateTime.replace(
                microsecond=0)

        if hasattr(options.endDateTime, 'microsecond'):
            options.endDateTime = options.endDateTime.replace(microsecond=0)

        return options

    # iterate through Bloomberg output creating a DataFrame output
    # implements abstract method
    def process_message(self, msg):
        data = msg.getElement(self.BAR_DATA).getElement(self.BAR_TICK_DATA)

        logger = LoggerManager().getLogger(__name__)

        data_vals = list(data.values())

        # Each price time point has multiple fields - marginally quicker
        tuple = [([bar.getElementAsFloat(self.OPEN),
                   bar.getElementAsFloat(self.HIGH),
                   bar.getElementAsFloat(self.LOW),
                   bar.getElementAsFloat(self.CLOSE),
                   bar.getElementAsInteger(self.VOLUME),
                   bar.getElementAsInteger(self.NUM_EVENTS)],
                   bar.getElementAsDatetime(self.TIME)) for bar in data_vals]

        data_table = list(map(itemgetter(0), tuple))
        time_list = list(map(itemgetter(1), tuple))

        try:
            logger.info("Dates between " + str(time_list[0]) + " - " + str(
                time_list[-1]))
        except:
            logger.info("No dates retrieved")
            return None

        # create pandas dataframe with the Bloomberg output
        return pd.DataFrame(data=data_table, index=time_list,
                            columns=["open", "high", "low", "close", "volume",
                                     "events"])

    # Implement abstract method: create request for data
    def send_bar_request(self, session, eventQueue, options, cid):
        logger = LoggerManager().getLogger(__name__)
        refDataService = session.getService("//blp/refdata")
        request = refDataService.createRequest("IntradayBarRequest")

        # only one security/eventType per request
        request.set("security", options.security)
        request.set("eventType", options.event)
        request.set("interval", options.barInterval)

        # self.add_override(request, 'TIME_ZONE_OVERRIDE', 'GMT')

        if options.startDateTime is not None and options.endDateTime is not None:
            request.set("startDateTime", options.startDateTime)
            request.set("endDateTime", options.endDateTime)

        if options.gapFillInitialBar:
            request.append("gapFillInitialBar", True)

        # Add user defined overrides for BBG request
        self.add_override_dict(request, options)

        logger.info("Sending Intraday Bloomberg Request...")

        session.sendRequest(request=request, correlationId=cid)


class BBGLowLevelTick(BBGLowLevelTemplate):

    def __init__(self):
        super(BBGLowLevelTick, self).__init__()

        # constants
        self.TICK_DATA = blpapi.Name("tickData")
        self.COND_CODE = blpapi.Name("conditionCodes")
        self.TICK_SIZE = blpapi.Name("size")
        self.TIME = blpapi.Name("time")
        self.TYPE = blpapi.Name("type")
        self.VALUE = blpapi.Name("value")
        self.RESPONSE_ERROR = blpapi.Name("responseError")
        self.CATEGORY = blpapi.Name("category")
        self.MESSAGE = blpapi.Name("message")
        self.SESSION_TERMINATED = blpapi.Name("SessionTerminated")

    def combine_slices(self, data_frame, data_frame_slice):
        # return data_frame.append(data_frame_slice)
        return data_frame_slice

    # Populate options for Bloomberg request for asset intraday request
    def fill_options(self, md_request):
        options = OptionsBBG()

        options.security = md_request.tickers[
            0]  # Get 1st ticker only!
        options.event = md_request.trade_side.upper()
        # self._options.barInterval = md_request.freq_mult
        options.startDateTime = md_request.start_date
        options.endDateTime = md_request.finish_date
        # self._options.gapFillInitialBar = False

        if hasattr(options.startDateTime, "microsecond"):
            options.startDateTime = options.startDateTime.replace(
                microsecond=0)

        if hasattr(options.endDateTime, "microsecond"):
            options.endDateTime = options.endDateTime.replace(microsecond=0)

        return options

    # iterate through Bloomberg output creating a DataFrame output
    # implements abstract method
    def process_message(self, msg):
        data = msg.getElement(self.TICK_DATA).getElement(self.TICK_DATA)
        logger = LoggerManager().getLogger(__name__)

        #  logger.info("Processing tick data for " + str(self._options.security))

        data_vals = data.values()

        # for item in list(data_vals):
        #     if item.hasElement(self.COND_CODE):
        #         cc = item.getElementAsString(self.COND_CODE)
        #     else:
        #         cc = ""
        #
        #     # each price time point has multiple fields - marginally quicker
        #     tuple.append(([item.getElementAsFloat(self.VALUE),
        #                     item.getElementAsInteger(self.TICK_SIZE)],
        #                     item.getElementAsDatetime(self.TIME)))

        # slightly faster this way (note, we are skipping trade & CC fields)
        tuple = [([item.getElementAsFloat(self.VALUE),
                   item.getElementAsInteger(self.TICK_SIZE)],
                  item.getElementAsDatetime(self.TIME)) for item in data_vals]

        data_table = list(map(itemgetter(0), tuple))
        time_list = list(map(itemgetter(1), tuple))

        try:
            logger.info("Dates between " + str(time_list[0]) + " - " + str(
                time_list[-1]))
        except:
            logger.info("No dates retrieved")
            return None

        # create pandas dataframe with the Bloomberg output
        return pd.DataFrame(data=data_table, index=time_list,
                            columns=["close", "ticksize"])

    # Implement abstract method: create request for data
    def send_bar_request(self, session, eventQueue, options, cid):
        logger = LoggerManager().getLogger(__name__)

        refDataService = session.getService("//blp/refdata")
        request = refDataService.createRequest("IntradayTickRequest")

        # only one security/eventType per request
        request.set("security", options.security)
        request.getElement("eventTypes").appendValue("TRADE")
        # request.set("eventTypes", self._options.event)
        request.set("includeConditionCodes", True)

        # self.add_override(request, 'TIME_ZONE_OVERRIDE', 'GMT')

        if options.startDateTime and options.endDateTime:
            request.set("startDateTime", options.startDateTime)
            request.set("endDateTime", options.endDateTime)

        # Add user defined overrides for BBG request
        self.add_override_dict(request, options)

        logger.info("Sending Tick Bloomberg Request...")

        session.sendRequest(request=request, correlationId=cid)


###############################################################################

from findatapy.util.loggermanager import LoggerManager
from datetime import datetime


class OptionsBBG(object):

    def __init__(self, options_bbg=None, security=None, event=None,
                 barInterval=None, startDateTime=None, endDateTime=None,
                 fields=None, overrides=None, gapFillInitialBar=False):
        if options_bbg is not None:
            security = copy.copy(options_bbg.security)
            event = copy.copy(options_bbg.event)
            barInterval = copy.copy(options_bbg.barInterval)
            startDateTime = copy.copy(options_bbg.startDateTime)
            endDateTime = copy.copy(options_bbg.endDateTime)
            fields = copy.copy(options_bbg.fields)
            overrides = copy.copy(options_bbg.overrides)
            gapFillInitialBar = copy.copy(options_bbg.gapFillInitialBar)

        self.security = security
        self.event = event
        self.barInterval = barInterval
        self.startDateTime = startDateTime
        self.endDateTime = endDateTime
        self.fields = fields
        self.overrides = overrides
        self.gapFillInitialBar = gapFillInitialBar

    @property
    def security(self):
        return self.__security

    @security.setter
    def security(self, security):
        self.__security = security

    @property
    def event(self):
        return self.__event

    @event.setter
    def event(self, event):
        self.__event = event

    @property
    def barInterval(self):
        return self.__barInterval

    @barInterval.setter
    def barInterval(self, barInterval):
        self.__barInterval = barInterval

    @property
    def startDateTime(self):
        return self.__startDateTime

    @startDateTime.setter
    def startDateTime(self, startDateTime):
        self.__startDateTime = startDateTime

    @property
    def endDateTime(self):
        return self.__endDateTime

    @endDateTime.setter
    def endDateTime(self, endDateTime):
        self.__endDateTime = endDateTime

    @property
    def fields(self):
        return self.__fields

    @fields.setter
    def fields(self, fields):
        self.__fields = fields

    @property
    def overrides(self):
        return self.__overrides

    @overrides.setter
    def overrides(self, overrides):
        self.__overrides = overrides

    @property
    def gapFillInitialBar(self):
        return self.__gapFillInitialBar

    @gapFillInitialBar.setter
    def gapFillInitialBar(self, gapFillInitialBar):
        self.__gapFillInitialBar = gapFillInitialBar
