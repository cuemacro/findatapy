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
LoaderBBG

Abstract class for download of Bloomberg daily, intraday data and reference data.

Implemented by
- LoaderBBGOpen (adapted version of new Bloomberg Open API for Python - recommended - although requires compilation)

"""

from findatapy.market.datavendor import DataVendor

import abc
import datetime
import copy

class DataVendorBBG(DataVendor):

    def __init__(self):
        super(DataVendorBBG, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        """
        load_ticker - Retrieves market data from external data source (in this case Bloomberg)

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains all the various parameters detailing time series start and finish, tickers etc

        Returns
        -------
        DataFrame
        """
        market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        data_frame = None
        self.logger.info("Request Bloomberg data")

        # do we need daily or intraday data?
        if (market_data_request.freq in ['daily', 'weekly', 'monthly', 'quarterly', 'yearly']):

            # for events times/dates separately needs ReferenceDataRequest (when specified)
            if 'release-date-time-full' in market_data_request.fields:

                # experimental!!
                # careful: make sure you copy the market data request object (when threading, altering that can
                # cause concurrency issues!)
                datetime_data_frame = self.get_reference_data(market_data_request_vendor, market_data_request)

                old_fields = copy.deepcopy(market_data_request.fields)
                old_vendor_fields = copy.deepcopy(market_data_request_vendor.fields)

                # remove fields 'release-date-time-full' from our request (and the associated field in the vendor)
                # if they are there
                try:
                    index = market_data_request.fields.index('release-date-time-full')

                    market_data_request.fields.pop(index)
                    market_data_request_vendor.fields.pop(index)
                except:
                    pass

                # download all the other event fields (uses HistoricalDataRequest to Bloomberg)
                # concatenate with date time fields
                if len(market_data_request_vendor.fields) > 0:
                    events_data_frame = self.get_daily_data(market_data_request, market_data_request_vendor)

                    col = events_data_frame.index.name
                    events_data_frame = events_data_frame.reset_index(drop = False)

                    data_frame = pandas.concat([events_data_frame, datetime_data_frame], axis = 1)
                    temp = data_frame[col]
                    del data_frame[col]
                    data_frame.index = temp
                else:
                    data_frame = datetime_data_frame

                market_data_request.fields = old_fields
                market_data_request_vendor.fields = old_vendor_fields

            # for all other daily/monthly/quarter data, we can use HistoricalDataRequest to Bloomberg
            else:
                data_frame = self.get_daily_data(market_data_request, market_data_request_vendor)

        # assume one ticker only
        # for intraday data we use IntradayDataRequest to Bloomberg
        if (market_data_request.freq in ['tick', 'intraday', 'second', 'minute', 'hourly']):
            market_data_request_vendor.tickers = market_data_request_vendor.tickers[0]

            if market_data_request.freq in ['tick', 'second']:
                data_frame = self.download_tick(market_data_request_vendor)
            else:
                data_frame = self.download_intraday(market_data_request_vendor)

            if data_frame is not None:
                if data_frame.empty:
                    try:
                        self.logger.info("No tickers returned for: " + market_data_request_vendor.tickers)
                    except:
                        pass

                    return None

                cols = data_frame.columns.values

                import pytz

                try:
                    data_frame = data_frame.tz_localize(pytz.utc)
                except:
                    data_frame = data_frame.tz_convert(pytz.utc)

                cols = market_data_request.tickers[0] + "." + cols
                data_frame.columns = cols

        self.logger.info("Completed request from Bloomberg.")

        return data_frame

    def get_daily_data(self, market_data_request, market_data_request_vendor):
        data_frame = self.download_daily(market_data_request_vendor)

        # convert from vendor to findatapy tickers/fields
        if data_frame is not None:
            if data_frame.empty:
                self.logger.info("No tickers returned for...")

                try:
                    self.logger.info(str(market_data_request_vendor.tickers))
                except: pass

                return None

            returned_fields = data_frame.columns.get_level_values(0)
            returned_tickers = data_frame.columns.get_level_values(1)

            # TODO if empty try downloading again a year later
            try:
                fields = self.translate_from_vendor_field(returned_fields, market_data_request)
            except:
                print('t')

            tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)

            ticker_combined = []

            for i in range(0, len(fields)):
                ticker_combined.append(tickers[i] + "." + fields[i])

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

        return data_frame

    def get_reference_data(self, market_data_request_vendor, market_data_request):
        end = datetime.utcnow()

        from datetime import timedelta
        end = end + timedelta(days=365)# end.replace(year = end.year + 1)

        market_data_request_vendor.finish_date = end

        self.logger.debug("Requesting ref for " + market_data_request_vendor.tickers[0] + " etc.")

        data_frame = self.download_ref(market_data_request_vendor)

        self.logger.debug("Waiting for ref...")

        # convert from vendor to findatapy tickers/fields
        if data_frame is not None:
            returned_fields = data_frame.columns.get_level_values(0)
            returned_tickers = data_frame.columns.get_level_values(1)

        if data_frame is not None:
            # TODO if empty try downloading again a year later
            fields = self.translate_from_vendor_field(returned_fields, market_data_request)
            tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)

            ticker_combined = []

            for i in range(0, len(fields)):
                ticker_combined.append(tickers[i] + "." + fields[i])

            data_frame.columns = ticker_combined

            # TODO coerce will be deprecated from pandas
            data_frame = data_frame.convert_objects(convert_dates = 'coerce', convert_numeric= 'coerce')

        return data_frame

    # implement method in abstract superclass
    @abc.abstractmethod
    def kill_session(self):
        return

    @abc.abstractmethod
    def download_tick(self, market_data_request):
        return

    @abc.abstractmethod
    def download_intraday(self, market_data_request):
        return

    @abc.abstractmethod
    def download_daily(self, market_data_request):
        return

    @abc.abstractmethod
    def download_ref(self, market_data_request):
        return

#######################################################################################################################

"""
LoaderBBGOpen

Calls the Bloomberg Open API to download market data: daily, intraday and reference data (needs blpapi).

"""

import abc
import copy
import collections
import datetime
import re

import pandas

try:
    import blpapi   # obtainable from Bloomberg website
except: pass

from findatapy.util.dataconstants import DataConstants
from findatapy.market.datavendorbbg import DataVendorBBG

from collections import defaultdict

class DataVendorBBGOpen(DataVendorBBG):
    def __init__(self):
        super(DataVendorBBGOpen, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

    def download_tick(self, market_data_request):
        # Bloomberg OpenAPI implementation
        low_level_loader = BBGLowLevelTick()

        # by default we download all available fields!
        data_frame = low_level_loader.load_time_series(market_data_request)

        # self.kill_session() # need to forcibly kill_session since can't always reopen

        return data_frame


    def download_intraday(self, market_data_request):
        # Bloomberg OpenAPI implementation
        low_level_loader = BBGLowLevelIntraday()

        # by default we download all available fields!
        data_frame = low_level_loader.load_time_series(market_data_request)

        # self.kill_session() # need to forcibly kill_session since can't always reopen

        return data_frame

    def download_daily(self, market_data_request):
        # Bloomberg Open API implementation
        low_level_loader = BBGLowLevelDaily()

        # by default we download all available fields!
        data_frame = low_level_loader.load_time_series(market_data_request)

        # self.kill_session() # need to forcibly kill_session since can't always reopen

        return data_frame

    def download_ref(self, market_data_request):

         # Bloomberg Open API implementation
        low_level_loader = BBGLowLevelRef()

        market_data_request_vendor_selective = copy.copy(market_data_request)
        market_data_request_vendor_selective.fields = ['ECO_FUTURE_RELEASE_DATE_LIST']

        data_frame =  low_level_loader.load_time_series(market_data_request_vendor_selective)

        # self.kill_session() # need to forcibly kill_session since can't always reopen

        return data_frame

    def kill_session(self):
        # TODO not really needed, because we automatically kill sessions
        BBGLowLevelDaily().kill_session(None)
        BBGLowLevelRef().kill_session(None)
        BBGLowLevelIntraday().kill_session(None)

########################################################################################################################
#### Lower level code to interact with Bloomberg Open API

class BBGLowLevelTemplate:

    _session = None

    def __init__(self):
        self._data_frame = None

        self.RESPONSE_ERROR = blpapi.Name("responseError")
        self.SESSION_TERMINATED = blpapi.Name("SessionTerminated")
        self.CATEGORY = blpapi.Name("category")
        self.MESSAGE = blpapi.Name("message")

        return

    def load_time_series(self, market_data_request):

        options = self.fill_options(market_data_request)

        #if(BBGLowLevelTemplate._session is None):
        session = self.start_bloomberg_session()
        #else:
        #    session = BBGLowLevelTemplate._session

        try:
            # if can't open the session, kill existing one
            # then try reopen (up to 5 times...)
            i = 0

            while i < 5:
                if session is not None:
                    if not session.openService("//blp/refdata"):
                        self.logger.info("Try reopening Bloomberg session... try " + str(i))
                        self.kill_session(session) # need to forcibly kill_session since can't always reopen
                        session = self.start_bloomberg_session()

                        if session is not None:
                            if session.openService("//blp/refdata"): i = 6
                else:
                    self.logger.info("Try opening Bloomberg session... try " + str(i))
                    session = self.start_bloomberg_session()

                i = i + 1

            # give error if still doesn't work after several tries..
            if not session.openService("//blp/refdata"):
                self.logger.error("Failed to open //blp/refdata")

                return

            self.logger.info("Creating request...")
            eventQueue = None # blpapi.EventQueue()

            # create a request
            self.send_bar_request(session, eventQueue)
            self.logger.info("Waiting for data to be returned...")

            # wait for events from session and collect the data
            self.event_loop(session, eventQueue)

        finally:
            # stop the session (will fail if NoneType)
            try:
                session.stop()
            except: pass

        return self._data_frame

    def event_loop(self, session, eventQueue):
        not_done = True

        data_frame = pandas.DataFrame()
        data_frame_slice = None

        while not_done:
            # nextEvent() method can be called with timeout to let
            # the program catch Ctrl-C between arrivals of new events
            event = session.nextEvent() # removed time out

            # Bloomberg will send us responses in chunks
            if event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                # self.logger.info("Processing Bloomberg Partial Response")
                data_frame_slice = self.process_response_event(event)
            elif event.eventType() == blpapi.Event.RESPONSE:
                # self.logger.info("Processing Bloomberg Full Response")
                data_frame_slice = self.process_response_event(event)
                not_done = False
            else:
                for msg in event:
                    if event.eventType() == blpapi.Event.SESSION_STATUS:
                        if msg.messageType() == self.SESSION_TERMINATED:
                            not_done = False

            # append DataFrame only if not empty
            if data_frame_slice is not None:
                if (data_frame.empty):
                    data_frame = data_frame_slice
                else:
                    # make sure we do not reattach a message we've already read
                    # sometimes Bloomberg can give us back the same message several times
                    # CAREFUL with this!
                    # compares the ticker names, and make sure they don't already exist in the time series
                    data_frame = self.combine_slices(data_frame, data_frame_slice)

        # make sure we do not have any duplicates in the time series
        if data_frame is not None:
            if data_frame.empty == False:
                data_frame.drop_duplicates(keep='last')

        self._data_frame = data_frame

    # process raw message returned by Bloomberg
    def process_response_event(self, event):
        data_frame = pandas.DataFrame()

        for msg in event:
            # generates a lot of output - so don't use unless for debugging purposes
            # self.logger.info(msg)

            if msg.hasElement(self.RESPONSE_ERROR):
                self.logger.error("REQUEST FAILED: " + str(msg.getElement(self.RESPONSE_ERROR)))
                continue

            data_frame_slice = self.process_message(msg)

            if (data_frame_slice is not None):
                if (data_frame.empty):
                    data_frame = data_frame_slice
                else:
                    data_frame = data_frame.append(data_frame_slice)
            else:
                data_frame = data_frame_slice

        return data_frame

    def get_previous_trading_date(self):
        tradedOn = datetime.date.today()

        while True:
            try:
                tradedOn -= datetime.timedelta(days=1)
            except OverflowError:
                return None

            if tradedOn.weekday() not in [5, 6]:
                return tradedOn

    # create a session for Bloomberg with appropriate server & port
    def start_bloomberg_session(self):
        tries = 0

        session = None

        # try up to 5 times to start a session
        while(tries < 5):
            try:
                # fill SessionOptions
                sessionOptions = blpapi.SessionOptions()
                sessionOptions.setServerHost(DataConstants().bbg_server)
                sessionOptions.setServerPort(DataConstants().bbg_server_port)

                self.logger.info("Starting Bloomberg session...")

                # create a Session
                session = blpapi.Session(sessionOptions)

                # start a Session
                if not session.start():
                    self.logger.error("Failed to start session.")
                    return

                self.logger.info("Returning session...")

                tries = 5
            except:
                tries = tries + 1

        # BBGLowLevelTemplate._session = session

        if session is None:
            self.logger.error("Failed to start session.")
            return


        return session

    def add_override(self, request, field, value):
        overrides = request.getElement("overrides")
        override1 = overrides.appendElement()
        override1.setElement("fieldId", field)
        override1.setElement("value", value)

    @abc.abstractmethod
    def process_message(self, msg):
        # to be implemented by subclass
        return

    # create request for data
    @abc.abstractmethod
    def send_bar_request(self, session, eventQueue):
        # to be implemented by subclass
        return

    # create request for data
    @abc.abstractmethod
    def combine_slices(self, data_frame, data_frame_slice):
        # to be implemented by subclass
        return

    def kill_session(self, session):
        if (session is not None):
            try:
                session.stop()

                self.logger.info("Stopping session...")
            finally:
                self.logger.info("Finally stopping session...")

            session = None

class BBGLowLevelDaily(BBGLowLevelTemplate):

    def __init__(self):
        super(BBGLowLevelDaily, self).__init__()

        self.logger = LoggerManager().getLogger(__name__)
        self._options = []

    def combine_slices(self, data_frame, data_frame_slice):
        if (data_frame_slice.columns.get_level_values(1).values[0]
            not in data_frame.columns.get_level_values(1).values):

            return data_frame.join(data_frame_slice, how="outer")

        return data_frame

    # populate options for Bloomberg request for asset daily request
    def fill_options(self, market_data_request):
        self._options = OptionsBBG()

        self._options.security = market_data_request.tickers
        self._options.startDateTime = market_data_request.start_date
        self._options.endDateTime = market_data_request.finish_date
        self._options.fields = market_data_request.fields

        return self._options

    def process_message(self, msg):
        # Process received events
        ticker = msg.getElement('securityData').getElement('security').getValue()
        fieldData = msg.getElement('securityData').getElement('fieldData')

        # SLOW loop (careful, not all the fields will be returned every time
        # hence need to include the field name in the tuple)
        data = defaultdict(dict)

        for i in range(fieldData.numValues()):
            for j in range(1, fieldData.getValue(i).numElements()):
                data[(str(fieldData.getValue(i).getElement(j).name()), ticker)][fieldData.getValue(i).getElement(0).getValue()] \
                    = fieldData.getValue(i).getElement(j).getValue()

        data_frame = pandas.DataFrame(data)

        # if obsolete ticker could return no values
        if (not(data_frame.empty)):
            # data_frame.columns = pandas.MultiIndex.from_tuples(data, names=['field', 'ticker'])
            data_frame.index = pandas.to_datetime(data_frame.index)
            self.logger.info("Read: " + ticker + ' ' + str(data_frame.index[0]) + ' - ' + str(data_frame.index[-1]))
        else:
            return None

        return data_frame

    # create request for data
    def send_bar_request(self, session, eventQueue):
        refDataService = session.getService("//blp/refdata")
        request = refDataService.createRequest("HistoricalDataRequest")

        request.set("startDate", self._options.startDateTime.strftime('%Y%m%d'))
        request.set("endDate", self._options.endDateTime.strftime('%Y%m%d'))

        # # only one security/eventType per request
        for field in self._options.fields:
            request.getElement("fields").appendValue(field)

        for security in self._options.security:
            request.getElement("securities").appendValue(security)

        self.logger.info("Sending Bloomberg Daily Request:" + str(request))
        session.sendRequest(request)

class BBGLowLevelRef(BBGLowLevelTemplate):

    def __init__(self):
        super(BBGLowLevelRef, self).__init__()

        self.logger = LoggerManager().getLogger(__name__)
        self._options = []

    # populate options for Bloomberg request for asset intraday request
    def fill_options(self, market_data_request):
        self._options = OptionsBBG()

        self._options.security = market_data_request.tickers
        self._options.startDateTime = market_data_request.start_date
        self._options.endDateTime = market_data_request.finish_date
        self._options.fields = market_data_request.fields

        return self._options

    def process_message(self, msg):
        data = collections.defaultdict(dict)

        # process received events
        securityDataArray = msg.getElement('securityData')

        index = 0

        for securityData in list(securityDataArray.values()):
            ticker = securityData.getElementAsString("security")
            fieldData = securityData.getElement("fieldData")

            for field in fieldData.elements():
                if not field.isValid():
                    field_name = "%s" % field.name()

                    self.logger.error(field_name + " is NULL")
                elif field.isArray():
                    # iterate over complex data returns.
                    field_name = "%s" % field.name()

                    for i, row in enumerate(field.values()):
                        data[(field_name, ticker)][index] = re.findall(r'"(.*?)"', "%s" % row)[0]

                        index = index + 1
                # else:
                    # vals.append(re.findall(r'"(.*?)"', "%s" % row)[0])
                    # print("%s = %s" % (field.name(), field.getValueAsString()))

            fieldExceptionArray = securityData.getElement("fieldExceptions")

            for fieldException in list(fieldExceptionArray.values()):
                errorInfo = fieldException.getElement("errorInfo")
                print(errorInfo.getElementAsString("category"), ":", \
                    fieldException.getElementAsString("fieldId"))

        data_frame = pandas.DataFrame(data)

        # if obsolete ticker could return no values
        if (not(data_frame.empty)):
            data_frame.columns = pandas.MultiIndex.from_tuples(data, names=['field', 'ticker'])
            self.logger.info("Reading: " + ticker + ' ' + str(data_frame.index[0]) + ' - ' + str(data_frame.index[-1]))
        else:
            return None

        return data_frame

    def combine_slices(self, data_frame, data_frame_slice):
        if (data_frame_slice.columns.get_level_values(1).values[0]
            not in data_frame.columns.get_level_values(1).values):

            return data_frame.join(data_frame_slice, how="outer")

        return data_frame

    # create request for data
    def send_bar_request(self, session, eventQueue):
        refDataService = session.getService("//blp/refdata")
        request = refDataService.createRequest('ReferenceDataRequest')

        self.add_override(request, 'TIME_ZONE_OVERRIDE', 23)    # force GMT time
        self.add_override(request, 'START_DT', self._options.startDateTime.strftime('%Y%m%d'))
        self.add_override(request, 'END_DT', self._options.endDateTime.strftime('%Y%m%d'))

        # only one security/eventType per request
        for field in self._options.fields:
            request.getElement("fields").appendValue(field)

        for security in self._options.security:
            request.getElement("securities").appendValue(security)

        self.logger.info("Sending Bloomberg Ref Request:" + str(request))
        session.sendRequest(request)

from operator import itemgetter

class BBGLowLevelIntraday(BBGLowLevelTemplate):

    def __init__(self):
        super(BBGLowLevelIntraday, self).__init__()

        self.logger = LoggerManager().getLogger(__name__)

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

    def combine_slices(self, data_frame, data_frame_slice):
        return data_frame.append(data_frame_slice)

    # populate options for Bloomberg request for asset intraday request
    def fill_options(self, market_data_request):
        self._options = OptionsBBG()

        self._options.security = market_data_request.tickers[0]    # get 1st ticker only!
        self._options.event = market_data_request.trade_side.upper()
        self._options.barInterval = market_data_request.freq_mult
        self._options.startDateTime = market_data_request.start_date
        self._options.endDateTime = market_data_request.finish_date
        self._options.gapFillInitialBar = False

        if hasattr(self._options.startDateTime, 'microsecond'):
            self._options.startDateTime = self._options.startDateTime.replace(microsecond=0)

        if hasattr(self._options.endDateTime, 'microsecond'):
            self._options.endDateTime = self._options.endDateTime.replace(microsecond=0)

        return self._options

    # iterate through Bloomberg output creating a DataFrame output
    # implements abstract method
    def process_message(self, msg):
        data = msg.getElement(self.BAR_DATA).getElement(self.BAR_TICK_DATA)

        self.logger.info("Processing intraday data for " + str(self._options.security))

        data_vals = list(data.values())

        # data_matrix = numpy.zeros([len(data_vals), 6])
        # data_matrix.fill(numpy.nan)
        #
        # date_index = [None] * len(data_vals)
        #
        # for i in range(0, len(data_vals)):
        #     data_matrix[i][0] = data_vals[i].getElementAsFloat(self.OPEN)
        #     data_matrix[i][1] = data_vals[i].getElementAsFloat(self.HIGH)
        #     data_matrix[i][2] = data_vals[i].getElementAsFloat(self.LOW)
        #     data_matrix[i][3] = data_vals[i].getElementAsFloat(self.CLOSE)
        #     data_matrix[i][4] = data_vals[i].getElementAsInteger(self.VOLUME)
        #     data_matrix[i][5] = data_vals[i].getElementAsInteger(self.NUM_EVENTS)
        #
        #     date_index[i] = data_vals[i].getElementAsDatetime(self.TIME)
        #
        # self.logger.info("Dates between " + str(date_index[0]) + " - " + str(date_index[-1]))
        #
        # # create pandas dataframe with the Bloomberg output
        # return pandas.DataFrame(data = data_matrix, index = date_index,
        #                columns=['open', 'high', 'low', 'close', 'volume', 'events'])

        ## for loop method is touch slower
        # time_list = []
        # data_table = []

        # for bar in data_vals:
        #     data_table.append([bar.getElementAsFloat(self.OPEN),
        #                  bar.getElementAsFloat(self.HIGH),
        #                  bar.getElementAsFloat(self.LOW),
        #                  bar.getElementAsFloat(self.CLOSE),
        #                  bar.getElementAsInteger(self.VOLUME),
        #                  bar.getElementAsInteger(self.NUM_EVENTS)])
        #
        #     time_list.append(bar.getElementAsDatetime(self.TIME))

        # each price time point has multiple fields - marginally quicker
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
            self.logger.info("Dates between " + str(time_list[0]) + " - " + str(time_list[-1]))
        except:
            self.logger.info("No dates retrieved")
            return None

        # create pandas dataframe with the Bloomberg output
        return pandas.DataFrame(data = data_table, index = time_list,
                      columns=['open', 'high', 'low', 'close', 'volume', 'events'])

    # implement abstract method: create request for data
    def send_bar_request(self, session, eventQueue):
        refDataService = session.getService("//blp/refdata")
        request = refDataService.createRequest("IntradayBarRequest")

        # only one security/eventType per request
        request.set("security", self._options.security)
        request.set("eventType", self._options.event)
        request.set("interval", self._options.barInterval)

        # self.add_override(request, 'TIME_ZONE_OVERRIDE', 'GMT')

        if self._options.startDateTime and self._options.endDateTime:
            request.set("startDateTime", self._options.startDateTime)
            request.set("endDateTime", self._options.endDateTime)

        if self._options.gapFillInitialBar:
            request.append("gapFillInitialBar", True)

        self.logger.info("Sending Intraday Bloomberg Request...")

        session.sendRequest(request)

class BBGLowLevelTick(BBGLowLevelTemplate):

    def __init__(self):
        super(BBGLowLevelTick, self).__init__()

        self.logger = LoggerManager().getLogger(__name__)

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
        return data_frame.append(data_frame_slice)

    # populate options for Bloomberg request for asset intraday request
    def fill_options(self, market_data_request):
        self._options = OptionsBBG()

        self._options.security = market_data_request.tickers[0]    # get 1st ticker only!
        self._options.event = market_data_request.trade_side.upper()
        # self._options.barInterval = market_data_request.freq_mult
        self._options.startDateTime = market_data_request.start_date
        self._options.endDateTime = market_data_request.finish_date
        # self._options.gapFillInitialBar = False

        if hasattr(self._options.startDateTime, 'microsecond'):
            self._options.startDateTime = self._options.startDateTime.replace(microsecond=0)

        if hasattr(self._options.endDateTime, 'microsecond'):
            self._options.endDateTime = self._options.endDateTime.replace(microsecond=0)

        return self._options

    # iterate through Bloomberg output creating a DataFrame output
    # implements abstract method
    def process_message(self, msg):
        data = msg.getElement(self.TICK_DATA).getElement(self.TICK_DATA)

        self.logger.info("Processing tick data for " + str(self._options.security))
        tuple = []

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
            self.logger.info("Dates between " + str(time_list[0]) + " - " + str(time_list[-1]))
        except:
            self.logger.info("No dates retrieved")
            return None

        # create pandas dataframe with the Bloomberg output
        return pandas.DataFrame(data = data_table, index = time_list,
                      columns=['close', 'ticksize'])

    # implement abstract method: create request for data
    def send_bar_request(self, session, eventQueue):
        refDataService = session.getService("//blp/refdata")
        request = refDataService.createRequest("IntradayTickRequest")

        # only one security/eventType per request
        request.set("security", self._options.security)
        request.getElement("eventTypes").appendValue("TRADE")
        # request.set("eventTypes", self._options.event)
        request.set("includeConditionCodes", True)

        # self.add_override(request, 'TIME_ZONE_OVERRIDE', 'GMT')

        if self._options.startDateTime and self._options.endDateTime:
            request.set("startDateTime", self._options.startDateTime)
            request.set("endDateTime", self._options.endDateTime)

        self.logger.info("Sending Tick Bloomberg Request...")

        session.sendRequest(request)

#######################################################################################################################

from findatapy.util.loggermanager import LoggerManager
from datetime import datetime

class OptionsBBG:

    # TODO create properties that cannot be manipulated
    def __init__(self):
        self.logger = LoggerManager().getLogger(__name__)