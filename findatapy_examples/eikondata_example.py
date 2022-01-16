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

if __name__ == '__main__':
    ###### below line CRUCIAL when running Windows, otherwise multiprocessing
    # doesn't work! (not necessary on Linux)
    from findatapy.util import SwimPool;

    SwimPool()

    from findatapy.market import Market, MarketDataRequest, MarketDataGenerator

    market = Market(market_data_generator=MarketDataGenerator())

    from findatapy.util.dataconstants import DataConstants

    eikon_api_key = DataConstants().eikon_api_key

    df = None

    if eikon_api_key is None:
        eikon_api_key = 'TYPE_YOUR_API'

    import datetime
    from datetime import timedelta

    # You need to have Eikon installed and to have a valid licence for this to
    # work

    # For intraday pricing, you can usually access a history back a few months
    # from the current date
    # (if you need older history there are other Refinitiv products like
    # Tick History)

    # Note: can sometimes get following error
    # eikon.eikonError.EikonError: Error code -1 | Port number was not
    # identified. Check if Eikon Desktop or Eikon API Proxy is running.

    # Steps to check if you get the above error when trying to download via the
    # Eikon Python package, which findatapy uses
    #
    # 1. Check Eikon Desktop is running, also sometimes downgrading your Eikon
    # Python package might help
    # 2. Usually Eikon Data API is running on port 9000, to check this is
    # running try accessing page
    # http://localhost:9000/ping?all
    #
    # The output should be something like this, if the Data API is running
    # {"port":9000,"mode":"eikon4","pid":23008,"hasSecure":true,"startedTime":"Wed Sep 09 2020 16:06:24 GMT+0100 (GMT Daylight Time)",
    # "subApps":[{"path":"/heap"},{"path":"/ping"},{"path":"/sxs","data":{"hasSecure":true,"sxsApps":{}}},{"path":"/api"}]}
    #
    # 3. In folder C:\Users\YOURUSERNAME\AppData\Roaming\Refinitiv\Data API
    # Proxy there should be a file .portInUse which has
    # only the port used by Eikon eg. the below (if this is different to the
    # port which the Data API Proxy is running, you'll likely get the above
    # error)
    #
    # 9000
    #
    # 4. On older versions of Eikon this folder might be
    # c:\Users\YOURUSERNAME\AppData\Thomson Reuters\Eikon API Proxy\
    # 5. It might be necessary to check Eikon logs, to see which port the
    # Data API is running on too, if it's not running on 9000
    # 6. Also check various Refinitiv forum pages such as
    # https://community.developers.refinitiv.com/questions/37673/app-cannot-connect-to-server-port-9000-target-mach.html

    # Get recent tick data for FX tick data
    md_request = MarketDataRequest(
        start_date=datetime.datetime.utcnow() - timedelta(hours=1),
        # Start date (download data over past decade)
        freq='tick',
        data_source='eikon',  # Use Eikon as data source
        tickers=['EURUSD'],  # ticker
        fields=['bid', 'ask'],  # which fields to download
        vendor_tickers=['EUR='],  # ticker (Eikon/RIC)
        vendor_fields=['BID', 'ASK'],  # Which Eikon fields to download
        eikon_api_key=eikon_api_key)

    df = market.fetch_market(md_request)

    print(df)

    # Get 1 minute intraday data for FX
    md_request = MarketDataRequest(
        start_date=datetime.datetime.utcnow() - timedelta(days=5),
        # Start date (download data over past decade)
        freq='intraday',
        data_source='eikon',  # use Eikon as data source
        tickers=['EURUSD', 'GBPUSD'],  # ticker
        fields=['close', 'open', 'high', 'low'],  # which fields to download
        vendor_tickers=['EUR=', 'GBP='],  # ticker (Eikon/RIC)
        vendor_fields=['CLOSE', 'OPEN', 'HIGH', 'LOW'],
        # which Eikon fields to download
        eikon_api_key=eikon_api_key)

    df = market.fetch_market(md_request)

    print(df)

    # We can also download G10 FX from Eikon using shortcuts, without
    # specifying RIC
    # we just need to specify category at 'fx'
    # You can add your own customized tickers by editing the various conf
    # CSV files
    md_request = MarketDataRequest(
        start_date=datetime.datetime.utcnow() - timedelta(days=5),
        # Start date (download data over past decade)
        freq='intraday',
        category='fx',
        cut='NYC',
        data_source='eikon',  # use Eikon as data source
        tickers=['EURUSD', 'GBPUSD'],  # ticker
        fields=['close', 'open', 'high', 'low'],  # which fields to download
        eikon_api_key=eikon_api_key)

    print(df)

    df = market.fetch_market(md_request)

    # Also let's do this for daily data
    md_request.freq = 'daily'

    df = market.fetch_market(md_request)

    print(df)
