__author__ = "saeedamen"  # Saeed Amen

#
# Copyright 2024 Cuemacro
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

from findatapy.market.datavendor import DataVendor

# Databento is an optional dependency
try:
    import databento as db
except:
    pass

class DataVendorDatabento(DataVendor):

    def __init__(self):
        super(DataVendorDatabento, self).__init__()

    def load_ticker(self, md_request):
        logger = LoggerManager().getLogger(__name__)
        md_request_vendor = self.construct_vendor_md_request(
            md_request)

        logger.info("Request Databento data")

        data_frame = self.download_daily(md_request_vendor)

        if data_frame is None or data_frame.index is []:
            return None

        # Convert from vendor to findatapy tickers/fields
        if data_frame is not None:
            if len(md_request.fields) == 1:
                data_frame.columns = data_frame.columns.str.cat(
                    md_request.fields * len(data_frame.columns),
                    sep='.')
            else:
                logger.warning("Inconsistent number of fields and tickers.")
                data_frame.columns = data_frame.columns.str.cat(
                    md_request.fields, sep='.')
            data_frame.index.name = 'Date'

        logger.info("Completed request from Databento")

        return data_frame

    def download_daily(self, md_request):
        logger = LoggerManager().getLogger(__name__)
        trials = 0

        data_frame = None

        if md_request.freq == "daily":
            schema ="ohlcv-1d"
        elif md_request.freq == "intraday":
            schema = "ohlcv-1m"

        while trials < 5:
            try:
                client = db.Historical(md_request.databento_api_key)

                data = client.timeseries.get_range(
                    dataset=md_request.data_source.split("-")[1],
                    symbols=md_request.vendor_tickers,
                    schema=schema,
                    start=pd.Timestamp(md_request.start_date),
                    end=pd.Timestamp(md_request.finish_date),
                    # limit=1,
                )
                data_frame = data.to_df()
            except:
                trials = trials + 1
                logger.info(f"Attempting... {str(trials)} request to download from Databento")

        data_frame = data_frame.set_index('symbol', append=True).unstack(level='symbol')
        fields = df.columns.levels[0]
        tickers = df.columns.levels[1]

        new_cols = []

        for fi in fields:
            for ti in tickers:
                new_cols.append(ti + "." + fi)

        data_frame.columns = new_cols
        data_frame.index.name = "Date"

        if trials == 5:
            logger.error("Couldn't download from Databento after several attempts!")

        return data_frame
