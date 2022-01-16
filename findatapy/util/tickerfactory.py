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


import pandas
import numpy
import pandas as pd

from findatapy.util.loggermanager import LoggerManager


class TickerFactory(object):
    """Creating lists of tickers from a shortcuts and also from Excel files.
    """

    def create_tickers_from_combinations(self, csv_file, out_csv_file):
        # reader = csv.DictReader(open(csv_file))

        if isinstance(csv_file, pd.DataFrame):
            data_frame = csv_file
        else:
            data_frame = pandas.read_csv(csv_file, dtype=object)

        rows = 100000
        # category	data_source	freq	ticker	cut	fields	vendor_tickers

        data_frame_out = pandas.DataFrame(index=numpy.arange(0, rows),
                                          columns=(
                                          "category", "data_source", "freq",
                                          "tickers", "cut", "fields",
                                          "vendor_tickers"))
        i = 0

        for category_source_freq_fields in data_frame[
            "category.data_source.freq.fields"]:
            if isinstance(category_source_freq_fields, str):
                spl = category_source_freq_fields.split(".")
                category = spl[0]
                data_source = spl[1]
                freq = spl[2]
                fields = spl[3]

                for cut_postfix in data_frame["cut.postfix"]:
                    if isinstance(cut_postfix, str):
                        spl1 = cut_postfix.split(".")
                        cut = spl1[0]
                        postfix = spl1[1]
                        for ticker, st in zip(data_frame["tickers"],
                                              data_frame["vendor_tickers"]):
                            if isinstance(ticker, str):
                                if "midfix" in data_frame.columns:
                                    for midfix in data_frame["midfix"]:
                                        if isinstance(midfix, str):
                                            for postmidfix in data_frame[
                                                "postmidfix"]:
                                                if isinstance(postmidfix, str):
                                                    ticker_ext = ticker + midfix + postmidfix
                                                    vendor_tickers = st + midfix + postmidfix + " " + postfix
                                                    data_frame_out.loc[i] = [
                                                        category, data_source,
                                                        freq, ticker_ext,
                                                        cut, fields,
                                                        vendor_tickers]

                                                    i = i + 1
                                else:
                                    for postmidfix in data_frame["postmidfix"]:
                                        if isinstance(postmidfix, str):
                                            ticker_ext = ticker + postmidfix
                                            vendor_tickers = st + postmidfix + " " + postfix
                                            data_frame_out.loc[i] = [category,
                                                                     data_source,
                                                                     freq,
                                                                     ticker_ext,
                                                                     cut,
                                                                     fields,
                                                                     vendor_tickers]

                                            i = i + 1

        data_frame_out = data_frame_out[0:i]

        if out_csv_file is not None:
            data_frame_out.to_csv(out_csv_file)

        return data_frame_out

        # for line in reader:
        #     category = line["category"]
        #     data_source = line["data_source"]
        #     freq = line["freq"]
        #     ticker = line["ticker"]
        #     cut = line["cut"]
        #     vendor_tickers = line["vendor_tickers"]

    def aggregate_ticker_excel(self, excel_file, out_csv, sheets=[],
                               skiprows=None, cols=None):

        df_list = []

        logger = LoggerManager.getLogger(__name__)

        for sh in sheets:
            logger.info("Reading from " + sh + " in " + excel_file)

            df = pd.read_excel(excel_file, sheet_name=sh, skiprows=skiprows)
            df = df.dropna(how="all")

            if "maker" in sh:
                df = self.create_tickers_from_combinations(df, None)

            df_list.append(df)

        df = pd.concat(df_list)

        if cols is not None:
            df = df[cols]

        df = df.reset_index()

        if isinstance(out_csv, list):
            for out in out_csv:
                logger.info("Writing to " + out)

                df.to_csv(out)

        else:
            logger.info("Writing to " + out_csv)
            df.to_csv(out_csv)

        return df
