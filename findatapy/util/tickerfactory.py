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
TickerFactory

Creating lists of tickers from a shortcuts.

"""

import pandas
import numpy

from findatapy.util.loggermanager import LoggerManager

class TickerFactory(object):

    def create_ticker(self, csv_file, out_csv_file):
        # reader = csv.DictReader(open(csv_file))

        data_frame = pandas.read_csv(csv_file, dtype=object)

        rows = 10000
        #category	source	freq	ticker	cut	fields	sourceticker

        data_frame_out = pandas.DataFrame(index=numpy.arange(0, rows),
                                          columns=('category', 'source', 'freq', 'ticker', 'cut', 'fields', 'sourceticker'))
        i = 0

        for category_source_freq_fields in data_frame['category.source.freq.fields']:
            if isinstance(category_source_freq_fields, str):
                spl = category_source_freq_fields.split('.')
                category = spl[0]
                source = spl[1]
                freq = spl[2]
                fields = spl[3]

                for cut_postfix in data_frame['cut.postfix']:
                    if isinstance(cut_postfix, str):
                        spl1 = cut_postfix.split('.')
                        cut = spl1[0]
                        postfix = spl1[1]
                        for ticker in data_frame['ticker']:
                            if isinstance(ticker, str):
                                for midfix in data_frame['midfix']:
                                    if isinstance(midfix, str):
                                        for postmidfix in data_frame['postmidfix']:
                                            if isinstance(postmidfix, str):
                                                ticker_ext = ticker + midfix + postmidfix
                                                sourceticker = ticker + midfix + postmidfix + ' ' + postfix
                                                data_frame_out.loc[i] = [category, source, freq, ticker_ext, cut, fields, sourceticker]
                                                i = i + 1


        data_frame_out = data_frame_out[0:i]

        data_frame_out.to_csv(out_csv_file)

        # for line in reader:
        #     category = line["category"]
        #     source = line["source"]
        #     freq = line["freq"]
        #     ticker = line["ticker"]
        #     cut = line["cut"]
        #     sourceticker = line["sourceticker"]


if __name__ == '__main__':

    logger = LoggerManager.getLogger(__name__)

    tf = TickerFactory()

    csv_file = 'E:/Local/canary/findatapy/conf/fx_vol_tickers_maker.csv'
    out_csv_file = 'E:/Local/canary/findatapy/conf/fx_vol_tickers.csv'

    tf.create_ticker(csv_file, out_csv_file)
