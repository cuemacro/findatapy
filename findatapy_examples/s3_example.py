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

#

# Note you"ll need to "pip install s3fs" for this work which is not installed 
# by default by findatapy

# You"ll also need to have setup your S3 bucket, and have all your AWS credentials set on your machine
# This article explains how to give S3 rights to other/accounts-users 
# https://stackoverflow.com/questions/45336781/amazon-s3-access-for-other-aws-accounts
# See below for an example

"""
{
    "Version": "2012-10-17",
    "Id": "S3AccessPolicy",
    "Statement": [
        {
            "Sid": "GiveFredAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123456789012:user/fred"
            },
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::bucket-a",
                "arn:aws:s3:::bucket-a/*"
            ]
        }
    ]
}
"""

# It is recommended NOT to give your S3 access public in general

# NOTE: you need to make sure you have the correct data licences before 
# storing data on disk (and whether other
# users can access it)

from findatapy.market import Market, MarketDataRequest

# In this case we are saving predefined tick tickers to disk, and then reading back
from findatapy.util.dataconstants import DataConstants
from findatapy.market.ioengine import IOEngine

# choose run_example = 0 for everything
# run_example = 1 - download FX tick data from Dukascopy and dump in S3
# run_example = 2 - list files in S3 according to a pattern

run_example = 2

folder = "s3://type_your_s3_bucket_here"

if run_example == 1 or run_example == 0:
    md_request = MarketDataRequest(
        start_date="04 Jan 2021",
        finish_date="05 Jan 2021",
        category="fx",
        data_source="dukascopy",
        freq="tick",
        tickers=["EURUSD"],
        fields=["bid", "ask", "bidv", "askv"],
    )

    market = Market()

    df = market.fetch_market(md_request=md_request)

    print(df)

    # Save to disk in a format friendly for reading later 
    # (ie. s3://bla_bla_bla/backtest.fx.tick.dukascopy.NYC.EURUSD.parquet)
    # Here it will automatically generate the filename from the folder we gave
    # and the MarketDataRequest we made (altenatively, we could have just given 
    # the filename directly)
    IOEngine().write_time_series_cache_to_disk(folder, df, engine="parquet",
                                               md_request=md_request)

    md_request.data_engine = folder + "/*.parquet"

    df = market.fetch_market(md_request)

    print(df)

    # Or we could have just read it directly using
    df = IOEngine().read_time_series_cache_from_disk(folder, df,
                                                     engine="parquet",
                                                     md_request=md_request)

    # We can try this using daily data
    import os

    quandl_api_key = os.environ["QUANDL_API_KEY"]

    df = market.fetch_market(md_request_str="fx.quandl.daily.NYC",
                             md_request=MarketDataRequest(
                                 start_date="01 Jan 2021",
                                 quandl_api_key=quandl_api_key))

    print(df)

if run_example == 2 or run_example == 0:
    io_engine = IOEngine()

    pattern = folder + "/*.parquet"

    matched_files = io_engine.list_files(pattern)

    print(matched_files)
