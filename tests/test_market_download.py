import pytest
import pandas as pd

from findatapy.market import Market, MarketDataGenerator, MarketDataRequest
from findatapy.util.dataconstants import DataConstants

market = Market(market_data_generator=MarketDataGenerator())

data_constants = DataConstants()
quandl_api_key = data_constants.quandl_api_key


def test_quandl_download():
    md_request = MarketDataRequest(start_date="year", category="fx",
                                   data_source="quandl", tickers=["AUDJPY"],
                                   quandl_api_key=quandl_api_key)

    df = market.fetch_market(md_request)

    assert df is not None


def test_yahoo_download():
    md_request = MarketDataRequest(
        start_date="year",  # start date
        data_source="yahoo",  # use Bloomberg as data source
        tickers=["Apple", "Citigroup"],  # ticker (findatapy)
        fields=["close"],  # which fields to download
        vendor_tickers=["aapl", "c"],  # ticker (Yahoo)
        vendor_fields=["Close"])  # which Bloomberg fields to download)

    df = market.fetch_market(md_request)

    assert df is not None

def test_bbg_download():

    freq = ["daily", "intraday"]

    for fr in freq:
        md_request = MarketDataRequest(
            start_date="week",  # start date
            data_source="bloomberg",  # use Bloomberg as data source
            freq=fr,
            tickers=["S&P 500", "EURUSD"],  # ticker (findatapy)
            fields=["close"],  # which fields to download
            vendor_tickers=["SPX Index", "EURUSD Curncy"],  # ticker (Yahoo)
            vendor_fields=["PX_LAST"])  # which Bloomberg fields to download)

        df = market.fetch_market(md_request)

        assert df is not None


def test_free_fx_tick_download():
    # First we can do it by defining all the vendor fields, tickers etc. 
    # so we bypass the configuration file
    md_request = MarketDataRequest(start_date="05 Dec 2016", 
                                   finish_date="07 Dec 2016",
                                   fields=["bid"], vendor_fields=["bid"],
                                   freq="tick", data_source="dukascopy",
                                   tickers=["EURUSD"], 
                                   vendor_tickers=["EURUSD"])

    md_request.data_source = "dukascopy"
    df = market.fetch_market(md_request).resample("1min").mean()

    assert df is not None


if __name__ == "__main__":
    pytest.main()

