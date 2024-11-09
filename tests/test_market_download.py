import pytest
import pandas as pd
import itertools

from findatapy.market import Market, MarketDataGenerator, MarketDataRequest
from findatapy.util.dataconstants import DataConstants
from typing import Optional

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

class MarketFetchTestRunner:

    def test_df_columns_return(self, md_request: MarketDataRequest, data_source_overwrite: Optional[str]=None):
        df = self.get_market_df(md_request, data_source_overwrite)
        for field in self.get_price_fields(md_request):
            assert field in df.columns

    def test_start_time_post_given_start_time(self, md_request: MarketDataRequest, data_source_overwrite: Optional[str]=None):
        df = self.get_market_df(md_request, data_source_overwrite)
        first_tick = df.index.tolist()[0]
        first_tick = first_tick.replace(tzinfo=None)
        assert first_tick >= md_request.start_date

    def test_finish_time_pre_given_finish_time(self, md_request: MarketDataRequest, data_source_overwrite: Optional[str]=None):
        df = self.get_market_df(md_request, data_source_overwrite)
        first_tick = df.index.tolist()[-1]
        first_tick = first_tick.replace(tzinfo=None)
        assert first_tick <= md_request.finish_date
 
    def test_valid_prices(self, md_request: MarketDataRequest, data_source_overwrite: Optional[str]=None):
        df = self.get_market_df(md_request, data_source_overwrite)
        quarter_null_fields = []
        for field in self.get_price_fields(md_request):
            if df[field].isnull().mean() > 0.25:
                quarter_null_fields.append(field)
        assert [] == quarter_null_fields
        
    def get_market_df(self, md_request: MarketDataRequest, data_source_overwrite: Optional[str]=None):
        md_request.data_source = data_source_overwrite if data_source_overwrite is not None else md_request.data_source
        df = market.fetch_market(md_request)
        assert isinstance(df, pd.DataFrame)
        return df
    
    def get_price_fields(self, md_request: MarketDataRequest):
        all_fields = ["high", "low", "close", "volume"]
        desired_fields = md_request.fields if md_request.fields is not None else all_fields
        fields = [f"{i[0]}.{i[1]}" for i in list(itertools.product(md_request.tickers, desired_fields))]
        return fields

md_requests = [
    # I dont have bloomberg or other keys to test...            
    MarketDataRequest(  
        start_date="year",  # start date
        data_source="yahoo",  # use Bloomberg as data source
        tickers=["Apple", "Citigroup"],  # ticker (findatapy)
        # fields=["close"],  # which fields to download
        vendor_tickers=["aapl", "c"],  # ticker (Yahoo)
        vendor_fields=["Close"],  # which Bloomberg fields to download)
        cache_algo='internet_load_return',  # Disable caching
        push_to_cache=False
    ),
    MarketDataRequest(start_date="05 Dec 2016", 
        finish_date="07 Dec 2016",
        fields=["bid"], vendor_fields=["bid"],
        freq="tick", data_source="dukascopy",
        tickers=["EURUSD"], 
        vendor_tickers=["EURUSD"]
    ),
]

fetch_runner = MarketFetchTestRunner()

@pytest.mark.parametrize("md_request", md_requests)
def test_df_columns(md_request: MarketDataRequest):
    fetch_runner.test_df_columns_return(md_request)

@pytest.mark.parametrize("md_request", md_requests)
def test_start_dates(md_request: MarketDataRequest):
    fetch_runner.test_start_time_post_given_start_time(md_request)

@pytest.mark.parametrize("md_request", md_requests)
def test_finish_dates(md_request: MarketDataRequest):
    fetch_runner.test_finish_time_pre_given_finish_time(md_request)

@pytest.mark.parametrize("md_request", md_requests)
def test_price_fields_not_quarter_null(md_request: MarketDataRequest):
    fetch_runner.test_valid_prices(md_request)

if __name__ == "__main__":
    pytest.main()

