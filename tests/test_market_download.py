import pytest
import pandas as pd

from findatapy.market import Market, MarketDataGenerator, MarketDataRequest
from findatapy.util.dataconstants import DataConstants

market = Market(market_data_generator=MarketDataGenerator())

data_constants = DataConstants()
quandl_api_key = data_constants.quandl_api_key

def test_quandl_download():
    md_request = MarketDataRequest(start_date='month', category='fx', data_source='quandl', tickers=['AUDJPY'],
                                   quandl_api_key=quandl_api_key)

    df = market.fetch_market(md_request)

    assert df is not None

if __name__ == '__main__':
    pytest.main()

