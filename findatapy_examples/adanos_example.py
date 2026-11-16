"""Daily sentiment research; requires the customer's own ADANOS_API_KEY."""

import os
from datetime import date

import pandas as pd

from findatapy.market import Market, MarketDataGenerator, MarketDataRequest
from findatapy.market.datavendoradanos import DataVendorAdanos


if __name__ == '__main__':
    vendor = DataVendorAdanos(os.environ['ADANOS_API_KEY'])
    market = Market(market_data_generator=MarketDataGenerator(
        data_vendor_dict={'adanos': vendor}))
    md_request = MarketDataRequest(
        # MarketDataRequest's legacy string parser does not accept ISO dates.
        start_date=pd.Timestamp(date.fromisoformat(os.environ['ADANOS_FROM'])),
        finish_date=pd.Timestamp(date.fromisoformat(os.environ['ADANOS_TO'])),
        data_source=os.environ['ADANOS_SOURCE'],
        tickers=[os.environ['ADANOS_SYMBOL']],
        vendor_tickers=[os.environ['ADANOS_SYMBOL']],
        fields=['sentiment_score', 'buzz_score'],
        vendor_fields=['sentiment_score', 'buzz_score'],
        freq='daily', push_to_cache=False)
    print(market.fetch_market(md_request))
