from findatapy.market import MarketDataGenerator, Market, MarketDataRequest

def generate_market_data_for_tests():

    # generate daily S&P500 data from Quandl
    md_request = MarketDataRequest(start_date='01 Jan 2001', finish_date='01 Dec 2008',
                                   tickers=['S&P500'], vendor_tickers=['YAHOO/INDEX_GSPC'], fields=['close'],
                                   data_source='quandl')

    market = Market(market_data_generator=MarketDataGenerator())

    df = market.fetch_market(md_request)
    df.to_csv("S&P500.csv")

    # generate tick data from DukasCopy for EURUSD
    md_request = MarketDataRequest(start_date='14 Jun 2016', finish_date='15 Jun 2016', cut='NYC', category='fx',
                                   fields=['bid'], freq='tick', data_source='dukascopy',
                                   tickers=['EURUSD'])

    market = Market(market_data_generator=MarketDataGenerator())

    df = market.fetch_market(md_request)
    df.to_csv("EURUSD_tick.csv")

if __name__ == '__main__':
    generate_market_data_for_tests()