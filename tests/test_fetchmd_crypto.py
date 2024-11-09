'''
    Test downloading tick data from various crypto-currency exchanges
'''
from findatapy.market import Market, MarketDataRequest, MarketDataGenerator
import datetime as dt

def test_bitcoincharts():

    market = Market(market_data_generator=MarketDataGenerator())
    md_request = MarketDataRequest(start_date='20 Dec 2017', finish_date='02 Feb 2018', cut='LOC',
                                   freq='tick', data_source='bitcoincharts', category='crypto',
                                   fields=['close', 'volume'], tickers=['XBTUSD_itbit'])

    df = market.fetch_market(md_request)
    assert not df.empty


def test_binance():

    market = Market(market_data_generator=MarketDataGenerator())
    md_request = MarketDataRequest(start_date='18 Feb 2018', finish_date='20 Feb 2018', cut='LOC',
                                   freq='daily', data_source='binance', category='crypto',
                                   fields=['close', 'volume', 'quote-asset-volume'],
                                   tickers=['WTCXBT'])

    df = market.fetch_market(md_request)
    assert not df.empty


def test_huobi():
    market = Market(market_data_generator=MarketDataGenerator())

    # test daily historical data
    md_request = MarketDataRequest(start_date='11 Apr 2018', finish_date='12 Apr 2018', cut='LOC',
                                   freq='daily', data_source='huobi', category='crypto',
                                   fields=['high', 'low'], tickers=['XBTUSD'])

    df = market.fetch_market(md_request)
    assert not df.empty

    # test historical tick (second) data, last 5 mins from 1 min ago
    finish_dt = dt.datetime.utcnow() - dt.timedelta(minutes=1)
    start_dt = finish_dt - dt.timedelta(minutes=5)
    md_request = MarketDataRequest(start_date=start_dt, finish_date=finish_dt, cut='LOC',
                                   freq='tick', data_source='huobi', category='crypto',
                                   fields=['high', 'low'], tickers=['XBTUSD'])

    df = market.fetch_market(md_request)
    assert not df.empty