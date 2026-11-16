"""Offline contract tests; no credentials or external services."""

from unittest.mock import Mock
import runpy

import pandas as pd
import pytest
import requests

from findatapy.market import Market, MarketDataGenerator, MarketDataRequest
from findatapy.market.datavendoradanos import DataVendorAdanos


DAILY_VALUES = {'sentiment_score': -0.4, 'buzz_score': 12,
                'bullish_pct': 20, 'bearish_pct': 60}


def request(source='adanos-news-stocks', **kwargs):
    settings = dict(
        start_date='2026-11-10', finish_date='2026-11-12',
        data_source=source, freq='daily',
        tickers=['Apple'], vendor_tickers=['AAPL'],
        fields=['sentiment_score', 'buzz_score'],
        vendor_fields=['sentiment_score', 'buzz_score'],
        push_to_cache=False)
    settings.update(kwargs)
    for name in ('start_date', 'finish_date'):
        settings[name] = pd.Timestamp(settings[name])
    return MarketDataRequest(**settings)


@pytest.fixture
def http_get(monkeypatch):
    get = Mock()
    get.return_value.status_code = 200
    monkeypatch.setattr('findatapy.market.datavendoradanos.requests.get', get)
    return get


@pytest.mark.parametrize('source,path,activity', [
    ('adanos-reddit-stocks', '/reddit/stocks/v1/stock/', 'mentions'),
    ('adanos-x-stocks', '/x/stocks/v1/stock/', 'mentions'),
    ('adanos-news-stocks', '/news/stocks/v1/stock/', 'mentions'),
    ('adanos-polymarket-stocks', '/polymarket/stocks/v1/stock/', 'trade_count'),
    ('adanos-reddit-crypto', '/reddit/crypto/v1/token/', 'mentions'),
])
def test_market_registration_and_daily_values(http_get, source, path, activity):
    http_get.return_value.json.return_value = {
        'sentiment_score': 0.99, 'buzz_score': 99,
        'daily_trend': [
            {'date': '2026-11-12', 'sentiment_score': None, activity: 0},
            {'date': '2026-11-10', **DAILY_VALUES, activity: 3},
        ]}
    vendor = DataVendorAdanos('offline-test-key')
    market = Market(market_data_generator=MarketDataGenerator(
        data_vendor_dict={'adanos': vendor}))
    symbol = 'BTC' if source.endswith('crypto') else 'AAPL'
    fields = list(DAILY_VALUES) + [activity]
    md_request = request(source, vendor_tickers=[symbol],
                         fields=fields, vendor_fields=fields)

    result = market.fetch_market(md_request)

    assert list(result.columns) == ['Apple.' + field for field in fields]
    assert list(result.index) == [pd.Timestamp('2026-11-10'),
                                 pd.Timestamp('2026-11-12')]
    assert result.index.name == 'Date'
    assert result.iloc[0].tolist() == pytest.approx([-0.4, 12, 20, 60, 3])
    assert pd.isna(result.iloc[1]['Apple.sentiment_score'])
    assert pd.isna(result.iloc[1]['Apple.buzz_score'])
    assert result.iloc[1]['Apple.' + activity] == 0
    http_get.assert_called_once_with(
        'https://api.adanos.com' + path + symbol,
        headers={'X-API-Key': 'offline-test-key'},
        params={'from': '2026-11-10', 'to': '2026-11-12'},
        timeout=30, allow_redirects=False)


def test_multiple_tickers_and_aliases(http_get):
    http_get.return_value.json.side_effect = [
        {'daily_trend': [{'date': '2026-11-10', 'sentiment_score': 0.2}]},
        {'daily_trend': [{'date': '2026-11-12', 'sentiment_score': -0.1}]},
    ]
    result = DataVendorAdanos('offline-test-key').load_ticker(request(
        tickers=['Apple', 'Berkshire'], vendor_tickers=['AAPL', 'BRK/B'],
        fields=['sentiment'], vendor_fields=['sentiment_score']))
    assert list(result.columns) == ['Apple.sentiment', 'Berkshire.sentiment']
    assert result.loc['2026-11-10', 'Apple.sentiment'] == 0.2
    assert pd.isna(result.loc['2026-11-10', 'Berkshire.sentiment'])
    assert result.loc['2026-11-12', 'Berkshire.sentiment'] == -0.1
    assert http_get.call_args.kwargs['params'] == {
        'from': '2026-11-10', 'to': '2026-11-12'}
    assert http_get.call_args.args[0].endswith('/BRK%2FB')


@pytest.mark.parametrize('rows', [None, []])
def test_no_data_is_empty_not_synthetic(http_get, rows):
    http_get.return_value.json.return_value = {'found': False, 'daily_trend': rows}
    result = DataVendorAdanos('offline-test-key').load_ticker(request())
    assert result.empty
    assert list(result.columns) == ['Apple.sentiment_score', 'Apple.buzz_score']
    assert isinstance(result.index, pd.DatetimeIndex)


@pytest.mark.parametrize('status', [401, 403, 404, 422, 429, 503])
def test_http_errors_propagate_without_retry_or_fallback(http_get, status):
    response = requests.Response()
    response.status_code = status
    http_get.return_value = response
    with pytest.raises(requests.HTTPError):
        DataVendorAdanos('offline-test-key').load_ticker(request())
    assert http_get.call_count == 1


def test_timeout_propagates(http_get):
    http_get.side_effect = requests.Timeout('offline timeout')
    with pytest.raises(requests.Timeout):
        DataVendorAdanos('offline-test-key').load_ticker(request())
    assert http_get.call_count == 1


@pytest.mark.parametrize('settings', [
    {'source': 'adanos-unknown'},
    {'freq': 'intraday'},
    {'start_date': '2026-11-13'},
    {'fields': ['close'], 'vendor_fields': ['close']},
    {'source': 'adanos-polymarket-stocks',
     'fields': ['mentions'], 'vendor_fields': ['mentions']},
    {'vendor_tickers': ['AAPL', 'MSFT']},
    {'vendor_fields': ['sentiment_score']},
])
def test_invalid_request_fails_before_network(http_get, settings):
    with pytest.raises(ValueError):
        DataVendorAdanos('offline-test-key').load_ticker(request(**settings))
    http_get.assert_not_called()


@pytest.mark.parametrize('key', ['', '   ', None, 'bad\nkey', 'bad\rkey', 'bad\x7fkey'])
def test_missing_key_rejected(key):
    with pytest.raises(ValueError, match='API key'):
        DataVendorAdanos(key)


@pytest.mark.parametrize('dates', [
    ['2026-11-10', '2026-11-10'], ['not-a-date'], [None],
])
def test_invalid_response_dates_rejected(http_get, dates):
    http_get.return_value.json.return_value = {
        'daily_trend': [{'date': date, 'sentiment_score': 0.2} for date in dates]}
    with pytest.raises(ValueError):
        DataVendorAdanos('offline-test-key').load_ticker(request())


def test_outside_window_rows_not_returned(http_get):
    http_get.return_value.json.return_value = {'daily_trend': [
        {'date': '2026-11-09', 'sentiment_score': 0.5},
        {'date': '2026-11-10', 'sentiment_score': 0.2},
        {'date': '2026-11-13', 'sentiment_score': -0.2},
    ]}
    result = DataVendorAdanos('offline-test-key').load_ticker(request())
    assert list(result.index) == [pd.Timestamp('2026-11-10')]


def test_example_sends_explicit_iso_dates(http_get, monkeypatch, capsys):
    for name, value in {
        'ADANOS_API_KEY': 'offline-test-key',
        'ADANOS_SOURCE': 'adanos-news-stocks',
        'ADANOS_SYMBOL': 'AAPL',
        'ADANOS_FROM': '2026-11-10', 'ADANOS_TO': '2026-11-12',
    }.items():
        monkeypatch.setenv(name, value)
    http_get.return_value.json.return_value = {'daily_trend': [
        {'date': '2026-11-10', 'sentiment_score': 0.2, 'buzz_score': 30}]}
    runpy.run_module('findatapy_examples.adanos_example', run_name='__main__')
    assert http_get.call_args.kwargs['params'] == {
        'from': '2026-11-10', 'to': '2026-11-12'}
    output = capsys.readouterr().out
    assert '2026-11-10' in output
    assert 'offline-test-key' not in output


def test_redirect_not_followed(http_get):
    http_get.return_value.status_code = 302
    with pytest.raises(ValueError, match='HTTP status: 302'):
        DataVendorAdanos('offline-test-key').load_ticker(request())
    assert http_get.call_count == 1
    assert http_get.call_args.kwargs['allow_redirects'] is False


@pytest.mark.parametrize('bound,value', [
    ('start_date', '2026-11-10T12:00:00'),
    ('finish_date', '2026-11-12T12:00:00'),
    ('start_date', '2026-11-10T00:00:00+02:00'),
    ('finish_date', '2026-11-12T00:00:00-02:00'),
])
def test_market_rejects_non_calendar_bounds(http_get, bound, value):
    http_get.return_value.json.return_value = {'daily_trend': [
        {'date': '2026-11-10', 'sentiment_score': 0.2, 'buzz_score': 30}]}
    market = Market(market_data_generator=MarketDataGenerator(
        data_vendor_dict={'adanos': DataVendorAdanos('offline-test-key')}))
    with pytest.raises(ValueError, match='tz-naive midnight'):
        market.fetch_market(request(**{bound: pd.Timestamp(value)}))
    http_get.assert_not_called()
