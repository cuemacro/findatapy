import pandas as pd

from findatapy.market.datavendorfxmacrodata import DataVendorFXMacroData
from findatapy.market.marketdatarequest import MarketDataRequest


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.ok = status_code < 400
        self.status_code = status_code

    def json(self):
        return self._payload


def test_fxmacrodata_vendor_builds_daily_fx_dataframe(monkeypatch):
    calls = {}

    def fake_get(url, params=None, headers=None, timeout=None,
                 allow_redirects=True):
        calls["url"] = url
        calls["params"] = dict(params)
        calls["headers"] = headers
        calls["timeout"] = timeout

        return _FakeResponse({
            "data": [
                {"date": "2026-01-02", "val": 1.10},
                {"date": "2026-01-03", "val": 1.11},
            ]
        })

    monkeypatch.setattr(
        "findatapy.market.datavendorfxmacrodata.requests.get", fake_get)

    md_request = MarketDataRequest(
        data_source="fxmacrodata",
        category="fx",
        start_date="2026-01-01",
        finish_date="2026-01-04",
        tickers=["EURUSD"],
        fields=["open", "high", "low", "close"],
        fxmacrodata_api_key="test-key",
    )

    data_frame = DataVendorFXMacroData().load_ticker(md_request)

    assert isinstance(data_frame, pd.DataFrame)
    assert list(data_frame.columns) == [
        "EURUSD.open", "EURUSD.high", "EURUSD.low", "EURUSD.close"]
    assert data_frame.loc[pd.Timestamp("2026-01-02"), "EURUSD.close"] == 1.10
    assert calls["url"] == "https://api.fxmacrodata.com/v1/forex/eur/usd"
    assert calls["headers"] == {"X-API-Key": "test-key"}
    assert "api_key" not in calls["params"]
    assert calls["params"]["start_date"] == "2026-01-01"
    assert calls["params"]["end_date"] == "2026-01-04"


def test_fxmacrodata_vendor_preserves_reference_ohlc(monkeypatch):
    calls = {}

    def fake_get(url, params=None, headers=None, timeout=None,
                 allow_redirects=True):
        calls["headers"] = headers
        return _FakeResponse({
            "data": [{
                "date": "2026-01-02",
                "val": 1.10,
                "open": 1.09,
                "high": 1.12,
                "low": 1.08,
                "close": 1.11,
            }]
        })

    monkeypatch.setattr(
        "findatapy.market.datavendorfxmacrodata.requests.get", fake_get)
    md_request = MarketDataRequest(
        data_source="fxmacrodata",
        category="fx",
        start_date="2026-01-01",
        finish_date="2026-01-04",
        tickers=["EURUSD"],
        fields=["open", "high", "low", "close"],
        fxmacrodata_api_key="",
    )

    data_frame = DataVendorFXMacroData().load_ticker(md_request)

    assert calls["headers"] == {}
    row = data_frame.loc[pd.Timestamp("2026-01-02")]
    assert row.to_dict() == {
        "EURUSD.open": 1.09,
        "EURUSD.high": 1.12,
        "EURUSD.low": 1.08,
        "EURUSD.close": 1.11,
    }


def _md_request(api_key):
    return MarketDataRequest(
        data_source="fxmacrodata",
        category="fx",
        start_date="2026-01-01",
        finish_date="2026-01-04",
        tickers=["EURUSD"],
        fields=["close"],
        fxmacrodata_api_key=api_key,
    )


def test_fxmacrodata_vendor_does_not_follow_redirects(monkeypatch):
    calls = {}

    def fake_get(url, params=None, headers=None, timeout=None,
                 allow_redirects=True):
        calls["allow_redirects"] = allow_redirects
        return _FakeResponse(None, status_code=302)

    monkeypatch.setattr(
        "findatapy.market.datavendorfxmacrodata.requests.get", fake_get)

    try:
        DataVendorFXMacroData().load_ticker(_md_request("test-key"))
    except RuntimeError as exc:
        assert "302" in str(exc)
        assert "test-key" not in str(exc)
    else:
        raise AssertionError("redirect response was accepted")
    assert calls["allow_redirects"] is False


def test_fxmacrodata_vendor_rejects_malformed_key_without_echo(monkeypatch):
    def fake_get(*args, **kwargs):
        raise AssertionError("request should not be sent")

    monkeypatch.setattr(
        "findatapy.market.datavendorfxmacrodata.requests.get", fake_get)

    try:
        DataVendorFXMacroData().load_ticker(_md_request("test-key\r\nX: y"))
    except ValueError as exc:
        assert "test-key" not in str(exc)
    else:
        raise AssertionError("malformed key was accepted")


def test_fxmacrodata_vendor_rejects_invalid_pair():
    try:
        DataVendorFXMacroData._split_pair("EUR1USD")
    except ValueError as exc:
        assert "six-letter" in str(exc)
    else:
        raise AssertionError("invalid currency pair was accepted")
