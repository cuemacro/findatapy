"""Optional daily attention and sentiment data from the Adanos API."""

from urllib.parse import quote

import pandas as pd
import requests

from findatapy.market.datavendor import DataVendor


class DataVendorAdanos(DataVendor):
    """Register with ``MarketDataGenerator(data_vendor_dict={'adanos': vendor})``.

    The API key belongs to the caller and is kept out of MarketDataRequest.
    Source names remain in data_source so caches cannot mix different sources.
    """

    sources = {
        'adanos-reddit-stocks': '/reddit/stocks/v1/stock/',
        'adanos-x-stocks': '/x/stocks/v1/stock/',
        'adanos-news-stocks': '/news/stocks/v1/stock/',
        'adanos-polymarket-stocks': '/polymarket/stocks/v1/stock/',
        'adanos-reddit-crypto': '/reddit/crypto/v1/token/',
    }

    def __init__(self, api_key):
        super().__init__()
        if (not isinstance(api_key, str) or not api_key or
                any(not 33 <= ord(char) <= 126 for char in api_key)):
            raise ValueError('A nonempty printable Adanos API key is required')
        self._api_key = api_key

    def load_ticker(self, md_request):
        """Return ticker.field columns indexed by UTC calendar dates (tz-naive)."""
        if md_request.data_source not in self.sources:
            raise ValueError('Choose an Adanos source from: ' + ', '.join(self.sources))
        if md_request.freq != 'daily':
            raise ValueError('Adanos supports daily data only')

        start = pd.Timestamp(md_request.start_date)
        finish = pd.Timestamp(md_request.finish_date)
        if pd.isna(start) or pd.isna(finish) or start.date() > finish.date():
            raise ValueError('A valid inclusive start_date and finish_date are required')
        # Market post-filters against the original bounds, not the API dates.
        if any(bound.tzinfo is not None or bound != bound.normalize()
               for bound in (start, finish)):
            raise ValueError('Supply tz-naive midnight timestamps for UTC calendar dates')

        vendor_request = self.construct_vendor_md_request(md_request)
        tickers = vendor_request.tickers
        fields = vendor_request.fields
        activity = ('trade_count' if md_request.data_source ==
                    'adanos-polymarket-stocks' else 'mentions')
        allowed = {activity, 'sentiment_score', 'buzz_score', 'bullish_pct', 'bearish_pct'}
        if not tickers or len(tickers) != len(md_request.tickers):
            raise ValueError('Supply matching tickers and vendor_tickers')
        if not fields or len(fields) != len(md_request.fields) or not set(fields) <= allowed:
            raise ValueError('Supply matching fields and vendor_fields from: ' +
                             ', '.join(sorted(allowed)))

        frames = []
        for ticker, label in zip(tickers, md_request.tickers):
            response = requests.get(
                'https://api.adanos.com' + self.sources[md_request.data_source] +
                quote(ticker, safe=''),
                headers={'X-API-Key': self._api_key},
                params={'from': start.date().isoformat(), 'to': finish.date().isoformat()},
                timeout=30, allow_redirects=False)
            response.raise_for_status()
            if response.status_code != 200:
                raise ValueError('Unexpected Adanos HTTP status: ' + str(response.status_code))
            rows = response.json().get('daily_trend') or []
            frame = pd.DataFrame(rows, columns=['date'] + fields)
            frame.index = pd.DatetimeIndex(pd.to_datetime(frame.pop('date'), utc=True)).tz_localize(None)
            frame.index.name = 'Date'
            if not frame.index.is_unique or frame.index.hasnans:
                raise ValueError('Adanos returned invalid or duplicate daily dates')
            frame = frame.sort_index().loc[
                start.date().isoformat():finish.date().isoformat()]
            frame = frame.apply(pd.to_numeric, errors='raise')
            frame.columns = [label + '.' + field for field in md_request.fields]
            frames.append(frame)

        return pd.concat(frames, axis=1).sort_index()
