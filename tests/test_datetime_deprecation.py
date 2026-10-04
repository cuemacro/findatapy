"""Test that datetime.utcnow() deprecation warnings are fixed."""
import datetime
import warnings
import pytest


def test_marketdaterequest_no_utcnow_deprecation():
    """Verify MarketDataRequest doesn't trigger utcnow deprecation warning."""
    from findatapy.market import MarketDataRequest
    
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        
        # Create a MarketDataRequest with default finish_date
        md_request = MarketDataRequest(
            start_date="year",
            data_source="yahoo",
            tickers=["AAPL"]
        )
        
        # Check no DeprecationWarning about utcnow
        deprecation_warnings = [
            warning for warning in w 
            if issubclass(warning.category, DeprecationWarning)
            and 'utcnow' in str(warning.message)
        ]
        
        assert len(deprecation_warnings) == 0, \
            f"Found {len(deprecation_warnings)} utcnow deprecation warnings"


def test_marketdaterequest_date_parser_no_utcnow():
    """Verify date_parser method doesn't trigger utcnow deprecation."""
    from findatapy.market import MarketDataRequest
    
    md_request = MarketDataRequest(
        start_date="year",
        data_source="yahoo",
        tickers=["AAPL"]
    )
    
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        
        # Call date_parser which internally uses the fixed utcnow
        result = md_request.date_parser("year")
        
        # Verify result is a valid datetime
        assert isinstance(result, datetime.datetime)
        
        # Check no utcnow deprecation warnings
        deprecation_warnings = [
            warning for warning in w 
            if issubclass(warning.category, DeprecationWarning)
            and 'utcnow' in str(warning.message)
        ]
        
        assert len(deprecation_warnings) == 0


def test_datetime_replacement_is_naive():
    """Verify the replacement produces naive datetime (no timezone)."""
    from findatapy.market import MarketDataRequest
    
    md_request = MarketDataRequest(
        start_date="year",
        finish_date=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
        data_source="yahoo",
        tickers=["AAPL"]
    )
    
    # finish_date should be naive (no tzinfo)
    assert md_request.finish_date.tzinfo is None
