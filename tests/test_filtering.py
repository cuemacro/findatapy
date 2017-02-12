import pytest
import pandas

from findatapy.timeseries import Filter

def test_filtering_by_dates():
    filter = Filter()

    # filter S&P500 between specific working days
    start_date = '01 Oct 2008'
    finish_date = '29 Oct 2008'

    # read CSV from disk, and make sure to parse dates
    df = pandas.read_csv("S&P500.csv", parse_dates=['Date'], index_col=['Date'])
    df = filter.filter_time_series_by_date(start_date=start_date, finish_date=finish_date, data_frame=df)

    assert df.index[0] == pandas.to_datetime(start_date)
    assert df.index[-1]== pandas.to_datetime(finish_date)

if __name__ == '__main__':
    pytest.main()