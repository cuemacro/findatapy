import pytest
import pandas as pd

from findatapy.util.dataconstants import DataConstants

data_constants = DataConstants()

redis_server = data_constants.db_cache_server
redis_port = data_constants.db_cache_port

def test_redis_caching():
    # Note: you need to install Redis in order for this to work!

    # read CSV from disk, and make sure to parse dates
    df = pd.read_csv("S&P500.csv", parse_dates=['Date'], index_col=['Date'])
    df.index = pd.to_datetime(df.index)

    from findatapy.market.ioengine import IOEngine

    io = IOEngine()

    use_cache_compression = [True, False]

    for u in use_cache_compression:
        # Write DataFrame to Redis (using pyarrow format)
        io.write_time_series_cache_to_disk('test_key', df, engine='redis', db_server=redis_server, db_port=redis_port,
                                           use_cache_compression=u)

        # Read back DataFrame from Redis (using pyarrow format)
        df_out = io.read_time_series_cache_from_disk('test_key', engine='redis', db_server=redis_server, db_port=redis_port)

        pd.testing.assert_frame_equal(df, df_out)

if __name__ == '__main__':
    pytest.main()

    # test_redis_caching()