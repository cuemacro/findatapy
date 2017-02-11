if __name__ == '__main__':
    import multiprocessing

    multiprocessing.freeze_support()
    from findatapy.market import Market, MarketDataRequest, MarketDataGenerator

    market = Market(market_data_generator=MarketDataGenerator())

    # download event data from Bloomberg
    # we have to use the special category "events" keyword for economic data events
    # so findatapy can correctly identify them (given the underlying Bloomberg API calls are all different, however,
    # this will appear transparent to the user)
    market_data_request = MarketDataRequest(
                    start_date = "year",
                    category = "events",
                    data_source = 'bloomberg',                                                              # use Bloomberg as data source
                    tickers = ['FOMC', 'NFP'],
                    fields = ['release-date-time-full', 'release-dt', 'actual-release'],                    # which fields to download
                    vendor_tickers = ['FDTR Index', 'NFP TCH Index'],                                       # ticker (Bloomberg)
                    vendor_fields = ['ECO_FUTURE_RELEASE_DATE_LIST', 'ECO_RELEASE_DT', 'ACTUAL_RELEASE'])   # which Bloomberg fields to download

    df = market.fetch_market(market_data_request)

    print(df)

    # we also have a few events defined in our configuation file
    # those tickers/fields which are predefined this way are easier to download
    # note how we don't have to use the vendor_tickers and vendor_fields for examples
    market_data_request = MarketDataRequest(
                    start_date = "year",
                    category = "events",
                    data_source = 'bloomberg',                      # use Bloomberg as data source
                    tickers = ['USD-US Employees on Nonfarm Payrolls Total MoM Net Change SA'],
                    fields = ['release-date-time-full', 'release-dt', 'actual-release', 'number-observations'])

    df = market.fetch_market(market_data_request)

    print(df)

    # now just download the event day
    market_data_request = MarketDataRequest(
                    start_date = "year",
                    category = "events",
                    data_source = 'bloomberg',              # use Bloomberg as data source
                    tickers = ['NFP'],
                    fields = ['release-date-time-full'],                # which fields to download
                    vendor_tickers = ['NFP TCH Index'],     # ticker (Bloomberg)
                    vendor_fields = ['ECO_FUTURE_RELEASE_DATE_LIST'])     # which Bloomberg fields to download

    df = market.fetch_market(market_data_request)

    print(df)