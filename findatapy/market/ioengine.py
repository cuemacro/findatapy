__author__ = 'saeedamen'  # Saeed Amen

#
# Copyright 2016 Cuemacro
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
# License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and limitations under the License.
#

import pandas
import codecs
import glob
import datetime
from dateutil.parser import parse
import shutil

try:
    import bcolz
except:
    pass

# Needs this for AWS S3 bucket support
try:
    from s3fs import S3FileSystem
except:
    pass

# pyarrow necessary for caching
try:
    import pyarrow as pa
except:
    pass

# for reading and writing to S3
try:
    import pyarrow.fs
    import pyarrow.parquet as pq

    from s3fs import S3FileSystem
except:
    pass

try:
    import redis
except:
    pass

import pandas as pd

from openpyxl import load_workbook
import os.path

from findatapy.util.dataconstants import DataConstants
from findatapy.util.loggermanager import LoggerManager

# NOTE: BCOLZ support is alpha!
_replace_chars = ['_a_',
                  '_d_',
                  '_h_',
                  '_o_',
                  '_c_',
                  '_s_',
                  '_p_',
                  '_e_',
                  '_'
                  ]

_invalid_chars = ['&',
                  '.',
                  '-',
                  '(',
                  ')',
                  '/',
                  '%',
                  '=',
                  ' ']

constants = DataConstants()

class IOEngine(object):
    """Write and reads time series data to disk in various formats, CSV, HDF5 (fixed and table formats) and MongoDB/Arctic.

    Can be used to save down output of finmarketpy backtests and also to cache market data locally.

    Also supports BColz (but not currently stable). Planning to add other interfaces such as SQL etc.

    """

    def __init__(self):
        pass

    ### functions to handle Excel on disk
    def write_time_series_to_excel(self, fname, sheet, data_frame, create_new=False):
        """Writes Pandas data frame to disk in Excel format

        Parameters
        ----------
        fname : str
            Excel filename to be written to
        sheet : str
            sheet in excel
        data_frame : DataFrame
            data frame to be written
        create_new : boolean
            to create a new Excel file
        """

        if (create_new):
            writer = pandas.ExcelWriter(fname, engine='xlsxwriter')
        else:
            if self.path_exists(fname):
                book = load_workbook(fname)
                writer = pandas.ExcelWriter(fname, engine='xlsxwriter')
                writer.book = book
                writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
            else:
                writer = pandas.ExcelWriter(fname, engine='xlsxwriter')

        data_frame.to_excel(writer, sheet_name=sheet, engine='xlsxwriter')

        writer.save()
        writer.close()

    def write_time_series_to_excel_writer(self, writer, sheet, data_frame):
        """Writes Pandas data frame to disk in Excel format for a writer

        Parameters
        ----------
        writer : ExcelWriter
            File handle to use for writing Excel file to disk
        sheet : str
            sheet in excel
        data_frame : DataFrame
            data frame to be written
        """
        data_frame.to_excel(writer, sheet, engine='xlsxwriter')

    def read_excel_data_frame(self, f_name, excel_sheet, freq, cutoff=None, dateparse=None,
                              postfix='.close', intraday_tz='UTC'):
        """Reads Excel from disk into DataFrame

        Parameters
        ----------
        f_name : str
            Excel file path to read
        freq : str
            Frequency of data to read (intraday/daily etc)
        cutoff : DateTime (optional)
            end date to read up to
        dateparse : str (optional)
            date parser to use
        postfix : str (optional)
            postfix to add to each columns
        intraday_tz : str
            timezone of file if uses intraday data

        Returns
        -------
        DataFrame
        """

        return self.read_csv_data_frame(f_name, freq, cutoff=cutoff, dateparse=dateparse,
                                        postfix=postfix, intraday_tz=intraday_tz, excel_sheet=excel_sheet)

    def remove_time_series_cache_on_disk(self, fname, engine='hdf5_fixed', db_server='127.0.0.1', db_port='6379',
                                         timeout=10, username=None,
                                         password=None):

        logger = LoggerManager().getLogger(__name__)

        if 'hdf5' in engine:
            engine = 'hdf5'

        if (engine == 'bcolz'):
            # convert invalid characters to substitutes (which Bcolz can't deal with)
            pass
        elif (engine == 'redis'):

            fname = os.path.basename(fname).replace('.', '_')

            try:
                r = redis.StrictRedis(host=db_server, port=db_port, db=0, socket_timeout=timeout,
                                      socket_connect_timeout=timeout)

                if (fname == 'flush_all_keys'):
                    r.flushall()
                else:
                    # allow deletion of keys by pattern matching

                    x = r.keys('*' + fname)

                    if len(x) > 0:
                        r.delete(x)

                    # r.delete(fname)

            except Exception as e:
                logger.warning("Cannot delete non-existent key " + fname + " in Redis: " + str(e))

        elif (engine == 'arctic'):
            from arctic import Arctic
            import pymongo

            socketTimeoutMS = 30 * 1000
            fname = os.path.basename(fname).replace('.', '_')

            logger.info('Load MongoDB library: ' + fname)

            if username is not None and password is not None:
                c = pymongo.MongoClient(
                    host="mongodb://" + username + ":" + password + "@" + str(db_server) + ":" + str(db_port),
                    connect=False)  # , username=username, password=password)
            else:
                c = pymongo.MongoClient(host="mongodb://" + str(db_server) + ":" + str(db_port), connect=False)

            store = Arctic(c, socketTimeoutMS=socketTimeoutMS, serverSelectionTimeoutMS=socketTimeoutMS,
                           connectTimeoutMS=socketTimeoutMS)

            store.delete_library(fname)

            c.close()

            logger.info("Deleted MongoDB library: " + fname)

        elif (engine == 'hdf5'):
            h5_filename = self.get_h5_filename(fname)

            # delete the old copy
            try:
                os.remove(h5_filename)
            except:
                pass

    ### functions to handle HDF5 on disk, arctic etc.
    def write_time_series_cache_to_disk(self, fname, data_frame,
                                        engine='hdf5_fixed', append_data=False, db_server=constants.db_server,
                                        db_port=constants.db_port, username=constants.db_username, password=constants.db_password,
                                        filter_out_matching=None, timeout=10,
                                        use_cache_compression=constants.use_cache_compression,
                                        parquet_compression=constants.parquet_compression, md_request=None, ticker=None):
        """Writes Pandas data frame to disk as HDF5 format or bcolz format or in Arctic

        Parmeters
        ---------
        fname : str
            path of file
        data_frame : DataFrame
            data frame to be written to disk
        engine : str
            'hdf5_fixed' - use HDF5 fixed format, very quick, but cannot append to this
            'hdf5_table' - use HDF5 table format, slower but can append to
            'parquet' - use Parquet
            'arctic' - use Arctic/MongoDB database
            'redis' - use Redis
        append_data : bool
            False - write a fresh copy of data on disk each time
            True - append data to disk
        db_server : str
            Database server for arctic (default: '127.0.0.1')
        timeout : int
            Number of seconds to do timeout
        """

        logger = LoggerManager().getLogger(__name__)

        if md_request is not None:
            fname = self.path_join(fname, md_request.create_category_key(ticker=ticker))

        # default HDF5 format
        hdf5_format = 'fixed'

        if 'hdf5' in engine:
            hdf5_format = engine.split('_')[1]
            engine = 'hdf5'

        if (engine == 'bcolz'):
            # convert invalid characters to substitutes (which Bcolz can't deal with)
            data_frame.columns = self.find_replace_chars(data_frame.columns, _invalid_chars, _replace_chars)
            data_frame.columns = ['A_' + x for x in data_frame.columns]

            data_frame['DTS_'] = pandas.to_datetime(data_frame.index, unit='ns')

            bcolzpath = self.get_bcolz_filename(fname)
            shutil.rmtree(bcolzpath, ignore_errors=True)
            zlens = bcolz.ctable.fromdataframe(data_frame, rootdir=bcolzpath)
        elif (engine == 'redis'):

            fname = os.path.basename(fname).replace('.', '_')

            # Will fail if Redis is not installed
            try:
                r = redis.StrictRedis(host=db_server, port=db_port, db=0, socket_timeout=timeout,
                                      socket_connect_timeout=timeout)

                ping = r.ping()

                # If Redis is alive, try pushing to it
                if ping:
                    if data_frame is not None:
                        if isinstance(data_frame, pandas.DataFrame):
                            mem = data_frame.memory_usage(deep='deep').sum()
                            mem_float = round(float(mem) / (1024.0 * 1024.0), 3)

                            if mem_float < 500:
                                # msgpack/blosc is deprecated
                                # r.set(fname, data_frame.to_msgpack(compress='blosc'))

                                # now uses pyarrow
                                context = pa.default_serialization_context()

                                ser = context.serialize(data_frame).to_buffer()

                                if use_cache_compression:
                                    comp = pa.compress(ser, codec='lz4', asbytes=True)
                                    siz = len(ser)  # siz = 3912

                                    r.set('comp_' + str(siz) + '_' + fname, comp)
                                else:
                                    r.set(fname, ser.to_pybytes())

                                logger.info("Pushed " + fname + " to Redis")
                            else:
                                logger.warn("Did not push " + fname + " to Redis, given size")
                    else:
                        logger.info("Object " + fname + " is empty, not pushed to Redis.")
                else:
                    logger.warning("Didn't push " + fname + " to Redis given not running")

            except Exception as e:
                logger.warning("Couldn't push " + fname + " to Redis: " + str(e))

        elif (engine == 'arctic'):
            from arctic import Arctic
            import pymongo

            socketTimeoutMS = 30 * 1000
            fname = os.path.basename(fname).replace('.', '_')

            logger.info('Load Arctic/MongoDB library: ' + fname)

            if username is not None and password is not None:
                c = pymongo.MongoClient(
                    host="mongodb://" + username + ":" + password + "@" + str(db_server) + ":" + str(db_port),
                    connect=False)  # , username=username, password=password)
            else:
                c = pymongo.MongoClient(host="mongodb://" + str(db_server) + ":" + str(db_port), connect=False)

            store = Arctic(c, socketTimeoutMS=socketTimeoutMS, serverSelectionTimeoutMS=socketTimeoutMS,
                           connectTimeoutMS=socketTimeoutMS)

            database = None

            try:
                database = store[fname]
            except:
                pass

            if database is None:
                store.initialize_library(fname, audit=False)
                logger.info("Created MongoDB library: " + fname)
            else:
                logger.info("Got MongoDB library: " + fname)

            # Access the library
            library = store[fname]

            if ('intraday' in fname):
                data_frame = data_frame.astype('float32')

            if filter_out_matching is not None:
                cols = data_frame.columns

                new_cols = []

                for col in cols:
                    if filter_out_matching not in col:
                        new_cols.append(col)

                data_frame = data_frame[new_cols]

            # Problems with Arctic when writing timezone to disk sometimes, so strip
            data_frame = data_frame.copy().tz_localize(None)

            try:
                # Can duplicate values if we have existing dates
                if append_data:
                    library.append(fname, data_frame)
                else:
                    library.write(fname, data_frame)

                c.close()
                logger.info("Written MongoDB library: " + fname)
            except Exception as e:
                logger.warning("Couldn't write MongoDB library: " + fname + " " + str(e))

        elif (engine == 'hdf5'):
            h5_filename = self.get_h5_filename(fname)

            # append data only works for HDF5 stored as tables (but this is much slower than fixed format)
            # removes duplicated entries at the end
            if append_data:
                store = pandas.HDFStore(h5_filename, format=hdf5_format, complib="zlib", complevel=9)

                if ('intraday' in fname):
                    data_frame = data_frame.astype('float32')

                # get last row which matches and remove everything after that (because append
                # function doesn't check for duplicated rows
                nrows = len(store['data'].index)
                last_point = data_frame.index[-1]

                i = nrows - 1

                while (i > 0):
                    read_index = store.select('data', start=i, stop=nrows).index[0]

                    if (read_index <= last_point): break

                    i = i - 1

                # remove rows at the end, which are duplicates of the incoming time series
                store.remove(key='data', start=i, stop=nrows)
                store.put(key='data', value=data_frame, format=hdf5_format, append=True)
                store.close()
            else:
                h5_filename_temp = self.get_h5_filename(fname + ".temp")

                # delete the old copy
                try:
                    os.remove(h5_filename_temp)
                except:
                    pass

                store = pandas.HDFStore(h5_filename_temp, complib="zlib", complevel=9)

                if ('intraday' in fname):
                    data_frame = data_frame.astype('float32')

                store.put(key='data', value=data_frame, format=hdf5_format)
                store.close()

                # delete the old copy
                try:
                    os.remove(h5_filename)
                except:
                    pass

                # once written to disk rename
                os.rename(h5_filename_temp, h5_filename)

            logger.info("Written HDF5: " + fname)

        elif (engine == 'parquet'):
            if '.parquet' not in fname:
                if fname[-5:] != '.gzip':
                    fname = fname + '.parquet'

            self.to_parquet(data_frame, fname, aws_region=constants.aws_region, parquet_compression=parquet_compression)
            # data_frame.to_parquet(fname, compression=parquet_compression)

            logger.info("Written Parquet: " + fname)
        elif engine == 'csv':
            if '.csv' not in fname:
                fname = fname + '.csv'

            data_frame.to_csv(fname)

            logger.info("Written CSV: " + fname)


    def get_h5_filename(self, fname):
        """Strips h5 off filename returning first portion of filename

        Parameters
        ----------
        fname : str
            h5 filename to strip

        Returns
        -------
        str
        """
        if fname[-3:] == '.h5':
            return fname

        return fname + ".h5"

    def get_bcolz_filename(self, fname):
        """Strips bcolz off filename returning first portion of filename

        Parameters
        ----------
        fname : str
            bcolz filename to strip

        Returns
        -------
        str
        """
        if fname[-6:] == '.bcolz':
            return fname

        return fname + ".bcolz"

    def write_r_compatible_hdf_dataframe(self, data_frame, fname, fields=None):
        """Write a DataFrame to disk in as an R compatible HDF5 file.

        Parameters
        ----------
        data_frame : DataFrame
            data frame to be written
        fname : str
            file path to be written
        fields : list(str)
            columns to be written
        """

        logger = LoggerManager().getLogger(__name__)

        fname_r = self.get_h5_filename(fname)

        logger.info("About to dump R binary HDF5 - " + fname_r)
        data_frame32 = data_frame.astype('float32')

        if fields is None:
            fields = data_frame32.columns.values

        # decompose date/time into individual fields (easier to pick up in R)
        data_frame32['Year'] = data_frame.index.year
        data_frame32['Month'] = data_frame.index.month
        data_frame32['Day'] = data_frame.index.day
        data_frame32['Hour'] = data_frame.index.hour
        data_frame32['Minute'] = data_frame.index.minute
        data_frame32['Second'] = data_frame.index.second
        data_frame32['Millisecond'] = data_frame.index.microsecond / 1000

        data_frame32 = data_frame32[
            ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Second', 'Millisecond'] + fields]

        cols = data_frame32.columns

        store_export = pandas.HDFStore(fname_r)
        store_export.put('df_for_r', data_frame32, data_columns=cols)
        store_export.close()

    def read_time_series_cache_from_disk(self, fname, engine='hdf5', start_date=None, finish_date=None,
                                         db_server=constants.db_server,
                                         db_port=constants.db_port, username=constants.db_username, password=constants.db_password):
        """Reads time series cache from disk in either HDF5 or bcolz

        Parameters
        ----------
        fname : str (or list)
            file to be read from
        engine : str (optional)
            'hd5' - reads HDF5 files (default)
            'arctic' - reads from Arctic/MongoDB database
            'bcolz' - reads from bcolz file (not fully implemented)
            'parquet' - reads from Parquet
        start_date : str/datetime (optional)
            Start date
        finish_date : str/datetime (optional)
            Finish data
        db_server : str
            IP address of MongdDB (default '127.0.0.1')

        Returns
        -------
        DataFrame
        """

        logger = LoggerManager.getLogger(__name__)

        data_frame_list = []

        if not(isinstance(fname, list)):
            if '*' in fname:
                fname = glob.glob(fname)
            else:
                fname = [fname]

        for fname_single in fname:
            logger.debug("Reading " + fname_single + "..")

            if engine == 'parquet' and '.gzip' not in fname_single and '.parquet'  not in fname_single:
                fname_single = fname_single + '.parquet'

            if (engine == 'bcolz'):
                try:
                    name = self.get_bcolz_filename(fname_single)
                    zlens = bcolz.open(rootdir=name)
                    data_frame = zlens.todataframe()

                    data_frame.index = pandas.DatetimeIndex(data_frame['DTS_'])
                    data_frame.index.name = 'Date'
                    del data_frame['DTS_']

                    # convert invalid characters (which Bcolz can't deal with) to more readable characters for pandas
                    data_frame.columns = self.find_replace_chars(data_frame.columns, _replace_chars, _invalid_chars)
                    data_frame.columns = [x[2:] for x in data_frame.columns]
                except:
                    data_frame = None

            elif (engine == 'redis'):
                fname_single = os.path.basename(fname_single).replace('.', '_')

                msg = None

                try:
                    # for pyarrow
                    context = pa.default_serialization_context()

                    r = redis.StrictRedis(host=db_server, port=db_port, db=0)

                    # is there a compressed key stored?)
                    k = r.keys('comp_*_' + fname_single)

                    # if so, then it means that we have stored it as a compressed object
                    # if have more than 1 element, take the last (which will be the latest to be added)
                    if (len(k) >= 1):
                        k = k[-1].decode('utf-8')

                        comp = r.get(k)

                        siz = int(k.split('_')[1])
                        dec = pa.decompress(comp, codec='lz4', decompressed_size=siz)

                        msg = context.deserialize(dec)
                    else:
                        msg = r.get(fname_single)

                        # print(fname_single)
                        if msg is not None:
                            msg = context.deserialize(msg)
                            # logger.warning("Key " + fname_single + " not in Redis cache?")

                except Exception as e:
                    logger.info("Cache not existent for " + fname_single + " in Redis: " + str(e))

                if msg is None:
                    data_frame = None
                else:
                    logger.info('Load Redis cache: ' + fname_single)

                    data_frame = msg # pandas.read_msgpack(msg)

            elif (engine == 'arctic'):
                socketTimeoutMS = 2 * 1000

                import pymongo
                from arctic import Arctic

                fname_single = os.path.basename(fname_single).replace('.', '_')

                logger.info('Load Arctic/MongoDB library: ' + fname_single)

                if username is not None and password is not None:
                    c = pymongo.MongoClient(
                        host="mongodb://" + username + ":" + password + "@" + str(db_server) + ":" + str(db_port),
                        connect=False)  # , username=username, password=password)
                else:
                    c = pymongo.MongoClient(host="mongodb://" + str(db_server) + ":" + str(db_port), connect=False)

                store = Arctic(c, socketTimeoutMS=socketTimeoutMS, serverSelectionTimeoutMS=socketTimeoutMS)

                # Access the library
                try:
                    library = store[fname_single]

                    if start_date is None and finish_date is None:
                        item = library.read(fname_single)

                    else:
                        from arctic.date import DateRange
                        item = library.read(fname_single, date_range=DateRange(start_date.replace(tzinfo=None), finish_date.replace(tzinfo=None)))

                    c.close()

                    logger.info('Read ' + fname_single)

                    data_frame = item.data

                except Exception as e:
                    logger.warning('Library may not exist or another error: ' + fname_single + ' & message is ' + str(e))
                    data_frame = None

            elif self.path_exists(self.get_h5_filename(fname_single)):
                store = pandas.HDFStore(self.get_h5_filename(fname_single))
                data_frame = store.select("data")

                if ('intraday' in fname_single):
                    data_frame = data_frame.astype('float32')

                store.close()

            elif self.path_exists(fname_single) and '.csv' in fname_single:
                data_frame = pandas.read_csv(fname_single, index_col=0)

                data_frame.index = pd.to_datetime(data_frame.index)

            elif self.path_exists(fname_single):
                data_frame = self.read_parquet(fname_single)
                # data_frame = pandas.read_parquet(fname_single)

            data_frame_list.append(data_frame)

        if len(data_frame_list) == 1:
            return data_frame_list[0]

        return data_frame_list

    ### functions for CSV reading and writing
    def write_time_series_to_csv(self, csv_path, data_frame):
        data_frame.to_csv(csv_path)

    def read_csv_data_frame(self, f_name, freq, cutoff=None, dateparse=None,
                            postfix='.close', intraday_tz='UTC', excel_sheet=None):
        """Reads CSV/Excel from disk into DataFrame

        Parameters
        ----------
        f_name : str
            CSV/Excel file path to read
        freq : str
            Frequency of data to read (intraday/daily etc)
        cutoff : DateTime (optional)
            end date to read up to
        dateparse : str (optional)
            date parser to use
        postfix : str (optional)
            postfix to add to each columns
        intraday_tz : str (optional)
            timezone of file if uses intraday data
        excel_sheet : str (optional)
            Excel sheet to be read

        Returns
        -------
        DataFrame
        """

        if (freq == 'intraday'):

            if dateparse is None:
                dateparse = lambda x: datetime.datetime(*map(int, [x[6:10], x[3:5], x[0:2],
                                                                   x[11:13], x[14:16], x[17:19]]))
            elif dateparse == 'dukascopy':
                dateparse = lambda x: datetime.datetime(*map(int, [x[0:4], x[5:7], x[8:10],
                                                                   x[11:13], x[14:16], x[17:19]]))
            elif dateparse == 'c':
                # use C library for parsing dates, several hundred times quicker
                # requires compilation of library to install
                import ciso8601
                dateparse = lambda x: ciso8601.parse_datetime(x)

            if excel_sheet is None:
                data_frame = pandas.read_csv(f_name, index_col=0, parse_dates=True, date_parser=dateparse)
            else:
                data_frame = pandas.read_excel(f_name, excel_sheet, index_col=0, na_values=['NA'])

            data_frame = data_frame.astype('float32')
            data_frame.index.names = ['Date']

            old_cols = data_frame.columns
            new_cols = []

            # add '.close' to each column name
            for col in old_cols:
                new_cols.append(col + postfix)

            data_frame.columns = new_cols
        else:
            # daily data
            if 'events' in f_name:

                data_frame = pandas.read_csv(f_name)

                # very slow conversion
                data_frame = data_frame.convert_objects(convert_dates='coerce')

            else:
                if excel_sheet is None:
                    try:
                        data_frame = pandas.read_csv(f_name, index_col=0, parse_dates=["DATE"], date_parser=dateparse)
                    except:
                        data_frame = pandas.read_csv(f_name, index_col=0, parse_dates=["Date"], date_parser=dateparse)
                else:
                    data_frame = pandas.read_excel(f_name, excel_sheet, index_col=0, na_values=['NA'])

        # convert Date to Python datetime
        # datetime data_frame['Date1'] = data_frame.index

        # slower method: lambda x: pandas.datetime.strptime(x, '%d/%m/%Y %H:%M:%S')
        # data_frame['Date1'].apply(lambda x: datetime.datetime(int(x[6:10]), int(x[3:5]), int(x[0:2]),
        #                                        int(x[12:13]), int(x[15:16]), int(x[18:19])))

        # data_frame.index = data_frame['Date1']
        # data_frame.drop('Date1')

        # slower method: data_frame.index = pandas.to_datetime(data_frame.index)

        if (freq == 'intraday'):
            # assume time series are already in UTC and assign this (can specify other time zones)
            data_frame = data_frame.tz_localize(intraday_tz)

        # end cutoff date
        if cutoff is not None:
            if (isinstance(cutoff, str)):
                cutoff = parse(cutoff)

            data_frame = data_frame.loc[data_frame.index < cutoff]

        return data_frame

    def find_replace_chars(self, array, to_find, replace_with):

        for i in range(0, len(to_find)):
            array = [x.replace(to_find[i], replace_with[i]) for x in array]

        return array

    def convert_csv_data_frame(self, f_name, category, freq, cutoff=None, dateparse=None):
        """Converts CSV file to HDF5 file

        Parameters
        ----------
        f_name : str
            File name to be read
        category : str
            data category of file (used in HDF5 filename)
        freq : str
            intraday/daily frequency (used in HDF5 filename)
        cutoff : DateTime (optional)
            filter dates up to here
        dateparse : str
            date parser to use
        """

        logger = LoggerManager().getLogger(__name__)

        logger.info("About to read... " + f_name)

        data_frame = self.read_csv_data_frame(f_name, freq, cutoff=cutoff, dateparse=dateparse)

        category_f_name = self.create_cache_file_name(category)

        self.write_time_series_cache_to_disk(category_f_name, data_frame)

    def clean_csv_file(self, f_name):
        """Cleans up CSV file (removing empty characters) before writing back to disk

        Parameters
        ----------
        f_name : str
            CSV file to be cleaned
        """
        logger = LoggerManager().getLogger(__name__)

        with codecs.open(f_name, 'rb', 'utf-8') as myfile:
            data = myfile.read()

            # clean file first if dirty
            if data.count('\x00'):
                logger.info('Cleaning CSV...')

                with codecs.open(f_name + '.tmp', 'w', 'utf-8') as of:
                    of.write(data.replace('\x00', ''))

                shutil.move(f_name + '.tmp', f_name)

    def create_cache_file_name(self, filename):
        return constants.folder_time_series_data + "/" + filename

    # TODO refactor IOEngine so that each database is implemented in a subclass of DBEngine

    def get_engine(self, engine='hdf5_fixed'):
        pass

    def read_parquet(self, path):
        return pd.read_parquet(path)

    def to_parquet(self, df, path, aws_region=constants.aws_region, parquet_compression=constants.parquet_compression):

        constants = DataConstants()

        # is_date = False
        #
        # # Force any date columns to default time units (Parquet with pyarrow has problems with ns dates)
        # for c in df.columns:
        #
        #     # If it's a date column don't append to convert to a float
        #     for d in constants.always_date_columns:
        #         if d in c or 'release-dt' in c:
        #             is_date = True
        #             break
        #
        #     if is_date:
        #         try:
        #             df[c] = pd.to_datetime(df[c], errors='coerce', unit=constants.default_time_units)
        #         except:
        #             pass

        try:
            df.index = pd.to_datetime(df.index, unit=constants.default_time_units)
        except:
            pass

        if 's3://' in path:
            s3 = pyarrow.fs.S3FileSystem(region=aws_region)
            table = pa.Table.from_pandas(df)

            path_in_s3 = path.replace("s3://", "")

            with s3.open_output_stream(path_in_s3) as f:
                pq.write_table(table, f, compression=parquet_compression, coerce_timestamps=constants.default_time_units, allow_truncated_timestamps=True,
                               )

        else:
            # Using pandas.to_parquet, doesn't let us pass in parameters to allow coersion of timestamps
            # ie. ns -> us
            table = pa.Table.from_pandas(df)

            pq.write_table(table, path, compression=parquet_compression,
                           coerce_timestamps=constants.default_time_units, allow_truncated_timestamps=True)
            # df.to_parquet(path, compression=parquet_compression)

    def path_exists(self, path):
        if 's3://' in path:
            path_in_s3 = path.replace("s3://", "")

            return S3FileSystem(anon=False).exists(path_in_s3)
        else:
            return os.path.exists(path)

    def path_join(self, folder, file):
        if 's3://' in folder:
            if folder[-1] == '/':
                return folder + file
            else:
                return folder + '/' + file
        else:
            return os.path.join(folder, file)



#######################################################################################################################

class SpeedCache(object):
    """Wrapper for cache hosted in external in memory database (by default Redis, although in practice, can use
    any database supported in this class). This allows us to share hash across Python instances, rather than having
    repopulate each time we restart Python. Also can let us share cache easily across threads, without replicating.

    """

    def __init__(self, db_cache_server=None, db_cache_port=None, engine='redis'):

        if db_cache_server is None:
            self.db_cache_server = constants.db_cache_server

        if db_cache_port is None:
            self.db_cache_port = constants.db_cache_port

        self.engine = engine
        self.io_engine = IOEngine()

    def put_dataframe(self, key, obj):
        if self.engine != 'no_cache':
            try:
                self.io_engine.write_time_series_cache_to_disk(key.replace('/', '_'), obj,
                                                               engine=self.engine, db_server=self.db_cache_server,
                                                               db_port=self.db_cache_port)
            except:
                pass

    def get_dataframe(self, key):
        if self.engine == 'no_cache': return None

        try:
            return self.io_engine.read_time_series_cache_from_disk(key.replace('/', '_'),
                                                                   engine=self.engine, db_server=self.db_cache_server,
                                                                   db_port=self.db_cache_port)
        except:
            pass

    def dump_all_keys(self):
        self.dump_key('flush_all_keys')

    def dump_key(self, key):
        if self.engine == 'no_cache': return

        try:
            return self.io_engine.remove_time_series_cache_on_disk(key, engine=self.engine,
                                                                   db_server=self.db_cache_server,
                                                                   db_port=self.db_cache_port)
        except:
            pass

    def generate_key(self, obj, key_drop=[]):
        """Create a unique hash key for object from its attributes (excluding those attributes in key drop), which can be
        used as a hashkey in the Redis hashtable

        Parameters
        ----------
        obj : class
            Any Python class
        key_drop : str (list)
            List of internal attributes to drop before hashing

        Returns
        -------
        hashkey
        """

        # never want to include Logger object!
        key_drop.append('logger')
        key = []

        for k in obj.__dict__:

            if 'api_key' not in k:
                # provided the key is not in one of the dropped keys
                if not(any(a == k for a in key_drop)):
                    add = obj.__dict__[k]

                    if add is not None:
                        if isinstance(add, list): add = '_'.join(str(e) for e in add)

                    key.append(str(k) + '-' + str(add))

        key.sort()
        key = '_'.join(str(e) for e in key).replace(type(obj).__name__, '').replace('___', '_')

        return type(obj).__name__ + "_" + str(len(str(key))) + "_" + str(key)


# TODO refactor code to use DBEngine
class DBEngine(object):
    pass


class DBEngineArctic(DBEngine):
    pass


class DBEngineHDF5(DBEngine):
    pass


class DBRedis(DBEngine):
    pass
