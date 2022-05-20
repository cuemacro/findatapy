__author__ = "saeedamen"  # Saeed Amen

#
# Copyright 2016 Cuemacro
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on a "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.
#

import io
import datetime
import json
from dateutil.parser import parse

import codecs
import glob
import shutil
import copy
import os.path

import math

import numpy as np
import pandas as pd

try:
    from arctic import Arctic
    import pymongo
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


from openpyxl import load_workbook

from findatapy.util.dataconstants import DataConstants
from findatapy.util.loggermanager import LoggerManager

constants = DataConstants()


class IOEngine(object):
    """Write and reads time series data to disk in various formats, CSV, HDF5 (fixed and table formats) and MongoDB/Arctic.

    Can be used to save down output of finmarketpy backtests and also to cache market data locally.

    Also supports BColz (but not currently stable). Planning to add other interfaces such as SQL etc.

    """

    def __init__(self):
        pass

    ### functions to handle Excel on disk
    def write_time_series_to_excel(self, fname, sheet, data_frame,
                                   create_new=False):
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
            writer = pd.ExcelWriter(fname, engine='xlsxwriter')
        else:
            if self.path_exists(fname):
                book = load_workbook(fname)
                writer = pd.ExcelWriter(fname, engine='xlsxwriter')
                writer.book = book
                writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
            else:
                writer = pd.ExcelWriter(fname, engine='xlsxwriter')

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

    def read_excel_data_frame(self, f_name, excel_sheet, freq, cutoff=None,
                              dateparse=None,
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

        return self.read_csv_data_frame(f_name, freq, cutoff=cutoff,
                                        dateparse=dateparse,
                                        postfix=postfix,
                                        intraday_tz=intraday_tz,
                                        excel_sheet=excel_sheet)

    def remove_time_series_cache_on_disk(self, fname, engine='hdf5_fixed',
                                         db_server=constants.db_server,
                                         db_port=constants.db_port,
                                         timeout=10, username=None,
                                         password=None):

        logger = LoggerManager().getLogger(__name__)

        if 'hdf5' in engine:
            engine = 'hdf5'

        if engine == 'redis':

            fname = os.path.basename(fname).replace('.', '_')

            try:
                r = redis.StrictRedis(host=db_server, port=db_port, db=0,
                                      socket_timeout=timeout,
                                      socket_connect_timeout=timeout)

                if fname == 'flush_all_keys':
                    r.flushall()
                else:
                    # Allow deletion of keys by pattern matching
                    matching_keys = r.keys('*' + fname)

                    if matching_keys:
                        # Use pipeline to speed up command
                        pipe = r.pipeline()

                        for key in matching_keys:
                            pipe.delete(key)

                        pipe.execute()

                    # r.delete(fname)

            except Exception as e:
                logger.warning(
                    "Cannot delete non-existent key " + fname + " in Redis: " + str(
                        e))

        elif (engine == 'arctic'):
            from arctic import Arctic
            import pymongo

            socketTimeoutMS = 30 * 1000
            fname = os.path.basename(fname).replace('.', '_')

            logger.info('Load MongoDB library: ' + fname)

            if username is not None and password is not None:
                c = pymongo.MongoClient(
                    host="mongodb://" + username + ":" + password + "@" + str(
                        db_server) + ":" + str(db_port),
                    connect=False)  # , username=username, password=password)
            else:
                c = pymongo.MongoClient(
                    host="mongodb://" + str(db_server) + ":" + str(db_port),
                    connect=False)

            store = Arctic(c, socketTimeoutMS=socketTimeoutMS,
                           serverSelectionTimeoutMS=socketTimeoutMS,
                           connectTimeoutMS=socketTimeoutMS)

            store.delete_library(fname)

            c.close()

            logger.info("Deleted MongoDB library: " + fname)

        elif engine == 'hdf5':
            h5_filename = self.get_h5_filename(fname)

            # delete the old copy
            try:
                os.remove(h5_filename)
            except:
                pass

    ### functions to handle HDF5 on disk, arctic etc.
    def write_time_series_cache_to_disk(self, fname, data_frame,
                                        engine='hdf5_fixed', append_data=False,
                                        db_server=constants.db_server,
                                        db_port=constants.db_port,
                                        username=constants.db_username,
                                        password=constants.db_password,
                                        filter_out_matching=None, timeout=10,
                                        use_cache_compression=constants.use_cache_compression,
                                        parquet_compression=constants.parquet_compression,
                                        use_pyarrow_directly=False,
                                        md_request=None, ticker=None,
                                        cloud_credentials=None):
        """Writes Pandas data frame to disk as Parquet, HDF5 format or bcolz format, in Arctic or to Redis

        Note, that Redis uses pickle (you must make sure that your Redis instance is not accessible
        from unverified users, given you should not unpickle from unknown sources)

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

        if cloud_credentials is None: cloud_credentials = constants.cloud_credentials

        if md_request is not None:
            fname = self.path_join(fname, md_request.create_category_key(
                ticker=ticker))

        # default HDF5 format
        hdf5_format = 'fixed'

        if 'hdf5' in engine:
            hdf5_format = engine.split('_')[1]
            engine = 'hdf5'

        if engine == 'redis':

            fname = os.path.basename(fname).replace('.', '_')

            # Will fail if Redis is not installed
            try:
                r = redis.StrictRedis(host=db_server, port=db_port, db=0,
                                      socket_timeout=timeout,
                                      socket_connect_timeout=timeout)

                ping = r.ping()

                # If Redis is alive, try pushing to it
                if ping:
                    if data_frame is not None:
                        if isinstance(data_frame, pd.DataFrame):
                            mem = data_frame.memory_usage(deep='deep').sum()
                            mem_float = round(float(mem) / (1024.0 * 1024.0),
                                              3)

                            if mem_float < 500:


                                if use_cache_compression:
                                    ser = io.BytesIO()
                                    data_frame.to_pickle(ser,
                                                         compression="gzip")
                                    ser.seek(0)

                                    r.set('comp_' + fname, ser.read())
                                else:
                                    ser = io.BytesIO()
                                    data_frame.to_pickle(ser)
                                    ser.seek(0)

                                    r.set(fname, ser.read())

                                logger.info("Pushed " + fname + " to Redis")
                            else:
                                logger.warn(
                                    "Did not push " + fname + " to Redis, given size")
                    else:
                        logger.info(
                            "Object " + fname + " is empty, not pushed to Redis.")
                else:
                    logger.warning(
                        "Didn't push " + fname + " to Redis given not running")

            except Exception as e:
                logger.warning(
                    "Couldn't push " + fname + " to Redis: " + str(e))

        elif engine == 'arctic':

            socketTimeoutMS = 30 * 1000
            fname = os.path.basename(fname).replace('.', '_')

            logger.info('Load Arctic/MongoDB library: ' + fname)

            if username is not None and password is not None:
                c = pymongo.MongoClient(
                    host="mongodb://" + username + ":" + password + "@" + str(
                        db_server) + ":" + str(db_port),
                    connect=False)  # , username=username, password=password)
            else:
                c = pymongo.MongoClient(
                    host="mongodb://" + str(db_server) + ":" + str(db_port),
                    connect=False)

            store = Arctic(c, socketTimeoutMS=socketTimeoutMS,
                           serverSelectionTimeoutMS=socketTimeoutMS,
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

            if 'intraday' in fname:
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
                logger.warning(
                    "Couldn't write MongoDB library: " + fname + " " + str(e))

        elif engine == 'hdf5':
            h5_filename = self.get_h5_filename(fname)

            # append data only works for HDF5 stored as tables (but this is much slower than fixed format)
            # removes duplicated entries at the end
            if append_data:
                store = pd.HDFStore(h5_filename, format=hdf5_format,
                                    complib="zlib", complevel=9)

                if ('intraday' in fname):
                    data_frame = data_frame.astype('float32')

                # get last row which matches and remove everything after that (because append
                # function doesn't check for duplicated rows
                nrows = len(store['data'].index)
                last_point = data_frame.index[-1]

                i = nrows - 1

                while (i > 0):
                    read_index = \
                    store.select('data', start=i, stop=nrows).index[0]

                    if (read_index <= last_point): break

                    i = i - 1

                # remove rows at the end, which are duplicates of the incoming time series
                store.remove(key='data', start=i, stop=nrows)
                store.put(key='data', value=data_frame, format=hdf5_format,
                          append=True)
                store.close()
            else:
                h5_filename_temp = self.get_h5_filename(fname + ".temp")

                # delete the old copy
                try:
                    os.remove(h5_filename_temp)
                except:
                    pass

                store = pd.HDFStore(h5_filename_temp, complib="zlib",
                                    complevel=9)

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

        elif engine == 'parquet':
            if '.parquet' not in fname:
                if fname[-5:] != '.gzip':
                    fname = fname + '.parquet'

            self.to_parquet(data_frame, fname,
                            cloud_credentials=cloud_credentials,
                            parquet_compression=parquet_compression,
                            use_pyarrow_directly=use_pyarrow_directly)
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
            ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Second',
             'Millisecond'] + fields]

        cols = data_frame32.columns

        store_export = pd.HDFStore(fname_r)
        store_export.put('df_for_r', data_frame32, data_columns=cols)
        store_export.close()

    def read_time_series_cache_from_disk(self, fname, engine='hdf5',
                                         start_date=None, finish_date=None,
                                         db_server=constants.db_server,
                                         db_port=constants.db_port,
                                         username=constants.db_username,
                                         password=constants.db_password):
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

        if not (isinstance(fname, list)):
            if '*' in fname:
                fname = glob.glob(fname)
            else:
                fname = [fname]

        for fname_single in fname:
            logger.debug("Reading " + fname_single + "..")

            if engine == 'parquet' and '.gzip' not in fname_single and '.parquet' not in fname_single:
                fname_single = fname_single + '.parquet'

            if (engine == 'redis'):
                fname_single = os.path.basename(fname_single).replace('.', '_')

                msg = None

                try:
                    r = redis.StrictRedis(host=db_server, port=db_port, db=0)

                    # is there a compressed key stored?)
                    k = r.keys('comp_' + fname_single)

                    # if so, then it means that we have stored it as a compressed object
                    # if have more than 1 element, take the last (which will be the latest to be added)
                    if (len(k) >= 1):
                        k = k[-1].decode('utf-8')

                        msg = r.get(k)
                        msg = io.BytesIO(msg)
                        msg = pd.read_pickle(msg, compression="gzip")
                    else:
                        msg = r.get(fname_single)
                        msg = pd.read_pickle(msg.read())

                except Exception as e:
                    logger.info(
                        "Cache not existent for " + fname_single + " in Redis: " + str(
                            e))

                if msg is None:
                    data_frame = None
                else:
                    logger.info('Load Redis cache: ' + fname_single)

                    data_frame = msg  # pd.read_msgpack(msg)

            elif (engine == 'arctic'):
                socketTimeoutMS = 2 * 1000

                import pymongo
                from arctic import Arctic

                fname_single = os.path.basename(fname_single).replace('.', '_')

                logger.info('Load Arctic/MongoDB library: ' + fname_single)

                if username is not None and password is not None:
                    c = pymongo.MongoClient(
                        host="mongodb://" + username + ":" + password + "@" + str(
                            db_server) + ":" + str(db_port),
                        connect=False)  # , username=username, password=password)
                else:
                    c = pymongo.MongoClient(
                        host="mongodb://" + str(db_server) + ":" + str(
                            db_port), connect=False)

                store = Arctic(c, socketTimeoutMS=socketTimeoutMS,
                               serverSelectionTimeoutMS=socketTimeoutMS)

                # Access the library
                try:
                    library = store[fname_single]

                    if start_date is None and finish_date is None:
                        item = library.read(fname_single)

                    else:
                        from arctic.date import DateRange
                        item = library.read(fname_single, date_range=DateRange(
                            start_date.replace(tzinfo=None),
                            finish_date.replace(tzinfo=None)))

                    c.close()

                    logger.info('Read ' + fname_single)

                    data_frame = item.data

                except Exception as e:
                    logger.warning(
                        'Library may not exist or another error: ' + fname_single + ' & message is ' + str(
                            e))
                    data_frame = None

            elif self.path_exists(self.get_h5_filename(fname_single)):
                store = pd.HDFStore(self.get_h5_filename(fname_single))
                data_frame = store.select("data")

                if ('intraday' in fname_single):
                    data_frame = data_frame.astype('float32')

                store.close()

            elif self.path_exists(fname_single) and '.csv' in fname_single:
                data_frame = pd.read_csv(fname_single, index_col=0)

                data_frame.index = pd.to_datetime(data_frame.index)

            elif self.path_exists(fname_single):
                data_frame = self.read_parquet(fname_single)
                # data_frame = pd.read_parquet(fname_single)

            data_frame_list.append(data_frame)

        if len(data_frame_list) == 0:
            return None

        if len(data_frame_list) == 1:
            return data_frame_list[0]

        return data_frame_list

    ### functions for CSV reading and writing
    def write_time_series_to_csv(self, csv_path, data_frame):
        data_frame.to_csv(csv_path)

    def read_csv_data_frame(self, f_name, freq, cutoff=None, dateparse=None,
                            postfix='.close', intraday_tz='UTC',
                            excel_sheet=None):
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
                dateparse = lambda x: datetime.datetime(
                    *map(int, [x[6:10], x[3:5], x[0:2],
                               x[11:13], x[14:16], x[17:19]]))
            elif dateparse == 'dukascopy':
                dateparse = lambda x: datetime.datetime(
                    *map(int, [x[0:4], x[5:7], x[8:10],
                               x[11:13], x[14:16], x[17:19]]))
            elif dateparse == 'c':
                # use C library for parsing dates, several hundred times quicker
                # requires compilation of library to install
                import ciso8601
                dateparse = lambda x: ciso8601.parse_datetime(x)

            if excel_sheet is None:
                data_frame = pd.read_csv(f_name, index_col=0, parse_dates=True,
                                         date_parser=dateparse)
            else:
                data_frame = pd.read_excel(f_name, excel_sheet, index_col=0,
                                           na_values=['NA'])

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

                data_frame = pd.read_csv(f_name)

                # very slow conversion
                data_frame = data_frame.convert_objects(convert_dates='coerce')

            else:
                if excel_sheet is None:
                    try:
                        data_frame = pd.read_csv(f_name, index_col=0,
                                                 parse_dates=["DATE"],
                                                 date_parser=dateparse)
                    except:
                        data_frame = pd.read_csv(f_name, index_col=0,
                                                 parse_dates=["Date"],
                                                 date_parser=dateparse)
                else:
                    data_frame = pd.read_excel(f_name, excel_sheet,
                                               index_col=0, na_values=['NA'])

        # convert Date to Python datetime
        # datetime data_frame['Date1'] = data_frame.index

        # slower method: lambda x: pd.datetime.strptime(x, '%d/%m/%Y %H:%M:%S')
        # data_frame['Date1'].apply(lambda x: datetime.datetime(int(x[6:10]), int(x[3:5]), int(x[0:2]),
        #                                        int(x[12:13]), int(x[15:16]), int(x[18:19])))

        # data_frame.index = data_frame['Date1']
        # data_frame.drop('Date1')

        # slower method: data_frame.index = pd.to_datetime(data_frame.index)

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

    def convert_csv_data_frame(self, f_name, category, freq, cutoff=None,
                               dateparse=None):
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

        data_frame = self.read_csv_data_frame(f_name, freq, cutoff=cutoff,
                                              dateparse=dateparse)

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

        with codecs.open(f_name, 'rb', 'utf-8') as file:
            data = file.read()

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

    def sanitize_path(self, path):
        """Will remove unnecessary // from a file path (eg. in the middle)

        Parameters
        ----------
        path : str
            path to be sanitized

        Returns
        -------
        str
        """
        if "s3://" in path:
            path = path.replace("s3://", "")
            path = path.replace("//", "/")

            return "s3://" + path

        return path

    def read_parquet(self, path, cloud_credentials=None):
        """Reads a Pandas DataFrame from a local or s3 path

        Parameters
        ----------
        path : str
            Path of Parquet file (can be S3)

        cloud_credentials : dict (optional)
            Credentials for logging into the cloud

        Returns
        -------
        DataFrame
        """
        if cloud_credentials is None: cloud_credentials = constants.cloud_credentials

        if "s3://" in path:
            storage_options = self._convert_cred(cloud_credentials,
                                                 convert_to_s3fs=True)

            return pd.read_parquet(self.sanitize_path(path),
                                   storage_options=storage_options)
        else:
            return pd.read_parquet(path)

    def _create_cloud_filesystem(self, cloud_credentials, filesystem_type):

        cloud_credentials = self._convert_cred(cloud_credentials)

        # os.environ["AWS_ACCESS_KEY_ID"] = cloud_credentials['aws_access_key']
        # os.environ["AWS_SECRET_ACCESS_KEY"] = cloud_credentials['aws_secret_key']
        # os.environ["AWS_SESSION_TOKEN"] = cloud_credentials['aws_session_token']

        if "s3_pyarrow" == filesystem_type:
            return pyarrow.fs.S3FileSystem(anon=cloud_credentials["aws_anon"],
                                           access_key=cloud_credentials[
                                               "aws_access_key"],
                                           secret_key=cloud_credentials[
                                               "aws_secret_key"],
                                           session_token=cloud_credentials[
                                               "aws_session_token"])

        elif "s3_filesystem" == filesystem_type:
            return S3FileSystem(anon=cloud_credentials["aws_anon"],
                                key=cloud_credentials["aws_access_key"],
                                secret=cloud_credentials["aws_secret_key"],
                                token=cloud_credentials["aws_session_token"])

    def _convert_cred(self, cloud_credentials, convert_to_s3fs=False):
        """Backfills the credential dictionary (usually for AWS login)
        """

        cloud_credentials = copy.copy(cloud_credentials)

        boolean_keys = {'aws_anon': False}

        mappings = {'aws_anon': 'anon',
                    'aws_access_key': 'key',
                    'aws_secret_key': 'secret',
                    'aws_session_token': 'token'
                    }

        for m in mappings.keys():
            if m not in cloud_credentials.keys():
                if m in boolean_keys:
                    cloud_credentials[m] = boolean_keys[m]
                else:
                    cloud_credentials[m] = None

        # Converts the field names eg. aws_access_key => key etc.
        # Mainly for using pd.read_parquet
        if convert_to_s3fs:

            cloud_credentials_temp = {}

            for m in cloud_credentials.keys():
                cloud_credentials_temp[mappings[m]] = cloud_credentials[m]

            return cloud_credentials_temp

        return cloud_credentials

    def to_parquet(self, df, path, filename=None, cloud_credentials=None,
                   parquet_compression=constants.parquet_compression,
                   use_pyarrow_directly=False):
        """Write a DataFrame to a local or s3 path as a Parquet file

        Parameters
        ----------
        df : DataFrame
            DataFrame to be written

        path : str(list)
            Paths where the DataFrame will be written

        filename : str (optional)
            Filename to be used (will be combined with the specified paths)

        cloud_credentials : str (optional)
            AWS credentials for S3 dump

        parquet_compression : str (optional)
            Parquet compression type to use when writing
        """
        logger = LoggerManager.getLogger(__name__)

        path, cloud_credentials = self._get_cloud_path(
            path, filename=filename, cloud_credentials=cloud_credentials)

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
            df.index = pd.to_datetime(df.index,
                                      unit=constants.default_time_units)
        except:
            pass

        cloud_credentials_ = self._convert_cred(cloud_credentials)

        # Tends to be slower than using pandas/pyarrow directly, but for very large files, we might have to split
        # before writing to disk
        def pyarrow_dump(df, path):
            # Trying to convert large Pandas DataFrames in one go to Arrow tables can result in out-of-memory
            # messages, so chunk them first, convert them one by one, and write to disk in chunks
            df_list = self.chunk_dataframes(df)

            # Using pandas.to_parquet, doesn't let us pass in parameters to allow coersion of timestamps
            # hence have to do it this way, using underlying pyarrow interface (/)
            # ie. ns -> us
            if not (isinstance(df_list, list)):
                df_list = [df_list]

            for p in path:
                p = self.sanitize_path(p)

                # Reference:
                # https://stackoverflow.com/questions/47113813/using-pyarrow-how-do-you-append-to-parquet-file
                # https://arrow.apache.org/docs/python/filesystems.html

                pqwriter = None
                counter = 1

                if 's3://' in p:
                    s3 = self._create_cloud_filesystem(cloud_credentials_,
                                                       's3_pyarrow')

                    p_in_s3 = p.replace("s3://", "")

                    for df_ in df_list:
                        logger.info(
                            "S3 chunk... " + str(counter) + " of " + str(
                                len(df_list)))
                        table = pa.Table.from_pandas(df_)

                        if pqwriter is None:
                            pqwriter = pq.ParquetWriter(p_in_s3, table.schema,
                                                        compression=parquet_compression,
                                                        coerce_timestamps=constants.default_time_units,
                                                        allow_truncated_timestamps=True,
                                                        filesystem=s3)

                        pqwriter.write_table(table)

                        counter = counter + 1

                else:
                    for df_ in df_list:
                        logger.info(
                            "Local chunk... " + str(counter) + " of " + str(
                                len(df_list)))
                        table = pa.Table.from_pandas(df_)

                        if pqwriter is None:
                            pqwriter = pq.ParquetWriter(p, table.schema,
                                                        compression=parquet_compression,
                                                        coerce_timestamps=constants.default_time_units,
                                                        allow_truncated_timestamps=True)

                        pqwriter.write_table(table)

                        counter = counter + 1

                # Close the parquet writer
                if pqwriter:
                    pqwriter.close()

                    # df.to_parquet(path, compression=parquet_compression)

        if use_pyarrow_directly:
            pyarrow_dump(df, path)
        else:
            # First try to use Pandas/pyarrow, if fails, which can occur with large DataFrames use chunked write
            try:
                for p in path:
                    p = self.sanitize_path(p)

                    if "s3://" in p:
                        storage_options = self._convert_cred(cloud_credentials,
                                                             convert_to_s3fs=True)

                        df.to_parquet(p, compression=parquet_compression,
                                      coerce_timestamps=constants.default_time_units,
                                      allow_truncated_timestamps=True,
                                      storage_options=storage_options)
                    else:

                        df.to_parquet(p, compression=parquet_compression,
                                      coerce_timestamps=constants.default_time_units,
                                      allow_truncated_timestamps=True)

            except pyarrow.lib.ArrowMemoryError as e:
                logger.warning(
                    "Couldn't dump using Pandas/pyarrow, will instead try chunking with pyarrow directly " + str(
                        e))

                pyarrow_dump(df, path)

    def split_array_chunks(self, array, chunks=None, chunk_size=None):
        """Splits an array or DataFrame into a list of equally sized chunks

        Parameters
        ----------
        array : NumPy array/pd DataFrame
            array to be split into chunks

        chunks : int (optional)
            number of chunks

        chunk_size : int (optional)
            size of each chunk (in rows)

        Returns
        -------
        list of arrays or DataFrames
        """

        if chunk_size is None and chunks is None:
            return array

        if chunk_size is None:
            chunk_size = int(array.shape[0] / chunks)

        if chunks is None:
            chunks = int(array.shape[0] / chunk_size)

        # alternative split array method (untested)

        # if isinstance(array, pd.DataFrame):
        #     array = array.copy()
        #     array_list = []
        #
        #     for start in range(0, array.shape[0], chunk_size):
        #         array_list.append(array.iloc[start:start + chunk_size])
        #
        #     return array_list

        if chunks > 0:
            # if isinstance(array, pd.DataFrame):
            #    array = [array[i:i + chunk_size] for i in range(0, array.shape[0], chunk_size)]

            return np.array_split(array, chunks)

        return array

    def get_obj_size_mb(self, obj):
        # Can sometime have very large dataframes, which need to be split, otherwise won't fit in a single Redis key
        mem = obj.memory_usage(deep='deep').sum()
        mem_float = round(float(mem) / (1024.0 * 1024.0), 3)

        return mem_float

    def chunk_dataframes(self, obj, chunk_size_mb=constants.chunk_size_mb):
        logger = LoggerManager.getLogger(__name__)

        # Can sometime have very large dataframes, which need to be split, otherwise won't fit in a single Redis key
        mem_float = self.get_obj_size_mb(obj)
        mem = '----------- ' + str(mem_float) + ' MB -----------'

        chunks = int(math.ceil(mem_float / chunk_size_mb))

        if chunks > 1:
            obj_list = self.split_array_chunks(obj, chunks=chunks)
        else:

            obj_list = [obj]

        if obj_list != []:
            logger.info("Pandas dataframe of size: " + mem + " in " + str(
                chunks) + " chunk(s)")

        return obj_list

    def read_csv(self, path, cloud_credentials=None, encoding='utf-8',
                 encoding_errors=None, errors='ignore'):
        if cloud_credentials is None: cloud_credentials = constants.cloud_credentials

        if "s3://" in path:
            s3 = self._create_cloud_filesystem(cloud_credentials,
                                               's3_filesystem')

            path_in_s3 = self.sanitize_path(path).replace("s3://", "")

            # Use 'w' for py3, 'wb' for py2
            with s3.open(path_in_s3, 'r', errors=errors) as f:
                if encoding_errors is not None:
                    return pd.read_csv(f, encoding=encoding,
                                       encoding_errors=encoding_errors)
                else:
                    return pd.read_csv(f, encoding=encoding)
        else:
            if encoding_errors is not None:
                return pd.read_csv(path, encoding=encoding,
                                   encoding_errors=encoding_errors)
            else:
                return pd.read_csv(path, encoding=encoding)

    def to_csv_parquet(self, df, path, filename=None, cloud_credentials=None,
                       parquet_compression=constants.parquet_compression,
                       use_pyarrow_directly=False):

        self.to_csv(df, path, filename=filename.replace(".parquet", ".csv"),
                    cloud_credentials=cloud_credentials)

        self.to_parquet(df, path,
                        filename=filename.replace(".csv", ".parquet"),
                        cloud_credentials=cloud_credentials,
                        parquet_compression=parquet_compression,
                        use_pyarrow_directly=use_pyarrow_directly)

    def _get_cloud_path(self, path, filename=None, cloud_credentials=None):
        if cloud_credentials is None: cloud_credentials = constants.cloud_credentials

        if isinstance(path, list):
            pass
        else:
            path = [path]

        if filename is not None:
            new_path = []

            for p in path:
                new_path.append(self.path_join(p, filename))

            path = new_path

        return path, cloud_credentials

    def to_csv(self, df, path, filename=None, cloud_credentials=None):

        path, cloud_credentials = self._get_cloud_path(
            path, filename=filename, cloud_credentials=cloud_credentials)

        for p in path:
            if "s3://" in p:
                s3 = self._create_cloud_filesystem(cloud_credentials,
                                                   's3_filesystem')

                path_in_s3 = self.sanitize_path(p).replace("s3://", "")

                # Use 'w' for py3, 'wb' for py2
                with s3.open(path_in_s3, 'w') as f:
                    df.to_csv(f)
            else:
                df.to_csv(p)

    def to_json(self, dictionary, path, filename=None, cloud_credentials=None):

        path, cloud_credentials = self._get_cloud_path(
            path, filename=filename, cloud_credentials=cloud_credentials)

        for p in path:
            if "s3://" in p:
                s3 = self._create_cloud_filesystem(cloud_credentials,
                                                   's3_filesystem')

                path_in_s3 = self.sanitize_path(p).replace("s3://", "")

                # Use 'w' for py3, 'wb' for py2
                with s3.open(path_in_s3, 'w') as f:
                    if isinstance(dictionary, dict):
                        json.dump(dictionary, f, indent=4)
                    else:
                        dictionary.to_json(f)
            else:
                if isinstance(dictionary, dict):
                    json.dump(dictionary, p, indent=4)
                elif isinstance(dictionary, pd.DataFrame):
                    dictionary.to_json(p)

    def path_exists(self, path, cloud_credentials=None):
        if cloud_credentials is None: cloud_credentials = constants.cloud_credentials

        if "s3://" in path:
            s3 = self._create_cloud_filesystem(cloud_credentials,
                                               's3_filesystem')

            path_in_s3 = path.replace("s3://", "")

            return s3.exists(path_in_s3)
        else:
            return os.path.exists(path)

    def path_join(self, folder, *file):

        file = list(file)

        if file[0][0] == '/':
            file[0] = file[0][1::]

        if 's3://' in folder:

            folder = folder.replace("s3://", "")
            folder = os.path.join(folder, *file)

            folder = folder.replace("//", "/")
            folder = folder.replace("\\\\", "/")
            folder = folder.replace("\\", "/")

            folder = "s3://" + folder

        else:

            folder = os.path.join(folder, *file)

        folder = folder.replace("\\\\", "/")
        folder = folder.replace("\\", "/")

        return folder

    def list_files(self, path, cloud_credentials=None):
        if cloud_credentials is None: cloud_credentials = constants.cloud_credentials

        if "s3://" in path:
            s3 = self._create_cloud_filesystem(cloud_credentials,
                                               "s3_filesystem")

            path_in_s3 = self.sanitize_path(path).replace("s3://", "")

            list_files = s3.glob(path_in_s3)

            if path_in_s3 in list_files:
                list_files.remove(path_in_s3)

            files = ['s3://' + x for x in list_files]

        else:
            files = glob.glob(path)

        list.sort(files)

        return files

    def delete(self, path, cloud_credentials=None):
        if cloud_credentials is None: cloud_credentials = constants.cloud_credentials

        if not (isinstance(path, list)):
            path = [path]

        for p in path:
            if "s3://" in p:
                s3 = self._create_cloud_filesystem(cloud_credentials,
                                                   's3_filesystem')

                path_in_s3 = self.sanitize_path(p).replace("s3://", "")

                if self.path_exists(path, cloud_credentials=cloud_credentials):
                    s3.delete(path_in_s3)
            else:
                if self.path_exists(p, cloud_credentials=cloud_credentials):
                    os.remove(p)

    def copy(self, source, destination, cloud_credentials=None,
             infer_dest_filename=False):
        if cloud_credentials is None: cloud_credentials = constants.cloud_credentials

        if destination is None:
            destination = ""
            infer_dest_filename = True

        if not (isinstance(source, list)):
            source = [source]

        for so in source:
            dest = destination

            # Special case for wildcard *
            if "*" in so:
                # Infer filename if destination in s3
                if "s3://" in dest:
                    infer_dest_filename = True

                list_files = self.list_files(so,
                                             cloud_credentials=cloud_credentials)

                self.copy(list_files, dest,
                          infer_dest_filename=infer_dest_filename)
            else:
                if infer_dest_filename:
                    dest = self.path_join(destination, os.path.basename(so))

                if "s3://" not in dest and "s3://" not in so:
                    shutil.copy(so, dest)
                else:
                    s3 = self._create_cloud_filesystem(cloud_credentials,
                                                       's3_filesystem')

                    if "s3://" in dest and "s3://" in so:
                        s3.cp(self.sanitize_path(so).replace("s3://", ""),
                              self.sanitize_path(dest).replace("s3://", ""),
                              recursive=True)

                    elif "s3://" in dest and "s3://" not in so:
                        s3.put(so,
                               self.sanitize_path(dest).replace("s3://", ""),
                               recursive=True)

                    elif "s3://" not in dest and "s3://" in so:
                        s3.get(self.sanitize_path(so).replace("s3://", ""),
                               dest, recursive=True)


#######################################################################################################################

class SpeedCache(object):
    """Wrapper for cache hosted in external in memory database (by default Redis, although in practice, can use
    any database supported in this class). This allows us to share hash across Python instances, rather than having
    repopulate each time we restart Python. Also can let us share cache easily across threads, without replicating.

    """

    def __init__(self, db_cache_server=None, db_cache_port=None,
                 engine='redis'):

        if db_cache_server is None:
            self.db_cache_server = constants.db_cache_server

        if db_cache_port is None:
            self.db_cache_port = constants.db_cache_port

        self.engine = engine
        self.io_engine = IOEngine()

    def put_dataframe(self, key, obj):
        if self.engine != 'no_cache':
            try:
                self.io_engine.write_time_series_cache_to_disk(
                    key.replace('/', '_'), obj,
                    engine=self.engine, db_server=self.db_cache_server,
                    db_port=self.db_cache_port)
            except:
                pass

    def get_dataframe(self, key):
        if self.engine == 'no_cache': return None

        try:
            return self.io_engine.read_time_series_cache_from_disk(
                key.replace('/', '_'),
                engine=self.engine, db_server=self.db_cache_server,
                db_port=self.db_cache_port)
        except:
            pass

    def dump_all_keys(self):
        self.dump_key('flush_all_keys')

    def dump_key(self, key):
        if self.engine == 'no_cache': return

        try:
            return self.io_engine.remove_time_series_cache_on_disk(key,
                                                                   engine=self.engine,
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
                if not (any(a == k for a in key_drop)):
                    add = obj.__dict__[k]

                    if add is not None:
                        if isinstance(add, list): add = '_'.join(
                            str(e) for e in add)

                    key.append(str(k) + '-' + str(add))

        key.sort()
        key = '_'.join(str(e) for e in key).replace(type(obj).__name__,
                                                    '').replace('___', '_')

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
