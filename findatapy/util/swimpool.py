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

from findatapy.util import DataConstants


class SwimPool(object):
    """Creating thread and process pools in a generic way. Allows users to
    specify the underlying thread or multiprocess library
    they wish to use. Note you can share Pool objects between processes.

    """

    def __init__(self, multiprocessing_library=None):
        self._pool = None

        if multiprocessing_library is None:
            multiprocessing_library = DataConstants().multiprocessing_library

        self._multiprocessing_library = multiprocessing_library
        self._thread_technique = 'na'

        if multiprocessing_library == 'multiprocess':
            try:
                import multiprocess;
                multiprocess.freeze_support()
            except:
                pass
        elif multiprocessing_library == 'multiprocessing_on_dill':
            try:
                import multiprocessing_on_dill;
                multiprocessing_on_dill.freeze_support()
            except:
                pass
        elif multiprocessing_library == 'multiprocessing':
            try:
                import multiprocessing;
                multiprocessing.freeze_support()
            except:
                pass

    def create_pool(self, thread_technique, thread_no, force_new=True,
                    run_in_parallel=True):

        self._thread_technique = thread_technique

        if not (force_new) and self._pool is not None:
            return self._pool

        if thread_technique == "thread" or run_in_parallel == False:
            from multiprocessing.dummy import Pool
        elif thread_technique == "multiprocessing":
            # most of the time is spend waiting for Bloomberg to return, so can use threads rather than multiprocessing
            # must use the multiprocessing_on_dill library otherwise can't pickle objects correctly
            # note: currently not very stable
            if self._multiprocessing_library == 'multiprocessing_on_dill':
                from multiprocessing_on_dill import Pool
            elif self._multiprocessing_library == 'multiprocess':
                from multiprocess import Pool
            elif self._multiprocessing_library == 'multiprocessing':
                from multiprocessing import Pool
            elif self._multiprocessing_library == 'pathos':
                from pathos.multiprocessing import Pool
                # from pathos.pools import ProcessPool as Pool
            elif self._multiprocessing_library == 'billiard':
                from billiard.pool import Pool

        if run_in_parallel == False: thread_no = 1

        self._pool = Pool(thread_no)

        return self._pool

    def close_pool(self, pool, force_process_respawn=False):
        if pool is not None:
            if (self._thread_technique != 'multiprocessing' and
                self._multiprocessing_library != 'pathos') \
                    or force_process_respawn:
                pool.close()
                pool.join()
