__author__ = 'saeedamen' # Saeed Amen

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

from findatapy.util import DataConstants

class SwimPool(object):
    """Creating thread and process pools in a generic way. Allows users to specify the underlying thread or multiprocess library
    they wish to use.

    """

    def __init__(self, multiprocessing_library = None):
        self._pool = None

        if multiprocessing_library is None:
            multiprocessing_library = DataConstants.multiprocessing_library

        self._multiprocessing_library = multiprocessing_library

    def create_pool(self, thread_technique, thread_no, force_new = False):

        if not(force_new) and self._pool is not None:
            return self._pool

        if thread_technique is "thread":
            from multiprocessing.dummy import Pool
        elif thread_technique is "multiprocessor":
            # most of the time is spend waiting for Bloomberg to return, so can use threads rather than multiprocessing
            # must use the multiprocessing_on_dill library otherwise can't pickle objects correctly
            # note: currently not very stable
            if self._multiprocessing_library == 'multiprocessing_on_dill':
                from multiprocessing_on_dill import Pool
            elif self._multiprocessing_library == 'multiprocess':
                from multiprocess import Pool
            # from pathos.pools import ProcessPool as Pool

        self._pool = Pool(thread_no)

        return self._pool