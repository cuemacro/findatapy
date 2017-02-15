class SwimPool(object):

    def __init__(self):
        self._pool = None

    def create_pool(self, thread_technique, thread_no, force_new = False):

        if not(force_new) and self._pool is not None:
            return self._pool

        if thread_technique is "thread":
            from multiprocessing.dummy import Pool
        elif thread_technique is "multiprocessor":
            # most of the time is spend waiting for Bloomberg to return, so can use threads rather than multiprocessing
            # must use the multiprocessing_on_dill library otherwise can't pickle objects correctly
            # note: currently not very stable
            from multiprocess import Pool
            # from pathos.pools import ProcessPool as Pool

        self._pool = Pool(thread_no)

        return self._pool