class SwimPool(object):

    def create_pool(self, thread_technique, thread_no):
        if thread_technique is "thread":
            from multiprocessing.dummy import Pool
        elif thread_technique is "multiprocessor":
            # most of the time is spend waiting for Bloomberg to return, so can use threads rather than multiprocessing
            # must use the multiprocessing_on_dill library otherwise can't pickle objects correctly
            # note: currently not very stable
            from multiprocessing_on_dill import Pool
            # from pathos.pools import ProcessPool as Pool

        return Pool(thread_no)