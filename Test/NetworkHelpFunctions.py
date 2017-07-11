class NetworkHelpFunctions():
    @staticmethod
    def date_filer_help(date1, date2):
        date1_arr = date1.split("-")
        date2_arr = date2.split("-")
        for i in range(len(date1_arr)):
            if int(date1_arr[i]) < int(date2_arr[i]):
                return True
            elif int(date1_arr[i]) > int(date2_arr[i]):
                return False
        return True

    @staticmethod
    def date_filter(prev_date, date, end_date):
        return NetworkHelpFunctions.date_filer_help(prev_date, date) and NetworkHelpFunctions.date_filer_help(date, end_date)

    '''
    incoming pairs 
    '''
    @staticmethod
    def filter_graph(sc, rdd_pair, in_threshold, out_threshold, N_iters):
        iteration = 0
        rdd_outcoming = rdd_pair.groupByKey().mapValues(len).filter(lambda x: x[1] >= out_threshold)
        rdd_incoming =  rdd_pair.map(lambda x: (x[1], x[0])).groupByKey().mapValues(len)\
            .filter(lambda x: x[1] >= in_threshold)
        uid_out = set(rdd_outcoming.map(lambda x: x[0]).collect())
        uid_in = set(rdd_incoming.map(lambda x: x[0]).collect())
        uid_set = uid_out.intersection(uid_in)
        sc.broadcast(uid_set)

        while len(uid_set) != len(uid_out) and iteration < N_iters:
            print "iteration : {}, with outuid: {}, uid: {}".format(iteration, len(uid_out), len(uid_set))





