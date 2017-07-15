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
    def filter_graph_by_incoming_degree(sc, rdd_pair, in_threshold,  n_iters):
        iteration = 0
        rdd_incoming =  rdd_pair.map(lambda x: (x[1], x[0])).groupByKey().mapValues(len)\
            .filter(lambda x: x[1] >= in_threshold)
        set1 = set(rdd_incoming.map(lambda x: x[0]).collect())
        set2 = set(rdd_incoming.map(lambda x: x[1]).collect())
        ell1 = len(set1)
        ell2 = len(set2)
        uid_set = set1.intersection(set2)
        uid_set_broad = sc.broadcast(uid_set)

        def filter_set(x):
            return x[0] in uid_set_broad and x[1] in uid_set_broad

        rdd_pair = rdd_pair.filter(filter_set)

        while ell1 != ell2 and iteration < n_iters:
            print "iteration : {}, with first: {}, second: {}".format(iteration, ell1, ell2)
            rdd_incoming = rdd_pair.map(lambda x: (x[1], x[0])).groupByKey().mapValues(len) \
                .filter(lambda x: x[1] >= in_threshold)
            set1 = set(rdd_incoming.map(lambda x: x[0]).collect())
            set2 = set(rdd_incoming.map(lambda x: x[1]).collect())
            uid_set = set1.intersection(set2)
            uid_set_broad = sc.broadcast(uid_set)
            ell1 = len(set1)
            ell2 = len(set2)

            def filter_set(x):
                return x[0] in uid_set_broad and x[1] in uid_set_broad
            rdd_pair = rdd_pair.filter(filter_set)

        return rdd_pair









