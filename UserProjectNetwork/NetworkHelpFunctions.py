import numpy as np
from scipy.sparse import csr_matrix

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

    @staticmethod
    def change_none_to_zero(x):
        if not x:
            return 0
        return x

    @staticmethod
    def calculate_popularity(num_comments, num_appreciations, comment_weight, appreciation_weight):
        return appreciation_weight * num_appreciations + comment_weight * num_comments


    @staticmethod
    def degree_filter(rdd_pair, in_coming_degree_threshold):
        rdd_follow = rdd_pair.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).cache()
        rdd_follow = rdd_follow.firlter(lambda x: len(x[1]) >= in_coming_degree_threshold)

    @staticmethod
    def filter_graph_by_incoming_degree(sc, rdd_pair, in_threshold, n_iters):
        iteration = 0
        prev_size = -1
        rdd_incoming = rdd_pair.map(lambda x: (x[1], x[0])).groupByKey().mapValues(len) \
            .filter(lambda x: x[1] >= in_threshold)
        uid_set = set(rdd_incoming.map(lambda x: x[0]).collect())
        uid_set_broad = sc.broadcast(uid_set)

        def filter_set(x):
            return x[0] in uid_set_broad.value and x[1] in uid_set_broad.value

        rdd_pair = rdd_pair.filter(filter_set)
        set1 = set(rdd_pair.map(lambda x: x[0]).distinct().collect())
        set2 = set(rdd_pair.map(lambda x: x[1]).distinct().collect())
        uid_set1 = set1.intersection(set2)
        intersection_set_broad = sc.broadcast(uid_set1)

        def intersection_filter(x):
            return x[0] in intersection_set_broad.value and x[1] in intersection_set_broad.value

        rdd_pair.filter(intersection_filter)
        cur_size = len(uid_set1)

        while cur_size != prev_size and iteration < n_iters:
            print "iteration : {}, cur size: {}, prev size: {}".format(iteration, cur_size, prev_size)
            prev_size = cur_size

            rdd_incoming = rdd_pair.map(lambda x: (x[1], x[0])).groupByKey().mapValues(len)
            rdd_incoming = rdd_incoming.filter(lambda x: x[1] >= in_threshold)
            uid_set = set(rdd_incoming.map(lambda x: x[0]).collect())
            uid_set_broad = sc.broadcast(uid_set)

            def filter_set(x):
                return x[0] in uid_set_broad.value and x[1] in uid_set_broad.value

            rdd_pair = rdd_pair.filter(filter_set)

            set1 = set(rdd_pair.map(lambda x: x[0]).distinct().collect())
            set2 = set(rdd_pair.map(lambda x: x[1]).distinct().collect())
            uid_set1 = set1.intersection(set2)
            intersection_set_broad = sc.broadcast(uid_set1)

            def intersection_filter(x):
                return x[0] in intersection_set_broad.value and x[1] in intersection_set_broad.value
            rdd_pair.filter(intersection_filter)
            cur_size = len(uid_set1)
            iteration += 1

        return rdd_pair



