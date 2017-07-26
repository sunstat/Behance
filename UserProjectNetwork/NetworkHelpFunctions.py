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
    def filter_graph_by_incoming_degree(sc, rdd_pair, in_threshold, n_iters):
        iteration = 0
        prev_size = -1
        rdd_incoming = rdd_pair.map(lambda x: (x[1], x[0])).groupByKey().mapValues(len) \
            .filter(lambda x: x[1] >= in_threshold)
        uid_set = set(rdd_incoming.map(lambda x: x[0]).collect())
        uid_set_broad = sc.broadcast(uid_set)

        def filter_set(x):
            return (x[0] in uid_set_broad.value) and (x[1] in uid_set_broad.value)

        rdd_pair = rdd_pair.filter(filter_set)
        set1 = set(rdd_pair.map(lambda x: x[0]).distinct().collect())
        set2 = set(rdd_pair.map(lambda x: x[1]).distinct().collect())
        uid_set1 = set1.intersection(set2)
        intersection_set_broad = sc.broadcast(uid_set1)

        def intersection_filter(x):
            return (x[0] in intersection_set_broad.value) and (x[1] in intersection_set_broad.value)

        rdd_pair = rdd_pair.filter(intersection_filter)
        cur_size = len(set(rdd_pair.flatMap(lambda x: (x[0], x[1])).collect()))

        while (cur_size != prev_size or len(set1) != len(set2)) and iteration < n_iters:
            print "iteration : {}, cur size: {}, prev size: {}".format(iteration, cur_size, prev_size)
            prev_size = cur_size

            rdd_incoming = rdd_pair.map(lambda x: (x[1], x[0])).groupByKey().mapValues(len)
            rdd_incoming = rdd_incoming.filter(lambda x: x[1] >= in_threshold)
            uid_set = set(rdd_incoming.map(lambda x: x[0]).collect())
            uid_set_broad = sc.broadcast(uid_set)

            def filter_set(x):
                return (x[0] in uid_set_broad.value) and (x[1] in uid_set_broad.value)

            rdd_pair = rdd_pair.filter(filter_set)

            set1 = set(rdd_pair.map(lambda x: x[0]).distinct().collect())
            set2 = set(rdd_pair.map(lambda x: x[1]).distinct().collect())
            uid_set1 = set1.intersection(set2)
            intersection_set_broad = sc.broadcast(uid_set1)
            print "first element is {}, second element is {} and intersection is {}".format(len(set1), len(set2), len(uid_set1))

            def intersection_filter(x):
                return (x[0] in intersection_set_broad.value) and (x[1] in intersection_set_broad.value)
            rdd_pair = rdd_pair.filter(intersection_filter)
            print(rdd_pair.take(5))
            cur_size = len(set(rdd_pair.flatMap(lambda x: (x[0],x[1])).collect()))
            iteration += 1

        return rdd_pair

    @staticmethod
    def date_2_value(date1):
        result = 0
        gaps = [365,30,1]
        arr1 = date1.split('-')
        arr1[0] = int(arr1[0])-2016
        arr1[1] = 11-int(arr1[1])
        arr1[2] = 30-int(arr1[2])
        for i in range(3):
            result += gaps[i]*arr1[i]
        return result

    @staticmethod
    def gap_popularity(arr):
        arr.sort()
        return arr[1]-arr[0]


    @staticmethod
    def next_month(date):
        year, month, day = date.split('-')
        if int(month) == 12:
            day = '30'
        else:
            month = str(int(month)+1)
        return ','.join([year, month, day])



    @staticmethod
    def extract_feature(create_date, date_array):
        feature = []
        feature.append(NetworkHelpFunctions.date_2_value(create_date))
        initial_day_views = len([x for x in date_array if x == create_date])
        feature.append(initial_day_views)

        def month_filter(cur_date, month):
            arr = cur_date.split("-")
            return int(arr[1]) == month

        feature.append(len([x for x in date_array if month_filter(x, month)]))

        return feature











