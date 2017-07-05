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
    def filter_social_cycle(sc, rdd_pair):
        incoming_set = set(rdd_pair.map(lambda x : x[1]).collect())
        outcoming_set = set(rdd_pair.map(lambda x : x[0]).collect())
        filter_set = incoming_set.intersection(outcoming_set)
        filter_set_broad = sc.broadcast(filter_set)

        def filter_user(uid):
            return uid in filter_set_broad.value

        rdd_filtered = rdd_pair.filter(lambda x: filter_user(x[0])).filter(lambda x: filter_user(x[1])).cache()
        while rdd_filtered.count() != len(filter_set):
            incoming_set = set(rdd_filtered.map(lambda x: x[1]).collect())
            outcoming_set = set(rdd_filtered.map(lambda x: x[0]).collect())
            filter_set = incoming_set.intersection(outcoming_set)
            filter_set_broad = sc.broadcast(filter_set)
            rdd_filtered = rdd_filtered.filter(lambda x: filter_user(x[0])).filter(lambda x: filter_user(x[1])).cache()
            print len(filter_set)
        return rdd_filtered




