def calculate_increase_popularity(self, sc, intermediate_dir, base_date, cur_date):
    def date_filer_help(date1, date2):
        date1_arr = date1.split("-")
        date2_arr = date2.split("-")
        for i in range(len(date1_arr)):
            if int(date1_arr[i]) < int(date2_arr[i]):
                return True
            elif int(date1_arr[i]) > int(date2_arr[i]):
                return False
        return True

    def date_filter(prev_date, date, end_date_filter):
        return date_filer_help(prev_date, date) and date_filer_help(date, end_date_filter)

    def calculate_popularity(num_comments, num_appreciations, comment_weight, appreciation_weight):
        if not num_comments:
            return appreciation_weight * num_appreciations
        elif not num_appreciations:
            return comment_weight * num_comments
        else:
            return appreciation_weight * num_appreciations + comment_weight * num_comments

    popularity_base_file = os.path.join(intermediate_dir, base_date, 'pid_2_popularity-csv')
    rdd_popularity_base = sc.textFile(popularity_base_file).map(lambda x: x.split(','))
    pid_set = set(rdd_popularity_base.map(lambda x: x[0]).collect())
    pid_set_broad = sc.broadcast(pid_set)

    def pid_filter(pid):
        return pid in pid_set_broad.value

    rdd_cur = sc.textFile(self.action_file).map(lambda x: x.split(',')).filter(
        lambda x: date_filter("0000-00-00", x[0], cur_date)) \
        .filter(lambda x: pid_filter(x[3])).map(lambda x: (x[3], x[4])).cache()
