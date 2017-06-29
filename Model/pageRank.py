class pageRank():
    def __init__(self, follow_file, uid_2_index_file, num_iters):
        self.follow_file = follow_file
        self.uid_2_index_file = uid_2_index_file
        self.num_iters = num_iters

    def

    def run(self, sc):
        rdd_uid_base = sc.textFile(self.uid_2_index_file).map(lambda x: (x[0],0))
        rdd_uid =  sc.textFile(self.follow_file).map(lambda x: x.split('\t')).groupByKey().mapValues(len)
        rid_uid = rdd_uid.union(rdd_uid_base).distinct().reduceByKey(lambda x, y : x+y)




