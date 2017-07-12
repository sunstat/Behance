class NetworkUtilities(object):
    '''
    methods used only within in this class
    '''

    def __extract_parameters(self):
        arguments_arr = []
        with open(self.config_file, 'r') as f:
            for line in f:
                arguments_arr.append(line.strip())
        return arguments_arr

    # compare two date strings "2016-12-01"

    def __init__(self, action_file, owner_file, config_file, comment_weight, appreciation_weight):

        self.action_file = action_file
        self.owners_file = owner_file
        self.config_file = config_file
        self.comment_weight = comment_weight
        self.appreciation_weight = appreciation_weight
        NetworkUtilities.shell_dir = "../EditData/ShellEdit"
        NetworkUtilities.local_intermediate_dir = "../IntermediateDir"
        NetworkUtilities.behance_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance"
        NetworkUtilities.behance_data_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
        NetworkUtilities.azure_intermediate_dir = os.path.join(NetworkUtilities.behance_dir, "IntermediateResult")

        '''
        two intermediate results for 
        '''
        self.uid_set = None
        '''
        ===============================
        '''
        self.arguments_arr = self.__extract_parameters()

    '''
    extract neighbors in user network and uids set which involved in the network built 
    '''
    def extract_neighbors_from_users_network(self, sc, base_date, output_dir):

        '''
        print follow_map to intermediate directory
        '''

        in_threshold = 5
        n_iters = 20
        output_file = os.path.join(output_dir, 'follow_map-psv')
        rdd_pair = sc.textFile(action_file).map(lambda x: x.split(','))\
            .filter(lambda x: NetworkHelpFunctions.date_filter("0000-00-00", x[0], base_date))\
            .filter(lambda x: x[4] == 'F').map(lambda x: (x[1], x[2])).cache()
        rdd_pair = NetworkHelpFunctions.filter_graph_by_incoming_degree(sc, rdd_pair, in_threshold, n_iters)
        print(rdd_pair.count())

        rdd_follow = rdd_pair.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).cache()
        IOutilities.print_rdd_to_file(rdd_follow, output_file, 'psv')
        '''
        print uid_index to intermediate directory
        '''
        output_file = os.path.join(output_dir, 'uid_2_index-csv')

        rdd_uid_index = rdd_pair.flatMap(lambda x: (x[0],x[1])).distinct().zipWithIndex().cache()
        IOutilities.print_rdd_to_file(rdd_uid_index, output_file, 'csv')
        self.uid_set = set(rdd_uid_index.map(lambda x: x[0]).collect())

        print("now checking")

        print rdd_pair.flatMap(lambda x: (x[0], x[1])).distinct().count()
        print rdd_pair.flatMap(lambda x: x[0]).distinct().count()
        print rdd_pair.flatMap(lambda x: x[1]).distinct().count()

        print("now checking")

    def handle_uid_pid(self, sc, base_date, output_dir):

        uid_set_broad = sc.broadcast(self.uid_set)

        def __filter_uid_incycle(uid):
            return uid in uid_set_broad.value

        '''
        print field_2_index to intermediate diretory
        '''

        rdd_owners = sc.textFile(self.owners_file).map(lambda x: x.split(',')) \
            .filter(lambda x: NetworkHelpFunctions.date_filter("0000-00-00", x[2], base_date)) \
            .filter(lambda x: __filter_uid_incycle(x[1])).persist()

        print(rdd_owners.take(5))
        #print("rdd.owners count :{}".format(rdd_owners.count()))

        rdd_fields_map_index = rdd_owners.flatMap(lambda x: (x[3], x[4], x[5])).filter(
            lambda x: x).distinct().zipWithIndex().cache()

        output_file = os.path.join(output_dir, 'fields_2_index-csv')
        IOutilities.print_rdd_to_file(rdd_fields_map_index, output_file, 'csv')

        '''
        build pid-2-fields-index
        '''

        fields_2_index = rdd_fields_map_index.collectAsMap()
        fields_2_index_broad = sc.broadcast(fields_2_index)

        def trim_str_array(str_arr):
            return [fields_2_index_broad.value[x] for x in str_arr if x]
        rdd = rdd_owners.map(lambda x: (x[0], trim_str_array(x[3:])))
        output_file = os.path.join(output_dir, 'pid_2_fields_index-psv')
        IOutilities.print_rdd_to_file(rdd, output_file, 'psv')

        '''
        print owners_map to intermediate directory
        '''

        rdd_owners_map = rdd_owners.map(lambda x: (x[0], x[1])).distinct().persist()
        output_file = os.path.join(output_dir, 'owners_map-csv')
        IOutilities.print_rdd_to_file(rdd_owners_map, output_file, 'csv')

        '''
        print pid_2_index-csv
        '''
        rdd_pid_index = rdd_owners.map(lambda x: x[0]).distinct().zipWithIndex().cache()
        output_file = os.path.join(output_dir, 'pid_2_index-csv')
        IOutilities.print_rdd_to_file(rdd_pid_index, output_file, 'csv')

    def create_popularity(self, sc, end_date, output_dir):

        rdd_popularity_base = sc.textFile(os.path.join(output_dir, 'pid_2_index-csv')).map(lambda x: x.split(',')) \
            .map(lambda x: (x[0], (0, 0)))

        print(rdd_popularity_base.take(10))

        pid_set = set(rdd_popularity_base.map(lambda x:x[0]).collect())

        pid_set_broad = sc.broadcast(pid_set)

        def pid_filter(pid):
            return pid in pid_set_broad.value

        rdd_pids = sc.textFile(self.action_file).map(lambda x: x.split(',')).filter(
            lambda x: NetworkHelpFunctions.date_filter("0000-00-00", x[0], end_date)) \
            .filter(lambda x: pid_filter(x[3])).map(lambda x: (x[3], x[4])).cache()

        rdd_pid_num_comments = rdd_pids.filter(lambda x: x[1] == 'C').groupByKey().mapValues(len)
        rdd_pid_num_appreciations = rdd_pids.filter(lambda x: x[1] == 'A').groupByKey().mapValues(len)
        temp_left = rdd_pid_num_comments.leftOuterJoin(rdd_pid_num_appreciations)
        print(temp_left.take(10))
        temp_right = rdd_pid_num_comments.rightOuterJoin(rdd_pid_num_appreciations).filter(lambda x: not x[1][0])
        print(temp_right.take(10))
        rdd_popularity = temp_left.union(temp_right).distinct()\
            .map(lambda x: (x[0], (NetworkHelpFunctions.change_none_to_zero(x[1][0]),
                                   NetworkHelpFunctions.change_none_to_zero(x[1][1]))))
        rdd_popularity = rdd_popularity.union(rdd_popularity_base)
        rdd_popularity = rdd_popularity.map(lambda x: (x[0], NetworkHelpFunctions.calculate_popularity(x[1][0],x[1][1],1,2)))
        output_file = os.path.join(output_dir, '-'.join(['pid_2_popularity', end_date, 'csv']))
        print(output_file)
        IOutilities.print_rdd_to_file(rdd_popularity, output_file, 'csv')

    def write_to_intermediate_directory(self, sc):
        base_date = self.arguments_arr[0]
        shell_file = os.path.join(NetworkUtilities.shell_dir, 'createIntermediateDateDirHdfs.sh')
        Popen('./%s %s %s' % (shell_file, intermediate_result_dir, base_date,), shell=True)
        output_dir = os.path.join(NetworkUtilities.azure_intermediate_dir, base_date)
        self.extract_neighbors_from_users_network(sc, base_date, output_dir)
        #self.handle_uid_pid(sc, end_date, output_dir)
        #self.create_popularity(sc, end_date, output_dir)