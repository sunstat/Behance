'''
    extract uid, pid and fields map
    '''

    '''
    def handle_uid_pid(self, uid_set):

        end_date = self.arguments_dict['end_day']

        def filter_uid_inCycle_(uid):
            return uid in uid_set_broad.value

        uid_set_broad = self.sc.broadcast(uid_set)

        '''
        build field map
        '''

        rdd_owners = self.sc.textFile(self.owners_file).map(lambda x: x.split(','))\
            .filter(lambda x: DateUtilities.date_filter("0000-00-00", x[2], end_date))\
            .filter(lambda x: filter_uid_inCycle_(x[1])).cache()

        print(rdd_owners.take(5))
        print(rdd_owners.count())

        fields_map_index = rdd_owners.flatMap(lambda x: (x[3], x[4], x[5])).filter(lambda x: x).distinct().zipWithIndex().collectAsMap()

        '''
        .map(map_field_to_count_).collectAsMap()
        '''

        IOutilities.print_dict(fields_map_index, 5)
        print ("field_map_index is with size {}".format(len(fields_map_index)))

        """
        Pid Uid pair in owner file
        """

        owners_map = rdd_owners.map(lambda x: (x[0], x[1])).collectAsMap()


        #pid map to index
        index = 0
        pid_map_index = dict()
        for pid, uid in owners_map.items():
            if pid not in pid_map_index:
                #print pid, index
                pid_map_index[pid] = index
                index += 1


        IOutilities.print_dict(pid_map_index, 20)

        self.fields_map_index = fields_map_index
        self.owners_map = owners_map
        self.pid_map_index = pid_map_index

        return fields_map_index, owners_map, pid_map_index

    def create_user_network(self):
        num_users = len(self.uid_set)
        self.user_network = csr_matrix((num_users, num_users))
        for uid1, uids in self.follow_map.items():
            for uid2 in uids:
                self.user_network[self.uid_map_index[uid1], self.uid_map_index[uid2]] = 1
        return self.user_network

    def create_popularity(self):
        end_date = self.arguments_dict['end_day']

        pid_map_index_broad = self.sc.broadcast(self.pid_map_index)

        def pid_filter(pid):
            return pid in pid_map_index_broad.value

        rdd_pids = self.sc.textFile(self.action_file).map(lambda x: x.split(',')).filter(lambda x: DateUtilities.date_filter("0000-00-00", x[0], end_date))\
            .filter(lambda x: pid_filter(x[3])).map(lambda x: (x[3], x[4])).cache()
        self.pid_map_num_comments = rdd_pids.filter(lambda x: x[1] == 'C').groupByKey().mapValues(len).collectAsMap()
        self.pid_map_num_appreciations = rdd_pids.filter(lambda x: x[1] == 'A').groupByKey().mapValues(len).collectAsMap()
        for pid in self.pid_map_index:
            popularity = 0
            if pid in self.pid_map_num_comments:
                popularity += self.comment_weight*self.pid_map_num_comments[pid]
            if pid in self.pid_map_num_appreciations:
                popularity += self.appreciation_weight*self.pid_map_num_appreciations[pid]
            self.pid_map_popularity[pid] = popularity

        IOutilities.print_dict(self.pid_map_num_appreciations, 20)
        return self.pid_map_num_comments, self.pid_map_num_appreciations, self.pid_map_popularity

    def write_to_intermediate_directory(self):
        self.extract_neighbors_from_users_network()
        self.handle_uid_pid(self.uid_set)
        self.create_popularity()

        end_date = self.arguments_dict['end_day']
        local_dir = os.path.join("../IntermediateDir", end_date)
        if local_run:
            shell_file = os.path.join(NetworkUtilities.shell_dir, 'createIntermediateDateDirLocally.sh')
            Popen('./%s %s %s' % (shell_file, intermediate_result_dir, end_date, ), shell=True)
        else:
            shell_file = os.path.join(NetworkUtilities.shell_dir, 'createIntermediateDateDirHdfs.sh')
            Popen('./%s %s %s' % (shell_file, intermediate_result_dir, end_date, ), shell=True)

        self.extract_neighbors_from_users_network()
        self.handle_uid_pid(self.uid_set)
        #self.create_user_network()
        self.create_popularity()

        '''
        now writing data to the intermediate direction
        '''
        print("now building follow map from uid to uid")
        if local_run:
            IOutilities.print_dict_to_file(self.follow_map, local_dir, 'follow_map')
            IOutilities.print_dict_to_file(self.uid_map_index, local_dir, 'uid_map_index')
            IOutilities.print_dict_to_file(self.owners_map, local_dir, 'owners_map')
            IOutilities.print_dict_to_file(self.pid_map_index, local_dir, 'pid_map_index')
            IOutilities.print_dict_to_file(self.pid_map_popularity, local_dir, 'pid_map_popularity')
        else:
            IOutilities.print_dict_to_file(self.follow_map, local_dir, 'follow_map',
                                           os.path.join(NetworkUtilities.azure_intermediate_dir,end_date))
            IOutilities.print_dict_to_file(self.uid_map_index, local_dir, 'uid_map_index',
                                           os.path.join(NetworkUtilities.azure_intermediate_dir, end_date))
            IOutilities.print_dict_to_file(self.owners_map, local_dir, 'owners_map',
                                           os.path.join(NetworkUtilities.azure_intermediate_dir, end_date))
            IOutilities.print_dict_to_file(self.pid_map_popularity, local_dir, 'pid_map_popularity',
                                           os.path.join(NetworkUtilities.azure_intermediate_dir, end_date))

    '''