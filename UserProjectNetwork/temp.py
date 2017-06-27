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

        rdd_pids = sc.textFile(self.action_file).map(lambda x: x.split(',')).filter(
            lambda x: DateUtilities.date_filter("0000-00-00", x[0], end_date)) \
            .filter(lambda x: pid_filter(x[3])).map(lambda x: (x[3], x[4])).cache()
        self.pid_map_num_comments = rdd_pids.filter(lambda x: x[1] == 'C').groupByKey().mapValues(len).collectAsMap()
        self.pid_map_num_appreciations = rdd_pids.filter(lambda x: x[1] == 'A').groupByKey().mapValues(
            len).collectAsMap()
        for pid in self.pid_map_index:
            popularity = 0
            if pid in self.pid_map_num_comments:
                popularity += self.comment_weight * self.pid_map_num_comments[pid]
            if pid in self.pid_map_num_appreciations:
                popularity += self.appreciation_weight * self.pid_map_num_appreciations[pid]
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
            Popen('./%s %s %s' % (shell_file, intermediate_result_dir, end_date,), shell=True)
        else:
            shell_file = os.path.join(NetworkUtilities.shell_dir, 'createIntermediateDateDirHdfs.sh')
            Popen('./%s %s %s' % (shell_file, intermediate_result_dir, end_date,), shell=True)

        self.extract_neighbors_from_users_network()
        self.handle_uid_pid(self.uid_set)
        # self.create_user_network()
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
                                           os.path.join(NetworkUtilities.azure_intermediate_dir, end_date))
            IOutilities.print_dict_to_file(self.uid_map_index, local_dir, 'uid_map_index',
                                           os.path.join(NetworkUtilities.azure_intermediate_dir, end_date))
            IOutilities.print_dict_to_file(self.owners_map, local_dir, 'owners_map',
                                           os.path.join(NetworkUtilities.azure_intermediate_dir, end_date))
            IOutilities.print_dict_to_file(self.pid_map_popularity, local_dir, 'pid_map_popularity',
                                           os.path.join(NetworkUtilities.azure_intermediate_dir, end_date))