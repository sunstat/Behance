#!/usr/bin/env python
# extract all user information from owners and actions:

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
from IOutilities import IOutilities
from subprocess import Popen






local_run = False

if local_run:
    action_file = "/Users/yimsun/PycharmProjects/Data/TinyData/action/actionDataTrimNoView-csv"
    owners_file = "/Users/yimsun/PycharmProjects/Data/TinyData/owners-csv"
    intermediate_result_dir = '../IntermediateDir'
else:
    behance_data_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
    action_file = os.path.join(behance_data_dir, "action", "actionDataTrimNoView-csv")
    owners_file = os.path.join(behance_data_dir, "owners-csv")
    intermediate_result_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/IntermediateResult"

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

    def __init__(self, action_file, owner_file, program_name, max_executors, config_file, comment_weight, appreciation_weight):

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
        properties needed to be filled
        '''
        self.follow_map = None
        self.uid_set = None
        self.uid_map_index = None
        self.fields_map_index = None
        self.owners_map = None
        self.pid_map_index = None
        self.user_network = None
        self.pid_map_num_comments = None
        self.pid_map_num_appreciations = None
        self.pid_map_popularity = dict()

        '''
        properties needed to be filled 
        '''

        self.arguments_arr = self.__extract_parameters()

    '''
    extract neighbors in user network and uids set which involved in the network built 
    '''

    def extract_neighbors_from_users_network(self, sc, end_date, output_dir):
        def date_filer_help(date1, date2):
            date1_arr = date1.split("-")
            date2_arr = date2.split("-")
            for i in range(len(date1_arr)):
                if int(date1_arr[i]) < int(date2_arr[i]):
                    return True
                elif int(date1_arr[i]) > int(date2_arr[i]):
                    return False
            return True

        def date_filter(prev_date, date, end_date):
            return date_filer_help(prev_date, date) and date_filer_help(date, end_date)

        '''
        follow_map
        '''
        output_file = os.path.join(output_dir, 'follow_map-csv')
        print output_file

        rdd = sc.textFile(action_file).map(lambda x: x.split(','))\
            .filter(lambda x: date_filter("0000-00-00", x[0], end_date))\
            .filter(lambda x: x[4] == 'F').cache()
        rdd_follow = rdd.map(lambda x: (x[1], [x[2]])).reduceByKey(lambda x, y: x + y).cache()
        print(rdd_follow.take(5))
        IOutilities.print_rdd_to_file(rdd_follow, output_file, 'tsv')

        '''
        uid_index
        '''
        output_file = os.path.join(output_dir, 'uid_2_index')

        rdd_uid_index = rdd.flatMap(lambda x: [x[1],x[2]]).distinct().zipWithIndex().cache()
        print (rdd_uid_index.take(5))
        IOutilities.print_rdd_to_file(rdd_uid_index, output_file, 'csv')

        ls = rdd_uid_index.map(lambda x: x[0]).collect()
        uid_set = set(ls)

        self.uid_set = uid_set


    def handle_uid_pid(self, sc, uid_set, end_date):
        def date_filer_help(date1, date2):
            date1_arr = date1.split("-")
            date2_arr = date2.split("-")
            for i in range(len(date1_arr)):
                if int(date1_arr[i]) < int(date2_arr[i]):
                    return True
                elif int(date1_arr[i]) > int(date2_arr[i]):
                    return False
            return True

        def date_filter(prev_date, date, end_date):
            return date_filer_help(prev_date, date) and date_filer_help(date, end_date)

        def __filter_uid_incycle(uid):
            return uid in uid_set_broad.value

        uid_set_broad = sc.broadcast(uid_set)

        '''
        build field map
        '''

        rdd_owners = sc.textFile(self.owners_file).map(lambda x: x.split(',')) \
            .filter(lambda x: date_filter("0000-00-00", x[2], end_date)) \
            .filter(lambda x: __filter_uid_incycle(x[1])).persist()

        print(rdd_owners.take(5))
        #print("rdd.owners count :{}".format(rdd_owners.count()))

        fields_map_index = rdd_owners.flatMap(lambda x: (x[3], x[4], x[5])).filter(
            lambda x: x).distinct().zipWithIndex().cache()

        print ("field_map_index is with size {}".format(len(fields_map_index)))

        """
        Pid Uid pair in owner file
        """

        rdd_owners_map = rdd_owners.map(lambda x: (x[0], x[1])).distinct().persist()
        print(rdd_owners_map.take(5))

        owners_map = rdd_owners_map.collectAsMap()

        # pid map to index
        index = 0
        pid_map_index = dict()
        for pid, uid in owners_map.items():
            if pid not in pid_map_index:
                # print pid, index
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

    def create_popularity(self,sc, end_date):

        def date_filer_help(date1, date2):
            date1_arr = date1.split("-")
            date2_arr = date2.split("-")
            for i in range(len(date1_arr)):
                if int(date1_arr[i]) < int(date2_arr[i]):
                    return True
                elif int(date1_arr[i]) > int(date2_arr[i]):
                    return False
            return True

        def date_filter(prev_date, date, end_date):
            return date_filer_help(prev_date, date) and date_filer_help(date, end_date)

        pid_map_index_broad = sc.broadcast(self.pid_map_index)

        def pid_filter(pid):
            return pid in pid_map_index_broad.value

        rdd_pids = sc.textFile(self.action_file).map(lambda x: x.split(',')).filter(
            lambda x: date_filter("0000-00-00", x[0], end_date)) \
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

    def write_to_intermediate_directory(self, sc):
        for end_date in self.arguments_arr:
            shell_file = os.path.join(NetworkUtilities.shell_dir, 'createIntermediateDateDirHdfs.sh')
            Popen('./%s %s %s' % (shell_file, intermediate_result_dir, end_date,), shell=True)
            output_dir = os.path.join(NetworkUtilities.azure_intermediate_dir, end_date)
            self.extract_neighbors_from_users_network(sc, end_date, output_dir)

            '''
            print("now implementing uid and pid")
            self.handle_uid_pid(sc, self.uid_set, end_date)
            print("now creating popularity")
            self.create_popularity(sc, end_date)

            end_date = self.arguments_dict['end_day']
            local_dir = os.path.join("../IntermediateDir", end_date)
            if local_run:
                shell_file = os.path.join(NetworkUtilities.shell_dir, 'createIntermediateDateDirLocally.sh')
                Popen('./%s %s %s' % (shell_file, intermediate_result_dir, end_date,), shell=True)
            else:
                shell_file = os.path.join(NetworkUtilities.shell_dir, 'createIntermediateDateDirHdfs.sh')
                Popen('./%s %s %s' % (shell_file, intermediate_result_dir, end_date,), shell=True)
            '''

