#!/usr/bin/env python
# extract all user information from owners and actions:

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
from IOutilities import IOutilities
from subprocess import Popen



local_run = True

if local_run:
    action_file = "/Users/yimsun/PycharmProjects/Data/TinyData/action/actionDataTrimNoView-csv"
    owners_file = "/Users/yimsun/PycharmProjects/Data/TinyData/owners-csv"
    intermediateResultDir = '/Users/yimsun/PycharmProjects/Behance/IntermediateDir'
else:
    behanceDataDir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
    action_file = os.path.join(behanceDataDir, "action", "actionDataTrimNoView-csv")
    owners_file = os.path.join(behanceDataDir, "owners-csv")
    intermediateResultDir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/IntermediateResult"

class NetworkUtilities(object):

    '''
    help functions for printing
    '''



    '''
    methods used only within in this class
    '''

    def init_spark_(self, name, max_excutors):
        conf = (SparkConf().setAppName(name)
                .set("spark.dynamicAllocation.enabled", "false")
                .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
        sc = SparkContext(conf=conf)
        sc.setLogLevel('ERROR')
        sqlContext = HiveContext(sc)
        return sc, sqlContext

    def extract_parameters_(self):
        arguments_arr = []
        with open(self.config_file, 'r') as f:
            for line in f:
                arguments_arr.append(line.strip())
        return arguments_arr

    # compare two date strings "2016-12-01"

    def __init__(self, action_file, owner_file, program_name, max_executors, config_file, comment_weight, appreciation_weight):
        self.action_file = action_file
        self.owners_file = owner_file
        self.sc, self.sqlContext = self.init_spark_(program_name, max_executors)
        self.config_file = config_file
        self.comment_weight = comment_weight
        self.appreciation_weight = appreciation_weight

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
        self.pid_map_popularity = None

        '''
        properties needed to be filled 
        '''


        print("==============================")
        print("now building the utilities\n")
        print("actionfile is {}\n".format(self.action_file))
        print("owners_file is {}\n".format(self.owners_file))
        print("==============================")

        '''
        arguments dictionary
        '''
        arguments_arr = self.extract_parameters_()
        self.arguments_dict = dict()
        self.arguments_dict['end_day'] = arguments_arr[0]
        print self.arguments_dict['end_day'].split("-")

    '''
    extract neighbors in user network and uids set which involved in the network built 
    '''

    def extract_neighbors_from_users_network(self):
        end_date = self.arguments_dict['end_day']
        print("===================================")
        print(end_date)
        print("===================================")
        print("===================================")

        def date_filter_(date, end_date):
            date_end_arr = end_date.split("-")
            date_arr = date.split("-")
            for i in range(len(date_arr)):
                if int(date_arr[i]) < int(date_end_arr[i]):
                    return True
                elif int(date_arr[i]) > int(date_end_arr[i]):
                    return False
            return True

        rdd = self.sc.textFile(action_file).map(lambda x: x.split(',')).filter(lambda x: date_filter_(x[0], end_date)).filter(lambda x: x[4] == 'F') \
            .map(lambda x: (x[1], [x[2]])).reduceByKey(lambda a, b: a + b).cache()
        '''
        print (rdd.take(5))
        '''

        follow_map = rdd.collectAsMap()
        uid_set = set()
        count = 0
        for key, value in follow_map.items():
            count += 1
            if count < 5:
                print('key is {} and corresponding value is {}'.format(key, value))
            uid_set.add(key)
            uid_set |= set(value)

        #build map from uid to index
        uid_map_index = dict()
        index = 0
        for uid in uid_set:
            uid_map_index[uid] = index
            index += 1

        self.follow_map = follow_map
        self.uid_set = uid_set
        self.uid_map_index = uid_map_index

        return follow_map, uid_set, uid_map_index



    '''
    extract uid, pid and fields map 
    '''
    def handle_uid_pid(self, uid_set):
        '''
        help functions
        '''

        end_date = self.arguments_dict['end_day']

        def date_filter_(date, end_date):
            def date_filter_help(date, date1):
                date_arr = date.split("-")
                date1_arr = date1.split("-")
                for i in range(len(date_arr)):
                    if int(date_arr[i]) < int(date1_arr[i]):
                        return True
                    elif int(date_arr[i]) > int(date1_arr[i]):
                        return False
                return True
            return date_filter_help("2013-01-01", date) and date_filter_help(date, end_date)

        def filter_uid_inCycle_(uid):
            return uid in uid_set_broad.value

        uid_set_broad = self.sc.broadcast(uid_set)

        '''
        build field map 
        '''

        rdd_owners = self.sc.textFile(self.owners_file).map(lambda x: x.split(','))\
            .filter(lambda x: date_filter_(x[2], end_date)).filter(lambda x: filter_uid_inCycle_(x[1])).cache()

        '''
        rdd_owners = sc.textFile(owners_file).map(lambda x:x.split(',')).filter(lambda x: date_filter_(x))
            .filter(filterUidInCycle_).cache()
        '''

        print(rdd_owners.take(5))
        print(rdd_owners.count())

        fields_map_index = rdd_owners.flatMap(lambda x: (x[3], x[4], x[5])).filter(lambda x: x).distinct().zipWithIndex().collectAsMap()

        '''
        .map(map_field_to_count_).collectAsMap()
        '''

        IOutilities.printDict(fields_map_index, 5)
        print ("field_map_index is with size {}".format(len(fields_map_index)))

        """
        Pid Uid pair in owner file
        """

        owners_map = rdd_owners.map(lambda x: (x[0], x[1])).collectAsMap()


        #pid map to index
        index = 0
        pid_map_index = dict()
        for uid, pid in owners_map.items():
            if pid not in pid_map_index:
                pid_map_index[pid] = index
                index += 1

        self.fields_map_index = fields_map_index
        self.owners_map = owners_map
        self.pid_map_index = pid_map_index

        return fields_map_index, owners_map, pid_map_index

    def create_user_network(self):
        num_users = len(self.uid_set)
        self.user_network = csr_matrix((num_users, num_users))
        for uid1, uids in self.follow_map.items():
            for uid2 in uids:
                print self.uid_map_index[uid1], self.uid_map_index[uid2]
                self.user_network[self.uid_map_index[uid1], self.uid_map_index[uid2]] = 1
        return self.user_network

    def create_popularity(self):
        end_date = self.arguments_dict['end_day']
        def date_filter_(date, end_date):
            date_end_arr = end_date.split("-")
            date_arr = date.split("-")
            for i in range(len(date_arr)):
                if int(date_arr[i]) < int(date_end_arr[i]):
                    return True
                elif int(date_arr[i]) > int(date_end_arr[i]):
                    return False
            return True

        pid_map_index_broad = self.sc.broadcast(self.pid_map_index)

        def pid_filter(pid):
            return pid in pid_map_index_broad.value

        rdd_pids = self.sc.textFile(self.action_file).map(lambda x: x.split(',')).filter(lambda x: date_filter_(x[0], end_date))\
            .filter(lambda x: pid_filter(x[3])).map(lambda x: (x[3], x[4])).collect()
        self.pid_map_num_comments = rdd_pids.filter(lambda x: x[4] == 'C').roupByKey().mapValues(len).collectAsMap()
        self.pid_map_num_appreciations = rdd_pids.filter(lambda x: x[4] == 'A').roupByKey().mapValues(len).collectAsMap()
        for pid in self.pid_map_index:
            popularity = 0
            if pid in self.pid_map_num_comments:
                popularity += self.pid_map_num_comments[pid]
            if pid in self.pid_map_num_appreciations:
                popularity += self.pid_map_num_appreciations[pid]
            self.pid_map_popularity[pid] = popularity

        return self.pid_map_num_comments, self.pid_map_num_appreciations, self.pid_map_popularity

    def writeToIntermediateDirectory(self):
        end_date = self.arguments_dict['end_day']
        if local_run:
            Process = Popen('./%s %s' % ('createIntermediateDateDirLocally.sh', intermediateResultDir, end_date), shell=True)
        else:
            Process = Popen('./%s %s' % ('createIntermediateDateDirLocally.sh', intermediateResultDir, end_date), shell=True)


    def close_utilities(self):
        self.sc.stop()


if __name__ == "__main__":
    utilities = NetworkUtilities(action_file, owners_file, 'user_project_network', 40, 'config',1 ,2)
    follow_map, uid_set, uid_map_index = utilities.extract_neighbors_from_users_network()

    print len(follow_map)
    print len(uid_set)

    fields_index_map, owners_map, pid_map_index = utilities.handle_uid_pid(uid_set)

    print (len(fields_index_map))

    user_network = utilities.create_user_network()

    pid_map_num_comments, pid_map_num_appreciations, pid_map_popularity = utilities.create_popularity()

    utilities.close_utilities()
