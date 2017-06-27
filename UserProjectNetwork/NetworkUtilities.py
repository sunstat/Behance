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
from dateUtilities import DateUtilities
from pyspark.sql.functions import udf






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
    help functions for printing
    '''

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
        return DateUtilities.date_filer_help(prev_date, date) and DateUtilities.date_filer_help(date, end_date)

    '''
    methods used only within in this class
    '''

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
        self.config_file = config_file
        self.comment_weight = comment_weight
        self.appreciation_weight = appreciation_weight
        NetworkUtilities.shell_dir = "../EditData/ShellEdit"
        NetworkUtilities.local_intermediate_dir = "../IntermediateDir"
        NetworkUtilities.behance_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance"
        NetworkUtilities.behance_data_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
        NetworkUtilities.azure_intermediate_dir = os.path.join(NetworkUtilities.behance_dir, "IntermediateDir")

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


    def extract_neighbors_from_users_network(self, sc):

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


        end_date = self.arguments_dict['end_day']
        date_utilities = DateUtilities()
        date_utilities_broad = sc.broadcast(date_utilities)


        rdd = sc.textFile(action_file).map(lambda x: x.split(','))\
            .filter(lambda x: date_filter("0000-00-00", x[0], "2016-06-30"))\
            .filter(lambda x: x[4] == 'F').map(lambda x: (x[1], [x[2]])).reduceByKey(lambda x, y: x + y)
        print(rdd.take(5))

        follow_map = rdd.collectAsMap()
        uid_set = set()
        for key, value in follow_map.items():
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
