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

    # compare two date strings "2016-12-01"


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
        return NetworkUtilities.date_filer_help(prev_date, date) and NetworkUtilities.date_filer_help(date, end_date)

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
    extract neighbors in user network and uids set which involved in the network built 
    '''


    def extract_neighbors_from_users_network(self, sc, end_date):

        '''
        print follow_map to intermediate directory 
        '''
        rdd = sc.textFile(action_file).map(lambda x: x.split(','))\
            .filter(lambda x: x[4] == 'F').cache()
        rdd_follow = rdd.map(lambda x: (x[1], [x[2]])).reduceByKey(lambda x, y: x + y).cache()
        print(rdd_follow.take(5))


        rdd_uid_index = rdd.flatMap(lambda x: [x[1],x[2]]).distinct().zipWithIndex().cache()
        print (rdd_uid_index.take(5))
        #IOutilities.print_rdd_to_file(rdd_uid_index, output_file, 'csv')

        ls = rdd_uid_index.map(lambda x: x[0]).collect()

