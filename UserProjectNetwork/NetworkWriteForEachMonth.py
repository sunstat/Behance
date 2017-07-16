#!/usr/bin/env python
# extract all user information from owners and actions:

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType
import os, sys
import operator
from subprocess import check_call
from subprocess import call
from scipy.sparse import coo_matrix, csr_matrix
from IOutilities import IOutilities
from subprocess import Popen
from NetworkHelpFunctions import NetworkHelpFunctions

def init_spark(name, max_excutors):
    conf = (SparkConf().setAppName(name)
            .set("spark.dynamicAllocation.enabled", "false")
            .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    sqlContext = HiveContext(sc)
    return sc, sqlContext


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
    def __extract_parameters(self):
        months_arr = []
        with open(self.config_file, 'r') as f:
            for line in f:
                months_arr.append(line.strip())
        return months_arr

    # compare two date strings "2016-12-01"

    def __init__(self, action_file, owner_file, config_file, comment_weight, appreciation_weight):
        self.action_file = action_file
        self.owners_file = owner_file
        self.config_file = config_file
        self.comment_weight = comment_weight
        self.appreciation_weight = appreciation_weight
        NetworkUtilities.shell_dir = "../EditData/ShellEdit"
        NetworkUtilities.behance_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance"
        NetworkUtilities.behance_data_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
        NetworkUtilities.azure_intermediate_dir = os.path.join(NetworkUtilities.behance_dir, "IntermediateResult")
        self.uid_set = None
        self.pid_set = None
        self.months_arr = self.__extract_parameters()
        rdd_uid_2_index = sc.textFile(os.path.join(intermediate_result_dir, 'base', 'uid_2_index-csv')).map(
            lambda x: x.split(','))
        rdd_pid_2_index = sc.textFile(os.path.join(intermediate_result_dir, 'base', 'pid_2_index-csv')).map(
            lambda x: x.split(','))
        self.uid_set = set(rdd_uid_2_index.map(lambda x: x[0]).collect())
        self.pid_set = set(rdd_pid_2_index.map(lambda x: x[0]).collect())

    def extract_neighbors_from_users_network(self, sc, end_date, output_dir):
        # read uid_set pid_set from base
        print end_date
        uid_set_broad = sc.broadcast(self.uid_set)

        def __filter_uid_incycle(x):
            return x[0] in uid_set_broad.value and x[1] in uid_set_broad.value

        rdd_pair = sc.textFile(action_file).map(lambda x: x.split(','))\
            .filter(lambda x: NetworkHelpFunctions.date_filter("0000-00-00", x[0], end_date))\
            .filter(lambda x: x[4] == 'F').map(lambda x: (x[1], x[2])).filter(__filter_uid_incycle).cache()

        rdd_follow = rdd_pair.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).cache()
        output_file = os.path.join(output_dir, 'follow_map-psv')
        IOutilities.print_rdd_to_file(rdd_follow, output_file, 'psv')

    def create_popularity(self, sc, end_date, output_dir):
        rdd_popularity_base = sc.textFile(os.path.join(intermediate_result_dir, 'base', 'pid_2_index-csv'))\
            .map(lambda x: x.split(',')) .map(lambda x: (x[0], (0, 0)))
        print (rdd_popularity_base.take(5))

        pid_set_broad = sc.broadcast(self.pid_set)

        print len(self.pid_set)

        def pid_filter(pid):
            return pid in pid_set_broad.value

        rdd_pids = sc.textFile(self.action_file).map(lambda x: x.split(','))\
            .filter(lambda x: NetworkHelpFunctions.date_filter("0000-00-00", x[0], end_date)) \
            .filter(lambda x: pid_filter(x[3])).map(lambda x: (x[3], x[4])).cache()

        rdd_pid_num_comments = rdd_pids.filter(lambda x: x[1] == 'C').groupByKey().mapValues(len)
        rdd_pid_num_appreciations = rdd_pids.filter(lambda x: x[1] == 'A').groupByKey().mapValues(len)
        temp_left = rdd_pid_num_comments.leftOuterJoin(rdd_pid_num_appreciations)
        temp_right = rdd_pid_num_comments.rightOuterJoin(rdd_pid_num_appreciations).filter(lambda x: not x[1][0])
        rdd_popularity = temp_left.union(temp_right).distinct()\
            .map(lambda x: (x[0], (NetworkHelpFunctions.change_none_to_zero(x[1][0]),
                                   NetworkHelpFunctions.change_none_to_zero(x[1][1]))))
        rdd_popularity = rdd_popularity.union(rdd_popularity_base)
        rdd_popularity = rdd_popularity.map(lambda x: (x[0], NetworkHelpFunctions.calculate_popularity(x[1][0],x[1][1],1,2)))
        rdd_popularity = rdd_popularity.reduceByKey(lambda x,y : x+y)
        output_file = os.path.join(output_dir, '-'.join(['pid_2_popularity', 'csv']))
        print(output_file)
        IOutilities.print_rdd_to_file(rdd_popularity, output_file, 'csv')

    def create_month_dir(self, sc, end_date):
        shell_file = os.path.join(NetworkUtilities.shell_dir, 'createIntermediateDateDirHdfs.sh')
        call('{} {} {} {}'.format("/usr/bin/env bash", shell_file, intermediate_result_dir, end_date), shell=True)
        output_dir = os.path.join(NetworkUtilities.azure_intermediate_dir, end_date)
        self.extract_neighbors_from_users_network(sc, end_date, output_dir)
        self.create_popularity(sc, end_date, output_dir)

    def run(self, sc):
        for end_date in self.months_arr:
            self.create_month_dir(sc, end_date)

if __name__ == "__main__":
    sc, _ = init_spark('olivia', 20)
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkHelpFunctions.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkUtilities.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/IOutilities.py')
    network_utilities = NetworkUtilities(action_file, owners_file, 'config', 1, 2)
    network_utilities.run(sc)
    sc.stop()

