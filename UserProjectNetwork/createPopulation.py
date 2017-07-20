#!/usr/bin/env python
# extract all user information from owners and actions:

import sys

sys.path.append('/home/yiming/Behance')
sys.path.append('/home/yiming/Behance/configuration')
sys.path.append('/home/yiming/Behance/UserProjectNetwork')

#sys.path.append('/home/yiming/Behance/configuration/constants.py')
import configuration.constants as C


from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
from IOutilities import IOutilities
from subprocess import Popen
from NetworkHelpFunctions import NetworkHelpFunctions
from subprocess import call
import configuration.constants as C

def init_spark(name, max_excutors):
    conf = (SparkConf().setAppName(name)
            .set("spark.dynamicAllocation.enabled", "false")
            .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    sqlContext = HiveContext(sc)
    return sc, sqlContext



class NetworkUtilities(object):

    def __init__(self):
        rdd_uid_2_index = sc.textFile(os.path.join(C.UID_2_INDEX_FILE))\
            .map(lambda x: x.split(','))
        rdd_pid_2_index = sc.textFile(os.path.join(C.PID_2_INDEX_FILE))\
            .map(lambda x: x.split(','))
        self.uid_set = set(rdd_uid_2_index.map(lambda x: x[0]).collect())
        self.pid_set = set(rdd_pid_2_index.map(lambda x: x[0]).collect())

    def extract_neighbors_from_users_network(self, sc, output_dir):
        # read uid_set pid_set from base
        uid_set_broad = sc.broadcast(self.uid_set)

        def __filter_uid_incycle(x):
            return x[0] in uid_set_broad.value and x[1] in uid_set_broad.value

        rdd_pair = sc.textFile(C.ACTION_FILE).map(lambda x: x.split(','))\
            .filter(lambda x: x[4] == 'F').map(lambda x: (x[1], x[2])).filter(__filter_uid_incycle).cache()

        rdd_follow = rdd_pair.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).cache()
        output_file = os.path.join(output_dir, 'follow_map-psv')
        IOutilities.print_rdd_to_file(rdd_follow, output_file, 'psv')

    def create_popularity(self, sc, output_dir):

        def pid_filter(pid):
            return pid in pid_set_broad.value

        rdd_popularity_base = sc.textFile(C.PID_2_INDEX_FILE)\
            .map(lambda x: x.split(',')) .map(lambda x: (x[0], (0, 0)))
        print (rdd_popularity_base.take(5))

        pid_set_broad = sc.broadcast(self.pid_set)

        print len(self.pid_set)

        rdd_pids = sc.textFile(C.ACTION_FILE).map(lambda x: x.split(','))\
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

    def run(self, sc):
        output_dir = C.BASE_DIR
        self.extract_neighbors_from_users_network(sc, output_dir)
        self.create_popularity(sc, output_dir)


if __name__ == "__main__":
    sc, _ = init_spark('olivia', 20)
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkHelpFunctions.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/IOutilities.py')
    sc.addFile('/home/yiming/Behance/constants.py')
    network_utilities = NetworkUtilities()
    network_utilities.run(sc)
    sc.stop()

