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

    sc, _ = init_spark('test', 20)

    rdd_follow_map_6 = sc.textFile(os.path.join(intermediate_result_dir, '2016-06-30', 'follow_map-psv')).map(
        lambda x: x.split('#'))
    rdd_pid_2_popularity_6 = sc.textFile(os.path.join(intermediate_result_dir, '2016-06-30', 'pid_2_popularity-csv')).map(
        lambda x: x.split(','))

    rdd_follow_map_7 = sc.textFile(os.path.join(intermediate_result_dir, '2016-07-30', 'follow_map-psv')).map(
        lambda x: x.split('#'))
    rdd_pid_2_popularity_7 = sc.textFile(
        os.path.join(intermediate_result_dir, '2016-07-30', 'pid_2_popularity-csv')).map(
        lambda x: x.split(','))


    set_6 = set(rdd_follow_map_6.map(lambda x: x[0]).collect())
    set_7 = set(rdd_follow_map_7.map(lambda x: x[0]).collect())

    print set_6 == set_7

    set_6 = set(rdd_pid_2_popularity_6.map(lambda x: x[0]).collect())
    set_7 = set(rdd_pid_2_popularity_7.map(lambda x: x[0]).collect())

    print set_6 == set_7

    delete_shell_azure = os.path.join(IOutilities.shell_dir, 'delete.sh')
    Popen('./%s %s' % (delete_shell_azure, output_file,), shell=True)
