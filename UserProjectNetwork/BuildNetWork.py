#!/usr/bin/env python
# extract all user information from owners and actions:

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType
import os, sys
import operator

sys.path.insert(1, os.path.join(sys.path[0], '..'))
run_local  = False



if run_local:
    action_file = "/Users/yimsun/PycharmProjects/Behance/TinyData/action/actionDataTrimNoView-csv"
    owners_file = "/Users/yimsun/PycharmProjects/Behance/TinyData/owners-csv"
else:
    behanceDataDir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
    action_file = os.path.join(behanceDataDir, "action", "actionDataTrimNoView-csv")
    owners_file = os.path.join(behanceDataDir, "owners-csv")




def init_spark(name, max_excutors):
    conf = (SparkConf().setAppName(name)
            .set("spark.dynamicAllocation.enabled", "false")
            .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    sqlContext = HiveContext(sc)
    return sc, sqlContext



class BuildNetwork(end_day):
