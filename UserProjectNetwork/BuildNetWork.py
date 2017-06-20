#!/usr/bin/env python
# extract all user information from owners and actions:

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType
import os, sys
import operator


'''
import from Utility 
'''
from NetworkUtility import init_spark
from NetworkUtility import extractNeighborsFromUsersNetwork
'''
import from Utility 
'''



sys.path.insert(1, os.path.join(sys.path[0], '..'))
run_local  = False



if run_local:
    action_file = "/Users/yimsun/PycharmProjects/Behance/TinyData/action/actionDataTrimNoView-csv"
    owners_file = "/Users/yimsun/PycharmProjects/Behance/TinyData/owners-csv"
else:
    behanceDataDir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
    action_file = os.path.join(behanceDataDir, "action", "actionDataTrimNoView-csv")
    owners_file = os.path.join(behanceDataDir, "owners-csv")




sc, sqlContext = init_spark('build Cycle', 40)

print "gogogo"
extractNeighborsFromUsersNetwork(sc, '2016-06-30')