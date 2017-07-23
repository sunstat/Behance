#!/usr/bin/env python
# extract all user information from owners and actions:

import sys
import re
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
import numpy as np
from IOutilities import IOutilities

import configuration.constants as C

def init_spark(name, max_excutors):
    conf = (SparkConf().setAppName(name)
            .set("spark.dynamicAllocation.enabled", "false")
            .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel('ERROR')
    sqlContext = HiveContext(sc)
    return sc, sqlContext

sc, sqlContext = init_spark('olivia', 20)

rdd_data = sc.textFile(C.PID_2_INDEX_FILE)

rdd_train, rdd_valid, rdd_test = rdd_data.map(lambda x: x.split(','))\
    .map(lambda x: x[0]).randomSplit(weights=[0.6, 0.2, 0.2], seed=1)

rdd_train.saveAsTextFile(C.TRAIN_PID_SET_FILE)
rdd_valid.saveAsTextFile(C.VALID_PID_SET_FILE)
rdd_test.saveAsTextFile(C.TEST_PID_SET_FILE)

print rdd_data.count()
print rdd_train.count()+rdd_valid.count()+rdd_test.count()


rdd_sample_train = rdd_train.sample(False, 0.1)
rdd_sample_valid = rdd_valid.sample(False, 0.1)
rdd_sample_test= rdd_test.sample(False, 0.1)

print rdd_train.count()
print rdd_sample_train.count()

rdd_sample_train.saveAsTextFile(C.TRAIN_PID_SAMPLE_SET_FILE)
rdd_sample_valid.saveAsTextFile(C.VALID_PID_SET_FILE)
rdd_sample_test.saveAsTextFile(C.TEST_PID_SET_FILE) 
