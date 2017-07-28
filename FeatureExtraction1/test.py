#!/usr/bin/env python
# extract all user information from owners and actions:

import sys

sys.path.append('/home/yiming/Behance')
sys.path.append('/home/yiming/Behance/configuration')
sys.path.append('/home/yiming/Behance/FeatureExtraction1')


# sys.path.append('/home/yiming/Behance/configuration/constants.py')

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
import configuration.constants1 as C1
import configuration.constants as C
from Utilities import Utilities
from datetime import *
from operator import add

'''
write pid_2_index, uid_2_index, field_2_index, pid_2_field_index, pid_2_uid to the base directory
'''


def init_spark(name, max_excutors):
    conf = (SparkConf().setAppName(name)
            .set("spark.dynamicAllocation.enabled", "false")
            .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    sqlContext = HiveContext(sc)
    return sc, sqlContext


sc, _ = init_spark('test', 10)
'''
(u'34826315', (u'34825983', u'34826703', u'40433195', u'40433291', u'40448357', u'40433985', u'41448993', u'41448259', u'42971677', u'44197921', u'44198001')
'''

pid_2_uid_dict = sc.textFile(C.PID_2_UID_FILE).map(lambda x: x.split(',')).collectAsMap()
pid_2_date_dict = sc.textFile(C.PID_2_DATE_FILE).map(lambda x: x.split(',')).collectAsMap()

ls = ['34826315','34825983','34826703','40433195', '40433291', '40448357', '40433985', '41448993', '41448259', '42971677', '44197921', '44198001']
for pid in ls:
    print "uid: {}, date:{}".format(pid_2_uid_dict[pid], pid_2_date_dict[pid])

