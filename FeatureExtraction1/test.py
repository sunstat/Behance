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


set1 =  sc.textFile(C.OWNER_FILE).map(lambda x: x.split(',')).map(lambda x: x[0])
set2 =  sc.textFile(C.IMAGE_TRIMMED_FILE).map(lambda x: x.split(',')).map(lambda x: x[0])

print set1.count()

set3 = set1.intersection(set2)

print set3.count()