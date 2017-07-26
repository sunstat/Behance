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
import configuration.constants1 as C1
import configuration.constants as C

import operator


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

class NetworkUtilities(object):
    def __init__(self):
        pass

    def truncate_last_project(self, sc):
        sc.textFile(C.PID_2_DATE_FILE)


if __name__ == "__main__":
    sc, _ = init_spark('base', 20)
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkHelpFunctions.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/IOutilities.py')
    sc.addFile('/home/yiming/Behance/configuration/constants.py')
    network_utilities = NetworkUtilities()
    network_utilities.run(sc)
    sc.stop()

