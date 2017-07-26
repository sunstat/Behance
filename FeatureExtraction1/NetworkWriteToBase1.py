#!/usr/bin/env python
# extract all user information from owners and actions:

import sys

sys.path.append('/home/yiming/Behance')
sys.path.append('/home/yiming/Behance/configuration')
sys.path.append('/home/yiming/Behance/FeatureExtraction1')

# sys.path.append('/home/yiming/Behance/configuration/constants.py')
import configuration.constants as C

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
from datetime import date
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
        def string_2_date(my_date):
            year,month,day = my_date.split('-')
            return date(int(year), int(month), int(day))


        pid_2_date = sc.textFile(C.PID_2_DATE_FILE).map(lambda x: x.split(',')).mapValues(lambda x: string_2_date(x))
        print pid_2_date.take(5)

    def run(self,sc):
        self.truncate_last_project(sc)


if __name__ == "__main__":
    sc, _ = init_spark('base', 20)
    sc.addFile('/home/yiming/Behance/FeatureExtraction1/Utilities.py')
    network_utilities = NetworkUtilities()
    network_utilities.run(sc)
    sc.stop()

