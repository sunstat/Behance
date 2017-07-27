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
from datetime import *
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


class WriteToBase1(object):
    def __init__(self):
        pass

    def truncate_last_project(self, sc):
        #uid : (pid, date)
        def separate(x):
            yield (x[1][0], (x[1][1], x[0]))

        pid_2_date = sc.textFile(C.PID_2_DATE_FILE).map(lambda x: x.split(',')).mapValues(lambda x: Utilities.string_2_date(x))
        print pid_2_date.count()
        pid_2_date = pid_2_date.filter(lambda x: x[1]<=date(2016,11,30))
        print pid_2_date.count()

        pid_set = set(pid_2_date.map(lambda x: x[0]).distinct().collect())
        pid_set_broad = sc.broad(pid_set)
        pid_2_uid = sc.textFile(C.PID_2_UID_FILE).map(lambda x: x.split(',')).filter(lambda x: x[0] in pid_set_broad)
        rdd = pid_2_uid.join(pid_2_date).flatMap(lambda x: separate(x)).mapValues(lambda x: [x]).reduceByKey(lambda x,y: x+y)
        print rdd.take(5)
        rdd_pid_neighbors = rdd.mapValues(lambda x: Utilities.extract_last_date(x)).map(lambda x: x[1])\
            .map(lambda x: (x[0], x[1:]))
        print rdd_pid_neighbors.take(5)
        Utilities.print_rdd_to_file(rdd_pid_neighbors, C1.PID_2_CO_OWNERS_FILE, 'psv')

    def view_feature_extraction(self, sc):

        def creation_day_view_amount(x):
            return len([y for y in x[1] if y == x[0]])

        def creation_first_month_view_amount(x):
            return len([y for y in x[1] if y < x[0]+timedelta(30)])

        def extraction_view_feature_help(x):
            return (creation_day_view_amount(x), creation_first_month_view_amount(x))

        pid_set = set(sc.textFile(C1.PID_2_CO_OWNERS_FILE).map(lambda x: x.split('#')).map(lambda x: x[0]).collect())
        print len(pid_set)
        pid_set_broadcast = sc.broadcast(pid_set)
        pid_2_creation_date = sc.textFile(C.PID_2_DATE_FILE).map(lambda x: x.split(','))\
            .filter(lambda x: x[0] in pid_set_broadcast).mapValues(lambda x: Utilities.string_2_date(x))
        pid_2_view_dates = sc.textFile(C.ACTION_VIEW_FILE).map(lambda x: x.split(','))\
            .filter(lambda x: x[3] in pid_set_broadcast)\
            .filter(lambda x: x[4] == 'V').map(lambda x: [x[3], x[0]]).mapValues(lambda x: [Utilities.string_2_date(x)])\
            .reduceByKey(lambda x, y: x+y)
        pid_2_view_feature = pid_2_creation_date.join(pid_2_view_dates).mapValues(lambda x: extraction_view_feature_help(x))
        Utilities.print_rdd_to_file(pid_2_view_feature , C1.PID_2_VIEWS_FEATURE_FILE, 'psv')



    def co_owner_popularity(self, sc):
        pid_2_popularity_dict = sc.textFile(C.PID_2_POPULARITY_FILE).map(lambda x: x.split(','))\
            .mapValues(lambda x: float(x)).collectAsMap()
        pid_2_popularity_dict_broad = sc.braodcast(pid_2_popularity_dict)

        def average_pop_co_owners(ls):
            if not ls:
                return 0.
            ls = [pid_2_popularity_dict_broad[x] for x in ls]
            return sum(ls)/float(len(ls))

        rdd_pid_2_co_owner_popularity = sc.textFile(C1.PID_2_CO_OWNERS_FILE).map(lambda x: x.spit('#'))\
            .mapValues(lambda x: x.split(',')).mapValues(lambda x: average_pop_co_owners(x))
        Utilities.print_rdd_to_file(rdd_pid_2_co_owner_popularity, C1.PID_2_CO_OWNERS_POPULARITY_FILE, 'csv')

    def extract_feature_file(self):
        pid_set = set(sc.textFile(C1.PID_2_CO_OWNERS_FILE).map(lambda x: x.split('#')).map(lambda x: x[0]).collect())
        print len(pid_set)
        pid_set_broadcast = sc.broadcast(pid_set)
        pid_2_field_index = sc.textFile(C.PID_2_FIELD_INDEX_FILE).map(lambda x: x.split('#'))\
            .filter(lambda x: x[0] in pid_set_broadcast).mapValues(lambda x: x.split(','))
        pid_2_score = sc.textFile(C.PID_2_SCORE_FILE).map(lambda x: x.split(',')).filter(lambda x: x[0] in pid_set_broadcast)
        pid_2_view_feature = sc.textFile(C1.PID_2_VIEWS_FEATURE_FILE).map(lambda x:x.split('#'))\
            .mapValues(lambda x: x.split(','))



    def run(self, sc):
        self.truncate_last_project(sc)

if __name__ == "__main__":
    print "feature extraction"
    sc, _ = init_spark('base', 20)
    sc.addFile('/home/yiming/Behance/FeatureExtraction1/Utilities.py')
    write_to_base1 = WriteToBase1()
    write_to_base1.run(sc)
    sc.stop()

