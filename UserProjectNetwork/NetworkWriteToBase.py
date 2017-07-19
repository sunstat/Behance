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
    '''
    methods used only within in this class
    '''

    def __extract_base_date(self):
        base_date = None
        with open(self.config_file, 'r') as f:
            for line in f:
                base_date = line.strip()
                return base_date

    # compare two date strings "2016-12-01"

    def __init__(self, base_date):
        self.uid_set = None
        self.pid_set = None
        self.base_date = base_date

    '''
    extract neighbors in user network and uids set which involved in the network built 
    '''
    def extract_neighbors_from_users_network(self, sc, output_dir):

        in_threshold = 5
        n_iters = 30
        rdd_pair = sc.textFile(C.ACTION_FILE).map(lambda x: x.split(',')) \
            .filter(lambda x: x[4] == 'F').map(lambda x: (x[1], x[2])).cache()
        print rdd_pair.take(5)

        rdd_pair = NetworkHelpFunctions.filter_graph_by_incoming_degree(sc, rdd_pair, in_threshold, n_iters)

        output_file = os.path.join(output_dir, 'uid_2_index-csv')

        rdd_uid_index = rdd_pair.flatMap(lambda x: (x[0],x[1])).distinct().zipWithIndex().cache()
        IOutilities.print_rdd_to_file(rdd_uid_index, output_file, 'csv')
        self.uid_set = set(rdd_uid_index.map(lambda x: x[0]).collect())

        uid_set_broad = sc.broadcast(self.uid_set)

        def __filter_uid_in_cycle(uid):
            return uid in uid_set_broad.value

        sc.textFile(C.ACTION_VIEW_FILE).filter(lambda x: x[4] == 'V')\
            .filter(lambda x: __filter_uid_in_cycle(x[1])).map(lambda x: (x[3], x[0]))


    def handle_uid_pid(self, sc, base_date, output_dir):
        self.uid_set = set(sc.textFile(C.UID_2_INDEX_FILE).map(lambda x: x.split(',')).map(lambda x: x[0]).collect())

        print(len(self.uid_set))

        uid_set_broad = sc.broadcast(self.uid_set)

        def __filter_uid_in_cycle(uid):
            return uid in uid_set_broad.value

        rdd_owners = sc.textFile(C.OWNER_FILE).map(lambda x: x.split(',')) \
            .filter(lambda x: NetworkHelpFunctions.date_filter("2016-01-01", x[2], base_date)) \
            .filter(lambda x: __filter_uid_in_cycle(x[1])).persist()
        print rdd_owners.take(5)

        pid_set1 = set(rdd_owners.map(lambda x: x[0]).collect())
        print len(pid_set1)

        # build views feature
        rdd_views = sc.textFile(C.ACTION_VIEW_FILE).map(lambda x: x.split(',')).filter(lambda x: x[4] == 'V')
        print rdd_views.take(5)
        pid_set2 = set(rdd_views.filter(lambda x: __filter_uid_in_cycle(x[1])).map(lambda x: x[3]).distinct().collect())

        print len(pid_set2)
        pid_set = pid_set1.intersection(pid_set2)
        pid_set_broad = sc.broadcast(pid_set)

        rdd_owners = rdd_owners.filter(lambda x: x[0] in pid_set_broad.value)
        rdd_pid_2_date = rdd_owners.map(lambda x: (x[0], x[2]))
        rdd_pid_2_view_dates = rdd_views.filter(lambda x : x[3] in pid_set_broad.value)\
            .map(lambda x: (x[3], [x[0]])).reduceByKey(lambda x, y: x+y)

        #print rdd_pid_2_date.count()
        #print rdd_pid_2_view_dates.count()

        print rdd_pid_2_view_dates.take(5)

        rdd_pid_2_view_dates = rdd_pid_2_date.join(rdd_pid_2_view_dates)

        print rdd_pid_2_view_dates.take(5)

        rdd_pid_2_date_feature = rdd_pid_2_view_dates.mapValues(lambda x: NetworkHelpFunctions.extract_feature(x[1][1],x[1][0]))



        '''


        # pid_2_date
        rdd_pid_2_date = rdd_owners.map(lambda x: (x[0], x[2]))
        output_file = os.path.join(output_dir, 'pid_2_date-csv')
        IOutilities.print_rdd_to_file(rdd_pid_2_date, output_file, 'csv')

        rdd_fields_map_index = rdd_owners.flatMap(lambda x: (x[3], x[4], x[5])).filter(
            lambda x: x).distinct().zipWithIndex().cache()

        output_file = os.path.join(output_dir, 'field_2_index-csv')
        IOutilities.print_rdd_to_file(rdd_fields_map_index, output_file, 'csv')

        # build pid-2-field-index
        field_2_index = rdd_fields_map_index.collectAsMap()
        field_2_index_broad = sc.broadcast(field_2_index)

        def trim_str_array(str_arr):
            return [field_2_index_broad.value[x] for x in str_arr if x]
        rdd = rdd_owners.map(lambda x: (x[0], trim_str_array(x[3:])))
        output_file = os.path.join(output_dir, 'pid_2_field_index-psv')
        IOutilities.print_rdd_to_file(rdd, output_file, 'psv')


        # print pid_2_uid to intermediate directory


        rdd_owners_map = rdd_owners.map(lambda x: (x[0], x[1])).distinct().persist()
        output_file = os.path.join(output_dir, 'pid_2_uid-csv')
        IOutilities.print_rdd_to_file(rdd_owners_map, output_file, 'csv')


        # print pid_2_index-csv

        rdd_pid_index = rdd_owners.map(lambda x: x[0]).distinct().zipWithIndex().cache()
        output_file = os.path.join(output_dir, 'pid_2_index-csv')
        IOutilities.print_rdd_to_file(rdd_pid_index, output_file, 'csv')
        
        '''


    def run(self, sc):
        #shell_file = os.path.join(C.SHELL_DIR, 'createIntermediateDateDirHdfs.sh')
        #call('./%s %s %s' % (shell_file, C.INTERMEDIATE_RESULT_DIR, 'base',), shell=True)
        output_dir = os.path.join(C.INTERMEDIATE_RESULT_DIR, 'base')
        #self.extract_neighbors_from_users_network(sc, self.base_date, output_dir)
        self.handle_uid_pid(sc, self.base_date, output_dir)

if __name__ == "__main__":
    sc, _ = init_spark('base', 20)
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkHelpFunctions.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/IOutilities.py')
    sc.addFile('/home/yiming/Behance/configuration/constants.py')
    network_utilities = NetworkUtilities("2016-12-30")
    network_utilities.run(sc)
    sc.stop()

