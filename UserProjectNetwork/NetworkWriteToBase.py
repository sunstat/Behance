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

    def __init__(self, action_file, owner_file, config_file):

        self.action_file = action_file
        self.owners_file = owner_file
        self.config_file = config_file
        NetworkUtilities.shell_dir = "../EditData/ShellEdit"
        NetworkUtilities.local_intermediate_dir = "../IntermediateDir"
        NetworkUtilities.behance_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance"
        NetworkUtilities.behance_data_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
        NetworkUtilities.azure_intermediate_dir = os.path.join(NetworkUtilities.behance_dir, "IntermediateResult")

        '''
        two intermediate results for 
        '''
        self.uid_set = None
        '''
        ===============================
        '''
        self.base_date = self.__extract_base_date


    '''
    extract neighbors in user network and uids set which involved in the network built 
    '''
    def extract_neighbors_from_users_network(self, sc, base_date, output_dir):

        in_threshold = 5
        n_iters = 20
        rdd_pair = sc.textFile(action_file).map(lambda x: x.split(','))\
            .filter(lambda x: NetworkHelpFunctions.date_filter("0000-00-00", x[0], base_date))
        rdd_pair = NetworkHelpFunctions.filter_graph_by_incoming_degree(sc, rdd_pair, in_threshold, n_iters)

        '''
        print uid_index to intermediate directory
        '''
        output_file = os.path.join(output_dir, 'uid_2_index-csv')

        rdd_uid_index = rdd_pair.flatMap(lambda x: (x[0],x[1])).distinct().zipWithIndex().cache()
        IOutilities.print_rdd_to_file(rdd_uid_index, output_file, 'csv')
        self.uid_set = set(rdd_uid_index.map(lambda x: x[0]).collect())

    def handle_uid_pid(self, sc, base_date, output_dir):

        uid_set_broad = sc.broadcast(self.uid_set)

        def __filter_uid_in_cycle(uid):
            return uid in uid_set_broad.value

        '''
        print field_2_index to intermediate diretory
        '''

        rdd_owners = sc.textFile(self.owners_file).map(lambda x: x.split(',')) \
            .filter(lambda x: NetworkHelpFunctions.date_filter("0000-00-00", x[2], base_date)) \
            .filter(lambda x: __filter_uid_in_cycle(x[1])).persist()

        rdd_fields_map_index = rdd_owners.flatMap(lambda x: (x[3], x[4], x[5])).filter(
            lambda x: x).distinct().zipWithIndex().cache()

        output_file = os.path.join(output_dir, 'field_2_index-csv')
        IOutilities.print_rdd_to_file(rdd_fields_map_index, output_file, 'csv')

        '''
        build pid-2-field-index
        '''

        field_2_index = rdd_fields_map_index.collectAsMap()
        field_2_index_broad = sc.broadcast(field_2_index)

        def trim_str_array(str_arr):
            return [field_2_index_broad.value[x] for x in str_arr if x]
        rdd = rdd_owners.map(lambda x: (x[0], trim_str_array(x[3:])))
        output_file = os.path.join(output_dir, 'pid_2_field_index-psv')
        IOutilities.print_rdd_to_file(rdd, output_file, 'psv')

        '''
        print pid_2_uid to intermediate directory
        '''

        rdd_owners_map = rdd_owners.map(lambda x: (x[0], x[1])).distinct().persist()
        output_file = os.path.join(output_dir, 'pid_2_uid-csv')
        IOutilities.print_rdd_to_file(rdd_owners_map, output_file, 'csv')

        '''
        print pid_2_index-csv
        '''
        rdd_pid_index = rdd_owners.map(lambda x: x[0]).distinct().zipWithIndex().cache()
        output_file = os.path.join(output_dir, 'pid_2_index-csv')
        IOutilities.print_rdd_to_file(rdd_pid_index, output_file, 'csv')

    def run(self, sc):
        shell_file = os.path.join(NetworkUtilities.shell_dir, 'createIntermediateDateDirHdfs.sh')
        Popen('./%s %s %s' % (shell_file, intermediate_result_dir, 'base',), shell=True)
        output_dir = os.path.join(NetworkUtilities.azure_intermediate_dir, 'base')
        self.extract_neighbors_from_users_network(sc, self.base_date, output_dir)
        self.handle_uid_pid(sc, self.base_date, output_dir)

if __name__ == "__main__":
    sc, _ = init_spark('base', 20)
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkHelpFunctions.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkUtilities.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/IOutilities.py')
    network_utilities = NetworkUtilities(action_file, owners_file, 'config')
    network_utilities.run(sc)
    sc.stop()

