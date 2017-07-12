#!/usr/bin/env python
# extract all user information from owners and actions:

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
from subprocess import Popen
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np


local_run = False
graph_dir = "../Graph"




if local_run:
    action_file = "/Users/yimsun/PycharmProjects/Data/TinyData/action/actionDataTrimNoView-csv"
    owners_file = "/Users/yimsun/PycharmProjects/Data/TinyData/owners-csv"
    intermediate_result_dir = '../IntermediateDir'
else:
    behance_data_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
    action_file = os.path.join(behance_data_dir, "action", "actionDataTrimNoView-csv")
    owners_file = os.path.join(behance_data_dir, "owners-csv")
    intermediate_result_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/IntermediateResult"


def init_spark(name, max_excutors):
    conf = (SparkConf().setAppName(name)
            .set("spark.dynamicAllocation.enabled", "false")
            .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))

    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel('ERROR')
    sqlContext = HiveContext(sc)
    return sc, sqlContext


class prerequisiteAnalysis():
    def __init__(self, action_file, owner_file, pid_2_field_index_file):
        self.action_file = action_file
        self.owners_file = owner_file
        self.pid_2_field_index_file = pid_2_field_index_file
        prerequisiteAnalysis.shell_dir = "../EditData/ShellEdit"
        prerequisiteAnalysis.local_intermediate_dir = "../IntermediateDir"
        prerequisiteAnalysis.behance_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance"
        prerequisiteAnalysis.behance_data_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
        prerequisiteAnalysis.azure_intermediate_dir = os.path.join(prerequisiteAnalysis.behance_dir, "IntermediateResult")

    def degree_distribution(self, sc, end_date):
        def date_filer_help(date1, date2):
            date1_arr = date1.split("-")
            date2_arr = date2.split("-")
            for i in range(len(date1_arr)):
                if int(date1_arr[i]) < int(date2_arr[i]):
                    return True
                elif int(date1_arr[i]) > int(date2_arr[i]):
                    return False
            return True

        def date_filter(prev_date, date, end_date):
            return date_filer_help(prev_date, date) and date_filer_help(date, end_date)

        rdd_pair = sc.textFile(self.action_file).map(lambda x: x.split(',')) \
            .filter(lambda x: date_filter("0000-00-00", x[0], end_date)) \
            .filter(lambda x: x[4] == 'F').map(lambda x: (x[1], x[2])).cache()
        rdd_out = rdd_pair.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).cache()
        out_degree_arr = rdd_out.map(lambda x: len(x[1])).collect()
        rdd_in = rdd_pair.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x, y: x + y).cache()
        in_degree_arr = rdd_in.map(lambda x: len(x[1])).collect()

        return out_degree_arr, in_degree_arr

    @staticmethod
    def tail_array(out_degree_arr, in_degree_arr, N):
        out_tail_arr = [0.]*N
        in_tail_arr = [0.]*N
        for i in range(1, N+1):
            out_tail_arr[i-1] = sum([ int(num) >= i for num in out_degree_arr])
            in_tail_arr[i-1] = sum([ int(num) >= i for num in in_degree_arr])
        print out_tail_arr
        print in_tail_arr

        return out_tail_arr, in_tail_arr

    def plot_field(self, sc):
        rdd_pid_2_field_index = sc.textFile(self.pid_2_field_index_file)
        field_2_frequency = rdd_pid_2_field_index.groupByKey().mapValues(len).collect()
        plt.figure()
        plt.hist(field_2_frequency)
        plt.title("The field Distribution")
        plt.xlabel("Field Index")
        plt.ylabel("Frequency")
        plt.savefig(os.path.join('../Graph/', 'histogram_of_fields.png'))
        plt.close()



if __name__ == "__main__":

    sc, _ = init_spark('olivia', 20)
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkHelpFunctions.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkHelpFunctions.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkUtilities.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/IOutilities.py')
    N = 100
    pid_2_field_index_file = os.path.join(intermediate_result_dir, 'base', 'pid_2_field_index-csv')
    prerequisite_analysis = prerequisiteAnalysis(action_file, owners_file, pid_2_field_index_file)
    out_degree_arr, in_degree_arr = prerequisite_analysis.degree_distribution(sc, '2016-06-30')
    prerequisite_analysis.plot_field(sc)
    print out_degree_arr[1:10]
    print in_degree_arr[1:10]

    out_tail_arr, in_tail_arr = prerequisiteAnalysis.tail_array(out_degree_arr, in_degree_arr, N)
    plt.figure()
    fig, ax_arr = plt.subplots(1, 2)
    ax_arr[0].set_xscale("symlog")
    ax_arr[0].plot(list(range(1, N+1)), np.log(out_tail_arr))
    ax_arr[0].set_title("Out Degree Tail Distribution")
    ax_arr[0].set_xlabel("Out Degree")
    ax_arr[0].set_ylabel("Log of Tail")

    ax_arr[1].set_xscale("symlog")
    ax_arr[1].plot(list(range(1, N+1)), np.log(in_tail_arr))
    ax_arr[1].set_title("In Degree Tail Distribution")
    ax_arr[1].set_xlabel("In Degree")
    ax_arr[1].set_ylabel("Log of Tail")

    plt.show()
    plt.pause(10)

    plt.savefig(os.path.join('../Graph/', 'degreeTailDistribution.png'))
    plt.close()
    sc.stop()

    '''
    comment out the histogram which is not good since the appearance of outliers
    '''

    '''    
    plt.figure()
    plt.subplot(121)
    print max(out_degree_arr)
    plt.hist(out_degree_arr, 30, range=[min(out_degree_arr)-1, max(out_degree_arr)+1])
    plt.title("Out Degree Distribution")
    plt.xlabel("Out Degree")
    plt.ylabel("Frequency")

    plt.subplot(122)
    print max(in_degree_arr)
    plt.hist(in_degree_arr, 30, range=[min(in_degree_arr)-1, max(in_degree_arr)+1])
    plt.title("In Degree Distribution")
    plt.xlabel("In Degree")
    plt.ylabel("Frequency")

    plt.show()
    plt.pause(10)

    plt.savefig(os.path.join('../Graph/', 'degreeDistribution.png'))
    plt.close()
    
    '''


