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
from UserProjectNetwork.NetworkHelpFunctions import NetworkHelpFunctions
import matplotlib.pyplot as plt


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
    def __init__(self, action_file, owner_file):
        self.action_file = action_file
        self.owners_file = owner_file
        self.appreciation_weight = appreciation_weight
        prerequisiteAnalysis.shell_dir = "../EditData/ShellEdit"
        prerequisiteAnalysis.local_intermediate_dir = "../IntermediateDir"
        prerequisiteAnalysis.behance_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance"
        prerequisiteAnalysis.behance_data_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
        prerequisiteAnalysis.azure_intermediate_dir = os.path.join(prerequisiteAnalysis.behance_dir, "IntermediateResult")

    @staticmethod
    def degree_distribution(self, sc, end_date):
        rdd_pair = sc.textFile(action_file).map(lambda x: x.split(',')) \
            .filter(lambda x: NetworkHelpFunctions.date_filter("0000-00-00", x[0], end_date)) \
            .filter(lambda x: x[4] == 'F').map(lambda x: (x[1], x[2])).cache()
        rdd_out = rdd_pair.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).cache()
        out_degree_arr = rdd_out.map(lambda x : len(x[1])).collect()
        rdd_in = rdd_pair.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x, y: x + y).cache()
        in_degree_arr = rdd_in.map(lambda x: len(x[1])).collect()

        plt.figure(1)
        plt.subplot(121)
        plt.hist(out_degree_arr)
        plt.title("Out Degree Distribution")
        plt.xlabel("Out Degree")
        plt.ylabel("Frequency")

        plt.subplot(122)
        plt.hist(in_degree_arr)
        plt.title("In Degree Distribution")
        plt.xlabel("In Degree")
        plt.ylabel("Frequency")

        plt.show()
        plt.pause(10)
        plt.close()


if  __name__ == "__main__":
    sc, _ = init_spark('olivia', 20)
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkHelpFunctions.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkHelpFunctions.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkUtilities.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/IOutilities.py')

    prerequisite_analysis = prerequisiteAnalysis(action_file, owners_file)
    prerequisite_analysis.degree_distribution(sc, '2016-06-30')
