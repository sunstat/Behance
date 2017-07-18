#!/usr/bin/env python
# extract all user information from owners and actions:

import sys

sys.path.append('/home/yiming/Behance')
sys.path.append('/home/yiming/Behance/configuration')
sys.path.append('/home/yiming/Behance/UserProjectNetwork')


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


class prerequisiteAnalysis():
    def __init__(self):
        pass

    def degree_distribution(self, sc, end_date):
        rdd_pair = sc.textFile(C.ACTION_FILE).map(lambda x: x.split(',')) \
            .filter(lambda x: NetworkHelpFunctions.NetworkHelpFunctions.date_filter("0000-00-00", x[0], end_date)) \
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

    def plot_orginal_degrees(self, sc, N):
        out_degree_arr, in_degree_arr = prerequisite_analysis.degree_distribution(sc, '2016-12-30')
        _, in_tail_arr = prerequisiteAnalysis.tail_array(out_degree_arr, in_degree_arr, N)
        plt.figure()
        fig, ax_arr = plt.subplots(1)
        ax_arr.set_xscale("symlog")
        ax_arr.plot(list(range(1, N + 1)), np.log(in_tail_arr))
        ax_arr.set_title("In Degree Tail Distribution")
        ax_arr.set_xlabel("In Degree")
        ax_arr.set_ylabel("Log of Tail")
        plt.savefig(os.path.join('../Graph/', 'originalDegreeTailDistribution.png'))
        plt.close()

    def plot_field(self, sc):
        rdd_pid_2_field_index = sc.textFile(C.PID_2_FIELD_INDEX_FILE)
        index_2_field = sc.textFile(C.FIELD_2_INDEX_FILE)
        index_2_field = index_2_field.map(lambda x: x.split(',')).map(lambda x: (x[1], x[0])).collectAsMap()
        print(index_2_field)
        print rdd_pid_2_field_index.take(5)

        field_index_2_frequency = rdd_pid_2_field_index.map(lambda x: x.split('#')).map(lambda x: x[1])
        field_index_2_frequency = field_index_2_frequency.filter(lambda x: x).flatMap(lambda x: x.split(',')).map(lambda x: (x,1))\
            .reduceByKey(lambda x, y: x + y).collect()
        print field_index_2_frequency
        arr = zip(*field_index_2_frequency)
        print arr
        pos = np.arange(len(arr[0])) + .5
        pos_y = np.arange(len(arr[0])) + .5
        ylabel = []
        for index in arr[0]:
            ylabel.append(index_2_field[index])

        print ylabel
        plt.figure()
        plt.barh(pos_y, arr[1], align='center', color='green', ecolor='black', alpha=0.5)
        #plt.yticks(pos_y, arr[0])
        plt.xlabel('Performance')
        plt.title('Fields Distribution')
        plt.savefig(os.path.join('../Graph/', 'histogram_of_fields.png'))
        plt.close()


    def pruned_network_preliminary_analysis(self, sc):
        N = 100
        def flat_2_pairs(x):
            for y in x[1]:
                yield (x[0], y)

        rdd_follow = sc.textFile(os.path.join(C.INTERMEDIATE_RESULT_DIR, '2016-06-30', 'follow_map-psv'))
        rdd_pair = rdd_follow.map(lambda x: x.split('#')).map(lambda x: (x[0],x[1].split(','))).flatMap(flat_2_pairs)

        # test cycle correct or not
        set1 = set(rdd_pair.map(lambda x: x[0]).distinct().collect())
        set2 = set(rdd_pair.map(lambda x: x[1]).distinct().collect())
        set3 = set1.intersection(set2)

        print("first is {}, second is {}, intersection is {}".format(len(set1), len(set2), len(set3)))

        out_degree_arr = rdd_follow.map(lambda x: x.split('#')).map(lambda x: len(x[1].split(','))).collect()
        in_degree_arr = rdd_pair.map(lambda x : (x[1], x[0])).reduceByKey(lambda x,y : x+y).map(lambda x: len(x[1])).collect()
        out_tail_arr, in_tail_arr = prerequisiteAnalysis.tail_array(out_degree_arr, in_degree_arr, N)
        plt.figure()
        fig, ax_arr = plt.subplots(1, 2, sharey=True)
        ax_arr[0].set_xscale("symlog")
        ax_arr[0].plot(list(range(1, N + 1)), np.log(in_tail_arr))
        ax_arr[0].set_title("In Degree Tail Distribution")
        ax_arr[0].set_xlabel("In Degree")
        ax_arr[0].set_ylabel("Log of Tail")
        ax_arr[1].set_xscale("symlog")
        ax_arr[1].plot(list(range(1, N + 1)), np.log(out_tail_arr))
        ax_arr[1].set_title("Out Degree Tail Distribution")
        ax_arr[1].set_xlabel("Out Degree")
        ax_arr[1].set_ylabel("Log of Tail")
        plt.savefig(os.path.join('../Graph/', 'DegreeTailDistributionInPrunedNetwork.png'))
        plt.close()

    def popularity_gap_analysis(self, sc):

        pid_2_date = sc.textFile(os.path.join(C.INTERMEDIATE_RESULT_DIR, 'base', 'pid_2_date-csv')).map(lambda x: x.split(','))

        pid_2_date = pid_2_date.filter(lambda x: NetworkHelpFunctions.NetworkHelpFunctions.date_filter("2016-01-30", x[1], "2016-12-30"))

        pid_set = set(pid_2_date.map(lambda x: x[0]).collect())

        pid_set_broad = sc.broadcast(pid_set)

        def pid_filter(pid):
            return pid in pid_set_broad.value

        rdd_pids = sc.textFile(C.ACTION_FILE).map(lambda x: x.split(',')).filter(lambda x: x[4] == 'C' or x[4] == 'A')\
            .filter(lambda x: NetworkHelpFunctions.NetworkHelpFunctions.date_filter("0000-00-00", x[0], "2016-12-30")) \
            .filter(lambda x: pid_filter(x[3])).map(lambda x: (x[3], x[0])).cache()

        rdd_pids = rdd_pids.union(pid_2_date).mapValues(NetworkHelpFunctions.NetworkHelpFunctions.date_2_value)

        rdd_pids = rdd_pids.mapValues(lambda x : [x]).reduceByKey(lambda x,y: x+y)\
            .map(lambda x: NetworkHelpFunctions.NetworkHelpFunctions.gap_popularity(x[1]))

        print rdd_pids.count()
        print rdd_pids.filter(lambda x: x==365).count()
        ls_gap = rdd_pids.filter(lambda x: x != 365).collect()

        plt.hist(ls_gap)
        plt.xlabel('gap of first action')
        plt.ylabel('frequency')
        plt.savefig(os.path.join('../Graph/', 'histogramOfFirstAction.png'))
        plt.close()




if __name__ == "__main__":
    sc, _ = init_spark('olivia', 20)
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkHelpFunctions.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/IOutilities.py')
    prerequisite_analysis = prerequisiteAnalysis()
    prerequisite_analysis.plot_orginal_degrees(sc, 100)
    #prerequisite_analysis.plot_field(sc)
    #prerequisite_analysis.pruned_network_preliminary_analysis(sc)
    #prerequisite_analysis.popularity_gap_analysis(sc)

    sc.stop()




