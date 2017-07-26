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

# import spark
import NetworkHelpFunctions

def init_spark(name, max_excutors):
    conf = (SparkConf().setAppName(name)
            .set("spark.dynamicAllocation.enabled", "false")
            .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel('ERROR')
    sqlContext = HiveContext(sc)
    return sc, sqlContext


sc, _ = init_spark('correlationAnalysis', 10)


def correlation_page_rank():
    pid_2_score = sc.textFile(C.PID_2_SCORE_FILE).map(lambda x: x.split(',')).mapValues(lambda x: float(x))
    pid_2_popularity = sc.textFile(C.PID_2_POPULARITY_FILE).map(lambda x: x.split(',')).mapValues(lambda x: float(x))
    data = pid_2_score.join(pid_2_popularity).map(lambda x: x[1]).collect()
    data = zip(*data)
    print len(data)
    print len(data[0])
    plt.figure()
    '''
    fig, ax_arr = plt.subplots(1)
    ax_arr.plot(data[0],data[1])
    ax_arr.set_title("correlation between page_rank Score and Popularity")
    ax_arr.set_xlabel("page_rank_score")
    ax_arr.set_ylabel("popularity")
    '''
    print data[0][1:100]
    print data[1][1:100]
    plt.xlabel('page_rank_score')
    plt.ylabel('popularity')
    plt.scatter(data[0][1:100],data[1][1:100])
    plt.savefig(os.path.join('../Graph/', 'cor_page_rank_popularity.png'))
    plt.close()

if __name__ == "__main__":
    correlation_page_rank()
