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
from NetworkHelpFunctions import NetworkHelpFunctions


# import spark

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
    plt.scatter(data[0],data[1])
    plt.savefig(os.path.join('../Graph/', 'cor_page_rank_popularity.png'))
    plt.close()


def correlation_view_popularity():
    pid_set = set(sc.textFile(C.PID_2_INDEX_FILE).map(lambda x: x.split(',')).map(lambda x: x[0]).collect())
    pid_set_broad = sc.broadcast(pid_set)
    pid_2_view = sc.textFile(C.ACTION_VIEW_FILE).map(lambda x: x.split(',')).filter(lambda x: x[3] in pid_set_broad.value)\
        .filter(lambda x: x[4] == 'V')\
        .map(lambda x: (x[3],[x[4]])).reduceByKey(lambda x,y: x+y).mapValues(lambda x: len(x))
    print pid_2_view.take(5)
    pid_2_popularity = sc.textFile(C.PID_2_POPULARITY_FILE).map(lambda x: x.split(',')).mapValues(lambda x: float(x))
    data = pid_2_view.join(pid_2_popularity).map(lambda x: x[1]).collect()
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
    plt.xlabel('view_amount')
    plt.ylabel('popularity')
    plt.scatter(data[0],data[1])
    plt.savefig(os.path.join('../Graph/', 'view_amount_popularity.png'))
    plt.close()


def correlation_incoming_popularity():

    def separate(x):
        for y in x[1]:
            yield (x[0], y)

    rdd_pair = sc.textFile(C.FOLLOW_MAP_FILE).map(lambda x: x.split('#')) \
        .mapValues(lambda x: x.split(',')).map(lambda x: separate(x)).map(lambda x: (x[1],[x[0]]))
    rdd_incoming = rdd_pair.reduceByKey(lambda x,y: x+y).mapValues(lambda x: len(x))

    rdd_uid_2_pid = sc.textFile(C.PID_2_UID_FILE).map(lambda x: x.split(',')).map(lambda x: (x[1], x[0]))
    rdd_pid_2_incoming = rdd_uid_2_pid.join(rdd_incoming).map(lambda x: x[1])
    print rdd_pid_2_incoming.take(5)
    pid_2_popularity = sc.textFile(C.PID_2_POPULARITY_FILE).map(lambda x: x.split(',')).mapValues(lambda x: float(x))
    data = rdd_pid_2_incoming.join(pid_2_popularity).map(lambda x:x[1]).collect()
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
    plt.xlabel('in_coming')
    plt.ylabel('popularity')
    plt.scatter(data[0], data[1])
    plt.savefig(os.path.join('../Graph/', 'incoming_popularity.png'))
    plt.close()

def correlation_outcoming_popularity():
    rdd_outcoming = sc.textFile(C.FOLLOW_MAP_FILE).map(lambda x:x.split('#'))\
        .mapValues(lambda x: x.split(',')).mapValues(lambda x: len(x))
    rdd_uid_2_pid = sc.textFile(C.PID_2_UID_FILE).map(lambda x: x.split(',')).map(lambda x: (x[1],x[0]))
    rdd_pid_2_outcoming = rdd_uid_2_pid.join(rdd_outcoming).map(lambda x: x[1])
    print rdd_pid_2_outcoming.take(5)
    pid_2_popularity = sc.textFile(C.PID_2_POPULARITY_FILE).map(lambda x: x.split(',')).mapValues(lambda x: float(x))
    data = rdd_pid_2_outcoming.join(pid_2_popularity).map(lambda x:x[1]).collect()
    data = zip(*data)
    plt.figure()
    '''
    fig, ax_arr = plt.subplots(1)
    ax_arr.plot(data[0],data[1])
    ax_arr.set_title("correlation between page_rank Score and Popularity")
    ax_arr.set_xlabel("page_rank_score")
    ax_arr.set_ylabel("popularity")
    '''
    plt.xlabel('out_coming')
    plt.ylabel('popularity')
    plt.scatter(data[0],data[1])
    plt.savefig(os.path.join('../Graph/', 'outcoming_popularity.png'))
    plt.close()

def born_date_hist():
    dates = sc.textFile(C.PID_2_DATE_FILE).map(lambda x: x.split(','))\
        .map(lambda x: NetworkHelpFunctions.date_2_value(x[1])).collect()
    plt.figure()
    '''
    fig, ax_arr = plt.subplots(1)
    ax_arr.plot(data[0],data[1])
    ax_arr.set_title("correlation between page_rank Score and Popularity")
    ax_arr.set_xlabel("page_rank_score")
    ax_arr.set_ylabel("popularity")
    '''
    plt.hist(dates)
    plt.savefig(os.path.join('../Graph/', 'creation_date_hist.png'))
    plt.close()


if __name__ == "__main__":
    #correlation_page_rank()
    #correlation_outcoming_popularity()
    #correlation_incoming_popularity()
    born_date_hist()
