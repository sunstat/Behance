#!/usr/bin/env python
# extract all user information from owners and actions:

import sys
import re
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
import numpy as np
from IOutilities import IOutilities

import configuration.constants as C



def init_spark(name, max_excutors):
    conf = (SparkConf().setAppName(name)
            .set("spark.dynamicAllocation.enabled", "false")
            .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    sqlContext = HiveContext(sc)
    return sc, sqlContext

class PageRank():
    def __init__(self, num_iters):
        self.num_iters = num_iters


    def run(self, sc):

        dif_array = []
        def compute_contribs(urls, rank):
            # Calculates URL contributions to the rank of other URLs.
            num_urls = len(urls)
            '''
            if len(urls) == 0:
                yield (source_url, 0)
            '''
            for url in urls:
                yield (url, rank / num_urls)

        ranks = sc.textFile(C.UID_2_INDEX_FILE).map(lambda x: x.split(',')).map(lambda x: (x[0], 1.)).cache()
        prev_ranks = ranks
        print ranks.count()
        links = sc.textFile(C.FOLLOW_MAP_FILE).map(lambda x: re.split('#', x))\
            .map(lambda x: (x[0], x[1].split(','))).cache()
        pid_2_uid = sc.textFile(C.PID_2_UID_FILE).map(lambda x: x.split(','))
        print pid_2_uid.take(5)

        for iteration in range(1, self.num_iters+1):
            # Calculates URL contributions to the rank of other URLs.
            contribs = links.join(ranks, 8)
            contribs = contribs.flatMap(lambda x: compute_contribs(x[1][0], x[1][1]))
            # Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(lambda x, y: x+y).mapValues(lambda x: x * 0.85 + 0.15)

            if iteration%10 == 0:
                temp_file = os.path.join(C.TEMPORARY_DIR, 'ranks-csv')
                print temp_file
                IOutilities.print_rdd_to_file(ranks, temp_file, 'csv')
                ranks = sc.textFile(temp_file).map(lambda x: x.split(',')).mapValues(lambda x: float(x))
                dif = ranks.join(prev_ranks).mapValues(lambda x: abs(x[0]-x[1])).map(lambda x: x[1]).reduce(lambda x,y: x+y)
                print "iteration : {} and the difference is {}".format(iteration, dif)
                prev_ranks = ranks
                dif_array.append(dif)

        print "finishing iterative algorithm"
        print ranks.take(5)
        uid_2_pid = pid_2_uid.map(lambda x: (x[1], x[0]))
        print uid_2_pid.take(5)
        pid_2_score = uid_2_pid.join(ranks).map(lambda x: (x[1][0], x[1][1])).cache()
        print pid_2_score.take(5)
        IOutilities.print_rdd_to_file(pid_2_score, C.PID_2_SCORE_FILE, 'csv')
        #import dif_array into Log
        log_file = open(os.path.join(C.MODEL_LOG_DIR, 'page_rank_log'), 'w')
        for item in dif_array:
            log_file.write("%s\n" % item)
        

if __name__ == "__main__":
    sc, _ = init_spark('pageRank', 30)
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkHelpFunctions.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/IOutilities.py')
    sc.addFile('/home/yiming/Behance/configuration/constants.py')
    sc.addFile('/home/yiming/Behance/UserProjectNetwork/pageRank.py')
    page_rank = PageRank(1000)
    page_rank.run(sc)



    '''
    test 
    '''
    print sc.textFile(C.PID_2_SCORE_FILE).count()
    print sc.textFile(C.PID_2_INDEX_FILE).count()

    sc.stop()







