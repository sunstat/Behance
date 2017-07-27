#!/usr/bin/env python
# extract all user information from owners and actions:

import sys

sys.path.append('/home/yiming/Behance')
sys.path.append('/home/yiming/Behance/configuration')
sys.path.append('/home/yiming/Behance/FeatureExtraction1')

# sys.path.append('/home/yiming/Behance/configuration/constants.py')
import constants as C

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
from datetime import *
import operator
from subprocess import call


class Utilities(object):
    '''
    static memebers
    '''

    def __init__(self):
        pass

    @staticmethod
    def _join_pair_rdds(rdd1, rdd2):
        rdd = rdd1.join(rdd2)

        def f(x):
            if isinstance(x[0], tuple) and isinstance(x[1], tuple):
                return x[0] + x[1]
            elif isinstance(x[0], tuple) and (not isinstance(x[1], tuple)):
                return x[0] + (x[1],)
            else:
                return (x[0],) + (x[1],)

        return rdd.mapValues(f)

    @staticmethod
    def _join_list_rdds(ls_rdds):
        rdd = ls_rdds[0]
        for i in range(1, len(ls_rdds)):
            rdd = Utilities._join_pair_rdds(rdd, ls_rdds[i])
        return rdd

    @staticmethod
    def print_rdd_to_file(rdd, output_file, output_format):
        def to_string(x):
            if not x:
                return " "
            return ",".join([str(y) for y in x])

        delete_shell_azure = os.path.join(C.SHELL_DIR, 'delete.sh')
        if os.system("hadoop fs -test -d {0}".format(output_file)) == 0:
            call('{} {} {}'.format("/usr/bin/env bash", delete_shell_azure, output_file), shell=True)

        # time.sleep(1)
        if output_format == 'csv':
            rdd.map(lambda x: to_string(x)).saveAsTextFile(output_file)
        elif output_format == 'psv':
            rdd.map(lambda x: str(x[0]) + "#" + (to_string(x[1]))).saveAsTextFile(output_file)

    @staticmethod
    def string_2_date(my_date):
        year, month, day = my_date.split('-')
        return date(int(year), int(month), int(day))

    # list of tuples of (pid, date)
    def extract_last_date(ls):
        ls_sorted = sorted(ls, key=lambda x: x[1])
        pids, _ = zip(*ls_sorted)
        return pids



