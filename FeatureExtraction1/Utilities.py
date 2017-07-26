import sys

sys.path.append('/home/yiming/Behance')
sys.path.append('/home/yiming/Behance/configuration')
#sys.path.append('/home/yiming/Behance/configuration/constants.py')

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
from subprocess import Popen
from subprocess import call
from subprocess import check_call
import configuration.constants as C

import time


class Utilities(object):
    '''
    static memebers
    '''

    def __init__(self):
        pass

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


