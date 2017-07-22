import os, sys

sys.path.append('/home/yiming/Behance')
sys.path.append('/home/yiming/Behance/configuration')
sys.path.append('/home/yiming/Behance/UserProjectNetwork')

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType
import operator
from scipy.sparse import coo_matrix, csr_matrix
from subprocess import Popen
import re
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
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




sc,_ = init_spark('human', 10)

sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkHelpFunctions.py')
sc.addFile('/home/yiming/Behance/UserProjectNetwork/IOutilities.py')
sc.addFile('/home/yiming/Behance/configuration/constants.py')


rdd_pid_2_score = sc.textFile(C.PID_2_SCORE_FILE)
rdd_pid_2_uid = sc.textFile(C.PID_2_UID_FILE)

rdd = rdd_pid_2_uid.map(lambda x: x.split(',')).filter(lambda x: x[0] == '32929425')
print rdd.take(5)

rdd = rdd_pid_2_uid.map(lambda x: x.split(',')).filter(lambda x: x[0] == '32782397')
print rdd.take(5)
