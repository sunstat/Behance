from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
from subprocess import Popen


def __join_pair_rdds(rdd1, rdd2):
    def f(x):
        if isinstance(x[0], tuple) and isinstance(x[1], tuple):
            return x[0] + x[1]
        elif isinstance(x[0], tuple) and (not isinstance(x[1], tuple)):
            return x[0] + (x[1], )
        else:
            return (x[0],)+(x[1],)
    return rdd1.join(rdd2).mapValues(f)

def __join_list_rdds(ls_rdds):
    rdd = ls_rdds[0]
    print rdd.take(5)
    for i in range(1, len(ls_rdds)):
        rdd = __join_pair_rdds(rdd, ls_rdds[i])
        print rdd.take(5)
    return rdd



def init_spark(name, max_excutors):
    conf = (SparkConf().setAppName(name)
            .set("spark.dynamicAllocation.enabled", "false")
            .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel('ERROR')
    sqlContext = HiveContext(sc)
    return sc, sqlContext

sc, sqlContext = init_spark('olivia', 20)
x = sc.parallelize([["a", 1], ["a", 2]])
y = sc.parallelize([("a", 2), ["a", 5]])
z = sc.parallelize([("a", 3), ["f", 6]])

