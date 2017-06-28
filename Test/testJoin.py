from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
from subprocess import Popen


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
x = sc.parallelize([("a", 1), ("b", 4)])
y = sc.parallelize([("a", 2), ["d", 5]])


x1 = x.leftOuterJoin(y)
x2 = x.rightOuterJoin(y)
print(x1.collect())
print(x2.collect())
x3 = x1.union(x2)
x3 = x3.map(lambda x: (x[0],x[1][0]))
x3 = x3.map(lambda x: [x[0],x[1]])
print(x3.collect())

x4 = x3.reduceByKey(lambda x,y : x+y)
print(x4.collect())