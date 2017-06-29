from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
from subprocess import Popen

local = True


if local:
    action_file = "/Users/yimsun/PycharmProjects/Data/TinyData/action/actionDataTrimNoView-csv"
    owners_file = "/Users/yimsun/PycharmProjects/Data/TinyData/owners-csv"
    intermediate_result_dir = '../IntermediateDir'
else:
    behance_data_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
    action_file = os.path.join(behance_data_dir, "action", "actionDataTrimNoView-csv")
    owners_file = os.path.join(behance_data_dir, "owners-csv")


def calculate_popularity(x, comment_weight, appreciation_weight):
    num_comments = x[1][0]
    num_appreciations = x[1][1]
    if not num_comments:
        return x[0], appreciation_weight * num_appreciations
    elif not num_appreciations:
        return x[0], comment_weight * num_comments
    else:
        return x[0], appreciation_weight * num_appreciations + comment_weight * num_comments


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

rdd_pids = sc.textFile(action_file).map(lambda x: x.split(',')).map(lambda x: (x[3], x[4])).cache()
rdd_num_comments = rdd_pids.filter(lambda x: x[1] == 'C').groupByKey().mapValues(len)
rdd_num_appreciations = rdd_pids.filter(lambda x: x[1] == 'A').groupByKey().mapValues(len)
temp_left = rdd_num_comments.leftOuterJoin(rdd_num_appreciations)
temp_right = rdd_num_comments.rightOuterJoin(rdd_num_appreciations)
rdd_appreciations = temp_left.union(temp_right).distinct()
print(rdd_appreciations.take(10))

rdd_popularity = rdd_appreciations.map(lambda x: calculate_popularity(x, 1, 2))
print(rdd_popularity.take(5))