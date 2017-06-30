from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType
import os, sys
from pageRank import PageRank

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

sc.addFile('/home/yiming/Behance/Model/FeatureExtraction/pageRank.py')

intermediate_result_dir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/IntermediateResult"
cur_date = "2016-06-30"
follow_file = os.path.join(intermediate_result_dir,cur_date, 'follow_map-csv')
uid_2_index_file = os.path.join(intermediate_result_dir,cur_date, 'uid_2_index-csv')

num_iters = 10
pageRank = PageRank(follow_file, uid_2_index_file, num_iters)

pageRank.run(sc)

sc.stop()

