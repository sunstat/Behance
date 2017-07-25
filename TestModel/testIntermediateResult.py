import os, sys

sys.path.append('/home/yiming/Behance')
sys.path.append('/home/yiming/Behance/configuration')
sys.path.append('/home/yiming/Behance/UserProjectNetwork')


from pyspark.mllib.linalg import SparseVector

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

'''
test popularity
'''

'''
15322919,87
38181025,4
42216791,10
34164251,116
'''

def testPopularity():

    rdd = sc.textFile(C.ACTION_FILE).map(lambda x: x.split(','))
    print 2*rdd.filter(lambda x: x[3] == '15322919' and x[4] == 'A').count()\
          +rdd.filter(lambda x: x[3] == '15322919' and x[4] == 'C').count()


'''
test pagerank
'''

'''
27019717,24.4312360806
38622569,24.4312360806
26998883,24.4312360806
26991685,24.4312360806
37192277,24.4312360806
35440053,24.4312360806
35446603,24.4312360806
37401579,0.176481458946
46419515,0.253337390282
42666453,0.253337390282
41448777,0.253337390282
33952470,0.253337390282
'''

# test pagerank makes sense or not

def test_page_rank():
    def separate(x):
        for y in x[1]:
            yield (x[0], y)

    rdd_popularity = sc.textFile(C.PID_2_POPULARITY_FILE).map(lambda x: x.split(','))
    rdd_score = sc.textFile(C.PID_2_SCORE_FILE).map(lambda x: x.spit(','))
    pid_2_uid_dict = sc.textFile(C.PID_2_UID_FILE).map(lambda x: x.split(',')).collectAsMap()
    rdd_follow = sc.textFile(C.FOLLOW_MAP_FILE).map(lambda x: x.split('#'))
    rdd_pair = rdd_follow.flatMap(lambda x: separate(x))
    print rdd_pair.take(5)
    uid1 = pid_2_uid_dict['27019717']
    uid2 = pid_2_uid_dict['38622569']
    uid3 = pid_2_uid_dict['46419515']

    print uid1 == uid2

    set1 = set(rdd_pair.map(lambda x: x[0]).distinct().collect())
    set2 = set(rdd_pair.map(lambda x: x[1]).distinct().collect())

    print len(set1) == len(set2)

    i1 = rdd_pair.filter(lambda x: x[1] == uid1).count()
    o1 = rdd_pair.filter(lambda x: x[0] == uid1).count()

    i3 = rdd_pair.filter(lambda x: x[1] == uid3).count()
    o3 = rdd_pair.filter(lambda x: x[0] == uid3).count()

    print "incoming degree of {}  is {} and outcoming is {}".format(uid1, i1, o1)
    print "incoming degree of {}  is {} and outcoming is {}".format(uid3, i3, o3)


if __name__ == "__main__":
    test_page_rank()







