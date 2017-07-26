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


# test pagerank makes sense or not

def test_page_rank():
    def separate(x):
        for y in x[1]:
            yield (x[0], y)

    rdd_popularity = sc.textFile(C.PID_2_POPULARITY_FILE).map(lambda x: x.split(','))
    rdd_score = sc.textFile(C.PID_2_SCORE_FILE).map(lambda x: x.spit(','))
    rdd_follow = sc.textFile(C.FOLLOW_MAP_FILE).map(lambda x: x.split('#')).mapValues(lambda x: x.split(','))

    pid_2_uid_dict = sc.textFile(C.PID_2_UID_FILE).map(lambda x: x.split(',')).collectAsMap()
    uid_set = set(sc.textFile(C.UID_2_INDEX_FILE).map(lambda x: x.split(',')).map(lambda x: x[0]).collect())
    uid_set_broad = sc.broadcast(uid_set)
    print len(uid_set)

    '''
    35145491,0.235518550181
    33407727,9.44813133033
    35699141,9.44813133033
    34978429,9.44813133033
    '''
    print rdd_follow.take(5)
    rdd_pair = rdd_follow.flatMap(lambda x: separate(x))
    print rdd_pair.take(5)
    uid1 = pid_2_uid_dict['35145491']
    uid2 = pid_2_uid_dict['33407727']
    uid3 = pid_2_uid_dict['34978429']

    print uid2 == uid3

    set1 = set(rdd_pair.map(lambda x: x[0]).distinct().collect())
    set2 = set(rdd_pair.map(lambda x: x[1]).distinct().collect())

    print set1 == set2

    i1 = rdd_pair.filter(lambda x: x[1] == uid1).count()
    o1 = rdd_pair.filter(lambda x: x[0] == uid1).count()

    i3 = rdd_pair.filter(lambda x: x[1] == uid3).count()
    o3 = rdd_pair.filter(lambda x: x[0] == uid3).count()

    print "incoming degree of {}  is {} and outcoming is {}".format(uid1, i1, o1)
    print "incoming degree of {}  is {} and outcoming is {}".format(uid3, i3, o3)

def test_view_feature():
    '''
    (u'42982845', [71, 2, 0, 0, 0, 0, 0, 0, 0, 0, 2, 5, 5, 1]), (
    u'33192963', [307, 2, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]
    '''
    rdd = sc.textFile(C.OWNER_FILE).map(lambda x: x.split(','))
    print rdd.filter(lambda x: x[0] == '42982845').take(5)
    print rdd.filter(lambda x: x[0] == '33192963').take(5)


if __name__ == "__main__":
    test_page_rank()
    #test_view_feature()
    sc.stop()







