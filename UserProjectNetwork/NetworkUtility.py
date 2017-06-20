#!/usr/bin/env python
# extract all user information from owners and actions:

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType
import os, sys
import operator

sys.path.insert(1, os.path.join(sys.path[0], '..'))
run_local  = False



if run_local:
    action_file = "/Users/yimsun/PycharmProjects/Behance/TinyData/action/actionDataTrimNoView-csv"
    owners_file = "/Users/yimsun/PycharmProjects/Behance/TinyData/owners-csv"
else:
    behanceDataDir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
    action_file = os.path.join(behanceDataDir, "action", "actionDataTrimNoView-csv")
    owners_file = os.path.join(behanceDataDir, "owners-csv")


'''
help functions
'''

def init_spark(name, max_excutors):
    conf = (SparkConf().setAppName(name)
            .set("spark.dynamicAllocation.enabled", "false")
            .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    sqlContext = HiveContext(sc)
    return sc, sqlContext



def extractParametersFromConfig():
    arguments_arr = []
    with open('config', 'r') as f:
        for line in f:
            arguments_arr.append(line.strip())
    return arguments_arr



#compare two date strings "2016-12-01"
def dateCompare(date1, date2):
    date1_arr = date1.split("-")
    date2_arr = date2.split("-")
    for i in range(len(date1_arr)):
        if(date1_arr[i] < date2_arr[i]):
            return True
        elif(date1_arr[i]>date2_arr[i]):
            return False
    return True


def dateFilter(date, start_date, end_date):
    return dateCompare(start_date, date) and dateCompare(date, end_date)

'''
end of help functions 
'''





'''
extract users
'''
def extractNeighborsFromUsersNetwork(sc, end_date):
    def date_filter_(x):
        return dateFilter(x[0], "0000-00-00", end_date)

    rdd = sc.textFile(action_file).map(lambda x:x.split(',')).filter(lambda x : date_filter_(x)).filter(lambda x:x[4] == 'F')\
        .map(lambda x: (x[1], [x[2]])).reduceByKey(lambda a,b : a+b).cache()

    print (rdd.take(5))
    '''
    followMap = rdd.collectAsMap()
    uidSet = set()
    count = 0
    for key, value in followMap.items():
        count+=1
        if count<5:
            print('key is {} and corresponding value is {}'.format(key, value))
        uidSet.add(key)
        uidSet |= set(value)

    
    return followMap, uidSet
    '''

'''
build fields map 
'''

def handleUidPid(sc, end_date, uidSet):
    '''
    help functions
    '''
    def date_filter_(x):
        return dateFilter(x[2], "2015-12-31", end_date)


    def mapFromFieldToCount_(x):
        global count
        count +=1
        return (x, count)


    def filterUidInCycle_(x):
        return x[2] in uidSet_broad


    count = sc.accumulator(1)

    uidSet_broad = sc.broadcast(uidSet)


    '''
    build field map 
    '''

    rdd_owners = sc.textFile(owners_file).map(lambda x: x.split(',')).filter(lambda x: date_filter_(x)).cache()

    '''
    rdd_owners = sc.textFile(owners_file).map(lambda x:x.split(',')).filter(lambda x: date_filter_(x))\
        .filter(filterUidInCycle_).cache()
    '''

    print(rdd_owners.take(5))


    fields_map = rdd_owners.flatMap(lambda x: (x[3],x[4],x[5])).map(mapFromFieldToCount_).collectAsMap()


    """
    Pid Uid pair in owner file
    """

    owners_map = rdd_owners.map(lambda x: (x[0],x[1])).collectAsMap()





### test
#print dateCompare("2017-01-09", "2016-10-01")


if __name__ == "__main__":
    arguements_arr = extractParametersFromConfig()
    end_day = arguements_arr[0]
    print end_day
    sc, sqlContex = init_spark('userID', 40)
    followMap, uidSet = extractNeighborsFromUsersNetwork(sc, end_day)
    handleUidPid(sc, end_day, uidSet)

    sc.stop()


    '''
    sc, sqlContext = init_spark("generate_score_summary", 40)
    data = sc.textFile(input_file).map(lambda x: x.split(',')).map(lambda x: x[4] == 'F')
    data.cache()
    '''



