#!/usr/bin/env python
# extract all user information from owners and actions:

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType
import os, sys
import operator


class NetworkUtilities(object):
    '''
    methods used only within in this class
    '''

    def init_spark_(self, name, max_excutors):
        conf = (SparkConf().setAppName(name)
                .set("spark.dynamicAllocation.enabled", "false")
                .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
        sc = SparkContext(conf=conf)
        sc.setLogLevel('ERROR')
        sqlContext = HiveContext(sc)
        return sc, sqlContext

    def extract_parameters_(self):
        arguments_arr = []
        with open(self.config_file, 'r') as f:
            for line in f:
                arguments_arr.append(line.strip())
        return arguments_arr

    # compare two date strings "2016-12-01"
    def date_compare_(self, date1, date2):
        date1_arr = date1.split("-")
        date2_arr = date2.split("-")
        for i in range(len(date1_arr)):
            if date1_arr[i] < date2_arr[i]:
                return True
            elif date1_arr[i] > date2_arr[i]:
                return False
        return True

    def date_filter_(self, date, start_date, end_date):
        return self.date_compare_(start_date, date) and self.date_compare_(date, end_date)

    def __init__(self, action_file, owner_file, program_name, max_executors, config_file):
        self.action_file = action_file
        self.owner_file = owner_file
        self.sc, self.sqlContext = self.init_spark_(program_name, max_executors)
        self.config_file = config_file

    def extract_neighbors_from_users_network(self, end_date):
        def date_filter(x):
            return self.date_filter_(x[0], "0000-00-00", end_date)

        rdd = self.sc.textFile(self.action_file).map(lambda x: x.split(',')).filter(lambda x: date_filter(x)).filter(lambda x:x[4] == 'F')\
            .map(lambda x: (x[1], [x[2]])).reduceByKey(lambda a,b : a+b).cache()
        '''
        print (rdd.take(5))
        '''
        follow_map = rdd.collectAsMap()
        uid_set = set()
        count=0
        for key, value in followMap.items():
            count+=1
            if count<5:
                print('key is {} and corresponding value is {}'.format(key, value))
            uid_set.add(key)
            uid_set |= set(value)
        return follow_map, uid_set

    def handle_uid_pid(self, end_date, uid_set):
        '''
        help functions
        '''
        def date_filter(x):
            return self.date_filter_(x[2], "2015-12-31", end_date)

        def map_field_to_count_(x):
            global count
            count += 1
            return (x, count)

        def filter_uid_inCycle_(x):
            return x[2] in uid_set_broad

        count = self.sc.accumulator(1)

        uid_set_broad = self.sc.broadcast(uid_set)

        '''
        build field map 
        '''

        rdd_owners = self.sc.textFile(self.owners_file).map(lambda x: x.split(',')).filter(lambda x: date_filter(x)).cache()

        '''
        rdd_owners = sc.textFile(owners_file).map(lambda x:x.split(',')).filter(lambda x: date_filter_(x))\
            .filter(filterUidInCycle_).cache()
        '''

        print(rdd_owners.take(5))

        fields_map = rdd_owners.flatMap(lambda x: (x[3], x[4], x[5])).map(map_field_to_count_).collectAsMap()

        """
        Pid Uid pair in owner file
        """

        owners_map = rdd_owners.map(lambda x: (x[0], x[1])).collectAsMap()

    def close_utilities(self):
        self.sc.stop()

