#!/usr/bin/env python
# extract all user information from owners and actions:

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType
import os, sys
import operator



'''
global directory 
'''
behanceDataDir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
action_file = os.path.join(behanceDataDir, "action", "actionDataTrimNoView-csv")
owners_file = os.path.join(behanceDataDir, "owners-csv")



from NetworkUtilities import  NetworkUtilities


if __name__ == "__main__":
    utilities = NetworkUtilities(action_file, owners_file, 'user_project_network', 40, 'config')
    followMap, uidSet = utilities.extract_neighbors_from_users_network()

    utilities.close_utilities()