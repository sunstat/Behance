from pyspark import SparkContext
from operator import add

from pyspark.sql import SQLContext
from pyspark.sql.types import *
import os
import sys
import re

local = False
csv = True

def f(x,y):
    return x+y


if local:
    input_file = "/Users/yimsun/PycharmProjects/Behance/TinyData/published_project_data.csv"
elif csv:
    behanceDataDir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
    input_file = os.path.join(behanceDataDir, "owners-csv")

if __name__ == "__main__":
    sc = SparkContext(appName="statistic")
    sc.setLogLevel("ERROR")
    lines = sc.textFile(input_file).map(lambda x: x.split(','))
    print lines.count()
    lines = lines.map(lambda x: (x[0], (x[1],1))).reduceByKey(lambda x,y: ((str(x[0])+","+str(y[0])),x[1]+y[1])).filter(lambda x:x[1][1]>1)
    print lines.count()
    sc.stop()



