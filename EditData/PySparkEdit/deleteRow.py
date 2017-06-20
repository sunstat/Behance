#from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import os
import sys

local =False
csv=True


if local:
    input_file = "../TinyData/action/action_data_20161.csv000"
else:
    behanceDataDir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
    input_file = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data/action/actionDataTrim-csv"
    output_file= "wasb://testing@adobedatascience.blob.core.windows.net/behance/data/action/actionDataTrimNoView-csv"
    parquetFile = ""

def toString(x): 
	return ",".join([str(y) for y in x])

if __name__ == "__main__":

    sc = SparkContext(appName="deleteRow")
    sc.setLogLevel("ERROR")

    lines = sc.textFile(input_file).map(lambda x: x.split(',')).filter(lambda x : x[4]!='V')  
 	
    print lines.count()    
    lines.map(lambda x: toString(x)).saveAsTextFile(output_file)  
    sc.stop()

