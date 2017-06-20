from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import os 
import sys
import re
from subprocess import Popen
          
local =True
csv=True

             
if local:
    input_file = "../../../TinyData/action/published_project_data.csv"
    output_file = "../../../TinyData/owners.csv"
elif csv:
    behanceDataDir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
    input_file = os.path.join(behanceDataDir, "published_project_data.csv") 
    output_file = os.path.join(behanceDataDir, "owners-csv")
    deleteShellFile = "../delete.sh"



def truncateTime(x):
	temp = re.split("\s+",x[2]) 
	x[2] = temp[0] 
	return x

def toString(x):
	return ",".join([str(y) for y in x])


if __name__ == "__main__":
    sc = SparkContext(appName="deleteRow")
    sc.setLogLevel("ERROR")
    lines = sc.textFile(input_file).map(lambda x: x.split(',')).map(lambda x : truncateTime(x)) 
    print lines.take(5)
    Process = Popen('./%s %s' %(deleteShellFile, output_file, ), shell=True)
    lines.map(lambda x: toString(x)).saveAsTextFile(output_file)
    sc.stop() 
	



