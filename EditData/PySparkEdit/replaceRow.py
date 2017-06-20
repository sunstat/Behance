from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import os


local = False
csv = True

if local:
    input_file = "../TinyData/action/action_data_20161.csv000"
elif csv:
    behanceDataDir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
    input_file = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data/action/action_data.csv"
    output_file  = os.path.join(behanceDataDir, "action", "actionDataTrim-csv") 
    print output_file
    parquetFile = ""




# pySpark Version of cut 
def cutCols(x, remainingCols):
	return [x[i] for i in remainingCols]

# pySpark Version of sed
def replaceCol(x, scr_str, des_str, col_ind):
    x[col_ind] = x[col_ind].replace(scr_str, des_str)
    return x

def toString(x):
    x_tostring = str(x[0])
    for i in range(len(x)-1):
		x_tostring = x_tostring+","+str(x[i+1])
    return x_tostring

if  __name__ == "__main__":
    sc = SparkContext(appName="replaceRow")
    sc.setLogLevel("ERROR")

    lines = sc.textFile(input_file).map(lambda x: x.split(',')).map(lambda x: replaceCol(x, "ProjectView", "V", 6))\
	    .map(lambda x: replaceCol(x, "Appreciate", "A", 6)).map(lambda x: replaceCol(x, "FollowUser", "F", 6))\
	    .map(lambda x: replaceCol(x, "ProjectComment", "C", 6)).map(lambda x : cutCols(x,[0,2,3,4,6]))
    #print lines.count()
	#lines.cache()
    print lines.take(5)
    
    lines.map(lambda x: toString(x)).saveAsTextFile(output_file)
    
	# counts = lines.map(lambda x: x.split(',')) 
    sc.stop()   


