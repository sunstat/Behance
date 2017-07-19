from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
from subprocess import Popen
import re
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel




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

# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in re.split(r'\s+',line)]
    return LabeledPoint(values[0], values[1:])

data = sc.textFile("file:///home/yiming/Behance/IntermediateDir/2016-06-30/data")
parsedData = data.map(parsePoint)

print parsedData.take(5)

# Build the model
model = LinearRegressionWithSGD.train(parsedData, iterations=100, step=0.00000001)

# Evaluate the model on training data
valuesAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
MSE = valuesAndPreds \
    .map(lambda vp: (vp[0] - vp[1])**2) \
    .reduce(lambda x, y: x + y) / valuesAndPreds.count()
print("Mean Squared Error = " + str(MSE))

# Save and load model
model.save(sc, "target/temp/pythonLinearRegressionWithSGDModel")
sameModel = LinearRegressionModel.load(sc, "target/tmp/pythonLinearRegressionWithSGDModel")