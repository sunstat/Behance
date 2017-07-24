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

sc.addFile('/home/yiming/Behance/UserProjectNetwork/NetworkHelpFunctions.py')
sc.addFile('/home/yiming/Behance/UserProjectNetwork/IOutilities.py')
sc.addFile('/home/yiming/Behance/configuration/constants.py')

# Load and parse the data
sparse_data = [
    LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
    LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
    LabeledPoint(0.0, SparseVector(2, {0: 1.0})),
    LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
]


sparse_data = sc.parallelize(sparse_data)

model = LinearRegressionWithSGD.train(sparse_data, iterations=100, step=0.00000001, intercept=True)

# Evaluate the model on training data
valuesAndPreds = sparse_data.map(lambda p: (p.label, model.predict(p.features)))
MSE = valuesAndPreds \
    .map(lambda vp: (vp[0] - vp[1])**2) \
    .reduce(lambda x, y: x + y) / valuesAndPreds.count()
print("Mean Squared Error = " + str(MSE))

# Save and load model

print model.weights
print model.intercept

model = LinearRegressionWithSGD.train(sparse_data, iterations=100, step=0.00000001, intercept=True, initialWeights=model.weights)


MSE = valuesAndPreds \
    .map(lambda vp: (vp[0] - vp[1])**2) \
    .reduce(lambda x, y: x + y) / valuesAndPreds.count()
print("Mean Squared Error = " + str(MSE))
sc.stop()