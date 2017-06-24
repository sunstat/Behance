from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
from subprocess import Popen



class IOutilities(object):

    @staticmethod
    def to_string(x):
        return ",".join([str(y) for y in x])

    @staticmethod
    def init_spark_(self, name, max_excutors):
        conf = (SparkConf().setAppName(name)
                .set("spark.dynamicAllocation.enabled", "false")
                .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
        sc = SparkContext(conf=conf)
        sc.setLogLevel('ERROR')
        sqlContext = HiveContext(sc)
        return sc, sqlContext


    def __init__(self):
        sc, _ = IOutilities.init_spark_('io_example', 2)
        shell_dir = "../EditData/ShellEdit"

    @staticmethod
    def printDict(my_dict, num_elements):
        print('the size of the dict is {}'.format(len(my_dict)))
        print('now printing first {} key value pairs of dict passed in'.format(num_elements))
        count = 0
        for key, value in my_dict.items():
            print("key is {} and corresponding value is {}".format(key, value))
            count += 1
            if count > num_elements:
                break

    @staticmethod
    def printDicttoFile(my_dict, local_intermediate_dir, filename, azure_intermediate_dir = None):
        local_file = os.path.join(local_intermediate_dir, filename)
        f = open(os.path.join(local_intermediate_dir, filename), 'w')
        for key, value in my_dict.items():
            f.write("{},{}\n".format(key, value))
        if azure_intermediate_dir:
            IOutilities.sc.SparkContext(appName="transfering to Azure")
            rdd_dict = IOutilities.sc.textFile(local_file).map(lambda x: x.split(',')).cache()
            output_file = os.path.join(azure_intermediate_dir, filename)
            rdd_dict.map(IOutilities.to_string)..saveAsTextFile(output_file)
            #now delete the original file
            shell_file = os.path.join(IOutilities.shell_dir, 'delete.sh')
            Popen('./%s %s' % (shell_file, local_file,), shell=True)

    @staticmethod
    def close():
        IOutilities.sc.stop()





