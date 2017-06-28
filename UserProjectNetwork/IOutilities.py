from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
from subprocess import Popen



class IOutilities(object):
    '''
    static memebers
    '''
    shell_dir = "../EditData/ShellEdit"


    @staticmethod
    def to_string(x):
        return ",".join([str(y) for y in x])

    @staticmethod
    def __init_spark(name, max_excutors):
        conf = (SparkConf().setAppName(name)
                .set("spark.dynamicAllocation.enabled", "false")
                .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
        sc = SparkContext(conf=conf)
        sc.setLogLevel('ERROR')
        sqlContext = HiveContext(sc)
        return sc, sqlContext

    def __init__(self):
        pass

    @staticmethod
    def print_dict(my_dict, num_elements):
        print('the size of the dict is {}'.format(len(my_dict)))
        print('now printing first {} key value pairs of dict passed in'.format(num_elements))
        count = 0
        for key, value in my_dict.items():
            print("key is {} and corresponding value is {}".format(key, value))
            count += 1
            if count > num_elements:
                break

    @staticmethod
    def print_dict_to_file_help(my_dict, filename):
        f = open(filename, 'w')
        for key, value in my_dict.items():
            f.write("{}".format(key))
            if isinstance(value, list):
                f.write("\t")
                ls = []
                for value_item in value:
                    ls.append(value_item)
                f.write(','.join(ls))
            else:
                f.write(",{}".format(value))
            f.write("\n")

    @staticmethod
    def print_dict_to_file(my_dict, local_intermediate_dir, filename, azure_intermediate_dir = None):
        local_file = os.path.join(local_intermediate_dir, filename)
        delete_shell_azure = os.path.join(IOutilities.shell_dir, 'delete.sh')
        delete_shell_local = os.path.join(IOutilities.shell_dir, 'deleteLocal.sh')
        if os.path.exists(local_file):
            Popen('./%s %s' % (delete_shell_local, local_file,), shell=True)
        IOutilities.print_dict_to_file_help(my_dict, local_file)
        if azure_intermediate_dir:
            sc, _ = IOutilities.init_spark_('io_example', 2)
            output_file = os.path.join(azure_intermediate_dir, filename)
            if os.path.exists(output_file):
                Popen('./%s %s' % (delete_shell_azure, output_file,), shell=True)
            sc.SparkContext(appName="transfering to Azure")
            rdd_dict = sc.textFile(local_file).map(lambda x: x.split(',')).cache()
            rdd_dict.map(IOutilities.to_string).saveAsTextFile(output_file)
            #now delete the original file
            Popen('./%s %s' % (delete_shell_local, local_file,), shell=True)
            sc.stop()

    @staticmethod
    def print_rdd_to_file(rdd, output_file, output_format):
        delete_shell_azure = os.path.join(IOutilities.shell_dir, 'delete.sh')
        sc, _ = IOutilities.__init_spark('io_example', 2)
        if os.path.exists(output_file):
            Popen('./%s %s' % (delete_shell_azure, output_file,), shell=True)
        if output_format == 'csv':
            rdd.map(lambda x: ",".join(x)).saveAsTextFile(output_file)
        elif output_format == 'tsv':
            rdd.map(lambda x: x[0]+"\t"+(",".join(x[1]))).saveAsTextFile(output_file)


