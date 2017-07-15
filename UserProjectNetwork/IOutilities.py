from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
from subprocess import Popen
from subprocess import check_call

import time


class IOutilities(object):
    '''
    static memebers
    '''
    shell_dir = "../EditData/ShellEdit"

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
    def print_dict_to_file_help(sc, my_dict, filename):
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
    def print_dict_to_file(sc, my_dict, local_intermediate_dir, filename, azure_intermediate_dir=None):
        local_file = os.path.join(local_intermediate_dir, filename)
        delete_shell_azure = os.path.join(IOutilities.shell_dir, 'delete.sh')
        delete_shell_local = os.path.join(IOutilities.shell_dir, 'deleteLocal.sh')
        if os.path.exists(local_file):
            Popen('./%s %s' % (delete_shell_local, local_file,), shell=True)
        IOutilities.print_dict_to_file_help(my_dict, local_file)
        if azure_intermediate_dir:
            output_file = os.path.join(azure_intermediate_dir, filename)
            if os.path.exists(output_file):
                Popen('./%s %s' % (delete_shell_azure, output_file,), shell=True)
            sc.SparkContext(appName="transfering to Azure")
            rdd_dict = sc.textFile(local_file).map(lambda x: x.split(',')).cache()
            rdd_dict.map(IOutilities.to_string).saveAsTextFile(output_file)
            # now delete the original file
            Popen('./%s %s' % (delete_shell_local, local_file,), shell=True)
            sc.stop()

    @staticmethod
    def print_rdd_to_file(rdd, output_file, output_format):
        def to_string(x):
            if not x:
                return " "
            return ",".join([str(y) for y in x])

        delete_shell_azure = os.path.join(IOutilities.shell_dir, 'delete.sh')
        args = ["/bin/bash"]
        args.append(delete_shell_azure)
        args.append(output_file)
        Popen(args)
        if os.system("hadoop fs -test -d {0}".format(output_file)) == 0:
            raise Exception("Folder already exists!!")

        # time.sleep(1)
        if output_format == 'csv':
            rdd.map(lambda x: to_string(x)).saveAsTextFile(output_file)
        elif output_format == 'psv':
            rdd.map(lambda x: str(x[0]) + "#" + (to_string(x[1]))).saveAsTextFile(output_file)
