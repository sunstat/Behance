import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors

from pyspark.ml.linalg import SparseVector, VectorUDT

class FieldExtract():
    def __init__(self, pid_2_fields_index_file, fields_2_index_file):
        self.pid_2_field_index_file = pid_2_fields_index_file
        self.fields_2_index_file = fields_2_index_file

    def build_feild_map(self, sc):
        rdd_pid_2_field_index = sc.textFile(self.pid_2_field_index_file)
        rdd_fields_2_index = sc.textFile(self.fields_2_index_file)

