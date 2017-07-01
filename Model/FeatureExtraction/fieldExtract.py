import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors


class FieldExtract():
    def __init__(self, pid_2_index_file, fields_2_index_file):
        self.pid_2_index_file = pid_2_index_file
        self.fields_2_index_file = fields_2_index_file

    def