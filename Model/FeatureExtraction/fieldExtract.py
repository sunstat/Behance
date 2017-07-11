import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors, LabeledPoint

from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint


class FieldExtract():
    def __init__(self, pid_2_fields_index_file, field_2_index_file):
        self.pid_2_field_index_file = pid_2_fields_index_file
        self.field_2_index_file = field_2_index_file

    '''
    rdd_ranks [pid, score]
    '''
    def build_data(self, sc, rdd_ranks, rdd_history_popularity, rdd_popularity):

        def sparse_label_points(vec, score, historical_popularity, popularity, N):
            feature = None
            if not vec:
                feature = SparseVector(N, [N-2, N-1], [score, historical_popularity])
            else:
                vec.append(N-2)
                vec.append(N-1)
                scores = [1.]*len(vec)
                scores.append(score)
                scores.append(historical_popularity)
                feature = SparseVector(N, vec, scores)
            return LabeledPoint(popularity, feature)

        rdd_pid_2_field_index = sc.textFile(self.pid_2_field_index_file)
        rdd_field_2_index = sc.textFile(self.field_2_index_file)
        rdd_data = rdd_pid_2_field_index.join(rdd_ranks)
        rdd_data = rdd_data.join(rdd_history_popularity).mapValues(lambda x: x[0] + (x[1], ))
        rdd_data = rdd_data.join(rdd_popularity).mapValues(lambda x: x[0] + (x[1], ))
        N = rdd_field_2_index.count()+2

        '''
        vec, score, historical_popularity, popularity
        '''
        rdd_label_data =  sparse_label_points(rdd_data[1][0], rdd_data[1][1], rdd_data[1][2], rdd_data[1][3], N)



