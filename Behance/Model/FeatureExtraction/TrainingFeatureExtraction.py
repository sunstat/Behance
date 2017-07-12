import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors, LabeledPoint

from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint


class TrainingFeatureExtraction():
    def __init__(self, pid_2_fields_index_file, field_2_index_file,
                 base_date, new_date):
        self.pid_2_field_index_file = pid_2_fields_index_file
        self.field_2_index_file = field_2_index_file
        self.base_date = base_date
        self.new_date = new_date


     def extract_features(self, sc):
        rdd_pid_2_field_index = sc.textFile(self.pid_2_field_index_file).map(lambda x: x.split("#"))\
            .map(lambda x: [x(0),tuple(x[1].split(','))])
        rdd_field_2_index = sc.textFile(self.rdd_field_2_index).map(lambda x: x.split(','))
        rdd_ranks = sc.textFile(self.pid_ranks_file).map(lambda x: x.split(','))
        rdd_base_popularity = sc.textFile(self.rdd_historic_popularity_file).map(lambda x: x.split(','))
        rdd_popularity = sc.textFile(self.rdd_historic_popularity_file).map(lambda x: x.split(','))
        rdd_label_data = self.build_data(rdd_pid_2_field_index, rdd_field_2_index, rdd_ranks, rdd_historic_popularity, rdd_popularity)
        return rdd_label_data

    '''
    rdd_ranks [pid, score] already split
    '''
    def build_data(self, rdd_pid_2_field_index, rdd_field_2_index, rdd_ranks, rdd_historic_popularity, rdd_increment_popularity):

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

        rdd_data = rdd_pid_2_field_index.join(rdd_ranks)
        rdd_data = rdd_data.join(rdd_historic_popularity).mapValues(lambda x: x[0] + (x[1], ))
        rdd_data = rdd_data.join(rdd_increment_popularity).mapValues(lambda x: x[0] + (x[1], ))
        N = rdd_field_2_index.count()+2
        '''
        vec, score, historical_popularity, popularity
        '''
        rdd_label_data =  sparse_label_points(rdd_data[1][0], rdd_data[1][1], rdd_data[1][2], rdd_data[1][3], N)
        return rdd_label_data



