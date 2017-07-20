import sys

sys.path.append('/home/yiming/Behance')
sys.path.append('/home/yiming/Behance/configuration')


import numpy as np
import os
from pyspark.mllib.linalg import Vectors

from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel


import configuration.constants as C

class Model():

    def __init__(self):
        self.training_pid_set = None
        self.valid_pid_set = None
        self.test_pid_set = None


    @staticmethod
    def __join_pair_rdds(rdd1, rdd2):
        def f(x):
            if isinstance(x[0], tuple):
                return x[0] + (x[1],)
            return (x[0], x[1])
        return rdd1.join(rdd2).mapValues(f)

    @staticmethod
    def __join_list_rdds(ls_rdds):
        rdd = ls_rdds[0]
        for i in range(1, len(ls_rdds)):
            rdd = Model.__join_pair_rdds(rdd, ls_rdds[i])
        return rdd

    def extract_data_rdd(self, sc, pid_set):
        '''
        :param sc:
        :param month_set:
        :return: rdd with column pid, fields(maybe empty), score, cur_popularity, next_popularity
        '''

        def __vec_2_int(vec):
            vec_result = []
            for y in vec:
                vec_result.append(int(y))
            return tuple(vec_result)

        def __vec_2_float(vec):
            vec_result = []
            for y in vec:
                vec_result.append(float(y))
            return tuple(vec_result)

        # build training rdd
        rdd_pid_2_field_index = sc.textFile(C.PID_2_FIELD_INDEX_FILE).map(lambda x: x.split('#'))\
            .map(lambda x: [x[0], tuple(x[1].split(','))]).mapValues(lambda x: __vec_2_int)
        rdd_pid_2_score = sc.textFile(C.PID_2_SCORE_FILE).map(lambda x: x.split(','))
        rdd_pid_2_view_feature = sc.textFile(C.PID_2_VIEWS_FEATURE_FILE).map(lambda x : x.split('#'))\
            .map(lambda x: [x[0], tuple(x[1].split(','))]).mapValues(lambda x: __vec_2_int)


            ls = []
            ls.append(rdd_pid_2_field_index)
            ls.append(rdd_score)
            ls.append(rdd_cur_popularity)
            ls.append(rdd_next_popularity)
            rdd_piece = Model.__join_list_rdds(ls)
            if not rdd_data:
                rdd_data = rdd_piece
            else:
                rdd_data = rdd_data.union(rdd_piece)
            print "================"
            print rdd_data.count()
            print rdd_data.take(5)
            print "================"
        return rdd_data

    '''
    rdd_ranks [pid, score] already split
    '''
    def generate_feature_response(self, sc, rdd_data):

        def sparse_label_points(vec, score, cur_popularity, next_popularity, N):
            feature = None
            if not vec:
                feature = SparseVector(N, [N-2, N-1], [score, cur_popularity])
            else:
                vec.append(N-2)
                vec.append(N-1)
                scores = [1.]*len(vec)
                scores.append(score)
                scores.append(cur_popularity)
                feature = SparseVector(N, vec, scores)
            return LabeledPoint(next_popularity, feature)
        rdd_field_2_index = sc.textFile(C.FIELD_2_INDEX)
        N = rdd_field_2_index.count()
        '''
        vec, score, historical_popularity, popularity
        '''
        rdd_label_data =  rdd_data.map(lambda x: sparse_label_points(x[1][0], x[1][1], x[1][2], x[1][3], N)
        return rdd_label_data

    def train_model(self)):
        model = LinearRegressionWithSGD.train(parsedData, iterations=100, step=0.00000001)



