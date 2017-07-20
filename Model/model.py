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
        :return: rdd with column pid, fields(maybe empty), view_feature, page_rank score, popularity
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

        pid_set_broad = sc.broadcast(pid_set)

        # build training rdd
        rdd_pid_2_field_index = sc.textFile(C.PID_2_FIELD_INDEX_FILE).map(lambda x: x.split('#'))\
            .filter(lambda x: x[0] in pid_set_broad.value)\
            .map(lambda x: [x[0], tuple(x[1].split(','))]).mapValues(__vec_2_int)

        rdd_pid_2_view_feature = sc.textFile(C.PID_2_VIEWS_FEATURE_FILE).map(lambda x : x.split('#')) \
            .filter(lambda x: x[0] in pid_set_broad.value)\
            .map(lambda x: [x[0], tuple(x[1].split(','))]).mapValues(__vec_2_float)

        rdd_pid_2_score = sc.textFile(C.PID_2_SCORE_FILE).map(lambda x: x.split(',')) \
            .filter(lambda x: x[0] in pid_set_broad.value).mapValues(__vec_2_float)

        rdd_pid_2_popularity = sc.textFile(C.PID_2_POPULARITY_FILE).map(lambda x: x.split(','))\
            .filter(lambda x: x[0] in pid_set_broad.value).mapValues(__vec_2_float)

        ls = []
        ls.append(rdd_pid_2_field_index)
        ls.append(rdd_pid_2_view_feature)
        ls.append(rdd_pid_2_score)
        ls.append(rdd_pid_2_popularity)
        rdd_data = Model.__join_list_rdds(ls)

        return rdd_data

    '''
    rdd_ranks [pid, score] already split
    '''
    def generate_feature_response(self, sc, rdd_data):

        def sparse_label_points(field_index_vec, view_feature, score, num_fields, popularity):
            N = num_fields+1+len(view_feature)
            index = field_index_vec
            values = [1.]*len(index)
            index.extend(range(num_fields, num_fields+len(view_feature)))
            index.append(N-1)
            values.extend(view_feature)
            values.append(score)
            feature = SparseVector(N, index, values)
            return LabeledPoint(popularity, feature)

        rdd_field_2_index = sc.textFile(C.FIELD_2_INDEX)
        num_fields = rdd_field_2_index.count()

        '''
        field_index_vec, view_feature, score, num_fields, popularity
        '''
        rdd_label_data =  rdd_data.map(lambda x: sparse_label_points(x[1][0], x[1][1], x[1][2], num_fields, x[1][3])
        return rdd_label_data

    def train_model(self, sc):






        model = LinearRegressionWithSGD.train(parsedData, iterations=100, step=0.00000001)



