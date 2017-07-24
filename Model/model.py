import os, sys

sys.path.append('/home/yiming/Behance')
sys.path.append('/home/yiming/Behance/configuration')
sys.path.append('/home/yiming/Behance/UserProjectNetwork')

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.mllib.linalg import SparseVector
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
    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel('ERROR')
    sqlContext = HiveContext(sc)
    return sc, sqlContext


class Model():

    def __init__(self):
        self.training_pid_set = None
        self.valid_pid_set = None
        self.test_pid_set = None

    @staticmethod
    def _join_pair_rdds(rdd1, rdd2):
        rdd = rdd1.join(rdd2)
        def f(x):
            if isinstance(x[0], tuple) and isinstance(x[1], tuple):
                return x[0] + x[1]
            elif isinstance(x[0], tuple) and (not isinstance(x[1], tuple)):
                return x[0] + (x[1], )
            else:
                return (x[0],)+(x[1],)

        return rdd.mapValues(f)

    @staticmethod
    def _join_list_rdds(ls_rdds):
        rdd = ls_rdds[0]
        for i in range(1, len(ls_rdds)):
            rdd = Model._join_pair_rdds(rdd, ls_rdds[i])
        return rdd

    def extract_data_rdd(self, sc, pid_set):
        '''
        :param sc:
        :param month_set:
        :return: rdd with column pid, fields(maybe empty), view_feature, page_rank score, popularity
        '''

        def _vec_2_int(vec):
            if not vec:
                return []
            vec_result = []
            for y in vec:
                vec_result.append(int(y))
            return vec_result

        def _vec_2_float(vec):
            if not vec:
                return []
            vec_result = []
            for y in vec:
                vec_result.append(float(y))
            return vec_result

        pid_set_broad = sc.broadcast(pid_set)

        # build training rdd
        rdd_pid_2_field_index = sc.textFile(C.PID_2_FIELD_INDEX_FILE).map(lambda x: x.split('#'))\
            .filter(lambda x: x[0] in pid_set_broad.value)\
            .map(lambda x: (x[0], x[1].split(','))).mapValues(_vec_2_int)


        '''
        ls = rdd_pid_2_field_index.map(lambda x: x[1]).collect()

        for elem in ls:
            try:
                _vec_2_int(elem)
            except:
                print elem
        set1 = set(rdd_pid_2_field_index.map(lambda x: x[0]).collect())
        '''

        rdd_pid_2_view_feature = sc.textFile(C.PID_2_VIEWS_FEATURE_FILE).map(lambda x : x.split('#')) \
            .filter(lambda x: x[0] in pid_set_broad.value)\
            .map(lambda x: (x[0], tuple(x[1].split(',')))).mapValues(_vec_2_float)

        rdd_pid_2_score = sc.textFile(C.PID_2_SCORE_FILE).map(lambda x: x.split(','))\
            .filter(lambda x: x[0] in pid_set_broad.value).mapValues(lambda x: float(x))

        #print rdd_pid_2_score.take(5)

        rdd_pid_2_popularity = sc.textFile(C.PID_2_POPULARITY_FILE).map(lambda x: x.split(','))\
            .filter(lambda x: x[0] in pid_set_broad.value).mapValues(lambda x: float(x))
        #print rdd_pid_2_popularity.take(5)

        #print  "==================="
        
        ls = []
        ls.append(rdd_pid_2_field_index)
        ls.append(rdd_pid_2_view_feature)
        ls.append(rdd_pid_2_score)
        ls.append(rdd_pid_2_popularity)
        rdd_data = Model._join_list_rdds(ls)
        return rdd_data

    '''
    rdd_ranks [pid, score] already split
    '''
    def generate_feature_response(self, sc, rdd_data):

        def sparse_label_points(field_index_vec, view_feature, score, num_fields, popularity):
            N = num_fields+1+len(view_feature)
            print "N is {}".format(N)
            index = sorted(field_index_vec)
            values = [1.]*len(index)
            index.extend(range(num_fields, num_fields+len(view_feature)))
            index.append(N-1)
            values.extend(view_feature)
            values.append(score)
            print  " =========="
            print len(index)
            print len(values)
            print index
            print values
            print  " =========="

            feature = SparseVector(N, index, values)
            print "feature **************"
            print feature
            print "**************"
            print popularity
            print LabeledPoint(popularity, feature)
            return LabeledPoint(popularity, feature)

        rdd_field_2_index = sc.textFile(C.FIELD_2_INDEX)
        num_fields = rdd_field_2_index.count()


        #field_index_vec, view_feature, score, num_fields, popularity
        '''
        ls = rdd_data.collect()
        for x in ls:
            try:
                label_point = sparse_label_points(sparse_label_points(x[1][0][:], x[1][1][:], x[1][2], num_fields, x[1][3]))
            except:
                print x
                sys.exit("STOP NOW")
        '''

        rdd_label_data =  rdd_data.map(lambda x: sparse_label_points(x[1][0][:], x[1][1][:], x[1][2], num_fields, x[1][3]))
        return rdd_label_data

    def train_model(self, sc, model_name, num_iter, pid_training_set):
        rdd_training_data = self.extract_data_rdd(sc, pid_training_set)
        rdd_training_labeled_data = self.generate_feature_response(sc, rdd_training_data)
        mse_array = []
        print rdd_training_labeled_data.take(5)
        linear_model = None
        for iteration in range(1, num_iter+1):
            if iteration == 1:
                linear_model = LinearRegressionWithSGD.train(rdd_training_labeled_data, intercept=True, iterations=100, step=1e-9)
            else:
                print linear_model.weights
                linear_model = LinearRegressionWithSGD.train(rdd_training_labeled_data,\
                        iterations=100, step=1e-9, intercept=True, initialWeights=linear_model.weights)
            values_pred = rdd_training_labeled_data.map(lambda p: (p.label, linear_model.predict(p.features)))
            MSE = values_pred.map(lambda vp: (vp[0] - vp[1]) ** 2) \
                      .reduce(lambda x, y: x + y) / values_pred.count()
            mse_array.append(MSE)
            print("iteration {}, Mean Squared Error = {}".format(iteration, str(MSE)))

        linear_model.save(sc, os.path.join(C.MODEL_DIR, model_name))

        sc.parallelize(mse_array).map(lambda x: str(x)).saveAsTextFile(os.path.join(C.MODEL_LOG_DIR, model_name, "mse_log"))

    def evaluation(self, sc, model_name, valid = True):
        if valid:
            pid_evaluation_set = set(sc.textFile(C.VALID_PID_SET_FILE).collect())
        else:
            pid_evaluation_set = set(sc.textFile(C.VALID_PID_SET_FILE).collect())
        rdd_evaluation_data = self.extract_data_rdd(sc, pid_evaluation_set)

        model = LinearRegressionModel.load(sc, os.path.join(C.MODEL_DIR, model_name))
        values_pred = rdd_evaluation_data.map(lambda p: (p.label, model.predict(p.features)))
        MSE = values_pred.map(lambda vp: (vp[0] - vp[1]) ** 2) \
                  .reduce(lambda x, y: x + y) / values_pred.count()
        print MSE


if __name__ == "__main__":
    sc, _ = init_spark('no_image_model', 40)
    model = Model()
    pid_train_set = set(sc.textFile(C.TRAIN_PID_SAMPLE_SET_FILE).collect())
    print "size of training set is {}".format(len(pid_train_set))
    pid_valid_set = set(sc.textFile(C.VALID_PID_SET_FILE).collect())
    print "size of valid set is {}".format(len(pid_valid_set))
    '''
    pid_test_set = set(sc.textFile(C.TEST_PID_SET_FILE).collect())
    rdd_train_data = model.extract_data_rdd(sc, pid_train_set)
    rdd_label_train_data = model.generate_feature_response(sc, rdd_train_data)
    print rdd_label_train_data.take(5)
    '''
    model.train_model(sc, 'linear-with-no-image', 100, pid_train_set)
    sc.stop()