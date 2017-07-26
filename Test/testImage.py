data = '../../Data/TinyData/image_url'
import urllib
import cStringIO
import numpy as np
import cv2
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from PIL import Image
import h5py
from keras.models import Model

from keras.layers.normalization import BatchNormalization
from keras.layers import Input, Dense, Dropout
from keras.models import load_model
from keras.losses import mean_absolute_error
from keras.callbacks import TensorBoard
from keras.backend import clear_session
from keras.optimizers import Adam

import keras.models

import csv
COL_SIZE = 224
ROW_SIZE = 224

#!/usr/bin/env python
# extract all user information from owners and actions:

import sys

sys.path.append('/home/yiming/Behance')
sys.path.append('/home/yiming/Behance/configuration')
sys.path.append('/home/yiming/Behance/UserProjectNetwork')


from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, IntegerType, BooleanType
import os, sys
import operator
from scipy.sparse import coo_matrix, csr_matrix
from subprocess import Popen
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import configuration.constants as C


# import spark
import NetworkHelpFunctions

def init_spark(name, max_excutors):
    conf = (SparkConf().setAppName(name)
            .set("spark.dynamicAllocation.enabled", "false")
            .set("spark.dynamicAllocation.maxExecutors", str(max_excutors))
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel('ERROR')
    sqlContext = HiveContext(sc)
    return sc, sqlContext

sc, _ =  init_spark('image_feature', 10)

'''
dataw = np.zeros([2,ROW_SIZE,COL_SIZE,3])

with open(data) as csvfile:
    row_num = 1
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        url = row[2]
        file = cStringIO.StringIO(urllib.urlopen(url).read())
        data1 = np.asarray(Image.open(file).resize((ROW_SIZE, COL_SIZE), Image.ANTIALIAS))
        data1 = np.expand_dims(data1, axis=0)
        print data1.shape
        Input(shape=(3, 200, 200), name='image_input')
        model = keras.applications.vgg16.VGG16(include_top=True, weights='imagenet', input_tensor=None,input_shape=None)
        feature = model.predict(dataw)
        model_extractfeatures = Model(input=model.input, output=model.get_layer('fc2').output)
        feature = model_extractfeatures.predict(dataw)
        print feature.shape
        break
'''


def image_2_numpy(url):
    file = cStringIO.StringIO(urllib.urlopen(url).read())
    numpy_array_image = np.asarray(Image.open(file).resize((ROW_SIZE, COL_SIZE), Image.ANTIALIAS))
    return numpy_array_image

with open(data) as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    list_features = []
    for row in readCSV:
        url = row[2]
        file = cStringIO.StringIO(urllib.urlopen(url).read())
        data1 = np.asarray(Image.open(file).resize((ROW_SIZE, COL_SIZE), Image.ANTIALIAS))
        data1 = np.expand_dims(data1, axis=0)
        print data1.shape
        Input(shape=(3, 200, 200), name='image_input')
        model = keras.applications.vgg16.VGG16(include_top=True, weights='imagenet', input_tensor=None,input_shape=None)
        feature = model.predict(dataw)
        model_extractfeatures = Model(input=model.input, output=model.get_layer('fc2').output)
        feature = model_extractfeatures.predict(dataw)
        print feature.shape
        break






