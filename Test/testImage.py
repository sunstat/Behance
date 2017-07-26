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

rdd = sc.textFile()




