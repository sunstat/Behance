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

class imageModel(object):
    def __init__(self):
        pass
    