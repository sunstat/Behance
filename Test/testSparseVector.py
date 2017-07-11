from pyspark.mllib.linalg import *
import numpy as np

a = SparseVector(4, [1, 3], [3.0, 4.0])
print a.dot(a)
print a.dot(np.array([1., 2., 3., 4.]))
b = SparseVector(4, [2, 3], [1.0, 2.0])
print b.dot(np.array([1., 2., 3., 4.])
