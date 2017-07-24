from pyspark.mllib.linalg import *
import numpy as np

index = [47, 66, 51, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102]
values = [1.0, 1.0, 1.0, 497.0, 5.0, 0.0, 0.0, 0.0, 10.0, 3.0, 2.0, 1.0, 2.0, 1.0, 1.0, 0.0, 0.0, 0.263263097793]

a = SparseVector(103, index, values)
