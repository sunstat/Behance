data = '../../Data/TinyData/image_url'
import urllib
import cv2
import numpy as np

import csv

with open(data) as csvfile:
    row_num = 1
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        url = row[2]
        resp = urllib.urlopen(url)
        image = np.asarray(bytearray(resp.read()), dtype="uint8")
        print image.shape
        break
