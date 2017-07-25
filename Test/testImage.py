data = '../../Data/TinyData/image_url'
import urllib
import cv2
import numpy as np
from PIL import Image


import csv

with open(data) as csvfile:
    row_num = 1
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        url = row[2]
        resp = urllib.urlopen(url)
        image = np.asarray(bytearray(resp.read()), dtype="uint8")
        image = cv2.imdecode(image, cv2.IMREAD_COLOR)
        img = Image.fromarray(data, 'RGB')
        img.show()
        print image.shape
        break
