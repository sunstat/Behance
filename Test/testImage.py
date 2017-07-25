data = '../../Data/TinyData/image_url'
import urllib
import numpy as np
import cv2
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
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
        im = Image.fromarray(image, 'RGB')
        im = im.resize([244,244], Image.ANTIALIAS)
        print im
        plt.imshow(im)
        image.resize()
        plt.pause(5)

        print image.shape
        break
