#!/bin/bash

cd /
wget $URL -O image.jpg
python /classify_image.py --image_file /image.jpg