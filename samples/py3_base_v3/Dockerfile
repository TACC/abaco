# Image: abacosamples/py3_base_v3
from python:3.7
ADD requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
# TODO -- remove the COPY once tapipy is up on pypi
COPY tapipy /usr/local/lib/python3.7/site-packages/tapipy