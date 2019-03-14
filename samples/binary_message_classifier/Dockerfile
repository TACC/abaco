# image: abacosamples/binary_message_classifier

FROM tensorflow/tensorflow:1.5.0-py3

# Requirements
RUN apt-get update && apt-get install -y wget
RUN pip install --upgrade h5py
RUN pip install --no-cache-dir agavepy

# Image
ADD classify_image.py /classify_image.py
ADD entry.sh /entry.sh
RUN chmod +x /entry.sh

ENTRYPOINT ["python", "/classify_image.py", "--bin_message"]
# ENTRYPOINT ["/entry.sh"]
