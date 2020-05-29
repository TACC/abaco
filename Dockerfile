# Test suite for abaco project.
# Image: abaco/core

# inherit from the flaskbase iamge:
from tapis/flaskbase

# set the name of the api, for use by some of the common modules.
ENV TAPIS_API actors-api

RUN apt-get install python3-dev g++ -y

# install additional requirements for the service.
COPY actors/requirements.txt /home/tapis/requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install -r /home/tapis/requirements.txt

# copy service source code
COPY configschema.json /home/tapis/configschema.json
COPY config-local.json /home/tapis/config.json
COPY actors /home/tapis/service
COPY actors /home/tapis/actors
COPY actors /actors

# create abaco.log file for logs
RUN touch /var/log/abaco.log

# todo -- add/remove to toggle between local channelpy and github instance
#ADD channelpy /channelpy
#RUN pip3 install /channelpy
# ----
RUN chmod +x /actors/health_check.sh

ADD entry.sh /home/tapis/actors/entry.sh
RUN chmod +x /home/tapis/actors/entry.sh

# Flaskbase stuff
RUN chown -R tapis:tapis /home/tapis
#USER tapis

# set default threads for gunicorn
ENV threads=3

EXPOSE 5000

CMD ["/home/tapis/actors/entry.sh"]