# Test suite for abaco project.
# Image: abaco/core

# inherit from the flaskbase iamge:
#from tapis/flaskbase
from notchristiangarcia/flaskbase

# set the name of the api, for use by some of the common modules.
ENV TAPIS_API actors-api

RUN apt-get install python3-dev g++ sudo -y

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

RUN mkdir -p /home/tapis/service/resources /home/tapis/runtime_files/logs /home/tapis/runtime_files/_abaco_results_sockets /home/tapis/runtime_files/_abaco_fifos /home/tapis/runtime_files
# create abaco.log file for logs
RUN touch /home/tapis/runtime_files/logs/service.log

COPY docs/specs/openapi_v3.yml /home/tapis/service/resources/openapi_v3.yml

# todo -- add/remove to toggle between local channelpy and github instance
#ADD channelpy /channelpy
#RUN pip3 install /channelpy
# ----
COPY actors /home/tapis/actors
RUN chmod +x /home/tapis/actors/health_check.sh

ADD entry.sh /home/tapis/entry.sh
RUN chmod +x /home/tapis/entry.sh

# Permissions
RUN echo "tapis ALL=NOPASSWD: /home/tapis/actors/folder_permissions.sh" >> /etc/sudoers
RUN chmod +x /home/tapis/actors/folder_permissions.sh
RUN groupadd -g 1000 host_gid
RUN groupadd -g 1001 docker_gid
RUN usermod -aG tapis,host_gid,docker_gid tapis
RUN chown -R tapis:tapis /home/tapis

#USER tapis

# set default threads for gunicorn
ENV threads=3

EXPOSE 5000

CMD ["/home/tapis/entry.sh"]
