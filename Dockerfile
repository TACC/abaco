# Core image for Abaco project - Includes files for testing.
# Image: abaco/core-v3

# inherit from the flaskbase iamge:
FROM tapis/flaskbase
# set the name of the api, for use by some of the common modules.
ENV TAPIS_API actors-api
ENV PYTHONPATH .:*:actors:actors/*
WORKDIR /home/tapis

## PACKAGE INITIALIZATION
COPY requirements.txt /home/tapis/

RUN apt-get update && apt-get install python3-dev g++ sudo -y
RUN pip3 install --upgrade pip
RUN pip3 install -r /home/tapis/requirements.txt
# rabbitmqadmin download for rabbit init
RUN wget https://raw.githubusercontent.com/rabbitmq/rabbitmq-management/v3.8.9/bin/rabbitmqadmin
RUN chmod +x rabbitmqadmin

## PERMISSION INITIALIZATION
RUN groupadd -g 1000 host_gid
RUN groupadd -g 1001 docker_gid
RUN usermod -aG tapis,host_gid,docker_gid tapis

## FILE INITIALIZATION
# we mkdir's instead of copying because we don't
# want to bring over folder contents by mistake.
RUN mkdir -p /home/tapis/runtime_files /home/tapis/runtime_files/_abaco_fifos /home/tapis/runtime_files/_abaco_results_sockets /home/tapis/runtime_files/logs /home/tapis/runtime_files/data1 /home/tapis/runtime_files/data2 /home/tapis/runtime_files/certs
# create abaco.log file for logs
RUN touch /home/tapis/runtime_files/logs/service.log
RUN touch /home/tapis/runtime_files/logs/tapisservice.log
# touch config.json
RUN touch /home/tapis/config.json
# regular abaco entrypoints
ADD entry.sh /home/tapis/entry.sh
RUN chmod +x /home/tapis/entry.sh
# tests entrypoints
COPY tests /home/tapis/tests
RUN chmod +x /home/tapis/tests/entry.sh
# config and spec
COPY configschema.json /home/tapis/configschema.json
COPY docs/specs/openapi_v3.yml /home/tapis/service/resources/openapi_v3.yml
# actors code
COPY actors /home/tapis/actors
# Some more permissions
RUN echo "tapis ALL=NOPASSWD: /home/tapis/actors/folder_permissions.sh" >> /etc/sudoers
RUN chmod +x /home/tapis/actors/folder_permissions.sh
RUN chmod +x /home/tapis/actors/health_check.sh
# Permission finalization
RUN chown -R tapis:tapis /home/tapis

## Note, set this env when running tests through Makefile.
#ENV _called_from_within_test=True

USER tapis

EXPOSE 5000

CMD ["/home/tapis/entry.sh"]
