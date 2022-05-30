# Core image for Abaco project - Includes files for testing.
# Image: abaco/core-v3

# inherit from the flaskbase iamge:
FROM tapis/flaskbase-plugins:latest
# set the name of the api, for use by some of the common modules.
ENV TAPIS_API actors-api
ENV PYTHONPATH .:*:actors:actors/*


## FILE INITIALIZATION
COPY configschema.json /home/tapis/configschema.json
COPY actors /home/tapis/service
COPY actors /home/tapis/actors
COPY actors /actors
COPY docs/specs/openapi_v3.yml /home/tapis/service/resources/openapi_v3.yml
# we mkdir's instead of copying because we don't
# want to bring over folder contents by mistake.
RUN mkdir -p /home/tapis/runtime_files /home/tapis/runtime_files/_abaco_fifos /home/tapis/runtime_files/_abaco_results_sockets /home/tapis/runtime_files/logs /home/tapis/runtime_files/data1 /home/tapis/runtime_files/data2 /home/tapis/runtime_files/certs
# touch config.json
RUN touch /home/tapis/config.json
# create abaco.log file for logs
RUN touch /home/tapis/runtime_files/logs/service.log
RUN touch /home/tapis/runtime_files/logs/tapisservice.log
# health_check and actors files.
COPY actors /home/tapis/actors
RUN chmod +x /home/tapis/actors/health_check.sh
# regular abaco entrypoints
ADD entry.sh /home/tapis/entry.sh
RUN chmod +x /home/tapis/entry.sh
# tests entrypoints
COPY tests /home/tapis/tests
RUN chmod +x /home/tapis/tests/entry.sh


## PACKAGE INITIALIZATION
RUN apt-get --allow-releaseinfo-change update                                                                                                                   
RUN apt-get update && apt-get install python-dev g++ sudo -y 
RUN pip3 install --upgrade pip
RUN pip3 install -r /home/tapis/actors/requirements.txt
RUN wget https://raw.githubusercontent.com/rabbitmq/rabbitmq-management/v3.8.9/bin/rabbitmqadmin
RUN chmod +x rabbitmqadmin
# rabbitmqadmin download for tests
RUN wget https://raw.githubusercontent.com/rabbitmq/rabbitmq-management/v3.8.9/bin/rabbitmqadmin
RUN chmod +x rabbitmqadmin


## PERMISSION INITIALIZATION
RUN echo "tapis ALL=NOPASSWD: /home/tapis/actors/folder_permissions.sh" >> /etc/sudoers
RUN chmod +x /home/tapis/actors/folder_permissions.sh
RUN groupadd -g 1000 host_gid
RUN groupadd -g 1001 docker_gid
RUN usermod -aG tapis,host_gid,docker_gid tapis
RUN chown -R tapis:tapis /home/tapis

## Note, set this env when running tests through Makefile.
#ENV _called_from_within_test=True

USER tapis

EXPOSE 5000

#CMD ["/home/tapis/tests/entry.sh"]
CMD ["/home/tapis/entry.sh"]
