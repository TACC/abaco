# Core image for Abaco project - Includes files for testing.
# Image: abaco/core-v3

# inherit from the flaskbase image. Pinned: an untagged FROM re-resolved to whatever
# `latest` was at build time, and this image overwrites tapisservice's auth.py and
# patches tapipy, both of which are verified against specific versions (see below).
FROM tapis/flaskbase:1.9.0
# set the name of the api, for use by some of the common modules.
ENV TAPIS_API=actors-api
ENV PYTHONPATH=.:*:actors:actors/*
WORKDIR /home/tapis

## PACKAGE INITIALIZATION
COPY requirements.txt /home/tapis/

RUN apt-get update && apt-get install python3-dev g++ sudo -y
RUN pip3 install --upgrade pip
RUN pip3 install -r /home/tapis/requirements.txt
# channelpy is an editable VCS install (see requirements.txt), so pip leaves a full
# git checkout in /home/tapis/src. Drop the VCS metadata -- it ships repo history and
# remote URLs in the image and the editable install does not need it.
RUN rm -rf /home/tapis/src/*/.git
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
# patch tapisservice auth with the vendored copy (service-token self-healing, JWT fixes).
# The vendored file derives from tapisservice 1.9.0 auth.py and has only been exercised
# against the versions listed here; refuse to build on anything else so a requirements or
# base-image bump cannot silently pair it with an incompatible tapisservice/tapipy.
RUN TS_VER=$(python3 -c "import importlib.metadata as m; print(m.version('tapisservice'))") && \
    case "$TS_VER" in 1.5.0|1.9.0) ;; *) echo "vendored auth_tapisservice.py is not verified against tapisservice $TS_VER" >&2; exit 1;; esac && \
    cp /home/tapis/actors/auth_tapisservice.py $(python3 -c "import tapisservice, os; print(os.path.dirname(tapisservice.__file__))")/auth.py
# patch tapipy to send HTTP requests with a timeout (connect 5s, read 30s). tapipy sends with
# no timeout at all, so a hung backend (SK, tokens) pins a request thread forever.
RUN TAPIS_PY=$(python3 -c "import tapipy, os; print(os.path.join(os.path.dirname(tapipy.__file__), 'tapis.py'))") && \
    sed -i 's/resp = self.requests_session.send(r, verify=self.verify)/resp = self.requests_session.send(r, verify=self.verify, timeout=(5, 30))/' $TAPIS_PY && \
    sed -i 's/resp = self.tapis_client.requests_session.send(r, verify=self.tapis_client.verify)/resp = self.tapis_client.requests_session.send(r, verify=self.tapis_client.verify, timeout=(5, 30))/' $TAPIS_PY && \
    test $(grep -c "timeout=(5, 30)" $TAPIS_PY) -eq 2
# Some more permissions
# folder_permissions.sh runs as root via sudo, so it must live OUTSIDE the tree the
# tapis user owns: with the script under /home/tapis/actors, tapis could rewrite it
# (or unlink and replace it -- the directory is tapis-owned too) and sudo it to root.
# Install a root-owned copy and point the sudo rule at that path only.
RUN install -o root -g root -m 0755 /home/tapis/actors/folder_permissions.sh /usr/local/sbin/folder_permissions.sh
# The rule also pins the only two arguments abaco ever passes (the script re-checks them).
RUN echo "tapis ALL=(root) NOPASSWD: /usr/local/sbin/folder_permissions.sh /home/tapis/runtime_files, /usr/local/sbin/folder_permissions.sh /var/run/docker.sock" >> /etc/sudoers && \
    visudo -c
RUN chmod +x /home/tapis/actors/health_check.sh /home/tapis/actors/metrics_cron_check.sh
# Permission finalization
RUN chown -R tapis:tapis /home/tapis

## Note, set this env when running tests through Makefile.
#ENV _called_from_within_test=True

USER tapis

EXPOSE 5000

CMD ["/home/tapis/entry.sh"]
