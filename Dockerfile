# Image: abaco/core

from python:3.9.2
RUN apt-get update && apt-get install -y vim

ADD actors/requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

RUN touch /var/log/abaco.log

# set default threads for gunicorn
ENV threads=3

# todo -- add/remove to toggle between local channelpy and github instance
#ADD channelpy /channelpy
#RUN pip3 install /channelpy
# ----

ADD actors /actors
ADD tests /tests
ADD entry.sh /entry.sh
RUN chmod +x /actors/health_check.sh /tests/entry.sh /entry.sh /actors/metrics_cron_check.sh
EXPOSE 5000

CMD ["./entry.sh"]
