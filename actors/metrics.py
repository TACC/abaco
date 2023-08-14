import requests
import datetime

from agaveflask.logs import get_logger
from stores import actors_store
from controllers import MetricsResource
from flask import Flask

logger = get_logger(__name__)

app = Flask(__name__)

def main():
    logger.info("Running Metrics check.")
    with app.app_context():
        MetricsResource().get()

if __name__ == '__main__':
    main()


## Archive
## This used to be ran a long time ago, we don't use it now, but it's here for reference
# def main():
#     logger.info("Running Metrics check.")
#     actor_ids = [actor['db_id'] for actor in actors_store[site()].items()]
#     for actor_id in actor_ids:
#         logger.debug("TOP OF CHECK METRICS")
#         query = {
#             'query': f"message_count_for_actor_{actor_id.decode('utf-8').replace('-', '_')}",
#             'time': datetime.datetime.utcnow().isoformat() + "Z"
#         }
#         r = requests.get(PROMETHEUS_URL + '/api/v1/query', params=query)
#         logger.debug(f"METRICS QUERY: {r.text}")