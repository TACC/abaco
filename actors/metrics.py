import requests
import datetime

from agaveflask.logs import get_logger
from stores import actors_store

logger = get_logger(__name__)

PROMETHEUS_URL = 'http://127.0.0.1:9090'


def main():
    logger.info("Running Metrics check.")
    actor_ids = [
        actor.db_id
        for actor
        in actors_store.items()
    ]
    for actor_id in actor_ids:
        logger.debug("TOP OF CHECK METRICS")
        query = {
            'query': 'message_count_for_actor_{}'.format(actor_id.decode("utf-8").replace('-', '_')),
            'time': datetime.datetime.utcnow().isoformat() + "Z"
        }
        r = requests.get(PROMETHEUS_URL + '/api/v1/query', params=query)
        logger.debug("METRICS QUERY: {}".format(r.text))

if __name__ == '__main__':
    main()