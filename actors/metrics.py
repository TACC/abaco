import requests
import datetime

from common.logs import get_logger
from stores import actors_store
from models import site

logger = get_logger(__name__)

PROMETHEUS_URL = 'http://127.0.0.1:9090'


def main():
    logger.info("Running Metrics check.")
    actor_ids = [actor['db_id'] for actor in actors_store[site()].items()]
    for actor_id in actor_ids:
        logger.debug("TOP OF CHECK METRICS")
        query = {
            'query': f"message_count_for_actor_{actor_id.decode('utf-8').replace('-', '_')}",
            'time': datetime.datetime.utcnow().isoformat() + "Z"
        }
        r = requests.get(PROMETHEUS_URL + '/api/v1/query', params=query)
        logger.debug(f"METRICS QUERY: {r.text}")

if __name__ == '__main__':
    main()