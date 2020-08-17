from common.auth import get_service_tapy_client
from common.logs import get_logger
logger = get_logger(__name__)

try:
    t = get_service_tapy_client(tenant_id='master', base_url='https://master.develop.tapis.io')
except Exception as e:
    logger.error(f'Could not instantiate tapy service client. Exception: {e}')
    raise e
