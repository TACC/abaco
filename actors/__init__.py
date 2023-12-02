import os
import time

# Initialize as normal if this module is NOT called from a test container.
# This allows us to not use flaskbase when calling codes.py
if os.environ.get('_called_from_within_test'):
    pass
else:
    from tapisservice.tenants import TenantCache
    from tapisservice.auth import get_service_tapis_client
    from tapisservice.logs import get_logger
    logger = get_logger(__name__)

    Tenants = TenantCache()
    for _ in range(5):
        try:
            t = get_service_tapis_client(tenants=Tenants)
            break  # Exit the loop if t is successfully created
        except Exception as e:
            logger.error(f'Could not instantiate tapy service client. Exception: {e}')
            time.sleep(2)  # Delay for 2 seconds before the next attempt
    else:
        msg = 'Failed to create tapy service client after 10 attempts, networking?'
        logger.error(msg)
        raise RuntimeError(msg)