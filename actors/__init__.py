import os

# Initialize as normal if this module is NOT called from a test container.
# This allows us to not use flaskbase when calling codes.py
if os.environ.get('_called_from_within_test'):
    pass
else:
    from common.auth import get_service_tapis_client, Tenants
    from common.logs import get_logger
    logger = get_logger(__name__)

    try:
        t = get_service_tapis_client(tenants=Tenants())
    except Exception as e:
        logger.error(f'Could not instantiate tapy service client. Exception: {e}')
        raise e