import os

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
    try:
        t = get_service_tapis_client(tenants=Tenants)
    except Exception as e:
        logger.error(f'Could not instantiate tapy service client. Exception: {e}')
        raise e