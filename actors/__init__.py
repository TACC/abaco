import os
import time

# Initialize as normal if this module is NOT called from a test container.
# This allows us to not use flaskbase when calling codes.py
if os.environ.get('_called_from_within_test'):
    pass
else:
    from tapisservice.config import conf
    from tapisservice.logs import get_logger
    logger = get_logger(__name__)

    # Boot needs the tenants AND tokens services (TenantCache() fetches the tenant list
    # in its constructor; the client then acquires service tokens). This used to be
    # fatal after ~10s, with TenantCache() outside the retry entirely -- and gunicorn
    # --max-requests recycles workers, so any auth outage longer than that turned a
    # healthy API into a crash loop. Retry with backoff for
    # service_client_boot_timeout_s (default 300s), then fail with a real error.
    boot_deadline = time.monotonic() + float(conf.get('service_client_boot_timeout_s') or 300)
    backoff = 2
    while True:
        try:
            # Imported INSIDE the retry: tapisservice.tenants builds a module-level
            # TenantCache (fetches the tenant list) at import time, and tapisservice.auth
            # imports it -- so an import at the top of this file crashed on any auth outage
            # before the retry ever ran. A failed import leaves nothing in sys.modules, so
            # the next attempt re-runs it.
            from tapisservice.tenants import TenantCache
            from tapisservice.auth import get_service_tapis_client
            Tenants = TenantCache()
            t = get_service_tapis_client(tenants=Tenants)
            break  # Exit the loop if t is successfully created
        except Exception as e:
            if time.monotonic() > boot_deadline:
                msg = f'Failed to create the tapis service client within the boot timeout; last error: {e}'
                logger.critical(msg)
                raise RuntimeError(msg)
            logger.error(f'Could not instantiate the tapis service client ({type(e).__name__}): {e}. '
                         f'Retrying in {backoff}s...')
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)