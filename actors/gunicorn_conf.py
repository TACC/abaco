"""gunicorn server hooks for the reg/mes/admin/metrics APIs (entry.sh passes -c).

The command-line flags in entry.sh stay authoritative; this file only adds hooks.

entry.sh runs gunicorn with --preload, so the app is imported once in the master and the
workers are forked from it. That is what lets the boot retry in __init__.py work: without
--preload every worker imported the app itself, gunicorn killed a worker that had not
finished booting after --timeout (120s), and the 300s service-client retry could never
complete. It also means a --max-requests recycle no longer re-runs boot (tenant fetch +
service-token acquisition) per new worker.

What must not cross the fork:
- MongoClients: store.py rebinds them per process (see MongoStore._check_fork).
- The service client's pooled HTTP connections: dropped here, so two workers never share
  one keep-alive socket to the tenants/tokens services.
"""
import sys


def post_fork(server, worker):
    boot = sys.modules.get('__init__')
    t = getattr(boot, 't', None)
    session = getattr(t, 'requests_session', None)
    if session is not None:
        # closes this process's copies of the pooled sockets; requests re-creates its
        # pools lazily on the next call. The master never uses them again.
        session.close()
        server.log.debug(f"worker {worker.pid}: dropped the service client's pooled connections")
