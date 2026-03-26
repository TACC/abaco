"""The RabbitMQ management HTTP API, in one place.

Abaco reads queue depths and deletes stale worker queues through the management plugin rather than AMQP:
a management call never opens a broker connection and never (re)declares the queue it asks about. Callers:
the autoscaler and admin endpoints (controllers.get_queue_depths) and health (worker queue cleanup and the
lost-message reconciler).

Config: rabbit_dash_host (default "rabbit"), rabbit_dash_port (default 15672), and admin_rabbitmq_user /
admin_rabbitmq_pass (default guest/guest), the same admin account stores.rabbit_initialization uses. Each
site's vhost is abaco_<site_id>.

Every function raises on failure (connection errors, non-2xx answers other than the documented 404s);
callers decide what a failure means for them.
"""
from urllib.parse import quote

import requests
from tapisservice.config import conf

# (connect, read) seconds. These calls run inside the health loop and on the autoscaler's request path.
TIMEOUT = (5, 10)


def _url(*parts):
    host = conf.get("rabbit_dash_host") or "rabbit"
    port = conf.get("rabbit_dash_port") or 15672
    return f"http://{host}:{port}/api/" + "/".join(quote(str(p), safe="") for p in parts)


def _auth():
    return conf.get("admin_rabbitmq_user", "guest"), conf.get("admin_rabbitmq_pass", "guest")


def vhost(site_id):
    return f"abaco_{site_id}"


def list_queues(site_id, columns):
    """Every queue in the site's vhost, as dicts holding only `columns`."""
    rsp = requests.get(_url("queues", vhost(site_id)), params={"columns": ",".join(columns)},
                       auth=_auth(), timeout=TIMEOUT)
    rsp.raise_for_status()
    return rsp.json()


def get_queue(site_id, name):
    """One queue's details, or None if it does not exist."""
    rsp = requests.get(_url("queues", vhost(site_id), name), auth=_auth(), timeout=TIMEOUT)
    if rsp.status_code == 404:
        return None
    rsp.raise_for_status()
    return rsp.json()


def delete_queue(site_id, name, if_unused=False):
    """Delete a queue; False if it did not exist. With if_unused the broker refuses (400) when the queue has
    gained a consumer since the caller looked at it."""
    params = {"if-unused": "true"} if if_unused else None
    rsp = requests.delete(_url("queues", vhost(site_id), name), params=params, auth=_auth(), timeout=TIMEOUT)
    if rsp.status_code == 404:
        return False
    rsp.raise_for_status()
    return True
