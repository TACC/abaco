"""Flask-RESTful error handling shared by the four API processes (reg, mes, admin, metrics)."""
import pika.exceptions
from pymongo.errors import ConnectionFailure, ExecutionTimeout
from tapisservice.tapisflask.utils import handle_error as tapis_handle_error
from tapisservice.logs import get_logger

from errors import ResourceError
from queues import ChannelClosedException

logger = get_logger(__name__)


def handle_error(exc):
    """tapisservice's handle_error maps BaseTapisError to its code and everything else to a
    500. These failures are not bugs in the request and deserve a code that tells
    clients and proxies to retry rather than "the service is broken":
    - ConnectionFailure (ServerSelectionTimeoutError, NetworkTimeout, AutoReconnect,
      NotPrimaryError): the database is unreachable -> 503.
    - ExecutionTimeout: the server gave up at maxTimeMS -> 504.
    - a RabbitMQ connection or channel that failed mid-request (StreamLostError,
      ConnectionClosedByBroker, ChannelClosedByBroker, ...): the broker is unavailable -> 503.
      (A failed connect is already a 503: RabbitUnavailableError is a ResourceError.)
    Everything else (DuplicateKeyError, OperationFailure, ...) still surfaces as a 500 so
    real bugs stay visible."""
    if isinstance(exc, ExecutionTimeout):
        logger.error(f"Mongo operation exceeded maxTimeMS: {exc}")
        exc = ResourceError("The database operation took too long; narrow the request or retry.", 504)
    elif isinstance(exc, ConnectionFailure):
        logger.error(f"Mongo unavailable while handling a request ({type(exc).__name__}): {exc}")
        exc = ResourceError(f"The database is unavailable ({type(exc).__name__}); please retry.", 503)
    elif isinstance(exc, (pika.exceptions.AMQPError, ChannelClosedException)):
        logger.error(f"RabbitMQ failed while handling a request ({type(exc).__name__}): {exc}")
        exc = ResourceError(f"The message broker is unavailable ({type(exc).__name__}); please retry.", 503)
    return tapis_handle_error(exc)
