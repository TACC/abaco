"""All exceptions thrown by the Abaco system"""
import os
from common.errors import BaseTapisError

from common.config import conf

TAG = os.environ.get('service_TAG') or conf.version

class DAOError(BaseTapisError):
    pass


class ResourceError(BaseTapisError):
    pass


class WorkerException(BaseTapisError):
    pass


class ExecutionException(BaseTapisError):
    pass


class PermissionsException(BaseTapisError):
    pass


class ClientException(BaseTapisError):
    pass

errors = {
    'MethodNotAllowed': {
        'message': "Invalid HTTP method on requested resource.",
        'status': "error",
        'version': TAG
    },
}