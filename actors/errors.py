"""All exceptions thrown by the Abaco system"""
import os
from agaveflask.errors import BaseAgaveflaskError

from common.config import conf

TAG = os.environ.get('service_TAG') or conf.version

class DAOError(BaseAgaveflaskError):
    pass


class ResourceError(BaseAgaveflaskError):
    pass


class WorkerException(BaseAgaveflaskError):
    pass


class ExecutionException(BaseAgaveflaskError):
    pass


class PermissionsException(BaseAgaveflaskError):
    pass


class ClientException(BaseAgaveflaskError):
    pass

errors = {
    'MethodNotAllowed': {
        'message': "Invalid HTTP method on requested resource.",
        'status': "error",
        'version': TAG
    },
}