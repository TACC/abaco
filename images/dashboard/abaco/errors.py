class BaseError(Exception):
    def __init__(self, msg=None, code=400):
        self.msg = msg
        self.code = code


class PermissionsError(BaseError):
    """Error checking permissions or insufficient permissions needed to perform the action."""
    def __init__(self, msg=None, code=404):
        super(PermissionsError, self).__init__(msg=msg, code=code)

