class BaseTapyException(Exception):
    """
    Base Tapy error class. All Error types should descend from this class.
    """
    def __init__(self, msg=None, version=None, request=None, response=None):
        """
        Create a new TapisError object.
        :param msg: (str) A helpful string
        :param request: (requests.Request) The HTTP request object from the request.
        :param response: (requests.Response) The HTTP response object from the request.
        """
        self.message = msg
        self.version = version
        self.request = request
        self.response = response

    def __str__(self):
        return f'message: {self.message}'

    def __repr__(self):
        return str(self)


class TapyClientNotImplementedError(BaseTapyException):
    """The Tapy client has not implemented the requested functionality yet."""
    pass


class TapyClientConfigurationError(BaseTapyException):
    """The Tapy client was improperly configured."""
    pass


class TokenInvalidError(BaseTapyException):
    """Error indicating the token on the request was invalid."""
    pass


class NotAuthorizedError(BaseTapyException):
    """Error indicating the user is not authorized for the request."""
    pass


class InvalidInputError(BaseTapyException):
    """The input provided to the function was not valid."""
    pass


class InvalidServerResponseError(BaseTapyException):
    """Tapy got a response from the Tapis service that it didn't understand."""
    pass


class ServerDownError(BaseTapyException):
    """Tapy got an error trying to communication with the Tapis server."""
    pass

