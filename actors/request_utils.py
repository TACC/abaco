import os

import flask.ext.restful.reqparse as reqparse
from flask import jsonify, request
from werkzeug.exceptions import ClientDisconnected
from flask_restful import Api

from common.config import conf
from errors import BaseAbacoError

TAG = os.environ.get('TAG') or conf.version


class RequestParser(reqparse.RequestParser):
    """Wrap reqparse to raise APIException."""

    def parse_args(self, *args, **kwargs):
        try:
            return super(RequestParser, self).parse_args(*args, **kwargs)
        except ClientDisconnected as exc:
            raise BaseAbacoError(exc.data['message'], 400)

class AgaveApi(Api):
    """General flask_restful Api subclass for all the Agave APIs."""
    pass


def handle_error(exc):
    show_traceback = conf.web_show_traceback
    if show_traceback == 'true':
        raise exc
    if isinstance(exc, BaseAbacoError):
        response = error(msg=exc.msg)
        response.status_code = exc.code
        return response
    else:
        response = error(msg='Unrecognized exception type: {}. Exception: {}'.format(type(exc), exc))
        response.status_code = 500
        return response

def pretty_print(request):
    """Return whether or not to pretty print based on request"""
    if hasattr(request.args.get('pretty'), 'upper') and request.args.get('pretty').upper() == 'TRUE':
        return True
    return False

def ok(result, msg="The request was successful", request=request):
    d = {'result': result,
         'status': 'success',
         'version': TAG,
         'message': msg}
    return jsonify(d)

def error(result=None, msg="Error processing the request.", request=request):
    d = {'result': result,
         'status': 'error',
         'version': TAG,
         'message': msg}
    return jsonify(d)