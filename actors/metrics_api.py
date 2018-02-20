from flask import Flask, render_template
from flask_cors import CORS

from agaveflask.utils import AgaveApi, handle_error

from auth import authn_and_authz
from controllers import MetricsResource
from errors import errors

app = Flask(__name__)
CORS(app)
api = AgaveApi(app, errors=errors)

# Authn/z
# @app.before_request
# def auth():
#     authn_and_authz()

api.handle_error = handle_error
api.handle_exception = handle_error
api.handle_user_exception = handle_error

# Resources
api.add_resource(MetricsResource, '/metrics')


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
