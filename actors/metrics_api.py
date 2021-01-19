from flask import Flask, render_template
from flask_cors import CORS
from prometheus_client import Summary, MetricsHandler, Counter

from agaveflask.utils import AgaveApi, handle_error

from controllers import MetricsResource, CronResource

from errors import errors

app = Flask(__name__)
CORS(app)
api = AgaveApi(app, errors=errors)

REQUEST_TIME = Summary('request_processing_seconds', 'DESC: Time spent processing request')

# todo - probably should add a basic auth check
# for now, we comment this out because we do not authenticate the calls from prometheus;

# Authn/z
# @app.before_request
# def auth():
#     authn_and_authz()

api.handle_error = handle_error
api.handle_exception = handle_error
api.handle_user_exception = handle_error

# Resources
api.add_resource(MetricsResource, '/metrics')
api.add_resource(CronResource, '/cron')

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
