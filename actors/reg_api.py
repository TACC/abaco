
from flask import Flask
from flask_cors import CORS

from tapisservice.tapisflask.utils import handle_error, TapisApi

from controllers import ActorResource, AliasesResource, AliasResource, AliasNoncesResource, AliasNonceResource, \
    ActorStateResource, ActorsResource, \
    ActorExecutionsResource, ActorExecutionResource, ActorExecutionResultsResource, \
    ActorExecutionLogsResource, ActorNoncesResource, ActorNonceResource, \
    AbacoUtilizationResource, SearchResource, CronResource, ActorConfigResource, ActorConfigsResource, \
    AdaptersResource, AdapterResource, AdapterMessagesResource
from auth import authn_and_authz
from errors import errors

app = Flask(__name__)
CORS(app)
api = TapisApi(app, errors=errors)

# Authn/z
@app.before_request
def auth():
    authn_and_authz()

# Set up error handling
api.handle_error = handle_error
api.handle_exception = handle_error
api.handle_user_exception = handle_error

# Resources
api.add_resource(ActorsResource, '/actors')
api.add_resource(AbacoUtilizationResource, '/actors/utilization')

api.add_resource(AliasesResource, '/actors/aliases')
api.add_resource(AliasResource, '/actors/aliases/<string:alias>')
api.add_resource(AliasNoncesResource, '/actors/aliases/<string:alias>/nonces')
api.add_resource(AliasNonceResource, '/actors/aliases/<string:alias>/nonces/<string:nonce_id>')

api.add_resource(SearchResource, '/actors/search/<string:search_type>')
api.add_resource(CronResource, '/cron')
api.add_resource(ActorConfigsResource, '/actors/configs')
api.add_resource(ActorConfigResource, '/actors/configs/<string:config_name>')

api.add_resource(ActorResource, '/actors/<string:actor_id>')
api.add_resource(ActorStateResource, '/actors/<string:actor_id>/state')
api.add_resource(ActorExecutionsResource, '/actors/<string:actor_id>/executions')
api.add_resource(ActorExecutionResource, '/actors/<string:actor_id>/executions/<string:execution_id>')
api.add_resource(ActorExecutionResultsResource, '/actors/<string:actor_id>/executions/<string:execution_id>/results')
api.add_resource(ActorNoncesResource, '/actors/<string:actor_id>/nonces')
api.add_resource(ActorNonceResource, '/actors/<string:actor_id>/nonces/<string:nonce_id>')
api.add_resource(ActorExecutionLogsResource, '/actors/<string:actor_id>/executions/<string:execution_id>/logs')

api.add_resource(AdaptersResource, '/adapters')
api.add_resource(AdapterResource, '/adapters/<string:adapter_id>')
api.add_resource(AdapterMessagesResource, '/adapters/<string:adapter_id>/data')
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
