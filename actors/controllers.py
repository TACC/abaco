import json
import configparser
import requests
import datetime

from flask import g, request, render_template, make_response, Response
from flask_restful import Resource, Api, inputs
from werkzeug.exceptions import BadRequest
from agaveflask.utils import RequestParser, ok

from auth import check_permissions, get_tas_data, tenant_can_use_tas, get_uid_gid_homedir
from channels import ActorMsgChannel, CommandChannel, ExecutionResultsChannel
from codes import SUBMITTED, PERMISSION_LEVELS, READ, UPDATE, PERMISSION_LEVELS, PermissionLevel
from config import Config
from errors import DAOError, ResourceError, PermissionsException, WorkerException
from models import dict_to_camel, Actor, Execution, ExecutionsSummary, Nonce, Worker, get_permissions, \
    set_permission, get_current_utc_time

from mounts import get_all_mounts
import codes
from stores import actors_store, workers_store, executions_store, logs_store, nonce_store, permissions_store
from worker import shutdown_workers, shutdown_worker

from prometheus_client import start_http_server, Summary, MetricsHandler, Counter, Gauge, generate_latest

from agaveflask.logs import get_logger
logger = get_logger(__name__)
CONTENT_TYPE_LATEST = str('text/plain; version=0.0.4; charset=utf-8')
PROMETHEUS_URL = 'http://172.17.0.1:9090'
message_gauges = {}
rate_gauges = {}
last_metric = {}


class MetricsResource(Resource):
    def get(self):
        actor_ids = self.get_metrics()
        self.check_metrics(actor_ids)
        # self.add_workers(actor_ids)
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

    def get_metrics(self):
        logger.debug("top of get in MetricResource")

        actor_ids = [
            db_id
            for db_id, _
            in actors_store.items()
        ]
        logger.debug("ACTOR IDS: {}".format(actor_ids))
        try:
            if actor_ids:
                for actor_id in actor_ids:
                    if actor_id not in message_gauges.keys():
                        try:
                            g = Gauge(
                                'message_count_for_actor_{}'.format(actor_id.decode("utf-8").replace('-', '_')),
                                'Number of messages for actor {}'.format(actor_id.decode("utf-8").replace('-', '_'))
                            )
                            message_gauges.update({actor_id: g})
                        except Exception as e:
                            logger.info("got exception trying to instantiate the Gauge: {}".format(e))
                    else:
                        g = message_gauges[actor_id]

                    try:
                        ch = ActorMsgChannel(actor_id=actor_id.decode("utf-8"))
                    except Exception as e:
                        logger.error("Exception connecting to ActorMsgChannel: {}".format(e))
                        raise e
                    result = {'messages': len(ch._queue._queue)}
                    ch.close()
                    g.set(result['messages'])
                    logger.debug("METRICS: {} messages found for actor: {}.".format(result['messages'], actor_id))
                return actor_ids
        except Exception as e:
            logger.info("Got exception in get_metrics: {}".format(e))
            return []

    def check_metrics(self, actor_ids):
        for actor_id in actor_ids:
            logger.debug("TOP OF CHECK METRICS")

            query = {
                'query': 'message_count_for_actor_{}'.format(actor_id.decode("utf-8").replace('-', '_')),
                'time': datetime.datetime.utcnow().isoformat() + "Z"
            }
            r = requests.get(PROMETHEUS_URL + '/api/v1/query', params=query)
            data = json.loads(r.text)['data']['result']

            change_rate = 0
            try:
                previous_data = last_metric[actor_id]
                try:
                    change_rate = int(data[0]['value'][1]) - int(previous_data[0]['value'][1])
                except:
                    logger.debug("Could not calculate change rate.")
            except:
                logger.info("No previous data yet for new actor {}".format(actor_id))

            last_metric.update({actor_id: data})
            # Add a worker if message count reaches a given number
            try:
                logger.debug("METRICS current message count: {}".format(data[0]['value'][1]))
                if int(data[0]['value'][1]) >= 1:
                    tenant, aid = actor_id.decode('utf8').split('_')
                    logger.debug('METRICS Attempting to create a new worker for {}'.format(actor_id))
                    try:
                        # create a worker & add to this actor
                        actor = Actor.from_db(actors_store[actor_id])
                        worker_ids = [Worker.request_worker(tenant=tenant, actor_id=aid)]
                        logger.info("New worker id: {}".format(worker_ids[0]))
                        ch = CommandChannel()
                        ch.put_cmd(actor_id=actor.db_id,
                                   worker_ids=worker_ids,
                                   image=actor.image,
                                   tenant=tenant,
                                   num=1,
                                   stop_existing=False)
                        ch.close()
                        logger.debug('METRICS Added worker successfully for {}'.format(actor_id))
                    except Exception as e:
                        logger.debug("METRICS - SOMETHING BROKE: {} - {} - {}".format(type(e), e, e.args))
                elif int(data[0]['value'][1]) <= 1:
                    logger.debug("METRICS made it to scale down block")
                    # Check the number of workers for this actor before deciding to scale down
                    workers = Worker.get_workers(actor_id)
                    logger.debug('METRICS NUMBER OF WORKERS: {}'.format(len(workers)))
                    try:
                        if len(workers) == 1:
                            logger.debug("METRICS only one worker, won't scale down")
                        else:
                            while len(workers) > 0:
                                logger.debug('METRICS made it STATUS check')
                                worker = workers.popitem()[1]
                                logger.debug('METRICS SCALE DOWN current worker: {}'.format(worker['status']))
                                # check status of the worker is ready
                                if worker['status'] == 'READY':
                                    logger.debug("METRICS I MADE IT")
                                    # scale down
                                    try:
                                        shutdown_worker(worker['id'])
                                        continue
                                    except Exception as e:
                                        logger.debug('METRICS ERROR shutting down worker: {} - {} - {}'.format(type(e), e, e.args))
                                    logger.debug('METRICS shut down worker {}'.format(worker['id']))

                    except IndexError:
                        logger.debug('METRICS only one worker found for actor {}. '
                                     'Will not scale down'.format(actor_id))
                    except Exception as e:
                        logger.debug("METRICS SCALE UP FAILED: {}".format(e))


            except Exception as e:
                logger.debug("METRICS - ANOTHER ERROR: {} - {} - {}".format(type(e), e, e.args))


    def test_metrics(self):
        logger.debug("METRICS TESTING")


class AdminActorsResource(Resource):
    def get(self):
        logger.debug("top of GET /admin/actors")
        actors = []
        for k, v in actors_store.items():
            actor = Actor.from_db(v)
            actor.workers = Worker.get_workers(actor.db_id)
            for id, worker in actor.workers.items():
                actor.worker = worker
                break
            ch = ActorMsgChannel(actor_id=actor.db_id)
            actor.messages = len(ch._queue._queue)
            ch.close()
            summary = ExecutionsSummary(db_id=actor.db_id)
            actor.executions = summary.total_executions
            actor.runtime = summary.total_runtime
            actors.append(actor)
        logger.info("actors retrieved.")
        return ok(result=actors, msg="Actors retrieved successfully.")

class AdminWorkersResource(Resource):
    def get(self):
        logger.debug("top of GET /admin/workers")
        workers_result = []
        summary = {'total_workers': 0,
                   'ready_workers': 0,
                   'requested_workers': 0,
                   'error_workers': 0,
                   'busy_workers': 0,
                   'actors_no_workers': 0}
        case = Config.get('web', 'case')
        # the workers_store objects have a kev:value structure where the key is the actor_id and
        # the value it the worker object (iself, a dictionary).
        for actor_id, workers in workers_store.items():
            # we keep entries in the store for actors that have no workers, so need to skip those:
            if not workers:
                summary['actors_no_workers'] += 1
                continue
            # otherwise, we have an actor with workers:
            for worker_id, worker in workers.items():
                worker.update({'id': worker_id})
                w = Worker(**worker)
                w = w.display()
                # add additional fields
                actor_display_id = Actor.get_display_id(worker.get('tenant'), actor_id.decode("utf-8"))
                w.update({'actor_id': actor_display_id})
                w.update({'actor_dbid': actor_id.decode("utf-8")})
                # convert additional fields to case, as needed
                if case == 'camel':
                    w = dict_to_camel(w)
                workers_result.append(w)
                summary['total_workers'] += 1
                if worker.get('status') == codes.REQUESTED:
                    summary['requested_workers'] += 1
                elif worker.get('status') == codes.READY:
                    summary['ready_workers'] += 1
                elif worker.get('status') == codes.ERROR:
                    summary['error_workers'] += 1
                elif worker.get('status') == codes.BUSY:
                    summary['busy_workers'] += 1
        logger.info("workers retrieved.")
        if case == 'camel':
            summary = dict_to_camel(summary)
        result = {'summary': summary,
                  'workers': workers_result}
        return ok(result=result, msg="Workers retrieved successfully.")


class AdminExecutionsResource(Resource):

    def get(self):
        logger.debug("top of GET /admin/workers")
        result = {'summary': {'total_actors_all': 0,
                              'total_executions_all': 0,
                              'total_execution_runtime_all': 0,
                              'total_execution_cpu_all': 0,
                              'total_execution_io_all': 0,
                              'total_actors_existing': 0,
                              'total_executions_existing': 0,
                              'total_execution_runtime_existing': 0,
                              'total_execution_cpu_existing': 0,
                              'total_execution_io_existing': 0,
                              },
                  'actors': []
        }
        case = Config.get('web', 'case')
        for actor_dbid, executions in executions_store.items():
            # determine if actor still exists:
            actor = None
            try:
                actor = Actor.from_db(actors_store[actor_dbid])
            except KeyError:
                pass
            # iterate over executions for this actor:
            actor_exs = 0
            actor_runtime = 0
            actor_io = 0
            actor_cpu = 0
            for ex_id, execution in executions.items():
                actor_exs += 1
                actor_runtime += execution.get('runtime', 0)
                actor_io += execution.get('io', 0)
                actor_cpu += execution.get('cpu', 0)
            # always add these to the totals:
            result['summary']['total_actors_all'] += 1
            result['summary']['total_executions_all'] += actor_exs
            result['summary']['total_execution_runtime_all'] += actor_runtime
            result['summary']['total_execution_io_all'] += actor_io
            result['summary']['total_execution_cpu_all'] += actor_cpu

            if actor:
                result['summary']['total_actors_existing'] += 1
                result['summary']['total_executions_existing'] += actor_exs
                result['summary']['total_execution_runtime_existing'] += actor_runtime
                result['summary']['total_execution_io_existing'] += actor_io
                result['summary']['total_execution_cpu_existing'] += actor_cpu
                actor_stats = {'actor_id': actor.get('id'),
                               'owner': actor.get('owner'),
                               'image': actor.get('image'),
                               'total_executions': actor_exs,
                               'total_execution_cpu': actor_cpu,
                               'total_execution_io': actor_io,
                               'total_execution_runtime': actor_runtime,
                               }
                if case == 'camel':
                    actor_stats = dict_to_camel(actor_stats)
                result['actors'].append(actor_stats)

        if case == 'camel':
            result['summary'] = dict_to_camel(result['summary'])
        return ok(result=result, msg="Executions retrieved successfully.")


class ActorsResource(Resource):

    def get(self):
        logger.debug("top of GET /actors")

        actors = []
        for k, v in actors_store.items():
            if v['tenant'] == g.tenant:
                actor = Actor.from_db(v)
                if check_permissions(g.user, actor.db_id, READ):
                    actors.append(actor.display())
        logger.info("actors retrieved.")
        return ok(result=actors, msg="Actors retrieved successfully.")

    def validate_post(self):
        parser = Actor.request_parser()
        try:
            args = parser.parse_args()
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            else:
                msg = '{}: {}'.format(msg, e)
            raise DAOError("Invalid actor description: {}".format(msg))
        return args

    def post(self):
        logger.info("top of POST to register a new actor.")
        args = self.validate_post()
        logger.debug("validate_post() successful")
        args['tenant'] = g.tenant
        args['api_server'] = g.api_server
        args['owner'] = g.user
        use_container_uid = args.get('use_container_uid')
        if Config.get('web', 'case') == 'camel':
            use_container_uid = args.get('useContainerUid')
        if not use_container_uid:
            uid, gid, home_dir = get_uid_gid_homedir(args, g.user, g.tenant)
            if uid:
                args['uid'] = uid
            if gid:
                args['gid'] = gid
            if home_dir:
                args['tasdir'] = home_dir
        args['mounts'] = get_all_mounts(args)
        logger.debug("create args: {}".format(args))
        actor = Actor(**args)
        actors_store[actor.db_id] = actor.to_db()
        logger.debug("new actor saved in db. id: {}. image: {}. tenant: {}".format(actor.db_id,
                                                                                   actor.image,
                                                                                   actor.tenant))
        actor.ensure_one_worker()
        logger.debug("ensure_one_worker() called")
        set_permission(g.user, actor.db_id, UPDATE)
        logger.debug("UPDATE permission added to user: {}".format(g.user))
        return ok(result=actor.display(), msg="Actor created successfully.", request=request)


class ActorResource(Resource):
    def get(self, actor_id):
        logger.debug("top of GET /actors/{}".format(actor_id))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            actor = Actor.from_db(actors_store[dbid])
        except KeyError:
            logger.debug("did not find actor with id: {}".format(actor_id))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        logger.debug("found actor {}".format(actor_id))
        return ok(result=actor.display(), msg="Actor retrieved successfully.")

    def delete(self, actor_id):
        logger.debug("top of DELETE /actors/{}".format(actor_id))
        id = Actor.get_dbid(g.tenant, actor_id)
        logger.info("calling shutdown_workers() for actor: {}".format(id))
        shutdown_workers(id)
        logger.debug("shutdown_workers() done")
        try:
            actor = Actor.from_db(actors_store[id])
            executions = actor.get('executions') or {}
            for ex_id, val in executions.items():
                del logs_store[ex_id]
        except KeyError as e:
            logger.info("got KeyError {} trying to retrieve actor or executions with id {}".format(
                e, id))
        # delete the actor's message channel
        # TODO - needs work; each worker is subscribed to the ActorMsgChannel. If the workers are not
        # closed before the ch.delete() below, the ActorMsgChannel will survive.
        try:
            ch = ActorMsgChannel(actor_id=id)
            ch.delete()
            logger.info("Deleted actor message channel for actor: {}".format(id))
        except Exception as e:
            # if we get an error trying to remove the inbox, log it but keep going
            logger.error("Unable to delete the actor's message channel for actor: {}, exception: {}".format(id, e))
        del actors_store[id]
        logger.info("actor {} deleted from store.".format(id))
        del permissions_store[id]
        logger.info("actor {} permissions deleted from store.".format(id))
        del nonce_store[id]
        logger.info("actor {} nonnces delete from nonce store.".format(id))
        return ok(result=None, msg='Actor deleted successfully.')

    def put(self, actor_id):
        logger.debug("top of PUT /actors/{}".format(actor_id))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            actor = Actor.from_db(actors_store[dbid])
        except KeyError:
            logger.debug("did not find actor {} in store.".format(dbid))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        previous_image = actor.image
        previous_status = actor.status
        previous_owner = actor.owner
        args = self.validate_put(actor)
        logger.debug("PUT args validated successfully.")
        args['tenant'] = g.tenant
        # user can force an update by setting the force param:
        update_image = args.get('force')
        if not update_image and args['image'] == previous_image:
            logger.debug("new image is the same and force was false. not updating actor.")
            logger.debug("Setting status to the actor's previous status which is: {}".format(previous_status))
            args['status'] = previous_status
        else:
            update_image = True
            args['status'] = SUBMITTED
            logger.debug("new image is different. updating actor.")
        args['api_server'] = g.api_server

        # we do not allow a PUT to override the owner in case the PUT is issued by another user
        args['owner'] = previous_owner

        use_container_uid = args.get('use_container_uid')
        if Config.get('web', 'case') == 'camel':
            use_container_uid = args.get('useContainerUid')
        if not use_container_uid:
            uid, gid, home_dir = get_uid_gid_homedir(args, g.user, g.tenant)
            if uid:
                args['uid'] = uid
            if gid:
                args['gid'] = gid
            if home_dir:
                args['tasdir'] = home_dir
        args['mounts'] = get_all_mounts(args)
        args['last_update_time'] = get_current_utc_time()
        logger.debug("update args: {}".format(args))
        actor = Actor(**args)
        actors_store[actor.db_id] = actor.to_db()
        logger.info("updated actor {} stored in db.".format(actor_id))
        if update_image:
            worker_ids = [Worker.request_worker(tenant=g.tenant, actor_id=actor.db_id)]
            ch = CommandChannel()
            ch.put_cmd(actor_id=actor.db_id, worker_ids=worker_ids, image=actor.image, tenant=args['tenant'])
            ch.close()
            logger.debug("put new command on command channel to update actor.")
        # put could have been issued by a user with
        if not previous_owner == g.user:
            set_permission(g.user, actor.db_id, UPDATE)
        return ok(result=actor.display(),
                  msg="Actor updated successfully.")

    def validate_put(self, actor):
        # inherit derived attributes from the original actor, including id and db_id:
        parser = Actor.request_parser()
        # remove since name is only required for POST, not PUT
        parser.remove_argument('name')
        parser.add_argument('force', type=bool, required=False, help="Whether to force an update of the actor image", default=False)

        # if camel case, need to remove fields snake case versions of fields that can be updated
        if Config.get('web', 'case') == 'camel':
            actor.pop('use_container_uid')
            actor.pop('default_environment')

        # this update overrides all required and optional attributes
        try:
            new_fields = parser.parse_args()
            logger.debug("new fields from actor PUT: {}".format(new_fields))
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            else:
                msg = '{}: {}'.format(msg, e)
            raise DAOError("Invalid actor description: {}".format(msg))
        if not actor.stateless and new_fields.get('stateless'):
            raise DAOError("Invalid actor description: an actor that was not stateless cannot be update to be stateless.")
        actor.update(new_fields)
        return actor


class ActorStateResource(Resource):
    def get(self, actor_id):
        logger.debug("top of GET /actors/{}/state".format(actor_id))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            actor = Actor.from_db(actors_store[dbid])
        except KeyError:
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        return ok(result={'state': actor.get('state') }, msg="Actor state retrieved successfully.")

    def post(self, actor_id):
        logger.debug("top of POST /actors/{}/state".format(actor_id))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            actor = Actor.from_db(actors_store[dbid])
        except KeyError:
            logger.debug("did not find actor with id: {}.".format(actor_id))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        if actor.stateless:
            logger.debug("cannot update state for stateless actor: {}".format(actor_id))
            raise ResourceError("actor is stateless.", 404)
        state = self.validate_post()
        logger.debug("state post params validated: {}".format(actor_id))
        actors_store.update(dbid, 'state', state)
        logger.info("state updated: {}".format(actor_id))
        actor = Actor.from_db(actors_store[dbid])
        return ok(result=actor.display(), msg="State updated successfully.")

    def validate_post(self):
        json_data = request.get_json()
        if not json_data:
            raise DAOError("Invalid actor state description: state must be JSON serializable.")
        return json_data


class ActorExecutionsResource(Resource):
    def get(self, actor_id):
        logger.debug("top of GET /actors/{}/executions".format(actor_id))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            actor = Actor.from_db(actors_store[dbid])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        try:
            summary = ExecutionsSummary(db_id=dbid)
        except DAOError as e:
            logger.debug("did not find executions summary: {}".format(actor_id))
            raise ResourceError("Could not retrieve executions summary for actor: {}. "
                                "Details: {}".format(actor_id, e), 404)
        return ok(result=summary.display(), msg="Actor executions retrieved successfully.")

    def post(self, actor_id):
        logger.debug("top of POST /actors/{}/executions".format(actor_id))
        id = Actor.get_dbid(g.tenant, actor_id)
        try:
            actor = Actor.from_db(actors_store[id])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        args = self.validate_post()
        logger.debug("execution post args validated: {}.".format(actor_id))
        Execution.add_execution(id, args)
        logger.info("execution added: {}.".format(actor_id))
        return ok(result=actor.display(), msg="Actor execution added successfully.")

    def validate_post(self):
        parser = RequestParser()
        parser.add_argument('runtime', type=str, required=True, help="Runtime, in milliseconds, of the execution.")
        parser.add_argument('cpu', type=str, required=True, help="CPU usage, in user jiffies, of the execution.")
        parser.add_argument('io', type=str, required=True, help="Block I/O usage, in number of 512-byte sectors read from and written to, by the execution.")
        # Accounting for memory is quite hard -- probably easier to cap all containers at a fixed amount or perhaps have
        # a graduated list of cap sized (e.g. small, medium and large).
        # parser.add_argument('mem', type=str, required=True, help="Memory usage, , of the execution.")
        try:
            args = parser.parse_args()
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            raise DAOError("Invalid actor execution description: {}".format(msg))

        for k,v in args.items():
            try:
                int(v)
            except ValueError:
                raise ResourceError(message="Argument {} must be an integer.".format(k))
        return args


class ActorNoncesResource(Resource):
    """Manage nonces for an actor"""

    def get(self, actor_id):
        logger.debug("top of GET /actors/{}/nonces".format(actor_id))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            Actor.from_db(actors_store[dbid])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        nonces = Nonce.get_nonces(actor_id=dbid)
        return ok(result=[n.display() for n in nonces], msg="Actor nonces retrieved successfully.")

    def post(self, actor_id):
        """Create a new nonce for an actor."""
        logger.debug("top of POST /actors/{}/nonces".format(actor_id))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            Actor.from_db(actors_store[dbid])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        args = self.validate_post()
        logger.debug("nonce post args validated: {}.".format(actor_id))

        # supply "provided" fields:
        args['tenant'] = g.tenant
        args['api_server'] = g.api_server
        args['db_id'] = dbid
        args['owner'] = g.user
        args['roles'] = g.roles

        # create and store the nonce:
        nonce = Nonce(**args)
        Nonce.add_nonce(dbid, nonce)
        logger.info("nonce added for actor: {}.".format(actor_id))
        return ok(result=nonce.display(), msg="Actor nonce created successfully.")

    def validate_post(self):
        parser = Nonce.request_parser()
        try:
            args = parser.parse_args()
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            raise DAOError("Invalid nonce description: {}".format(msg))
        # additional checks
        if 'level' in args:
            if not args['level'] in PERMISSION_LEVELS:
                raise DAOError("Invalid nonce description. "
                               "The level attribute must be one of: {}".format(PERMISSION_LEVELS))
        if Config.get('web', 'case') == 'snake':
            if 'max_uses' in args:
                self.validate_max_uses(args['max_uses'])
        else:
            if 'maxUses' in args:
                self.validate_max_uses(args['maxUses'])
        return args

    def validate_max_uses(self, max_uses):
        try:
            m = int(max_uses)
        except Exception:
            raise DAOError("The max uses parameter must be an integer.")
        if m ==0 or m < -1:
            raise DAOError("The max uses parameter must be a positive integer or -1 "
                           "(to denote unlimited uses).")


class ActorNonceResource(Resource):
    """Manage a specific nonce for an actor"""

    def get(self, actor_id, nonce_id):
        """Lookup details about a nonce."""
        logger.debug("top of GET /actors/{}/nonces/{}".format(actor_id, nonce_id))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            Actor.from_db(actors_store[dbid])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        nonce = Nonce.get_nonce(actor_id=dbid, nonce_id=nonce_id)
        return ok(result=nonce.display(), msg="Actor nonce retrieved successfully.")


    def delete(self, actor_id, nonce_id):
        """Delete a nonce."""
        logger.debug("top of DELETE /actors/{}/nonces/{}".format(actor_id, nonce_id))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            Actor.from_db(actors_store[dbid])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        Nonce.delete_nonce(dbid, nonce_id)
        return ok(result=None, msg="Actor nonce deleted successfully.")


class ActorExecutionResource(Resource):
    def get(self, actor_id, execution_id):
        logger.debug("top of GET /actors/{}/executions/{}.".format(actor_id, execution_id))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            actors_store[dbid]
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        try:
            excs = executions_store[dbid]
        except KeyError:
            logger.debug("did not find executions: {}.".format(actor_id))
            raise ResourceError("No executions found for actor {}.".format(actor_id))
        try:
            exc = Execution.from_db(excs[execution_id])
        except KeyError:
            logger.debug("did not find execution: {}. actor: {}.".format(execution_id,
                                                                         actor_id))
            raise ResourceError("Execution not found {}.".format(execution_id))
        return ok(result=exc.display(), msg="Actor execution retrieved successfully.")


class ActorExecutionResultsResource(Resource):
    def get(self, actor_id, execution_id):
        logger.debug("top of GET /actors/{}/executions/{}/results".format(actor_id, execution_id))
        # check that actor exists
        id = Actor.get_dbid(g.tenant, actor_id)
        try:
            Actor.from_db(actors_store[id])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        ch = ExecutionResultsChannel(actor_id=id, execution_id=execution_id)
        try:
            result = ch.get(timeout=0.1)
        except:
            result = ''
        response = make_response(result)
        response.headers['content-type'] = 'application/octet-stream'
        ch.close()
        return response
        # todo -- build support a list of results as a multipart response with boundaries?
        # perhaps look at the requests toolbelt MultipartEncoder: https://github.com/requests/toolbelt
        # result = []
        # num = 0
        # limit = request.args.get('limit', 1)
        # logger.debug("limit: {}".format(limit))
        # while num < limit:
        #     try:
        #         result.append(ch.get(timeout=0.1))
        #         num += 1
        #     except Exception:
        #         break
        # logger.debug("collected {} results".format(num))
        # ch.close()
        # return Response(result)


class ActorExecutionLogsResource(Resource):
    def get(self, actor_id, execution_id):
        def get_hypermedia(actor, exc):
            return {'_links': {'self': '{}/actors/v2/{}/executions/{}/logs'.format(actor.api_server, actor.id, exc.id),
                               'owner': '{}/profiles/v2/{}'.format(actor.api_server, actor.owner),
                               'execution': '{}/actors/v2/{}/executions/{}'.format(actor.api_server, actor.id, exc.id)},
                    }
        logger.debug("top of GET /actors/{}/executions/{}/logs.".format(actor_id, execution_id))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            actor = Actor.from_db(actors_store[dbid])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        try:
            excs = executions_store[dbid]
        except KeyError:
            logger.debug("did not find executions. actor: {}.".format(actor_id))
            raise ResourceError("No executions found for actor {}.".format(actor_id))
        try:
            exc = Execution.from_db(excs[execution_id])
        except KeyError:
            logger.debug("did not find execution: {}. actor: {}.".format(execution_id, actor_id))
            raise ResourceError("Execution {} not found.".format(execution_id))
        try:
            logs = logs_store[execution_id]
        except KeyError:
            logger.debug("did not find logs. execution: {}. actor: {}.".format(execution_id, actor_id))
            logs = ""
        result={'logs': logs}
        result.update(get_hypermedia(actor, exc))
        return ok(result, msg="Logs retrieved successfully.")


class MessagesResource(Resource):

    def get(self, actor_id):
        def get_hypermedia(actor):
            return {'_links': {'self': '{}/actors/v2/{}/messages'.format(actor.api_server, actor.id),
                               'owner': '{}/profiles/v2/{}'.format(actor.api_server, actor.owner),
                               },
                       }
        logger.debug("top of GET /actors/{}/messages".format(actor_id))
        # check that actor exists
        id = Actor.get_dbid(g.tenant, actor_id)
        try:
            actor = Actor.from_db(actors_store[id])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        ch = ActorMsgChannel(actor_id=id)
        result = {'messages': len(ch._queue._queue)}
        ch.close()
        logger.debug("messages found for actor: {}.".format(actor_id))
        result.update(get_hypermedia(actor))
        return ok(result)

    def validate_post(self):
        logger.debug("validating message payload.")
        parser = RequestParser()
        parser.add_argument('message', type=str, required=False, help="The message to send to the actor.")
        args = parser.parse_args()
        # if a special 'message' object isn't passed, use entire POST payload as message
        if not args.get('message'):
            logger.debug("POST body did not have a message field.")
            # first check for binary data:
            if request.headers.get('Content-Type') == 'application/octet-stream':
                # ensure not sending too much data
                length = request.headers.get('Content-Length')
                if not length:
                    raise ResourceError("Content Length required for application/octet-stream.")
                try:
                    int(length)
                except Exception:
                    raise ResourceError("Content Length must be an integer.")
                if int(length) > int(Config.get('web', 'max_content_length')):
                    raise ResourceError("Message exceeds max content length of: {}".format(Config.get('web', 'max_content_length')))
                logger.debug("using get_data, setting content type to application/octet-stream.")
                args['message'] = request.get_data()
                args['_abaco_Content_Type'] = 'application/octet-stream'
                return args
            json_data = request.get_json()
            if json_data:
                logger.debug("message was JSON data.")
                args['message'] = json_data
                args['_abaco_Content_Type'] = 'application/json'
            else:
                logger.debug("message was NOT JSON data.")
                # try to get data for mime types not recognized by flask. flask creates a python string for these
                try:
                    args['message'] = json.loads(request.data)
                except TypeError:
                    logger.debug("message POST body could not be serialized. args: {}".format(args))
                    raise DAOError('message POST body could not be serialized. Pass JSON data or use the message attribute.')
                args['_abaco_Content_Type'] = 'str'
        else:
            # the special message object is a string
            logger.debug("POST body has a message field. Setting _abaco_Content_type to 'str'.")
            args['_abaco_Content_Type'] = 'str'
        return args

    def post(self, actor_id):
        def get_hypermedia(actor, exc):
            return {'_links': {'self': '{}/actors/v2/{}/executions/{}'.format(actor.api_server, actor.id, exc),
                               'owner': '{}/profiles/v2/{}'.format(actor.api_server, actor.owner),
                               'messages': '{}/actors/v2/{}/messages'.format(actor.api_server, actor.id)},}

        logger.debug("top of POST /actors/{}/messages.".format(actor_id))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            Actor.from_db(actors_store[dbid])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError("No actor found with id: {}.".format(actor_id), 404)
        args = self.validate_post()
        d = {}
        # build a dictionary of k:v pairs from the query parameters, and pass a single
        # additional object 'message' from within the post payload. Note that 'message'
        # need not be JSON data.
        logger.debug("POST body validated. actor: {}.".format(actor_id))
        for k, v in request.args.items():
            if k == 'message':
                continue
            d[k] = v
        logger.debug("extra fields added to message from query parameters: {}.".format(d))
        if hasattr(g, 'user'):
            d['_abaco_username'] = g.user
            logger.debug("_abaco_username: {} added to message.".format(g.user))
        if hasattr(g, 'api_server'):
            d['_abaco_api_server'] = g.api_server
            logger.debug("_abaco_api_server: {} added to message.".format(g.api_server))
        # if hasattr(g, 'jwt'):
        #     d['_abaco_jwt'] = g.jwt
        # if hasattr(g, 'jwt_server'):
        #     d['_abaco_jwt_server'] = g.jwt_server
        if hasattr(g, 'jwt_header_name'):
            d['_abaco_jwt_header_name'] = g.jwt_header_name
            logger.debug("abaco_jwt_header_name: {} added to message.".format(g.jwt_header_name))

        # create an execution
        exc = Execution.add_execution(dbid, {'cpu': 0,
                                             'io': 0,
                                             'runtime': 0,
                                             'status': SUBMITTED,
                                             'executor': g.user})
        logger.info("Execution {} added for actor {}".format(exc, actor_id))
        d['_abaco_execution_id'] = exc
        d['_abaco_Content_Type'] = args.get('_abaco_Content_Type', '')
        logger.debug("Final message dictionary: {}".format(d))
        ch = ActorMsgChannel(actor_id=dbid)
        ch.put_msg(message=args['message'], d=d)
        ch.close()
        logger.debug("Message added to actor inbox. id: {}.".format(actor_id))
        # make sure at least one worker is available
        actor = Actor.from_db(actors_store[dbid])
        actor.ensure_one_worker()
        logger.debug("ensure_one_worker() called. id: {}.".format(actor_id))
        if args.get('_abaco_Content_Type') == 'application/octet-stream':
            result = {'execution_id': exc, 'msg': 'binary - omitted'}
        else:
            result={'execution_id': exc, 'msg': args['message']}
        result.update(get_hypermedia(actor, exc))
        case = Config.get('web', 'case')
        if not case == 'camel':
            return ok(result)
        else:
            return ok(dict_to_camel(result))


class WorkersResource(Resource):
    def get(self, actor_id):
        logger.debug("top of GET /actors/{}/workers for tenant {}.".format(actor_id, g.tenant))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            Actor.from_db(actors_store[dbid])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError("No actor found with id: {}.".format(actor_id), 404)
        try:
            workers = Worker.get_workers(dbid)
        except WorkerException as e:
            logger.debug("did not find workers for actor: {}.".format(actor_id))
            raise ResourceError(e.msg, 404)
        result = []
        for id, worker in workers.items():
            worker.update({'id': id})
            try:
                w = Worker(**worker)
                result.append(w.display())
            except Exception as e:
                logger.error("Unable to instantiate worker in workers endpoint from description: {}. ".format(worker))
        return ok(result=result, msg="Workers retrieved successfully.")

    def validate_post(self):
        parser = RequestParser()
        parser.add_argument('num', type=int, help="Number of workers to start (default is 1).")
        try:
            args = parser.parse_args()
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            raise DAOError("Invalid POST: {}".format(msg))
        return args

    def post(self, actor_id):
        """Ensure a certain number of workers are running for an actor"""
        logger.debug("top of POST /actors/{}/workers.".format(actor_id))
        id = Actor.get_dbid(g.tenant, actor_id)
        try:
            actor = Actor.from_db(actors_store[id])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError("No actor found with id: {}.".format(actor_id), 404)
        args = self.validate_post()
        logger.debug("workers POST params validated. actor: {}.".format(actor_id))
        num = args.get('num')
        if not num or num == 0:
            logger.debug("did not get a num: {}.".format(actor_id))
            num = 1
        logger.debug("ensuring at least {} workers. actor: {}.".format(num, actor_id))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            workers = Worker.get_workers(dbid)
        except WorkerException as e:
            logger.debug("did not find workers for actor: {}.".format(actor_id))
            raise ResourceError(e.msg, 404)
        current_number_workers = len(workers.items())
        if current_number_workers < num:
            logger.debug("There were only {} workers for actor: {} so we're adding more.".format(current_number_workers,
                                                                                                 actor_id))
            num_to_add = int(num) - len(workers.items())
            logger.info("adding {} more workers for actor {}".format(num_to_add, actor_id))
            for idx in range(num_to_add):
                # send num_to_add messages to add 1 worker so that messages are spread across multiple
                # spawners.
                worker_ids = [Worker.request_worker(tenant=g.tenant,
                                                        actor_id=dbid)]
                logger.info("New worker id: {}".format(worker_ids[0]))
                ch = CommandChannel()
                ch.put_cmd(actor_id=actor.db_id,
                           worker_ids=worker_ids,
                           image=actor.image,
                           tenant=g.tenant,
                           num=1,
                           stop_existing=False)
            ch.close()
            logger.info("Message put on command channel for new worker ids: {}".format(worker_ids))
            return ok(result=None, msg="Scheduled {} new worker(s) to start. Previously, there were {} workers.".format(num_to_add, current_number_workers))
        else:
            return ok(result=None, msg="Actor {} already had {} worker(s).".format(actor_id, num))


class WorkerResource(Resource):
    def get(self, actor_id, worker_id):
        logger.debug("top of GET /actors/{}/workers/{}.".format(actor_id, worker_id))
        id = Actor.get_dbid(g.tenant, actor_id)
        try:
            Actor.from_db(actors_store[id])
        except KeyError:
            logger.debug("Did not find actor: {}.".format(actor_id))
            raise ResourceError("No actor found with id: {}.".format(actor_id), 404)
        try:
            worker = Worker.get_worker(id, worker_id)
        except WorkerException as e:
            logger.debug("Did not find worker: {}. actor: {}.".format(worker_id, actor_id))
            raise ResourceError(e.msg, 404)
        # worker is an honest python dictionary with a single key, the id of the worker. need to
        # convert it to a Worker object
        worker.update({'id': worker_id})
        w = Worker(**worker)
        return ok(result=w.display(), msg="Worker retrieved successfully.")

    def delete(self, actor_id, worker_id):
        logger.debug("top of DELETE /actors/{}/workers/{}.".format(actor_id, worker_id))
        id = Actor.get_dbid(g.tenant, actor_id)
        try:
            worker = Worker.get_worker(id, worker_id)
        except WorkerException as e:
            logger.debug("Did not find worker: {}. actor: {}.".format(worker_id, actor_id))
            raise ResourceError(e.msg, 404)
        # if the worker is in requested status, we shouldn't try to shut it down because it doesn't exist yet;
        # we just need to remove the worker record from the workers_store.
        # TODO - if worker.status == 'REQUESTED' ....
        logger.info("calling shutdown_worker(). worker: {}. actor: {}.".format(worker_id, actor_id))
        shutdown_worker(worker['id'])
        logger.info("shutdown_worker() called for worker: {}. actor: {}.".format(worker_id, actor_id))
        return ok(result=None, msg="Worker scheduled to be stopped.")


class PermissionsResource(Resource):
    def get(self, actor_id):
        logger.debug("top of GET /actors/{}/permissions.".format(actor_id))
        id = Actor.get_dbid(g.tenant, actor_id)
        try:
            Actor.from_db(actors_store[id])
        except KeyError:
            logger.debug("Did not find actor: {}.".format(actor_id))
            raise ResourceError("No actor found with id: {}.".format(actor_id), 404)
        try:
            permissions = get_permissions(id)
        except PermissionsException as e:
            logger.debug("Did not find permissions. actor: {}.".format(actor_id))
            raise ResourceError(e.msg, 404)
        return ok(result=permissions, msg="Permissions retrieved successfully.")

    def validate_post(self):
        parser = RequestParser()
        parser.add_argument('user', type=str, required=True, help="User owning the permission.")
        parser.add_argument('level', type=str, required=True,
                            help="Level of the permission: {}".format(PERMISSION_LEVELS))
        try:
            args = parser.parse_args()
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            raise DAOError("Invalid permissions description: {}".format(msg))

        if not args['level'] in PERMISSION_LEVELS:
            raise ResourceError("Invalid permission level: {}. \
            The valid values are {}".format(args['level'], PERMISSION_LEVELS))
        return args

    def post(self, actor_id):
        """Add new permissions for an actor"""
        logger.debug("top of POST /actors/{}/permissions.".format(actor_id))
        id = Actor.get_dbid(g.tenant, actor_id)
        try:
            Actor.from_db(actors_store[id])
        except KeyError:
            logger.debug("Did not find actor: {}.".format(actor_id))
            raise ResourceError(
                "actor not found: {}'".format(actor_id), 404)
        args = self.validate_post()
        logger.debug("POST permissions body validated for actor: {}.".format(actor_id))
        set_permission(args['user'], id, PermissionLevel(args['level']))
        logger.info("Permission added for user: {} actor: {} level: {}".format(args['user'], id, args['level']))
        permissions = get_permissions(id)
        return ok(result=permissions, msg="Permission added successfully.")
