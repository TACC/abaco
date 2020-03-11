import configparser
import datetime
import json
import os
import requests
import threading
import time
import timeit

from channelpy.exceptions import ChannelClosedException, ChannelTimeoutException
from flask import g, request, render_template, make_response, Response
from flask_restful import Resource, Api, inputs
from werkzeug.exceptions import BadRequest
from agaveflask.utils import RequestParser, ok

from auth import check_permissions, get_tas_data, tenant_can_use_tas, get_uid_gid_homedir, get_token_default
from channels import ActorMsgChannel, CommandChannel, ExecutionResultsChannel, WorkerChannel
from codes import SUBMITTED, COMPLETE, SHUTTING_DOWN, PERMISSION_LEVELS, ALIAS_NONCE_PERMISSION_LEVELS, READ, UPDATE, EXECUTE, PERMISSION_LEVELS, PermissionLevel
from config import Config
from errors import DAOError, ResourceError, PermissionsException, WorkerException
from models import dict_to_camel, display_time, is_hashid, Actor, Alias, Execution, ExecutionsSummary, Nonce, Worker, get_permissions, \
    set_permission, get_current_utc_time

from mounts import get_all_mounts
import codes
from stores import actors_store, alias_store, clients_store, workers_store, executions_store, logs_store, nonce_store, permissions_store
from worker import shutdown_workers, shutdown_worker
import metrics_utils

from prometheus_client import start_http_server, Summary, MetricsHandler, Counter, Gauge, generate_latest

from agaveflask.logs import get_logger
logger = get_logger(__name__)
CONTENT_TYPE_LATEST = str('text/plain; version=0.0.4; charset=utf-8')
PROMETHEUS_URL = 'http://172.17.0.1:9090'
message_gauges = {}
rate_gauges = {}
last_metric = {}

clients_gauge = Gauge('clients_count_for_clients_store',
                      'Number of clients currently in the clients_store')



try:
    ACTOR_MAX_WORKERS = Config.get("spawner", "max_workers_per_actor")
except:
    ACTOR_MAX_WORKERS = os.environ.get('MAX_WORKERS_PER_ACTOR', 20)
ACTOR_MAX_WORKERS = int(ACTOR_MAX_WORKERS)
logger.info("METRICS - running with ACTOR_MAX_WORKERS = {}".format(ACTOR_MAX_WORKERS))

try:
    num_init_workers = int(Config.get('workers', 'init_count'))
except:
    num_init_workers = 1


class MetricsResource(Resource):
    def get(self):
        enable_autoscaling = Config.get('workers', 'autoscaling')
        if hasattr(enable_autoscaling, 'lower'):
            if not enable_autoscaling.lower() == 'true':
                return
        else:
            return
        actor_ids, inbox_lengths, cmd_length = self.get_metrics()
        self.check_metrics(actor_ids, inbox_lengths, cmd_length)
        # self.add_workers(actor_ids)
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

    def get_metrics(self):
        logger.debug("top of get in MetricResource")
        actor_ids = [actor['db_id'] for actor in actors_store.items()
            if actor.get('stateless')
            and not actor.get('status') == 'ERROR'
            and not actor.get('status') == SHUTTING_DOWN]

        try:
            if actor_ids:
                # Create a gauge for each actor id
                actor_ids, inbox_lengths, cmd_length = metrics_utils.create_gauges(actor_ids)

                # return the actor_ids so we can use them again for check_metrics
                return actor_ids, inbox_lengths, cmd_length
        except Exception as e:
            logger.info("Got exception in get_metrics: {}".format(e))
            return []

    def check_metrics(self, actor_ids, inbox_lengths, cmd_length):
        for actor_id in actor_ids:
            logger.debug("TOP OF CHECK METRICS for actor_id {}".format(actor_id))

            try:
                current_message_count = inbox_lengths[actor_id.decode("utf-8")]
            except KeyError as e:
                logger.error("Got KeyError trying to get current_message_count. Exception: {}".format(e))
                current_message_count = 0
            workers = Worker.get_workers(actor_id)
            actor = actors_store[actor_id]
            logger.debug('METRICS: MAX WORKERS TEST {}'.format(actor))

            # If this actor has a custom max_workers, use that. Otherwise use default.
            max_workers = None
            if actor.get('max_workers'):
                try:
                    max_workers = int(actor['max_workers'])
                except Exception as e:
                    logger.error("max_workers defined for actor_id {} but could not cast to int. "
                                 "Exception: {}".format(actor_id, e))
            if not max_workers:
                try:
                    conf = Config.get('spawner', 'max_workers_per_actor')
                    max_workers = int(conf)
                except Exception as e:
                    logger.error("Unable to get/cast max_workers_per_actor config ({}) to int. "
                                 "Exception: {}".format(conf, e))
                    max_workers = 1

            # Add an additional worker if message count reaches a given number
            try:
                logger.debug("METRICS current message count: {}".format(current_message_count))
                if metrics_utils.allow_autoscaling(max_workers, len(workers), cmd_length):
                    if current_message_count >= 1:
                        channel = metrics_utils.scale_up(actor_id)
                        if channel == 'default':
                            cmd_length = cmd_length + 1
                        logger.debug("METRICS current message count: {}".format(current_message_count))
                else:
                    logger.warning('METRICS - COMMAND QUEUE is getting full. Skipping autoscale.')
                if current_message_count == 0:
                    # first check if this is a "sync" actor
                    is_sync_actor = False
                    try:
                        hints = list(actor.get("hints"))
                    except:
                        hints = []
                    for hint in hints:
                        if hint == Actor.SYNC_HINT:
                            is_sync_actor = True
                            break
                    metrics_utils.scale_down(actor_id, is_sync_actor)
                    logger.debug("METRICS made it to scale down block")
                else:
                    logger.warning('METRICS - COMMAND QUEUE is getting full. Skipping autoscale.')
            except Exception as e:
                logger.debug("METRICS - ANOTHER ERROR: {} - {} - {}".format(type(e), e, e.args))

    def test_metrics(self):
        logger.debug("METRICS TESTING")


class AdminActorsResource(Resource):
    def get(self):
        logger.debug("top of GET /admin/actors")
        case = Config.get('web', 'case')
        actors = []
        for actor_info in actors_store.items():
            actor = Actor.from_db(actor_info)
            actor.workers = []
            for id, worker in Worker.get_workers(actor.db_id).items():
                if case == 'camel':
                    worker = dict_to_camel(worker)
                actor.workers.append(worker)
            ch = ActorMsgChannel(actor_id=actor.db_id)
            actor.messages = len(ch._queue._queue)
            ch.close()
            summary = ExecutionsSummary(db_id=actor.db_id)
            actor.executions = summary.total_executions
            actor.runtime = summary.total_runtime
            if case == 'camel':
                actor = dict_to_camel(actor)
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
        # the workers_store objects have a key:value structure where the key is the actor_id and
        # the value it the worker object (iself, a dictionary).
        for workers in workers_store.items(proj_inp=None):
            actor_id = workers['_id']
            del workers['_id']
            # we keep entries in the store for actors that have no workers, so need to skip those:
            if not workers:
                summary['actors_no_workers'] += 1
                continue
            # otherwise, we have an actor with workers:
            for worker_id, worker in workers.items():
                worker.update({'id': worker_id})
                w = Worker(**worker)
                # add additional fields
                actor_display_id = Actor.get_display_id(worker.get('tenant'), actor_id.decode("utf-8"))
                w.update({'actor_id': actor_display_id})
                w.update({'actor_dbid': actor_id.decode("utf-8")})
                # convert additional fields to case, as needed
                logger.debug("worker before case conversion: {}".format(w))
                last_execution_time_str = w.pop('last_execution_time')
                last_health_check_time_str = w.pop('last_health_check_time')
                create_time_str = w.pop('create_time')
                w['last_execution_time'] = display_time(last_execution_time_str)
                w['last_health_check_time'] = display_time(last_health_check_time_str)
                w['create_time'] = display_time(create_time_str)
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
        for executions_by_actor in executions_store.items():
            # determine if actor still exists:
            actor = None
            try:
                actor = Actor.from_db(actors_store[executions_by_actor['_id']])
            except KeyError:
                pass
            # iterate over executions for this actor:
            actor_exs = 0
            actor_runtime = 0
            actor_io = 0
            actor_cpu = 0
            for execution in executions_by_actor:
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


class AliasesResource(Resource):
    def get(self):
        logger.debug("top of GET /aliases")

        aliases = []
        for alias in alias_store.items():
            if alias['tenant'] == g.tenant:
                aliases.append(Alias.from_db(alias).display())
        logger.info("aliases retrieved.")
        return ok(result=aliases, msg="Aliases retrieved successfully.")

    def validate_post(self):
        parser = Alias.request_parser()
        try:
            args = parser.parse_args()
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            raise DAOError("Invalid alias description. Missing required field: {}".format(msg))
        if is_hashid(args.get('alias')):
            raise DAOError("Invalid alias description. Alias cannot be an Abaco hash id.")
        return args

    def post(self):
        logger.info("top of POST to register a new alias.")
        args = self.validate_post()
        actor_id = args.get('actor_id')
        if Config.get('web', 'case') == 'camel':
            actor_id = args.get('actorId')
        logger.debug("alias post args validated: {}.".format(actor_id))
        dbid = Actor.get_dbid(g.tenant, actor_id)
        try:
            Actor.from_db(actors_store[dbid])
        except KeyError:
            logger.debug("did not find actor: {}.".format(dbid))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        # update 10/2019: check that use has UPDATE permission on the actor -
        if not check_permissions(user=g.user, identifier=dbid, level=codes.UPDATE):
            raise PermissionsException(f"Not authorized -- you do not have access to {actor_id}.")

        # supply "provided" fields:
        args['tenant'] = g.tenant
        args['db_id'] = dbid
        args['owner'] = g.user
        args['alias_id'] = Alias.generate_alias_id(g.tenant, args['alias'])
        args['api_server'] = g.api_server
        logger.debug("Instantiating alias object. args: {}".format(args))
        alias = Alias(**args)
        logger.debug("Alias object instantiated; checking for uniqueness and creating alias. "
                     "alias: {}".format(alias))
        alias.check_and_create_alias()
        logger.info("alias added for actor: {}.".format(dbid))
        set_permission(g.user, alias.alias_id, UPDATE)
        return ok(result=alias.display(), msg="Actor alias created successfully.")

class AliasResource(Resource):
    def get(self, alias):
        logger.debug("top of GET /actors/aliases/{}".format(alias))
        alias_id = Alias.generate_alias_id(g.tenant, alias)
        try:
            alias = Alias.from_db(alias_store[alias_id])
        except KeyError:
            logger.debug("did not find alias with id: {}".format(alias))
            raise ResourceError(
                "No alias found: {}.".format(alias), 404)
        logger.debug("found alias {}".format(alias))
        return ok(result=alias.display(), msg="Alias retrieved successfully.")

    def validate_put(self):
        logger.debug("top of validate_put")
        try:
            data = request.get_json()
        except:
            data = None
        if data and 'alias' in data or 'alias' in request.form:
            logger.debug("found alias in the PUT.")
            raise DAOError("Invalid alias update description. The alias itself cannot be updated in a PUT request.")
        parser = Alias.request_parser()
        logger.debug("got the alias parser")
        # remove since alias is only required for POST, not PUT
        parser.remove_argument('alias')
        try:
            args = parser.parse_args()
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            raise DAOError("Invalid alias description. Missing required field: {}".format(msg))
        return args

    def put(self, alias):
        logger.debug("top of PUT /actors/aliases/{}".format(alias))
        alias_id = Alias.generate_alias_id(g.tenant, alias)
        try:
            alias_obj = Alias.from_db(alias_store[alias_id])
        except KeyError:
            logger.debug("did not find alias with id: {}".format(alias))
            raise ResourceError("No alias found: {}.".format(alias), 404)
        logger.debug("found alias {}".format(alias_obj))
        args = self.validate_put()
        actor_id = args.get('actor_id')
        if Config.get('web', 'case') == 'camel':
            actor_id = args.get('actorId')
        dbid = Actor.get_dbid(g.tenant, actor_id)
        # update 10/2019: check that use has UPDATE permission on the actor -
        if not check_permissions(user=g.user, identifier=dbid, level=codes.UPDATE, roles=g.roles):
            raise PermissionsException(f"Not authorized -- you do not have UPDATE "
                                       f"access to the actor you want to associate with this alias.")
        logger.debug(f"dbid: {dbid}")
        # supply "provided" fields:
        args['tenant'] = alias_obj.tenant
        args['db_id'] = dbid
        args['owner'] = alias_obj.owner
        args['alias'] = alias_obj.alias
        args['alias_id'] = alias_obj.alias_id
        args['api_server'] = alias_obj.api_server
        logger.debug("Instantiating alias object. args: {}".format(args))
        new_alias_obj = Alias(**args)
        logger.debug("Alias object instantiated; updating alias in alias_store. "
                     "alias: {}".format(new_alias_obj))
        alias_store[alias_id] = new_alias_obj
        logger.info("alias updated for actor: {}.".format(dbid))
        set_permission(g.user, new_alias_obj.alias_id, UPDATE)
        return ok(result=new_alias_obj.display(), msg="Actor alias updated successfully.")

    def delete(self, alias):
        logger.debug("top of DELETE /actors/aliases/{}".format(alias))
        alias_id = Alias.generate_alias_id(g.tenant, alias)
        try:
            alias = Alias.from_db(alias_store[alias_id])
        except KeyError:
            logger.debug("did not find alias with id: {}".format(alias))
            raise ResourceError(
                "No alias found: {}.".format(alias), 404)

        # update 10/2019: check that use has UPDATE permission on the actor -
        # TODO - check: do we want to require UPDATE on the actor to delete the alias? Seems like UPDATE
        #               on the alias iteself should be sufficient...
        # if not check_permissions(user=g.user, identifier=alias.db_id, level=codes.UPDATE):
        #     raise PermissionsException(f"Not authorized -- you do not have UPDATE "
        #                                f"access to the actor associated with this alias.")
        try:
            del alias_store[alias_id]
            # also remove all permissions - there should be at least one permissions associated
            # with the owner
            del permissions_store[alias_id]
            logger.info("alias {} deleted from alias store.".format(alias_id))
        except Exception as e:
            logger.info("got Exception {} trying to delete alias {}".format(e, alias_id))
        return ok(result=None, msg='Alias {} deleted successfully.'.format(alias))


class AliasNoncesResource(Resource):
    """Manage nonces for an alias"""

    def get(self, alias):
        logger.debug("top of GET /actors/aliases/{}/nonces".format(alias))
        dbid = g.db_id
        alias_id = Alias.generate_alias_id(g.tenant, alias)
        try:
            alias = Alias.from_db(alias_store[alias_id])
        except KeyError:
            logger.debug("did not find alias with id: {}".format(alias))
            raise ResourceError(
                "No alias found: {}.".format(alias), 404)
        nonces = Nonce.get_nonces(actor_id=None, alias=alias_id)
        return ok(result=[n.display() for n in nonces], msg="Alias nonces retrieved successfully.")

    def post(self, alias):
        """Create a new nonce for an alias."""
        logger.debug("top of POST /actors/aliases/{}/nonces".format(alias))
        dbid = g.db_id
        alias_id = Alias.generate_alias_id(g.tenant, alias)
        try:
            alias = Alias.from_db(alias_store[alias_id])
        except KeyError:
            logger.debug("did not find alias with id: {}".format(alias))
            raise ResourceError(
                "No alias found: {}.".format(alias), 404)
        args = self.validate_post()
        logger.debug("nonce post args validated: {}.".format(alias))

        # supply "provided" fields:
        args['tenant'] = g.tenant
        args['api_server'] = g.api_server
        args['alias'] = alias_id
        args['owner'] = g.user
        args['roles'] = g.roles

        # create and store the nonce:
        nonce = Nonce(**args)
        logger.debug("able to create nonce object: {}".format(nonce))
        Nonce.add_nonce(actor_id=None, alias=alias_id, nonce=nonce)
        logger.info("nonce added for alias: {}.".format(alias))
        return ok(result=nonce.display(), msg="Alias nonce created successfully.")

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
            if not args['level'] in ALIAS_NONCE_PERMISSION_LEVELS:
                raise DAOError("Invalid nonce description. "
                               "The level attribute must be one of: {}".format(ALIAS_NONCE_PERMISSION_LEVELS))
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


class AliasNonceResource(Resource):
    """Manage a specific nonce for an alias"""

    def get(self, alias, nonce_id):
        """Lookup details about a nonce."""
        logger.debug("top of GET /actors/aliases/{}/nonces/{}".format(alias, nonce_id))
        # check that alias exists -
        alias_id = Alias.generate_alias_id(g.tenant, alias)
        try:
            alias = Alias.from_db(alias_store[alias_id])
        except KeyError:
            logger.debug("did not find alias with id: {}".format(alias))
            raise ResourceError(
                "No alias found: {}.".format(alias), 404)

        nonce = Nonce.get_nonce(actor_id=None, alias=alias_id, nonce_id=nonce_id)
        return ok(result=nonce.display(), msg="Alias nonce retrieved successfully.")

    def delete(self, alias, nonce_id):
        """Delete a nonce."""
        logger.debug("top of DELETE /actors/aliases/{}/nonces/{}".format(alias, nonce_id))
        dbid = g.db_id
        # check that alias exists -
        alias_id = Alias.generate_alias_id(g.tenant, alias)
        try:
            alias = Alias.from_db(alias_store[alias_id])
        except KeyError:
            logger.debug("did not find alias with id: {}".format(alias))
            raise ResourceError(
                "No alias found: {}.".format(alias), 404)
        Nonce.delete_nonce(actor_id=None, alias=alias_id, nonce_id=nonce_id)
        return ok(result=None, msg="Alias nonce deleted successfully.")


def check_for_link_cycles(db_id, link_dbid):
    """
    Check if a link from db_id -> link_dbid would not create a cycle among linked actors.
    :param dbid: actor linking to link_dbid 
    :param link_dbid: id of actor being linked to.
    :return: 
    """
    logger.debug("top of check_for_link_cycles; db_id: {}; link_dbid: {}".format(db_id, link_dbid))
    # create the links graph, resolving each link attribute to a db_id along the way:
    # start with the passed in link, this is the "proposed" link -
    links = {db_id: link_dbid}
    for actor in actors_store.items():
        if actor.get('link'):
            try:
                link_id = Actor.get_actor_id(actor.get('tenant'), actor.get('link'))
                link_dbid = Actor.get_dbid(g.tenant, link_id)
            except Exception as e:
                logger.error("corrupt link data; could not resolve link attribute in "
                             "actor: {}; exception: {}".format(actor, e))
                continue
            # we do not want to override the proposed link passed in, as this actor could already have
            # a link (that was valid) and we need to check that the proposed link still works
            if not actor.get('db_id') == db_id:
                links[actor.get('db_id')] = link_dbid
    logger.debug("actor links dictionary built. links: {}".format(links))
    if has_cycles(links):
        raise DAOError("Error: this update would result in a cycle of linked actors.")


def has_cycles(links):
    """
    Checks whether the `links` dictionary contains a cycle.
    :param links: dictionary of form d[k]=v where k->v is a link
    :return: 
    """
    logger.debug("top of has_cycles. links: {}".format(links))
    # consider each link entry as the starting node:
    for k, v in links.items():
        # list of visited nodes on this iteration; starts with the two links.
        # if we visit a node twice, we have a cycle.
        visited = [k, v]
        # current node we are on
        current = v
        while current:
            # look up current to see if it has a link:
            current = links.get(current)
            # if it had a link, check if it was alread in visited:
            if current and current in visited:
                return True
            visited.append(current)
    return False


def validate_link(args):
    """
    Method to validate a request trying to set a link on an actor. Called for both POSTs (new actors)
    and PUTs (updates to existing actors).
    :param args:
    :return:
    """
    logger.debug("top of validate_link. args: {}".format(args))
    # check permissions - creating a link to an actor requires EXECUTE permissions
    # on the linked actor.
    try:
        link_id = Actor.get_actor_id(g.tenant, args['link'])
        link_dbid = Actor.get_dbid(g.tenant, link_id)
    except Exception as e:
        msg = "Invalid link parameter; unable to retrieve linked actor data. The link " \
              "must be a valid actor id or alias for which you have EXECUTE permission. "
        logger.info("{}; exception: {}".format(msg, e))
        raise DAOError(msg)
    try:
        check_permissions(g.user, link_dbid, EXECUTE)
    except Exception as e:
        logger.info("Got exception trying to check permissions for actor link. "
                    "Exception: {}; link: {}".format(e, link_dbid))
        raise DAOError("Invalid link parameter. The link must be a valid "
                       "actor id or alias for which you have EXECUTE permission. "
                       "Additional info: {}".format(e))
    logger.debug("check_permissions passed.")
    # POSTs to create new actors do not have db_id's assigned and cannot result in
    # cycles
    if not g.db_id:
        logger.debug("returning from validate_link - no db_id")
        return
    if link_dbid == g.db_id:
        raise DAOError("Invalid link parameter. An actor cannot link to itself.")
    check_for_link_cycles(g.db_id, link_dbid)


class AbacoUtilizationResource(Resource):

    def get(self):
        logger.debug("top of GET /actors/utilization")
        num_current_actors = len(actors_store)
        num_actors = len(workers_store)
        num_workers = 0
        for workers in workers_store.items():
            del workers['_id']
            num_workers += len(workers)

        ch = CommandChannel()
        result = {'currentActors': num_current_actors,
                  'totalActors': num_actors,
                  'workers': num_workers,
                  'commandQueue': len(ch._queue._queue)
                  }
        return ok(result=result, msg="Abaco utilization returned successfully.")

class ActorsResource(Resource):

    def get(self):
        logger.debug("top of GET /actors")

        actors = []
        for actor_info in actors_store.items():
            if actor_info['tenant'] == g.tenant:
                actor = Actor.from_db(actor_info)
                if check_permissions(g.user, actor.db_id, READ):
                    actors.append(actor.display())
        logger.info("actors retrieved.")
        return ok(result=actors, msg="Actors retrieved successfully.")

    def validate_post(self):
        logger.debug("top of validate post in /actors")
        parser = Actor.request_parser()
        try:
            args = parser.parse_args()
            logger.debug(f"initial actor args from parser: {args}")
            if args['queue']:
                queues_list = Config.get('spawner', 'host_queues').replace(' ', '')
                valid_queues = queues_list.split(',')
                if args['queue'] not in valid_queues:
                    raise BadRequest('Invalid queue name.')
            if args['link']:
                validate_link(args)
            if args['hints']:
                # a combination of the for loop iteration and the check for bad characters, including '[' and '{'
                # ensures that the hints parameter is a single string or a simple list of strings.
                for hint in args['hints']:
                    for bad_char in ['"', "'", '{', '}', '[', ']']:
                        if bad_char in hint:
                            raise BadRequest(f"Hints must be simple stings or numbers, no lists or dicts. "
                                             f"Error character: {bad_char}")

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

        # There are two options for the uid and gid to run in within the container. 1) the UID and GID to use
        # are computed by Abaco based on various configuration for the Abaco instance and tenant (such as
        # whether to use TAS, use a fixed UID, etc.) and 2) use the uid and gid created in the container.
        # Case 2) allows containers to be run as root and requires admin role in Abaco.
        use_container_uid = args.get('use_container_uid')
        if Config.get('web', 'case') == 'camel':
            use_container_uid = args.get('useContainerUid')
        logger.debug("request set use_container_uid: {}; type: {}".format(use_container_uid, type(use_container_uid)))
        if not use_container_uid:
            logger.debug("use_container_uid was false. looking up uid and gid...")
            uid, gid, home_dir = get_uid_gid_homedir(args, g.user, g.tenant)
            logger.debug(f"got uid: {uid}, gid: {gid}, home_dir: {home_dir} from get_().")
            if uid:
                args['uid'] = uid
            if gid:
                args['gid'] = gid
            if home_dir:
                args['tasdir'] = home_dir
        # token attribute - if the user specifies whether the actor requires a token, we always use that.
        # otherwise, we determine the default setting based on configs.
        if 'token' in args and args.get('token') is not None:
            token = args.get('token')
            logger.debug(f"user specified token: {token}")
        else:
            token = get_token_default()
        args['token'] = token
        if Config.get('web', 'case') == 'camel':
            max_workers = args.get('maxWorkers')
            args['max_workers'] = max_workers
        else:
            max_workers = args.get('max_workers')
            args['maxWorkers'] = max_workers
        if max_workers and 'stateless' in args and not args.get('stateless'):
            raise DAOError("Invalid actor description: stateful actors can only have 1 worker.")
        args['mounts'] = get_all_mounts(args)
        logger.debug("create args: {}".format(args))
        actor = Actor(**args)
        # Change function
        actors_store.add_if_empty([actor.db_id], actor)
        # initialize the actor's executions to the empty dictionary
        executions_store.add_if_empty([actor.db_id], {'_id': actor.db_id})
        logger.debug("new actor saved in db. id: {}. image: {}. tenant: {}".format(actor.db_id,
                                                                                   actor.image,
                                                                                   actor.tenant))
        if num_init_workers > 0:
            actor.ensure_one_worker()
        logger.debug("ensure_one_worker() called")
        set_permission(g.user, actor.db_id, UPDATE)
        logger.debug("UPDATE permission added to user: {}".format(g.user))
        return ok(result=actor.display(), msg="Actor created successfully.", request=request)


class ActorResource(Resource):
    def get(self, actor_id):
        logger.debug("top of GET /actors/{}".format(actor_id))
        try:
            actor = Actor.from_db(actors_store[g.db_id])
        except KeyError:
            logger.debug("did not find actor with id: {}".format(actor_id))
            raise ResourceError(
                "No actor found with identifier: {}.".format(actor_id), 404)
        logger.debug("found actor {}".format(actor_id))
        return ok(result=actor.display(), msg="Actor retrieved successfully.")

    def delete(self, actor_id):
        logger.debug("top of DELETE /actors/{}".format(actor_id))
        id = g.db_id
        try:
            actor = Actor.from_db(actors_store[id])
        except KeyError:
            actor = None

        if actor:
            # first set actor status to SHUTTING_DOWN so that no further autoscaling takes place
            actor.set_status(id, SHUTTING_DOWN)
            # delete all logs associated with executions -
            try:
                try:
                    executions = executions_store[id]
                except KeyError:
                    executions = {}
                for ex_id, val in executions.items():
                    del logs_store[ex_id]
            except KeyError as e:
                logger.info("got KeyError {} trying to retrieve actor or executions with id {}".format(
                    e, id))
        # shutdown workers ----
        logger.info("calling shutdown_workers() for actor: {}".format(id))
        shutdown_workers(id)
        logger.debug("returned from call to shutdown_workers().")
        # wait up to 20 seconds for all workers to shutdown; since workers could be running an execution this could
        # take some time, however, issuing a DELETE force halts all executions now, so this should not take too long.
        idx = 0
        shutdown = False
        workers = None
        while idx < 20 and not shutdown:
            # get all workers in db:
            try:
                workers = Worker.get_workers(id)
            except WorkerException as e:
                logger.debug("did not find workers for actor: {}; escaping.".format(actor_id))
                shutdown = True
                break
            if workers == {}:
                logger.debug(f"all workers gone, escaping. idx: {idx}")
                shutdown = True
            else:
                logger.debug(f"still some workers left; idx: {idx}; workers: {workers}")
                idx = idx + 1
                time.sleep(1)
        logger.debug(f"out of sleep loop waiting for workers to shut down; final workers var: {workers}")
        # delete the actor's message channel ----
        # NOTE: If the workers are not yet completed deleted, since they subscribe to the ActorMsgChannel,
        # there is a chance the ActorMsgChannel will survive.
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
        logger.info("actor {} nonces delete from nonce store.".format(id))
        msg = 'Actor deleted successfully.'
        if not workers == {}:
            msg = "Actor deleted successfully, though Abaco is still cleaning up some of the actor's resources."
        return ok(result=None, msg=msg)

    def put(self, actor_id):
        logger.debug("top of PUT /actors/{}".format(actor_id))
        dbid = g.db_id
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
        if args['queue']:
            queues_list = Config.get('spawner', 'host_queues').replace(' ', '')
            valid_queues = queues_list.split(',')
            if args['queue'] not in valid_queues:
                raise BadRequest('Invalid queue name.')
        if args['link']:
            validate_link(args)
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

        # token is an attribute that gets defaulted at the Abaco instance or tenant level. as such, we want
        # to use the default unless the user specified a value explicitly.
        if 'token' in args and args.get('token') is not None:
            token = args.get('token')
            logger.debug("token in args; using: {token}")
        else:
            token = get_token_default()
            logger.debug("token not in args; using default: {token}")
        args['token'] = token
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
            worker_id = Worker.request_worker(tenant=g.tenant, actor_id=actor.db_id)
            # get actor queue name
            ch = CommandChannel(name=actor.queue)
            # stop_existing defaults to True, so this command will also stop existing workers:
            ch.put_cmd(actor_id=actor.db_id, worker_id=worker_id, image=actor.image, tenant=args['tenant'])
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
            actor.pop('max_workers')
            actor.pop('mem_limit')
            actor.pop('max_cpus')

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
            raise DAOError("Invalid actor description: an actor that was not stateless cannot be updated to be stateless.")
        if not actor.stateless and (new_fields.get('max_workers') or new_fields.get('maxWorkers')):
            raise DAOError("Invalid actor description: stateful actors can only have 1 worker.")
        if new_fields['hints']:
            for hint in new_fields['hints']:
                for bad_char in ['"', "'", '{', '}', '[', ']']:
                    if bad_char in hint:
                        raise BadRequest(f"Hints must be simple stings or numbers, no lists or dicts. Error character: {bad_char}")    
        actor.update(new_fields)
        return actor


class ActorStateResource(Resource):
    def get(self, actor_id):
        logger.debug("top of GET /actors/{}/state".format(actor_id))
        dbid = g.db_id
        try:
            actor = Actor.from_db(actors_store[dbid])
        except KeyError:
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        return ok(result={'state': actor.get('state') }, msg="Actor state retrieved successfully.")

    def post(self, actor_id):
        logger.debug("top of POST /actors/{}/state".format(actor_id))
        dbid = g.db_id
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
        actors_store[dbid, 'state'] = state
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
        dbid = g.db_id
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
        id = g.db_id
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
        dbid = g.db_id
        nonces = Nonce.get_nonces(actor_id=dbid, alias=None)
        return ok(result=[n.display() for n in nonces], msg="Actor nonces retrieved successfully.")

    def post(self, actor_id):
        """Create a new nonce for an actor."""
        logger.debug("top of POST /actors/{}/nonces".format(actor_id))
        dbid = g.db_id
        args = self.validate_post()
        logger.debug("nonce post args validated; dbid: {}; actor_id: {}.".format(dbid, actor_id))

        # supply "provided" fields:
        args['tenant'] = g.tenant
        args['api_server'] = g.api_server
        args['db_id'] = dbid
        args['owner'] = g.user
        args['roles'] = g.roles

        # create and store the nonce:
        nonce = Nonce(**args)
        try:
            logger.debug("nonce.actor_id: {}".format(nonce.actor_id))
        except Exception as e:
            logger.debug("got exception trying to log actor_id on nonce; e: {}".format(e))
        Nonce.add_nonce(actor_id=dbid, alias=None, nonce=nonce)
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
        dbid = g.db_id
        nonce = Nonce.get_nonce(actor_id=dbid, alias=None, nonce_id=nonce_id)
        return ok(result=nonce.display(), msg="Actor nonce retrieved successfully.")

    def delete(self, actor_id, nonce_id):
        """Delete a nonce."""
        logger.debug("top of DELETE /actors/{}/nonces/{}".format(actor_id, nonce_id))
        dbid = g.db_id
        Nonce.delete_nonce(actor_id=dbid, alias=None, nonce_id=nonce_id)
        return ok(result=None, msg="Actor nonce deleted successfully.")


class ActorExecutionResource(Resource):
    def get(self, actor_id, execution_id):
        logger.debug("top of GET /actors/{}/executions/{}.".format(actor_id, execution_id))
        dbid = g.db_id
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

    def delete(self, actor_id, execution_id):
        logger.debug("top of DELETE /actors/{}/executions/{}.".format(actor_id, execution_id))
        dbid = g.db_id
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
        # check status of execution:
        if not exc.status == codes.RUNNING:
            logger.debug("execution not in {} status: {}".format(codes.RUNNING, exc.status))
            raise ResourceError("Cannot force quit an execution not in {} status. "
                                "Execution was found in status: {}".format(codes.RUNNING, exc.status))
        # send force_quit message to worker:
        # TODO - should we set the execution status to FORCE_QUIT_REQUESTED?
        logger.debug("issuing force quit to worker: {} "
                     "for actor_id: {} execution_id: {}".format(exc.worker_id, actor_id, execution_id))
        ch = WorkerChannel(worker_id=exc.worker_id)
        ch.put('force_quit')
        msg = 'Issued force quit command for execution {}.'.format(execution_id)
        return ok(result=None, msg=msg)


class ActorExecutionResultsResource(Resource):
    def get(self, actor_id, execution_id):
        logger.debug("top of GET /actors/{}/executions/{}/results".format(actor_id, execution_id))
        id = g.db_id
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
        dbid = g.db_id
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
            logs = logs_store[execution_id]['logs']
        except KeyError:
            logger.debug("did not find logs. execution: {}. actor: {}.".format(execution_id, actor_id))
            logs = ""
        result={'logs': logs}
        result.update(get_hypermedia(actor, exc))
        return ok(result, msg="Logs retrieved successfully.")


def get_messages_hypermedia(actor):
    return {'_links': {'self': '{}/actors/v2/{}/messages'.format(actor.api_server, actor.id),
                       'owner': '{}/profiles/v2/{}'.format(actor.api_server, actor.owner),
                       },
            }


class MessagesResource(Resource):
    def get(self, actor_id):
        logger.debug("top of GET /actors/{}/messages".format(actor_id))
        # check that actor exists
        id = g.db_id
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
        result.update(get_messages_hypermedia(actor))
        return ok(result)

    def delete(self, actor_id):
        logger.debug("top of DELETE /actors/{}/messages".format(actor_id))
        # check that actor exists
        id = g.db_id
        try:
            actor = Actor.from_db(actors_store[id])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError(
                "No actor found with id: {}.".format(actor_id), 404)
        ch = ActorMsgChannel(actor_id=id)
        ch._queue._queue.purge()
        result = {'msg': "Actor mailbox purged."}
        ch.close()
        logger.debug("messages purged for actor: {}.".format(actor_id))
        result.update(get_messages_hypermedia(actor))
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
                except (TypeError, json.decoder.JSONDecodeError):
                    logger.debug("message POST body could not be serialized. args: {}".format(args))
                    raise DAOError('message POST body could not be serialized. Pass JSON data or use the message attribute.')
                args['_abaco_Content_Type'] = 'str'
        else:
            # the special message object is a string
            logger.debug("POST body has a message field. Setting _abaco_Content_type to 'str'.")
            args['_abaco_Content_Type'] = 'str'
        return args

    def post(self, actor_id):
        start_timer = timeit.default_timer()
        def get_hypermedia(actor, exc):
            return {'_links': {'self': '{}/actors/v2/{}/executions/{}'.format(actor.api_server, actor.id, exc),
                               'owner': '{}/profiles/v2/{}'.format(actor.api_server, actor.owner),
                               'messages': '{}/actors/v2/{}/messages'.format(actor.api_server, actor.id)}, }

        logger.debug("top of POST /actors/{}/messages.".format(actor_id))
        synchronous = False
        dbid = g.db_id
        try:
            Actor.from_db(actors_store[dbid])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError("No actor found with id: {}.".format(actor_id), 404)
        got_actor_timer = timeit.default_timer()
        args = self.validate_post()
        val_post_timer = timeit.default_timer()
        d = {}
        # build a dictionary of k:v pairs from the query parameters, and pass a single
        # additional object 'message' from within the post payload. Note that 'message'
        # need not be JSON data.
        logger.debug("POST body validated. actor: {}.".format(actor_id))
        for k, v in request.args.items():
            if k == '_abaco_synchronous':
                try:
                    if v.lower() == 'true':
                        logger.debug("found synchronous and value was true")
                        synchronous = True
                    else:
                        logger.debug("found synchronous and value was false")
                except Execution as e:
                    logger.info("Got exception trying to parse the _abaco_synchronous; e: {}".format(e))
            if k == 'message':
                continue
            d[k] = v
        request_args_timer = timeit.default_timer()
        logger.debug("extra fields added to message from query parameters: {}.".format(d))
        if synchronous:
            # actor mailbox length must be 0 to perform a synchronous execution
            ch = ActorMsgChannel(actor_id=id)
            box_len = len(ch._queue._queue)
            ch.close()
            if box_len > 3:
                raise ResourceError("Cannot issue synchronous execution when actor message queue > 0.")
        if hasattr(g, 'user'):
            d['_abaco_username'] = g.user
            logger.debug("_abaco_username: {} added to message.".format(g.user))
        if hasattr(g, 'api_server'):
            d['_abaco_api_server'] = g.api_server
            logger.debug("_abaco_api_server: {} added to message.".format(g.api_server))
        if hasattr(g, 'jwt_header_name'):
            d['_abaco_jwt_header_name'] = g.jwt_header_name
            logger.debug("abaco_jwt_header_name: {} added to message.".format(g.jwt_header_name))
        # create an execution
        before_exc_timer = timeit.default_timer()
        exc = Execution.add_execution(dbid, {'cpu': 0,
                                             'io': 0,
                                             'runtime': 0,
                                             'status': SUBMITTED,
                                             'executor': g.user})
        after_exc_timer = timeit.default_timer()
        logger.info("Execution {} added for actor {}".format(exc, actor_id))
        d['_abaco_execution_id'] = exc
        d['_abaco_Content_Type'] = args.get('_abaco_Content_Type', '')
        logger.debug("Final message dictionary: {}".format(d))
        before_ch_timer = timeit.default_timer()
        ch = ActorMsgChannel(actor_id=dbid)
        after_ch_timer = timeit.default_timer()
        ch.put_msg(message=args['message'], d=d)
        after_put_msg_timer = timeit.default_timer()
        ch.close()
        after_ch_close_timer = timeit.default_timer()
        logger.debug("Message added to actor inbox. id: {}.".format(actor_id))
        # make sure at least one worker is available
        actor = Actor.from_db(actors_store[dbid])
        after_get_actor_db_timer = timeit.default_timer()
        actor.ensure_one_worker()
        after_ensure_one_worker_timer = timeit.default_timer()
        logger.debug("ensure_one_worker() called. id: {}.".format(actor_id))
        if args.get('_abaco_Content_Type') == 'application/octet-stream':
            result = {'execution_id': exc, 'msg': 'binary - omitted'}
        else:
            result = {'execution_id': exc, 'msg': args['message']}
        result.update(get_hypermedia(actor, exc))
        case = Config.get('web', 'case')
        end_timer = timeit.default_timer()
        time_data = {'total': (end_timer - start_timer) * 1000,
                     'get_actor': (got_actor_timer - start_timer) * 1000,
                     'validate_post': (val_post_timer - got_actor_timer) * 1000,
                     'parse_request_args': (request_args_timer - val_post_timer) * 1000,
                     'create_msg_d': (before_exc_timer - request_args_timer) * 1000,
                     'add_execution': (after_exc_timer - before_exc_timer) * 1000,
                     'final_msg_d': (before_ch_timer - after_exc_timer) * 1000,
                     'create_actor_ch': (after_ch_timer - before_ch_timer) * 1000,
                     'put_msg_ch': (after_put_msg_timer - after_ch_timer) * 1000,
                     'close_ch': (after_ch_close_timer - after_put_msg_timer) * 1000,
                     'get_actor_2': (after_get_actor_db_timer - after_ch_close_timer) * 1000,
                     'ensure_1_worker': (after_ensure_one_worker_timer - after_get_actor_db_timer) * 1000,
                     }
        logger.info("Times to process message: {}".format(time_data))
        if synchronous:
            return self.do_synch_message(exc)
        if not case == 'camel':
            return ok(result)
        else:
            return ok(dict_to_camel(result))

    def do_synch_message(self, execution_id):
        """Monitor for the termination of a synchronous message execution."""

        dbid = g.db_id
        ch = ExecutionResultsChannel(actor_id=dbid, execution_id=execution_id)
        result = None
        complete = False
        check_results_channel = True
        binary_result = False
        timeout = 0.1
        while not complete:
            # check for a result on the results channel -
            if check_results_channel:
                try:
                    result = ch.get(timeout=timeout)
                    ch.close()
                    complete = True
                    binary_result = True
                    logger.debug("check_results_channel thread got a result.")
                except ChannelClosedException:
                    # the channel unexpectedly closed, so just return
                    logger.info("unexpected ChannelClosedException in check_results_channel thread: {}".format(e))
                    # check_results_channel = False
                except ChannelTimeoutException:
                    pass
                except Exception as e:
                    logger.info("unexpected exception in check_results_channel thread: {}".format(e))
                    check_results_channel = False

            # check to see if execution has completed:
            if not complete:
                try:
                    excs = executions_store[dbid]
                    exc = Execution.from_db(excs[execution_id])
                    complete = exc.status == COMPLETE
                except Exception as e:
                    logger.info("got exception trying to check execution status: {}".format(e))
            if complete:
                logger.debug("execution is complete")
                if not result:
                    # first try one more time to get a result -
                    if check_results_channel:
                        try:
                            result = ch.get(timeout=timeout)
                            binary_result = True
                        except:
                            pass
                    # if we still have no result, get the logs -
                    if not result:
                        try:
                            result = logs_store[execution_id]['logs']
                        except KeyError:
                            logger.debug("did not find logs. execution: {}. actor: {}.".format(execution_id, actor_id))
                            result = ""
        response = make_response(result)
        if binary_result:
            response.headers['content-type'] = 'application/octet-stream'
        try:
            ch.close()
        except:
            pass
        return response


class WorkersResource(Resource):
    def get(self, actor_id):
        logger.debug("top of GET /actors/{}/workers for tenant {}.".format(actor_id, g.tenant))
        dbid = g.db_id
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
        dbid = g.db_id
        try:
            actor = Actor.from_db(actors_store[dbid])
        except KeyError:
            logger.debug("did not find actor: {}.".format(actor_id))
            raise ResourceError("No actor found with id: {}.".format(actor_id), 404)
        args = self.validate_post()
        logger.debug("workers POST params validated. actor: {}.".format(actor_id))
        num = args.get('num')
        if not num or num == 0:
            logger.debug("did not get a num: {}.".format(actor_id))
            num = 1
        logger.debug("ensuring at least {} workers. actor: {}.".format(num, dbid))
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
                worker_id = Worker.request_worker(tenant=g.tenant,
                                                  actor_id=dbid)
                logger.info("New worker id: {}".format(worker_id[0]))
                ch = CommandChannel(name=actor.queue)
                ch.put_cmd(actor_id=actor.db_id,
                           worker_id=worker_id,
                           image=actor.image,
                           tenant=g.tenant,
                           stop_existing=False)
            ch.close()
            logger.info("Message put on command channel for new worker ids: {}".format(worker_id))
            return ok(result=None, msg="Scheduled {} new worker(s) to start. Previously, there were {} workers.".format(num_to_add, current_number_workers))
        else:
            return ok(result=None, msg="Actor {} already had {} worker(s).".format(actor_id, num))


class WorkerResource(Resource):
    def get(self, actor_id, worker_id):
        logger.debug("top of GET /actors/{}/workers/{}.".format(actor_id, worker_id))
        id = g.db_id
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
        id = g.db_id
        try:
            worker = Worker.get_worker(id, worker_id)
        except WorkerException as e:
            logger.debug("Did not find worker: {}. actor: {}.".format(worker_id, actor_id))
            raise ResourceError(e.msg, 404)
        # if the worker is in requested status, we shouldn't try to shut it down because it doesn't exist yet;
        # we just need to remove the worker record from the workers_store.
        # TODO - if worker.status == 'REQUESTED' ....
        logger.info("calling shutdown_worker(). worker: {}. actor: {}.".format(worker_id, actor_id))
        shutdown_worker(id, worker['id'], delete_actor_ch=False)
        logger.info("shutdown_worker() called for worker: {}. actor: {}.".format(worker_id, actor_id))
        return ok(result=None, msg="Worker scheduled to be stopped.")


class PermissionsResource(Resource):
    """This class handles permissions endpoints for all objects that need permissions.
    The `identifier` is the human-readable id (e.g., actor_id, alias).
    The code uses the request rule to determine which object is being referenced.
    """
    def get(self, identifier):
        if 'actors/aliases/' in request.url_rule.rule:
            logger.debug("top of GET /actors/aliases/{}/permissions.".format(identifier))
            id = Alias.generate_alias_id(g.tenant, identifier)
        else:
            logger.debug("top of GET /actors/{}/permissions.".format(identifier))
            id = g.db_id
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

    def post(self, identifier):
        """Add new permissions for an object `identifier`."""
        if 'actors/aliases/' in request.url_rule.rule:
            logger.debug("top of POST /actors/aliases/{}/permissions.".format(identifier))
            id = Alias.generate_alias_id(g.tenant, identifier)
        else:
            logger.debug("top of POST /actors/{}/permissions.".format(identifier))
            id = g.db_id
        args = self.validate_post()
        logger.debug("POST permissions body validated for identifier: {}.".format(id))
        set_permission(args['user'], id, PermissionLevel(args['level']))
        logger.info("Permission added for user: {} actor: {} level: {}".format(args['user'], id, args['level']))
        permissions = get_permissions(id)
        return ok(result=permissions, msg="Permission added successfully.")

class ActorPermissionsResource(PermissionsResource):
    pass

class AliasPermissionsResource(PermissionsResource):
    pass
