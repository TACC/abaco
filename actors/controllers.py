import datetime
from http import server
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
from parse import parse

from auth import check_permissions, check_config_permissions, get_uid_gid_homedir, get_token_default, tenant_can_use_tas
from channels import ActorMsgChannel, CommandChannel, ExecutionResultsChannel, WorkerChannel
from codes import ERROR, SUBMITTED, COMPLETE, SHUTTING_DOWN, PERMISSION_LEVELS, ALIAS_NONCE_PERMISSION_LEVELS, READ, \
    UPDATE, EXECUTE, PERMISSION_LEVELS, PermissionLevel, READY, REQUESTED, SPAWNER_SETUP, PULLING_IMAGE, CREATING_CONTAINER, \
    UPDATING_STORE, SHUTDOWN_REQUESTED
import codes
from errors import DAOError, ResourceError, PermissionsException, WorkerException, AdapterMessageError
from models import dict_to_camel, display_time, is_hashid, Actor, ActorConfig, Alias, Execution, ExecutionsSummary, Nonce, Worker, Search, get_permissions, \
    get_config_permissions, set_permission, get_current_utc_time, set_config_permission, site, Adapter, AdapterServer, get_adapter_permissions, set_adapter_permission
from mounts import get_all_mounts
from stores import actors_store, alias_store, configs_store, configs_permissions_store, workers_store, \
    executions_store, logs_store, nonce_store, permissions_store, abaco_metrics_store, adapters_store, adapter_servers_store, adapter_permissions_store, SITE_LIST
from worker import shutdown_workers, shutdown_worker
import encrypt_utils

from prometheus_client import start_http_server, Summary, MetricsHandler, Counter, Gauge, generate_latest

from tapisservice.tapisflask.utils import RequestParser, ok
from tapisservice.config import conf
from tapisservice.logs import get_logger
logger = get_logger(__name__)

CONTENT_TYPE_LATEST = str('text/plain; version=0.0.4; charset=utf-8')
PROMETHEUS_URL = 'http://172.17.0.1:9090'
DEFAULT_SYNC_MAX_IDLE_TIME = 600 # defaults to 10*60 = 600 s = 10 min
message_gauges = {}
rate_gauges = {}
last_metric = {}


try:
    ACTOR_MAX_WORKERS = conf.get("spawner_max_workers_per_actor")
except:
    ACTOR_MAX_WORKERS = os.environ.get('MAX_WORKERS_PER_ACTOR', 20)
ACTOR_MAX_WORKERS = int(ACTOR_MAX_WORKERS)
logger.info(f"METRICS - running with ACTOR_MAX_WORKERS = {ACTOR_MAX_WORKERS}")

ACTOR_MAX_WORKERS = conf.spawner_max_workers_per_actor or int(os.environ.get('MAX_WORKERS_PER_ACTOR', 20))
logger.info(f"METRICS - running with ACTOR_MAX_WORKERS = {ACTOR_MAX_WORKERS}")

num_init_workers = conf.worker_init_count

class SearchResource(Resource):
    def get(self, search_type):
        """
        Does a broad search with args and search_type passed to this resource.
        """
        args = request.args
        result = Search(args, search_type, g.request_tenant_id, g.username).search()
        return ok(result=result, msg="Search completed successfully.")


class CronResource(Resource):
    def get(self):
        logger.debug("top of GET /cron")
        actor_ids = [actor['db_id'] for actor in actors_store[site()].items()]
        logger.debug(f"actor ids are {actor_ids}")
        # Loop through all actor ids to check for cron schedules
        for actor_id in actor_ids:
            # Create actor based on the actor_id
            actor = actors_store[site()][actor_id]
            logger.debug(f"cron_on equals {actor.get('cron_on')} for actor {actor_id}")
            try:
                # Check if next execution == UTC current time
                if self.cron_execution_datetime(actor) == "now":
                    #executes the actor and cron is updated
                    self.actor_exec(actor,actor_id)
                elif self.cron_execution_datetime(actor) == "past":
                    logger.debug("Cron_next_ex was in the past")
                    #increments the cron_next_ex so the actor is executed when it is next expected
                    actors_store[site()][actor_id, 'cron_next_ex'] = Actor.set_next_ex_past(actor, actor_id)
                    logger.debug("Now Cron_next_ex is in the present or future")
                    #if cron_next_ex is in the present than the actor is executed and cron is updated
                    if self.cron_execution_datetime(actor) == "now":
                        self.actor_exec(actor,actor_id)
                    else:
                        logger.debug("now is not the time")
                else:
                    logger.debug("now is not the time")
            except:
                logger.debug("Actor has no cron setup")

    def cron_execution_datetime(self, actor):
        logger.debug("inside cron_execution_datetime method")
        now = get_current_utc_time()
        now = datetime.datetime(now.year, now.month, now.day, now.hour)
        logger.debug(f"the current utc time is {now}")
        # Get cron execution datetime
        cron = actor['cron_next_ex']
        logger.debug(f"cron_next_ex is {cron}")
        # Parse the next execution into a list of the form: [year,month,day,hour]
        cron_datetime = parse("{}-{}-{} {}", cron)
        logger.debug(f"cron datetime is {cron_datetime}")
        # Create a datetime out of cron_datetime
        cron_execution = datetime.datetime(int(cron_datetime[0]), int(cron_datetime[1]), int(cron_datetime[2]), int(cron_datetime[3]))
        logger.debug(f"cron execution is {cron_execution}")
        # Return true/false comparing now with the next cron execution
        logger.debug(f"does cron == now? {cron_execution == now}")
        if cron_execution == now:
            return "now"
        elif cron_execution < now:
            return "past"
        else:
            return "future"
    
    def actor_exec(self, actor, actor_id):
        # Check if cron switch is on
        logger.debug("inside actor_exec method")
        if actor.get('cron_on'):
            d = {}
            logger.debug("the current time is the same as the next cron scheduled, adding execution")
            # Execute actor
            before_exc_time = timeit.default_timer()
            exc = Execution.add_execution(actor_id, {'cpu': 0,
                                                     'io': 0,
                                                     'runtime': 0,
                                                     'status': codes.SUBMITTED,
                                                     'executor': 'cron'})
            logger.debug("execution has been added, now making message")
            # Create & add message to the queue 
            d['Time_msg_queued'] = before_exc_time
            d['_abaco_execution_id'] = exc
            d['_abaco_Content_Type'] = 'str'
            d['_abaco_actor_revision'] = actor.get('revision')
            ch = ActorMsgChannel(actor_id=actor_id)
            ch.put_msg(message="This is your cron execution", d=d)
            ch.close()
            logger.debug(f"Message added to actor inbox. id: {actor_id}.")
            # Update the actor's next execution
            actors_store[site()][actor_id, 'cron_next_ex'] = Actor.set_next_ex(actor, actor_id)
        else:
            logger.debug("Actor's cron is not activated, but next execution will be incremented")
            actors_store[site()][actor_id, 'cron_next_ex'] = Actor.set_next_ex(actor, actor_id)


class MetricsResource(Resource):
    def get(self):
        logger.debug("AUTOSCALER initiating new run --------")
        enable_autoscaling = conf.get("worker_autoscaling")
        if not enable_autoscaling:
            logger.debug("Autoscaler turned off in Abaco configuration; exiting.")
            return Response("Autoscaler disabled in Abaco.")

        for site in SITE_LIST:
            self.autoscaler(site)

        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

    def autoscaler(self, site_id):
        # Autoscaler that goes through all actors in a site and tries to auto scale
        # up or down depending on worker and message criteria.
        g.site_id = site_id
        logger.debug(f"AUTOSCALER starting run for site: {site()} --------")

        # autoscaler does not manage stateful actors or actors in ERROR or SHUTTING_DOWN status:
        actors = actors_store[site()].items({"stateless": True, "status": {"$nin": [ERROR, SHUTTING_DOWN]}})

        # iterate over each actor and check how many pending message it has:
        for actor in actors:
            try:
                ch = ActorMsgChannel(actor_id=actor["db_id"])
                inbox_length = len(ch._queue._queue)
            except Exception as e:
                logger.error(f"Exception connecting to ActorMsgChannel: {e}")
                # if we get an exception, move on to the next actor
                continue
            if inbox_length == 0:
                self.check_for_scale_down(actor)
            else:
                self.check_for_scale_up(actor, inbox_length)

    def check_for_scale_down(self, actor):
        # Check each actor and see if we need to scale down
        actor_id = actor["db_id"]
        logger.debug(f"top of check_for_scale_down for actor_id: {actor_id}")
        # if the actor is a sync actor, we need to look for the existence of 1 worker that is not
        is_sync_actor = Actor.is_sync_actor(actor_id)
        viable_workers = workers_store[site()].items({'actor_id': actor_id,
                                                      'status' : {'$nin': [ERROR,
                                                                           SHUTTING_DOWN,
                                                                           SHUTDOWN_REQUESTED]}})
        ready_workers = [w for w in viable_workers if w['status'] == 'READY']
        if is_sync_actor:
            sync_max_idle_time = conf.workers_sync_max_idle_time or DEFAULT_SYNC_MAX_IDLE_TIME
            # if all the viable workers are ready workers, we need to save one ready worker because it is a sync
            # actor. however, if some viable worker is not ready, we can save that one and therefore shutdown all
            # ready workers.
            if len(ready_workers) == len(viable_workers):
                # iterate through the ready workers and see if any of them are within the sync ttl
                worker_idx_to_save = -1
                best_worker_ttl = sync_max_idle_time
                for idx, worker in enumerate(ready_workers):
                    last_worker_activity_time = worker.get('last_execution_time') or worker.get('create_time') or 0
                    # this worker is a candidate to be saved
                    if datetime.datetime.now() - last_worker_activity_time < sync_max_idle_time:
                        if datetime.datetime.now() - last_worker_activity_time < best_worker_ttl:
                            # we just found a worker with a more recent activity, so update:
                            worker_idx_to_save = idx
                            best_worker_ttl = datetime.datetime.now() - last_worker_activity_time
                if worker_idx_to_save >= 0:
                    ready_workers.pop(worker_idx_to_save)
        for worker in ready_workers:
            logger.info(f"autoscaler stopping worker {worker['id']} for actor {actor_id}")
            shutdown_worker(actor_id, worker['id'], delete_actor_ch=False)

    def check_for_scale_up(self, actor, inbox_length):
        # Check each actor and see if we need to scale up
        actor_id = actor["db_id"]
        logger.debug(f"top of check_for_scale_up; actor_id: {actor_id}; inbox_length: {inbox_length}")

        channel_name = actor.get('queue') or 'default'
        ch = CommandChannel(name=channel_name)
        cmd_length = len(ch._queue._queue)

        try:
            max_cmd_length = int(conf.spawner_max_cmd_length)
        except Exception as e:
            logger.info(f"Autoscaler got exception trying to compute the max_cmd_length; using 10. e: {e}")
            max_cmd_length = 10

        if cmd_length > max_cmd_length:
            logger.warning(f"Will NOT scale up actor {actor_id}: Current cmd_length ({cmd_length}) is greater "
                           f"than ({max_cmd_length}) for the {channel_name} command channel.")
            return False

        # if the actor has more pending messages than it has pending workers, and the total number of workers the actor
        # has is less than the max number of workers per actor, then we scale up.
        workers = workers_store[site()].items({'actor_id': actor_id, 'status' : {'$nin': [ERROR,
                                                                                          SHUTTING_DOWN,
                                                                                          SHUTDOWN_REQUESTED]}})
        pending_workers = [w for w in workers if w['status'] in [READY, REQUESTED, SPAWNER_SETUP, PULLING_IMAGE,
                                                                 CREATING_CONTAINER, UPDATING_STORE]]

        try:
            max_workers = actor["max_workers"] or conf.spawner_max_workers_per_actor
        except:
            max_workers = conf.spawner_max_workers_per_actor

        if inbox_length - len(pending_workers) > 0 and len(workers) < max_workers:
            tenant = actor["tenant"]
            worker_id = Worker.request_worker(tenant=tenant, actor_id=actor_id)
            try:
                logger.info("New worker id: {}".format(worker_id))
                ch = CommandChannel(name=channel_name)
                ch.put_cmd(actor_id=actor_id,
                        worker_id=worker_id,
                        image=actor["image"],
                        revision=actor["revision"],
                        tenant=tenant,
                        site_id=site(),
                        stop_existing=False)
                ch.close()
                logger.info(f'autoscaler added worker successfully for actor {actor_id}; new worker id: {worker_id}')
            except Exception as e:
                # We continue as this is not a users fault.
                logger.critical(f"Error adding command to command channel during autoscale up: {e}")
        else:
            logger.debug(f"autoscaler not adding worker for actor {actor_id}")


class AdminActorsResource(Resource):
    def get(self):
        logger.debug("top of GET /admin")
        case = conf.web_case
        actors = []
        try:
            for actor in actors_store[site()].items():
                actor = Actor.from_db(actor)
                actor.workers = []
                for worker in Worker.get_workers(actor.db_id):
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
        except Exception as e:
            logger.critical(f'LOOK AT ME: {e}')
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
        case = conf.web_case
        # the workers_store objects have a key:value structure where the key is the actor_id and
        # the value is the worker object (iself, a dictionary).
        actors_with_workers = set()
        for worker in workers_store[site()].items(proj_inp=None):
            actor_id = worker['actor_id']
            actors_with_workers.add(actor_id)
            w = Worker(**worker)
            actor_display_id = Actor.get_display_id(worker.get('tenant'), actor_id)
            w.update({'actor_id': actor_display_id})
            w.update({'actor_dbid': actor_id})
            # convert additional fields to case, as needed
            logger.debug(f"worker before case conversion: {w}")
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
        summary['actors_no_workers'] = len(actors_store[site()]) - len(actors_with_workers)
        logger.info("workers retrieved.")
        if case == 'camel':
            summary = dict_to_camel(summary)
        result = {'summary': summary,
                'workers': workers_result}
        return ok(result=result, msg="Workers retrieved successfully.")


class AdminExecutionsResource(Resource):
    def get(self):
        logger.debug("top of GET /admin/executions")
        result = {'summary': {'total_actors_all': 0,
                              'total_actors_all_with_executions': 0,
                              'total_executions_all': 0,
                              'total_execution_runtime_all': 0,
                              'total_execution_cpu_all': 0,
                              'total_execution_io_all': 0,
                              'total_actors_existing': 0,
                              'total_actors_existing_with_executions': 0,
                              'total_executions_existing': 0,
                              'total_execution_runtime_existing': 0,
                              'total_execution_cpu_existing': 0,
                              'total_execution_io_existing': 0,
                              },
                  'actors': []
        }
        case = conf.web_case
        actor_stats = {}
        actor_does_not_exist = []
        for execution in executions_store[site()].items():
            actor_id = execution['actor_id']
            actor_cpu = execution['cpu']
            actor_io = execution['io']
            actor_runtime = execution['runtime']
            if actor_id in actor_does_not_exist:
                pass
            else:
                try:
                    # checks if actor existance has already been tested
                    if not actor_id in actor_stats:
                        result['summary']['total_actors_all_with_executions'] += 1
                        # determine if actor still exists:
                        actor = Actor.from_db(actors_store[site()][actor_id])
                        # creates dict if actor does exist
                        actor_stats[actor_id] = {'actor_id': actor.get('id'),
                                                'owner': actor.get('owner'),
                                                'image': actor.get('image'),
                                                'total_executions': 0,
                                                'total_execution_cpu': 0,
                                                'total_execution_io': 0,
                                                'total_execution_runtime': 0}
                        result['summary']['total_actors_existing_with_executions'] += 1

                    # write actor information if actor does exist
                    actor_stats[actor_id]['total_executions'] += 1
                    actor_stats[actor_id]['total_execution_runtime'] += actor_runtime
                    actor_stats[actor_id]['total_execution_io'] += actor_io
                    actor_stats[actor_id]['total_execution_cpu'] += actor_cpu
                    # write result information if actor does exist
                    result['summary']['total_executions_existing'] += 1
                    result['summary']['total_execution_runtime_existing'] += actor_runtime
                    result['summary']['total_execution_io_existing'] += actor_io
                    result['summary']['total_execution_cpu_existing'] += actor_cpu
                except KeyError:
                    actor_does_not_exist.append(actor_id)
            # always add these to the totals:
            result['summary']['total_executions_all'] += 1
            result['summary']['total_execution_runtime_all'] += actor_runtime
            result['summary']['total_execution_io_all'] += actor_io
            result['summary']['total_execution_cpu_all'] += actor_cpu

        # Get total actors from abaco_metrics_store (stats are stored on a daily basis)
        total_actors = 0
        for stat_doc in abaco_metrics_store[site()].items():
            try:
                total_actors += stat_doc['actor_total']
            except KeyError as e:
                # Stat docs created daily, so some days there might not be stats in them
                continue

        result['summary']['total_actors_all'] += total_actors
        result['summary']['total_actors_existing'] += len(actors_store[site()])

        for _, actor_stat in actor_stats.values():
            if case == 'camel':
                actor_stat = dict_to_camel(actor_stat)
            result['actors'].append(actor_stat)

        if case == 'camel':
            result['summary'] = dict_to_camel(result['summary'])
        return ok(result=result, msg="Executions retrieved successfully.")


class AliasesResource(Resource):
    def get(self):
        logger.debug("top of GET /aliases")

        aliases = []
        for alias in alias_store[site()].items():
            if alias['tenant'] == g.request_tenant_id:
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
            raise DAOError(f"Invalid alias description. Missing required field: {msg}")
        if is_hashid(args.get('alias')):
            raise DAOError("Invalid alias description. Alias cannot be an Abaco hash id.")
        return args

    def post(self):
        logger.info("top of POST to register a new alias.")
        args = self.validate_post()
        actor_id = args.get('actor_id')
        if conf.web_case == 'camel':
            actor_id = args.get('actorId')
        logger.debug(f"alias post args validated: {actor_id}.")
        dbid = Actor.get_dbid(g.request_tenant_id, actor_id)
        try:
            Actor.from_db(actors_store[site()][dbid])
        except KeyError:
            logger.debug(f"did not find actor: {dbid}.")
            raise ResourceError(
                f"No actor found with id: {actor_id}.", 404)
        # update 10/2019: check that use has UPDATE permission on the actor -
        if not check_permissions(user=g.username, identifier=dbid, level=codes.UPDATE):
            raise PermissionsException(f"Not authorized -- you do not have access to {actor_id}.")

        # supply "provided" fields:
        args['tenant'] = g.request_tenant_id
        args['db_id'] = dbid
        args['owner'] = g.username
        args['alias_id'] = Alias.generate_alias_id(g.request_tenant_id, args['alias'])
        args['api_server'] = g.api_server
        logger.debug(f"Instantiating alias object. args: {args}")
        alias = Alias(**args)
        logger.debug(f"Alias object instantiated; checking for uniqueness and creating alias. alias: {alias}")
        alias.check_and_create_alias()
        logger.info(f"alias added for actor: {dbid}.")
        set_permission(g.username, alias.alias_id, UPDATE)
        return ok(result=alias.display(), msg="Actor alias created successfully.")

class AliasResource(Resource):
    def get(self, alias):
        logger.debug(f"top of GET /actors/aliases/{alias}")
        alias_id = Alias.generate_alias_id(g.request_tenant_id, alias)
        try:
            alias = Alias.from_db(alias_store[site()][alias_id])
        except KeyError:
            logger.debug(f"did not find alias with id: {alias}")
            raise ResourceError(
                f"No alias found: {alias}.", 404)
        logger.debug(f"found alias {alias}")
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
            raise DAOError(f"Invalid alias description. Missing required field: {msg}")
        return args

    def put(self, alias):
        logger.debug(f"top of PUT /actors/aliases/{alias}")
        alias_id = Alias.generate_alias_id(g.request_tenant_id, alias)
        try:
            alias_obj = Alias.from_db(alias_store[site()][alias_id])
        except KeyError:
            logger.debug(f"did not find alias with id: {alias}")
            raise ResourceError(f"No alias found: {alias}.", 404)
        logger.debug(f"found alias {alias_obj}")
        args = self.validate_put()
        actor_id = args.get('actor_id')
        if conf.web_case == 'camel':
            actor_id = args.get('actorId')
        dbid = Actor.get_dbid(g.request_tenant_id, actor_id)
        # update 10/2019: check that use has UPDATE permission on the actor -
        if not check_permissions(user=g.username, identifier=dbid, level=codes.UPDATE, roles=g.roles):
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
        logger.debug(f"Instantiating alias object. args: {args}")
        new_alias_obj = Alias(**args)
        logger.debug(f"Alias object instantiated; updating alias in alias_store. alias: {new_alias_obj}")
        alias_store[site()][alias_id] = new_alias_obj
        logger.info(f"alias updated for actor: {dbid}.")
        set_permission(g.username, new_alias_obj.alias_id, UPDATE)
        return ok(result=new_alias_obj.display(), msg="Actor alias updated successfully.")

    def delete(self, alias):
        logger.debug(f"top of DELETE /actors/aliases/{alias}")
        alias_id = Alias.generate_alias_id(g.request_tenant_id, alias)
        try:
            alias = Alias.from_db(alias_store[site()][alias_id])
        except KeyError:
            logger.debug(f"did not find alias with id: {alias}")
            raise ResourceError(
                f"No alias found: {alias}.", 404)

        # update 10/2019: check that use has UPDATE permission on the actor -
        # TODO - check: do we want to require UPDATE on the actor to delete the alias? Seems like UPDATE
        #               on the alias iteself should be sufficient...
        # if not check_permissions(user=g.username, identifier=alias.db_id, level=codes.UPDATE):
        #     raise PermissionsException(f"Not authorized -- you do not have UPDATE "
        #                                f"access to the actor associated with this alias.")
        try:
            del alias_store[site()][alias_id]
            # also remove all permissions - there should be at least one permissions associated
            # with the owner
            del permissions_store[site()][alias_id]
            logger.info(f"alias {alias_id} deleted from alias store.")
        except Exception as e:
            logger.info(f"got Exception {e} trying to delete alias {alias_id}")
        return ok(result=None, msg=f'Alias {alias} deleted successfully.')


class AliasNoncesResource(Resource):
    """Manage nonces for an alias"""

    def get(self, alias):
        logger.debug(f"top of GET /actors/aliases/{alias}/nonces")
        dbid = g.db_id
        alias_id = Alias.generate_alias_id(g.request_tenant_id, alias)
        try:
            alias = Alias.from_db(alias_store[site()][alias_id])
        except KeyError:
            logger.debug(f"did not find alias with id: {alias}")
            raise ResourceError(
                f"No alias found: {alias}.", 404)
        nonces = Nonce.get_nonces(actor_id=None, alias=alias_id)
        return ok(result=[n.display() for n in nonces], msg="Alias nonces retrieved successfully.")

    def post(self, alias):
        """Create a new nonce for an alias."""
        logger.debug(f"top of POST /actors/aliases/{alias}/nonces")
        dbid = g.db_id
        alias_id = Alias.generate_alias_id(g.request_tenant_id, alias)
        try:
            alias = Alias.from_db(alias_store[site()][alias_id])
        except KeyError:
            logger.debug(f"did not find alias with id: {alias}")
            raise ResourceError(
                f"No alias found: {alias}.", 404)
        args = self.validate_post()
        logger.debug(f"nonce post args validated: {alias}.")

        # supply "provided" fields:
        args['tenant'] = g.request_tenant_id
        args['api_server'] = g.api_server
        args['alias'] = alias_id
        args['owner'] = g.username
        args['roles'] = g.roles

        # create and store the nonce:
        nonce = Nonce(**args)
        logger.debug(f"able to create nonce object: {nonce}")
        Nonce.add_nonce(actor_id=None, alias=alias_id, nonce=nonce)
        logger.info(f"nonce added for alias: {alias}.")
        return ok(result=nonce.display(), msg="Alias nonce created successfully.")

    def validate_post(self):
        parser = Nonce.request_parser()
        try:
            args = parser.parse_args()
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            raise DAOError(f"Invalid nonce description: {msg}")
        # additional checks

        if 'level' in args:
            if not args['level'] in ALIAS_NONCE_PERMISSION_LEVELS:
                raise DAOError("Invalid nonce description. "
                               f"The level attribute must be one of: {ALIAS_NONCE_PERMISSION_LEVELS}")
        if conf.web_case == 'snake':
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
        logger.debug(f"top of GET /actors/aliases/{alias}/nonces/{nonce_id}")
        # check that alias exists -
        alias_id = Alias.generate_alias_id(g.request_tenant_id, alias)
        try:
            alias = Alias.from_db(alias_store[site()][alias_id])
        except KeyError:
            logger.debug(f"did not find alias with id: {alias}")
            raise ResourceError(
                f"No alias found: {alias}.", 404)

        nonce = Nonce.get_nonce(actor_id=None, alias=alias_id, nonce_id=nonce_id)
        return ok(result=nonce.display(), msg="Alias nonce retrieved successfully.")

    def delete(self, alias, nonce_id):
        """Delete a nonce."""
        logger.debug(f"top of DELETE /actors/aliases/{alias}/nonces/{nonce_id}")
        dbid = g.db_id
        # check that alias exists -
        alias_id = Alias.generate_alias_id(g.request_tenant_id, alias)
        try:
            alias = Alias.from_db(alias_store[site()][alias_id])
        except KeyError:
            logger.debug(f"did not find alias with id: {alias}")
            raise ResourceError(
                f"No alias found: {alias}.", 404)
        Nonce.delete_nonce(actor_id=None, alias=alias_id, nonce_id=nonce_id)
        return ok(result=None, msg="Alias nonce deleted successfully.")


def check_for_link_cycles(db_id, link_dbid):
    """
    Check if a link from db_id -> link_dbid would not create a cycle among linked actors.
    :param dbid: actor linking to link_dbid 
    :param link_dbid: id of actor being linked to.
    :return: 
    """
    logger.debug(f"top of check_for_link_cycles; db_id: {db_id}; link_dbid: {link_dbid}")
    # create the links graph, resolving each link attribute to a db_id along the way:
    # start with the passed in link, this is the "proposed" link -
    links = {db_id: link_dbid}
    for actor in actors_store[site()].items():
        if actor.get('link'):
            try:
                link_id = Actor.get_actor_id(actor.get('tenant'), actor.get('link'))
                link_dbid = Actor.get_dbid(g.request_tenant_id, link_id)
            except Exception as e:
                logger.error("corrupt link data; could not resolve link attribute in "
                             f"actor: {actor}; exception: {e}")
                continue
            # we do not want to override the proposed link passed in, as this actor could already have
            # a link (that was valid) and we need to check that the proposed link still works
            if not actor.get('db_id') == db_id:
                links[actor.get('db_id')] = link_dbid
    logger.debug(f"actor links dictionary built. links: {links}")
    if has_cycles(links):
        raise DAOError("Error: this update would result in a cycle of linked actors.")


def has_cycles(links):
    """
    Checks whether the `links` dictionary contains a cycle.
    :param links: dictionary of form d[k]=v where k->v is a link
    :return: 
    """
    logger.debug(f"top of has_cycles. links: {links}")
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
    logger.debug(f"top of validate_link. args: {args}")
    # check permissions - creating a link to an actor requires EXECUTE permissions
    # on the linked actor.
    try:
        link_id = Actor.get_actor_id(g.request_tenant_id, args['link'])
        link_dbid = Actor.get_dbid(g.request_tenant_id, link_id)
    except Exception as e:
        msg = "Invalid link parameter; unable to retrieve linked actor data. The link " \
              "must be a valid actor id or alias for which you have EXECUTE permission. "
        logger.info(f"{msg}; exception: {e}")
        raise DAOError(msg)
    try:
        check_permissions(g.username, link_dbid, EXECUTE)
    except Exception as e:
        logger.info("Got exception trying to check permissions for actor link. "
                    f"Exception: {e}; link: {link_dbid}")
        raise DAOError("Invalid link parameter. The link must be a valid "
                       "actor id or alias for which you have EXECUTE permission. "
                       f"Additional info: {e}")
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
        num_current_actors = len(actors_store[site()])
        num_workers = len(workers_store[site()])
        # Get total actors from abaco_metrics_store (stats are stored on a daily basis)
        total_actors = 0
        for stat_doc in abaco_metrics_store[site()].items():
            try:
                total_actors += stat_doc['actor_total']
            except KeyError as e:
                # Stat docs created daily, so some days there might not be stats in them
                continue
        ch = CommandChannel()
        result = {'currentActors': num_current_actors,
                  'totalActors': total_actors,
                  'workers': num_workers,
                  'commandQueue': len(ch._queue._queue)
                  }
        return ok(result=result, msg="Abaco utilization returned successfully.")

class ActorsResource(Resource):

    def get(self):
        logger.debug("top of GET /actors")
        if len(request.args) > 1 or (len(request.args) == 1 and not 'x-nonce' in request.args):
            args_given = request.args
            args_full = {}
            args_full.update(args_given)
            result = Search(args_full, 'actors', g.request_tenant_id, g.username).search()
            return ok(result=result, msg="Actors search completed successfully.")
        else:
            actors = []
            for actor_info in actors_store[site()].items():
                if actor_info['tenant'] == g.request_tenant_id:
                    actor = Actor.from_db(actor_info)
                    if check_permissions(g.username, actor.db_id, READ):
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
                valid_queues = conf.spawner_host_queues
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
                msg = f'{msg}: {e}'
            logger.debug(f"Validate post - invalid actor description: {msg}")
            raise DAOError(f"Invalid actor description: {msg}")
        return args

    def post(self):
        logger.info("top of POST to register a new actor.")
        args = self.validate_post()

        logger.debug("validate_post() successful")
        args['tenant'] = g.request_tenant_id
        args['api_server'] = g.api_server
        args['revision'] = 1
        args['owner'] = g.username

        # There are two options for the uid and gid to run in within the container. 1) the UID and GID to use
        # are computed by Abaco based on various configuration for the Abaco instance and tenant (such as
        # whether to use TAS, use a fixed UID, etc.) and 2) use the uid and gid created in the container.
        # Case 2) allows containers to be run as root and requires admin role in Abaco.
        use_container_uid = args.get('use_container_uid')
        run_as_executor = args.get('run_as_executor')
        if conf.web_case == 'camel':
            use_container_uid = args.get('useContainerUid')
            run_as_executor = args.get('runAsExecutor')
        logger.debug(f"request set use_container_uid: {use_container_uid}; type: {type(use_container_uid)}")
        if use_container_uid and run_as_executor:
            if conf.web_case == 'camel':
                raise DAOError("Cannot set both useContainerUid and runAsExecutor as true")
            else:
                raise DAOError("Cannot set both use_container_uid and run_as_executor as true")
        if run_as_executor:
            if not tenant_can_use_tas(g.tenant_id):
                raise DAOError("run_as_executor isn't supported for your tenant")
        if not use_container_uid:
            logger.debug("use_container_uid was false. looking up uid and gid...")
            uid, gid, home_dir = get_uid_gid_homedir(args, g.username, g.request_tenant_id)
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
         # Checking for 'log_ex' input arg.
        if conf.web_case == 'camel':
            if 'logEx' in args and args.get('logEx') is not None:
                log_ex = int(args.get('logEx'))
                logger.debug(f"Found log_ex in args; using: {log_ex}")
                args['logEx'] = log_ex
        else:
            if 'log_ex' in args and args.get('log_ex') is not None:
                log_ex = int(args.get('log_ex'))
                logger.debug(f"Found log_ex in args; using: {log_ex}")
                args['log_ex'] = log_ex
        cron = None
        if conf.web_case == 'camel':
            logger.debug("Case is camel")
            if 'cronSchedule' in args and args.get('cronSchedule') is not None:
                cron = args.get('cronSchedule')
        else:
            if 'cron_schedule' in args and args.get('cron_schedule') is not None:
                logger.debug("Case is snake")
                cron = args.get('cron_schedule')
        if cron is not None:
            logger.debug("Cron has been posted")
            # set_cron checks for the 'now' alias 
            # It also checks that the cron schedule is greater than or equal to the current UTC time
            r = Actor.set_cron(cron)
            logger.debug(f"r is {r}")
            if r.fixed[2] in ['hours', 'hour', 'days', 'day', 'weeks', 'week', 'months', 'month']:
                args['cron_schedule'] = cron
                logger.debug(f"setting cron_next_ex to {r.fixed[0]}")
                args['cron_next_ex'] = r.fixed[0]
                args['cron_on'] = True
            else:
                raise BadRequest(f'{r.fixed[2]} is an invalid unit of time')
        else:
            logger.debug("Cron schedule was not sent in")
        if conf.web_case == 'camel':
            max_workers = args.get('maxWorkers')
            args['max_workers'] = max_workers
        else:
            max_workers = args.get('max_workers')
            args['maxWorkers'] = max_workers
        if max_workers and 'stateless' in args and not args.get('stateless'):
            raise DAOError("Invalid actor description: stateful actors can only have 1 worker.")
        args['mounts'] = get_all_mounts(args)
        logger.debug(f"create args: {args}")
        actor = Actor(**args)
        # Change function
        actors_store[site()].add_if_empty([actor.db_id], actor)
        abaco_metrics_store[site()].full_update(
            {'_id': f'{datetime.date.today()}-stats'},
            {'$inc': {'actor_total': 1},
             '$addToSet': {'actor_dbids': actor.db_id}},
             upsert=True)

        logger.debug(f"new actor saved in db. id: {actor.db_id}. image: {actor.image}. tenant: {actor.tenant}")
        if num_init_workers > 0:
            actor.ensure_one_worker()
        logger.debug("ensure_one_worker() called")
        set_permission(g.username, actor.db_id, UPDATE)
        logger.debug(f"UPDATE permission added to user: {g.username}")
        return ok(result=actor.display(), msg="Actor created successfully.", request=request)


class ActorResource(Resource):
    def get(self, actor_id):
        logger.debug(f"top of GET /actors/{actor_id}")
        try:
            actor = Actor.from_db(actors_store[site()][g.db_id])
        except KeyError:
            logger.debug(f"did not find actor with id: {actor_id}")
            raise ResourceError(
                f"No actor found with identifier: {actor_id}.", 404)
        logger.debug(f"found actor {actor_id}")
        return ok(result=actor.display(), msg="Actor retrieved successfully.")

    def delete(self, actor_id):
        logger.debug(f"top of DELETE /actors/{actor_id}")
        id = g.db_id
        try:
            actor = Actor.from_db(actors_store[site()][id])
        except KeyError:
            actor = None

        if actor:
            # first set actor status to SHUTTING_DOWN so that no further autoscaling takes place
            actor.set_status(id, SHUTTING_DOWN)
            # delete all logs associated with executions -
            try:
                executions_by_actor = executions_store[site()].items({'actor_id': id})
                for execution in executions_by_actor:
                    del logs_store[site()][execution['id']]
            except KeyError as e:
                logger.info(f"got KeyError {e} trying to retrieve actor or executions with id {id}")
        # shutdown workers ----
        logger.info(f"calling shutdown_workers() for actor: {id}")
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
                logger.debug(f"did not find workers for actor: {actor_id}; escaping.")
                shutdown = True
                break
            if not workers:
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
            logger.info(f"Deleted actor message channel for actor: {id}")
        except Exception as e:
            # if we get an error trying to remove the inbox, log it but keep going
            logger.error(f"Unable to delete the actor's message channel for actor: {id}, exception: {e}")
        del actors_store[site()][id]
        logger.info(f"actor {id} deleted from store.")
        del permissions_store[site()][id]
        logger.info(f"actor {id} permissions deleted from store.")
        del nonce_store[site()][id]
        logger.info(f"actor {id} nonces delete from nonce store.")
        msg = 'Actor deleted successfully.'
        if workers:
            msg = "Actor deleted successfully, though Abaco is still cleaning up some of the actor's resources."
        return ok(result=None, msg=msg)

    def put(self, actor_id):
        logger.debug(f"top of PUT /actors/{actor_id}")
        dbid = g.db_id
        try:
            actor = Actor.from_db(actors_store[site()][dbid])
        except KeyError:
            logger.debug(f"did not find actor {dbid} in store.")
            raise ResourceError(
                f"No actor found with id: {actor_id}.", 404)
        previous_image = actor.image
        previous_status = actor.status
        previous_owner = actor.owner
        previous_revision = actor.revision
        args = self.validate_put(actor)
        logger.debug("PUT args validated successfully.")
        args['tenant'] = g.request_tenant_id
         # Checking for 'log_ex' input arg.
        if conf.web_case == 'camel':
            if 'logEx' in args and args.get('logEx') is not None:
                log_ex = int(args.get('logEx'))
                logger.debug(f"Found log_ex in args; using: {log_ex}")
                args['logEx'] = log_ex
        else:
            if 'log_ex' in args and args.get('log_ex') is not None:
                log_ex = int(args.get('log_ex'))
                logger.debug(f"Found log_ex in args; using: {log_ex}")
                args['log_ex'] = log_ex
        # Check for both camel and snake catenantse
        cron = None
        if conf.web_case == 'camel':
            if 'cronSchedule' in args and args.get('cronSchedule') is not None:
                cron = args.get('cronSchedule')
            if 'cronOn' in args and args.get('cronOn') is not None:
                actor['cron_on'] = args.get('cronOn')
            if 'runAsExecutor' in args and args.get('runAsExecutor') is not None:
                actor['run_as_executor'] = args.get('runAsExecutor')
        else:
            if 'cron_schedule' in args and args.get('cron_schedule') is not None:
                cron = args.get('cron_schedule')
            if 'cron_on' in args and args.get('cron_on') is not None:
                actor['cron_on'] = args.get('cron_on')
            if 'run_as_executor' in args and args.get('run_as_executor') is not None:
                actor['run_as_executor'] = args.get('run_as_executor')
        #run_as_executor only works for TAS tenants so we need to check if the tenant uses TAS
        if actor['run_as_executor']:
            if not tenant_can_use_tas(g.tenant_id):
                raise DAOError("run_as_executor isn't supported for your tenant")
        if cron is not None:
            # set_cron checks for the 'now' alias 
            # It also checks that the cron schedule is greater than or equal to the current UTC time
            # Check for proper unit of time
            r = Actor.set_cron(cron)
            if r.fixed[2] in ['hours', 'hour', 'days', 'day', 'weeks', 'week', 'months', 'month']: 
                args['cron_schedule'] = cron
                logger.debug(f"setting cron_next_ex to {r.fixed[0]}")
                args['cron_next_ex'] = r.fixed[0]
            else:
                raise BadRequest(f'{r.fixed[2]} is an invalid unit of time')
        else:
            logger.debug("No cron schedule has been sent")
        if args['queue']:
            valid_queues = conf.spawner_host_queues
            if args['queue'] not in valid_queues:
                raise BadRequest('Invalid queue name.')
        if args['link']:
            validate_link(args)
        # user can force an update by setting the force param:
        update_image = args.get('force')
        if not update_image and args['image'] == previous_image:
            logger.debug("new image is the same and force was false. not updating actor.")
            logger.debug(f"Setting status to the actor's previous status which is: {previous_status}")
            args['status'] = previous_status
            args['revision'] = previous_revision
        else:
            update_image = True
            args['status'] = SUBMITTED
            args['revision'] = previous_revision + 1
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
        if conf.web_case == 'camel':
            use_container_uid = args.get('useContainerUid')
        if actor.get('use_container_uid') and actor.get('run_as_executor'):
            if conf.web_case == 'camel':
                raise DAOError("Cannot set both useContainerUid and runAsExecutor as true")
            else:
                raise DAOError("Cannot set both use_container_uid and run_as_executor as true")
                 
        if not use_container_uid:
            uid, gid, home_dir = get_uid_gid_homedir(args, g.username, g.request_tenant_id)
            if uid:
                args['uid'] = uid
            if gid:
                args['gid'] = gid
            if home_dir:
                args['tasdir'] = home_dir

        args['mounts'] = get_all_mounts(args)
        args['last_update_time'] = get_current_utc_time()
        logger.debug(f"update args: {args}")
        actor = Actor(**args)

        actors_store[site()][actor.db_id] = actor.to_db()

        logger.info(f"updated actor {actor_id} stored in db.")
        if update_image:
            worker_id = Worker.request_worker(tenant=g.request_tenant_id, actor_id=actor.db_id)
            # get actor queue name
            ch = CommandChannel(name=actor.queue)
            # stop_existing defaults to True, so this command will also stop existing workers:
            ch.put_cmd(actor_id=actor.db_id,
                       worker_id=worker_id,
                       image=actor.image,
                       revision=actor.revision,
                       tenant=args['tenant'],
                       site_id=site())
            ch.close()
            logger.debug("put new command on command channel to update actor.")
        # put could have been issued by a user with
        if not previous_owner == g.username:
            set_permission(g.username, actor.db_id, UPDATE)
        return ok(result=actor.display(),
                  msg="Actor updated successfully.")

    def validate_put(self, actor):
        # inherit derived attributes from the original actor, including id and db_id:
        parser = Actor.request_parser()
        # remove since name is only required for POST, not PUT
        parser.remove_argument('name')
        parser.add_argument('force', type=bool, required=False, help="Whether to force an update of the actor image", default=False)

        # if camel case, need to remove fields snake case versions of fields that can be updated
        if conf.web_case == 'camel':
            actor.pop('use_container_uid')
            actor.pop('default_environment')
            actor.pop('max_workers')
            actor.pop('mem_limit')
            actor.pop('max_cpus')
            actor.pop('log_ex')

        # this update overrides all required and optional attributes
        try:
            new_fields = parser.parse_args()
            logger.debug(f"new fields from actor PUT: {new_fields}")
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            else:
                msg = f'{msg}: {e}'
            raise DAOError(f"Invalid actor description: {msg}")
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
        logger.debug(f"top of GET /actors/{actor_id}/state")
        dbid = g.db_id
        try:
            actor = Actor.from_db(actors_store[site()][dbid])
        except KeyError:
            raise ResourceError(
                f"No actor found with id: {actor_id}.", 404)
        return ok(result={'state': actor.get('state') }, msg="Actor state retrieved successfully.")

    def post(self, actor_id):
        logger.debug(f"top of POST /actors/{actor_id}/state")
        dbid = g.db_id
        try:
            actor = Actor.from_db(actors_store[site()][dbid])
        except KeyError:
            logger.debug(f"did not find actor with id: {actor_id}.")
            raise ResourceError(
                f"No actor found with id: {actor_id}.", 404)
        if actor.stateless:
            logger.debug(f"cannot update state for stateless actor: {actor_id}")
            raise ResourceError("actor is stateless.", 404)
        state = self.validate_post()
        logger.debug(f"state post params validated: {actor_id}")
        actors_store[site()][dbid, 'state'] = state
        logger.info(f"state updated: {actor_id}")
        actor = Actor.from_db(actors_store[site()][dbid])
        return ok(result=actor.display(), msg="State updated successfully.")

    def validate_post(self):
        json_data = request.get_json()
        if not json_data:
            raise DAOError("Invalid actor state description: state must be JSON serializable.")
        return json_data


class ActorConfigsResource(Resource):
    def get(self):
        logger.debug("top of GET /configs")
        # who can see and modify this config?
        # permission model
        # permissions endpoint
        configs = []
        logger.debug(f"CONFIGS: {configs_store[site()].items()}")
        for v in configs_store[site()].items():
            logger.debug(f"item is {v}")
            if v['tenant'] == g.request_tenant_id:
                configs.append(ActorConfig.from_db(v).display())
        logger.info("actor configs retrieved.")
        return ok(result=configs, msg="Actor Configs retrieved successfully.")

    def validate_post(self):
        parser = ActorConfig.request_parser()
        try:
            args = parser.parse_args()
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            raise DAOError(f"Invalid config description. Missing required field: {msg}")
        return args

    def post(self):
        logger.info("top of POST to register a new config.")
        args = self.validate_post()

        # Check that the actor ids correspond to real actors or aliases and that the user
        # has UPDATE access to each of them ---
        # first, split the string by comma
        actors = args.get('actors')
        try:
            actors_list = actors.replace(" ", "").split(',')
            for count, wrd in enumerate(actors_list):
                actors_list[count] = wrd.strip()
        except Exception as e:
            logger.info(f"Got exception trying to parse actor parameter. e: {e}")
            raise DAOError(f"Could not parse the actors parameter ({actors}). It should be a comma-separated list of "
                           f"actors ids or alias ids. Details: {e}")
        logger.debug(f"actors are {actors_list}")
        # Loop through ids and check existence
        for a in actors_list:
            try:
                # need to check if this identifier is an actor id or an alias
                db_id = f"{g.request_tenant_id}_{a}"
                actors_store[site()][db_id]
            except KeyError:
                logger.debug(f"did not find actor: {db_id}. now checking alias")
                try:
                    alias_store[site()][db_id]
                except KeyError:
                    raise ResourceError(f"No actor or alias found with id: {a}.", 404)
            # also check that user has update access to actor --
            check_permissions(user=g.username, identifier=db_id, level=codes.UPDATE, roles=g.roles)

        args['tenant'] = g.request_tenant_id
        if args.get('isSecret') or args.get('is_secret'):
            args['value'] = encrypt_utils.encrypt(args.get('value'))
        logger.debug("config post args validated")
        # create the ActorConfig object with the args --
        actor_config = ActorConfig(**args)
        # additional checks for reserved words, forbidden characters and uniqueness
        actor_config.check_and_create_config()
        # save the config to the db
        config_id = ActorConfig.get_config_db_key(tenant_id=g.request_tenant_id, name=actor_config.name)
        configs_store[site()][config_id] = actor_config.to_db()
        # set permissions for this config
        set_config_permission(g.username, config_id, UPDATE)
        return ok(result=actor_config.display(), msg="Actor config created successfully.")


class ActorConfigResource(Resource):
    def get(self, config_name):
        logger.debug(f"top of GET /actors/configs/{config_name}")
        config_id = ActorConfig.get_config_db_key(tenant_id=g.request_tenant_id, name=config_name)
        try:
            config = ActorConfig.from_db(configs_store[site()][config_id])
        except KeyError:
            logger.debug(f"did not find config with id: {config_id}")
            raise ResourceError(f"No config found: {config_name}.", 404)
        logger.debug(f"found config {config}")
        return ok(result=config.display(), msg="Config retrieved successfully.")


    def validate_put(self):
        logger.debug("top of validate_put")
        try:
            data = request.get_json()
        except:
            data = None
        # if data and 'alias' in data or 'alias' in request.form:
        #     logger.debug("found alias in the PUT.")
        #     raise DAOError("Invalid alias update description. The alias itself cannot be updated in a PUT request.")
        parser = ActorConfig.request_parser()
        logger.debug("got the actor config parser")
        # # remove since alias is only required for POST, not PUT
        # parser.remove_argument('config')
        try:
            args = parser.parse_args()
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            raise DAOError(f"Invalid alias description. Could not determine if required fields are present. "
                           f"Is the body valid JSON? Details: {msg}")
        return args

    def put(self, config_name):
        logger.debug(f"top of PUT /actors/configs/{config_name}")
        config_id = ActorConfig.get_config_db_key(tenant_id=g.request_tenant_id, name=config_name)
        try:
            config_obj = ActorConfig.from_db(configs_store[site()][config_id])
        except KeyError:
            logger.debug(f"did not find config with name: {config_name}")
            raise ResourceError(f"No config found: {config_name}.", 404)
        logger.debug(f"found config {config_obj}")
        args = self.validate_put()
        if not check_config_permissions(user=g.username, config_id=config_id, level=codes.UPDATE, roles=g.roles):
            raise PermissionsException(f"Not authorized -- you do not have UPDATE access to this actor config.")

        # Check that the actor ids correspond to real actors or aliases and that the user
        # has UPDATE access to each of them ---
        # first, split the string by comma
        actors = args.get('actors')
        try:
            actors_list = actors.replace(" ", "").split(',')
            for count, wrd in enumerate(actors_list):
                actors_list[count] = wrd.strip()
        except Exception as e:
            logger.info(f"Got exception trying to parse actor parameter. e: {e}")
            raise DAOError(f"Could not parse the actors parameter ({actors}). It should be a comma-separated list of "
                           f"actors ids. Details: {e}")
        logger.debug(f"actors are {actors_list}")
        # Loop through ids and check existence
        for a in actors_list:
            try:
                # need to check if this identifier is an actor id or an alias
                db_id = f"{g.request_tenant_id}_{a}"
                actors_store[site()][db_id]
            except KeyError:
                logger.debug(f"did not find actor: {db_id}. now checking alias")
                try:
                    alias_store[site()][db_id]
                except KeyError:
                    raise ResourceError(f"No actor or alias found with id: {a}.", 404)
            # also check that user has update access to actor --
            check_permissions(user=g.username, identifier=db_id, level=codes.UPDATE, roles=g.roles)

        # supply "provided" fields:
        args['tenant'] = config_obj.tenant
        args['name'] = config_obj.name
        if args.get('isSecret') or args.get('is_secret'):
            args['value'] = encrypt_utils.encrypt(args.get('value'))
        logger.debug(f"Instantiating actor config object. args: {args}")
        new_config_obj = ActorConfig(**args)
        logger.debug("Check that the config is not a reserved word")
        new_config_obj.check_reserved_words()
        logger.debug("Check that the config has no forbidden characters")
        new_config_obj.check_forbidden_char()
        logger.debug(f"Actor Config object instantiated; updating actor config in configs_store. config: {new_config_obj}")
        configs_store[site()][config_id] = new_config_obj
        logger.debug(f"NEW CONFIG OBJ {new_config_obj}")
        logger.info(f"actor config updated for config: {config_id}.")
        return ok(result=new_config_obj.display(), msg="Actor config updated successfully.")

    def delete(self, config_name):
        logger.debug(f"top of DELETE /actors/configs/{config_name}")
        config_id = ActorConfig.get_config_db_key(tenant_id=g.request_tenant_id, name=config_name)
        try:
            ActorConfig.from_db(configs_store[site()][config_id])
        except KeyError:
            raise ResourceError(f"No config found with name: {config_name}.", 404)

        # check that the user has update access to the config
        if not check_config_permissions(user=g.username, config_id=config_id, level=codes.UPDATE, roles=g.roles):
            raise PermissionsException(f"Not authorized -- you do not have UPDATE access to this actor config.")

        # delete the config and associated permissions
        try:
            del configs_store[site()][config_id]
            # also remove all permissions - there should be at least one permissions associated
            # with the owner
            del configs_permissions_store[site()][config_id]
            logger.info(f"Actor config {config_id} deleted from actor config store.")
        except Exception as e:
            logger.info(f"got Exception {e} trying to delete alias {config_id}")
        return ok(result=None, msg=f'Actor config {config_name} deleted successfully.')


class ActorExecutionsResource(Resource):
    def get(self, actor_id):
        logger.debug(f"top of GET /actors/{actor_id}/executions")
        if len(request.args) > 1 or (len(request.args) == 1 and not 'x-nonce' in request.args):
            args_given = request.args
            args_full = {'actor_id': f'{g.request_tenant_id}_{actor_id}'}
            args_full.update(args_given)
            result = Search(args_full, 'executions', g.request_tenant_id, g.username).search()
            return ok(result=result, msg="Executions search completed successfully.")
        else:
            dbid = g.db_id
            try:
                actor = Actor.from_db(actors_store[site()][dbid])
            except KeyError:
                logger.debug(f"did not find actor: {actor_id}.")
                raise ResourceError(
                    f"No actor found with id: {actor_id}.", 404)
            try:
                summary = ExecutionsSummary(db_id=dbid)
            except DAOError as e:
                logger.debug(f"did not find executions summary: {actor_id}")
                raise ResourceError(f"Could not retrieve executions summary for actor: {actor_id}. Details: {e}", 404)
            return ok(result=summary.display(), msg="Actor executions retrieved successfully.")

    def post(self, actor_id):
        logger.debug(f"top of POST /actors/{actor_id}/executions")
        id = g.db_id
        try:
            actor = Actor.from_db(actors_store[site()][id])
        except KeyError:
            logger.debug(f"did not find actor: {actor_id}.")
            raise ResourceError(
                f"No actor found with id: {actor_id}.", 404)
        args = self.validate_post()
        logger.debug(f"execution post args validated: {actor_id}.")
        Execution.add_execution(id, args)
        logger.info(f"execution added: {actor_id}.")
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
            raise DAOError(f"Invalid actor execution description: {msg}")

        for k,v in args.items():
            try:
                int(v)
            except ValueError:
                raise ResourceError(message=f"Argument {k} must be an integer.")
        return args


class ActorNoncesResource(Resource):
    """Manage nonces for an actor"""

    def get(self, actor_id):
        logger.debug(f"top of GET /actors/{actor_id}/nonces")
        dbid = g.db_id
        nonces = Nonce.get_nonces(actor_id=dbid, alias=None)
        return ok(result=[n.display() for n in nonces], msg="Actor nonces retrieved successfully.")

    def post(self, actor_id):
        """Create a new nonce for an actor."""
        logger.debug(f"top of POST /actors/{actor_id}/nonces")
        dbid = g.db_id
        args = self.validate_post()
        logger.debug(f"nonce post args validated; dbid: {dbid}; actor_id: {actor_id}.")

        # supply "provided" fields:
        args['tenant'] = g.request_tenant_id
        args['api_server'] = g.api_server
        args['db_id'] = dbid
        args['owner'] = g.username
        args['roles'] = g.roles

        # create and store the nonce:
        nonce = Nonce(**args)
        try:
            logger.debug(f"nonce.actor_id: {nonce.actor_id}")
        except Exception as e:
            logger.debug(f"got exception trying to log actor_id on nonce; e: {e}")
        Nonce.add_nonce(actor_id=dbid, alias=None, nonce=nonce)
        logger.info(f"nonce added for actor: {actor_id}.")
        return ok(result=nonce.display(), msg="Actor nonce created successfully.")

    def validate_post(self):
        parser = Nonce.request_parser()
        try:
            args = parser.parse_args()
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            raise DAOError(f"Invalid nonce description: {msg}")
        # additional checks
        if 'level' in args:
            if not args['level'] in PERMISSION_LEVELS:
                raise DAOError("Invalid nonce description. "
                               f"The level attribute must be one of: {PERMISSION_LEVELS}")
        if conf.web_case == 'snake':
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
        logger.debug(f"top of GET /actors/{actor_id}/nonces/{nonce_id}")
        dbid = g.db_id
        nonce = Nonce.get_nonce(actor_id=dbid, alias=None, nonce_id=nonce_id)
        return ok(result=nonce.display(), msg="Actor nonce retrieved successfully.")

    def delete(self, actor_id, nonce_id):
        """Delete a nonce."""
        logger.debug(f"top of DELETE /actors/{actor_id}/nonces/{nonce_id}")
        dbid = g.db_id
        Nonce.delete_nonce(actor_id=dbid, alias=None, nonce_id=nonce_id)
        return ok(result=None, msg="Actor nonce deleted successfully.")


class ActorExecutionResource(Resource):
    def get(self, actor_id, execution_id):
        logger.debug(f"top of GET /actors/{actor_id}/executions/{execution_id}.")
        dbid = g.db_id
        try:
            exc = Execution.from_db(executions_store[site()][f'{dbid}_{execution_id}'])
        except KeyError:
            logger.debug(f"did not find execution with actor id of {actor_id} and execution id of {execution_id}.")
            raise ResourceError(f"No executions found with actor id of {actor_id} and execution id of {execution_id}.")
        return ok(result=exc.display(), msg="Actor execution retrieved successfully.")

    def delete(self, actor_id, execution_id):
        logger.debug(f"top of DELETE /actors/{actor_id}/executions/{execution_id}.")
        dbid = g.db_id
        try:
            exc = Execution.from_db(executions_store[site()][f'{dbid}_{execution_id}'])
        except KeyError:
            logger.debug(f"did not find execution with actor id of {actor_id} and execution id of {execution_id}.")
            raise ResourceError(f"No executions found with actor id of {actor_id} and execution id of {execution_id}.")
        # check status of execution:
        if not exc.status == codes.RUNNING:
            logger.debug(f"execution not in {codes.RUNNING} status: {exc.status}")
            raise ResourceError(f"Cannot force quit an execution not in {codes.RUNNING} status. "
                                f"Execution was found in status: {exc.status}")
        # send force_quit message to worker:
        # TODO - should we set the execution status to FORCE_QUIT_REQUESTED?
        logger.debug(f"issuing force quit to worker: {exc.worker_id} "
                     f"for actor_id: {actor_id} execution_id: {execution_id}")
        ch = WorkerChannel(worker_id=exc.worker_id)
        ch.put('force_quit')
        msg = f'Issued force quit command for execution {execution_id}.'
        return ok(result=None, msg=msg)


class ActorExecutionResultsResource(Resource):
    def get(self, actor_id, execution_id):
        logger.debug(f"top of GET /actors/{actor_id}/executions/{execution_id}/results")
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
        # logger.debug(f"limit: {limit}")
        # while num < limit:
        #     try:
        #         result.append(ch.get(timeout=0.1))
        #         num += 1
        #     except Exception:
        #         break
        # logger.debug(f"collected {num} results")
        # ch.close()
        # return Response(result)


class ActorExecutionLogsResource(Resource):
    def get(self, actor_id, execution_id):
        def get_hypermedia(actor, exc):
            return {'_links': {'self': f'{actor.api_server}/v3/actors/{actor.id}/executions/{exc.id}/logs',
                               'owner': f'{actor.api_server}/v3/oauth2/profiles/{actor.owner}',
                               'execution': f'{actor.api_server}/v3/actors/{actor.id}/executions/{exc.id}'},
                    }
        logger.debug(f"top of GET /actors/{actor_id}/executions/{execution_id}/logs.")
        if len(request.args) > 1 or (len(request.args) == 1 and not 'x-nonce' in request.args):
            args_given = request.args
            args_full = {'actor_id': f'{g.request_tenant_id}_{actor_id}', '_id': execution_id}
            args_full.update(args_given)
            result = Search(args_full, 'logs', g.request_tenant_id, g.username).search()
            return ok(result=result, msg="Log search completed successfully.")
        else:
            dbid = g.db_id
            try:
                actor = Actor.from_db(actors_store[site()][dbid])
            except KeyError:
                logger.debug(f"did not find actor: {actor_id}.")
                raise ResourceError(
                    f"No actor found with id: {actor_id}.", 404)
            try:
                exc = Execution.from_db(executions_store[site()][f'{dbid}_{execution_id}'])
            except KeyError:
                logger.debug(f"did not find execution with actor id of {actor_id} and execution id of {execution_id}.")
                raise ResourceError(f"No executions found with actor id of {actor_id} and execution id of {execution_id}.")
            try:
                logs = logs_store[site()][execution_id]['logs']
            except KeyError:
                logger.debug(f"did not find logs. execution: {execution_id}. actor: {actor_id}.")
                logs = ""
            result={'logs': logs}
            result.update(get_hypermedia(actor, exc))
            return ok(result, msg="Logs retrieved successfully.")


def get_messages_hypermedia(actor):
    return {'_links': {'self': f'{actor.api_server}/v3/actors/{actor.id}/messages',
                       'owner': f'{actor.api_server}/v3/oauth2/profiles/{actor.owner}',
                       },
            }


class MessagesResource(Resource):
    def get(self, actor_id):
        logger.debug(f"top of GET /actors/{actor_id}/messages")
        # check that actor exists
        id = g.db_id
        try:
            actor = Actor.from_db(actors_store[site()][id])
        except KeyError:
            logger.debug(f"did not find actor: {actor_id}.")
            raise ResourceError(
                f"No actor found with id: {actor_id}.", 404)
        ch = ActorMsgChannel(actor_id=id)
        result = {'messages': len(ch._queue._queue)}
        ch.close()
        logger.debug(f"messages found for actor: {actor_id}.")
        result.update(get_messages_hypermedia(actor))
        return ok(result)

    def delete(self, actor_id):
        logger.debug(f"top of DELETE /actors/{actor_id}/messages")
        # check that actor exists
        id = g.db_id
        try:
            actor = Actor.from_db(actors_store[site()][id])
        except KeyError:
            logger.debug(f"did not find actor: {actor_id}.")
            raise ResourceError(
                f"No actor found with id: {actor_id}.", 404)
        ch = ActorMsgChannel(actor_id=id)
        ch._queue._queue.purge()
        result = {'msg': "Actor mailbox purged."}
        ch.close()
        logger.debug(f"messages purged for actor: {actor_id}.")
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
                if int(length) > conf.web_max_content_length:
                    raise ResourceError(f"Message exceeds max content length of: {conf.web_max_content_length}")
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
                    logger.debug(f"message POST body could not be serialized. args: {args}")
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
            return {'_links': {'self': f'{actor.api_server}/v3/actors/{actor.id}/executions/{exc}',
                               'owner': f'{actor.api_server}/v3/oauth2/profiles/{actor.owner}',
                               'messages': f'{actor.api_server}/v3/actors/{actor.id}/messages'}, }

        logger.debug(f"top of POST /actors/{actor_id}/messages.")
        synchronous = False
        dbid = g.db_id
        try:
            actor = Actor.from_db(actors_store[site()][dbid])
        except KeyError:
            logger.debug(f"did not find actor: {actor_id}.")
            raise ResourceError(f"No actor found with id: {actor_id}.", 404)
        got_actor_timer = timeit.default_timer()
        args = self.validate_post()
        val_post_timer = timeit.default_timer()
        d = {}
        # build a dictionary of k:v pairs from the query parameters, and pass a single
        # additional object 'message' from within the post payload. Note that 'message'
        # need not be JSON data.
        logger.debug(f"POST body validated. actor: {actor_id}.")
        for k, v in request.args.items():
            if k == '_abaco_synchronous':
                try:
                    if v.lower() == 'true':
                        logger.debug("found synchronous and value was true")
                        synchronous = True
                    else:
                        logger.debug("found synchronous and value was false")
                except Exception as e:
                    logger.info(f"Got exception trying to parse the _abaco_synchronous; e: {e}")
            if k == 'message':
                continue
            d[k] = v
        request_args_timer = timeit.default_timer()
        logger.debug(f"extra fields added to message from query parameters: {d}.")
        if synchronous:
            # actor mailbox length must be 0 to perform a synchronous execution
            ch = ActorMsgChannel(actor_id=actor_id)
            box_len = len(ch._queue._queue)
            ch.close()
            if box_len > 3:
                raise ResourceError("Cannot issue synchronous execution when actor message queue > 0.")
        if hasattr(g, 'username'):
            d['_abaco_username'] = g.username
            logger.debug(f"_abaco_username: {g.username} added to message.")
        if hasattr(g, 'jwt_header_name'):
            d['_abaco_jwt_header_name'] = g.jwt_header_name
            logger.debug(f"abaco_jwt_header_name: {g.jwt_header_name} added to message.")
        # create an execution
        before_exc_timer = timeit.default_timer()
        if actors_store[site()][dbid]["run_as_executor"]:
            #get the uid and gid from the executor
            uid, gid, homedir = get_uid_gid_homedir(actor_id, g.username, g.tenant_id)
            exc = Execution.add_execution(dbid, {'cpu': 0,
                                                'io': 0,
                                                'runtime': 0,
                                                'status': SUBMITTED,
                                                'executor': g.username,
                                                'executor_uid': uid,               #this is the uid and gid of the executor not the owner which is needed if the actor is ran as an executor
                                                'executor_gid': gid})
        else:
            exc = Execution.add_execution(dbid, {'cpu': 0,
                                                'io': 0,
                                                'runtime': 0,
                                                'status': SUBMITTED,
                                                'executor': g.username})
        after_exc_timer = timeit.default_timer()
        logger.info(f"Execution {exc} added for actor {actor_id}")
        d['_abaco_execution_id'] = exc
        d['_abaco_Content_Type'] = args.get('_abaco_Content_Type', '')
        d['_abaco_actor_revision'] = actor.revision
        logger.debug(f"Final message dictionary: {d}")
        
        before_ch_timer = timeit.default_timer()
        ch = ActorMsgChannel(actor_id=dbid)
        after_ch_timer = timeit.default_timer()
        ch.put_msg(message=args['message'], d=d)
        after_put_msg_timer = timeit.default_timer()
        ch.close()
        after_ch_close_timer = timeit.default_timer()
        logger.debug(f"Message added to actor inbox. id: {actor_id}.")
        # make sure at least one worker is available
        actor = Actor.from_db(actors_store[site()][dbid])
        after_get_actor_db_timer = timeit.default_timer()
        actor.ensure_one_worker()
        after_ensure_one_worker_timer = timeit.default_timer()
        logger.debug(f"ensure_one_worker() called. id: {actor_id}.")
        if args.get('_abaco_Content_Type') == 'application/octet-stream':
            result = {'execution_id': exc, 'msg': 'binary - omitted'}
        else:
            result = {'execution_id': exc, 'msg': args['message']}
        result.update(get_hypermedia(actor, exc))
        case = conf.web_case
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
        logger.info(f"Times to process message: {time_data}")
        if synchronous:
            return self.do_synch_message(exc)
        if not case == 'camel':
            return ok(result)
        else:
            return ok(dict_to_camel(result))

    def do_synch_message(self, execution_id):
        """Monitor for the termination of a synchronous message execution."""
        logger.debug("top of do_synch_message")
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
                logger.debug("checking for result on the results channel...")
                try:
                    result = ch.get(timeout=timeout)
                    ch.close()
                    complete = True
                    binary_result = True
                    logger.debug("check_results_channel thread got a result.")
                except ChannelClosedException as e:
                    # the channel unexpectedly closed, so just return
                    logger.info(f"unexpected ChannelClosedException in check_results_channel thread: {e}")
                    check_results_channel = False
                except ChannelTimeoutException:
                    pass
                except Exception as e:
                    logger.info(f"unexpected exception in check_results_channel thread: {e}")
                    check_results_channel = False

            # check to see if execution has completed:
            if not complete:
                try:
                    exc = Execution.from_db(executions_store[site()][f'{dbid}_{execution_id}'])
                    complete = exc.status == COMPLETE
                except Exception as e:
                    logger.info(f"got exception trying to check execution status: {e}")
            if complete:
                logger.debug("execution is complete")
                if not result:
                    # first try one more time to get a result -
                    if check_results_channel:
                        logger.debug("looking for result on results channel.")
                        try:
                            result = ch.get(timeout=timeout)
                            binary_result = True
                            logger.debug("got binary result.")
                        except Exception as e:
                            logger.debug(f"got exception: {e} -- did not get binary result")
                            pass
                    # if we still have no result, get the logs -
                    if not result:
                        logger.debug("stll don't have result; looking for logs...")
                        try:
                            result = logs_store[site()][execution_id]['logs']
                            logger.debug("got logs; returning result.")
                        except KeyError:
                            logger.debug(f"did not find logs. execution: {execution_id}. actor: {dbid}.")
                            result = ""
        response = make_response(result)
        if binary_result:
            response.headers['content-type'] = 'application/octet-stream'
        try:
            ch.close()
        except:
            pass
        logger.debug("returning synchronous response.")
        return response


class WorkersResource(Resource):
    def get(self, actor_id):
        logger.debug(f"top of GET /actors/{actor_id}/workers for tenant {g.request_tenant_id}.")
        if len(request.args) > 1 or (len(request.args) == 1 and not 'x-nonce' in request.args):
            args_given = request.args
            args_full = {'actor_id': f'{g.request_tenant_id}_{actor_id}'}
            args_full.update(args_given)
            result = Search(args_full, 'workers', g.request_tenant_id, g.username).search()
            return ok(result=result, msg="Workers search completed successfully.")
        else:
            dbid = g.db_id
            try:
                workers = Worker.get_workers(dbid)
            except WorkerException as e:
                logger.debug(f"did not find workers for actor: {actor_id}.")
                raise ResourceError(e.msg, 404)
            result = []
            for worker in workers:
                try:
                    w = Worker(**worker)
                    result.append(w.display())
                except Exception as e:
                    logger.error(f"Unable to instantiate worker in workers endpoint from description: {worker}. ")
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
            raise DAOError(f"Invalid POST: {msg}")
        return args

    def post(self, actor_id):
        """Ensure a certain number of workers are running for an actor"""
        logger.debug(f"top of POST /actors/{actor_id}/workers.")
        dbid = g.db_id
        try:
            actor = Actor.from_db(actors_store[site()][dbid])
        except KeyError:
            logger.debug(f"did not find actor: {actor_id}.")
            raise ResourceError(f"No actor found with id: {actor_id}.", 404)
        args = self.validate_post()
        logger.debug(f"workers POST params validated. actor: {actor_id}.")
        num = args.get('num')
        if not num or num == 0:
            logger.debug(f"did not get a num: {actor_id}.")
            num = 1
        logger.debug(f"ensuring at least {num} workers. actor: {dbid}.")
        try:
            workers = Worker.get_workers(dbid)
        except WorkerException as e:
            logger.debug(f"did not find workers for actor: {actor_id}.")
            raise ResourceError(e.msg, 404)
        current_number_workers = len(workers)
        if current_number_workers < num:
            logger.debug(f"There were only {current_number_workers} workers for actor: {actor_id} so we're adding more.")
            num_to_add = int(num) - len(workers)
            logger.info(f"adding {num_to_add} more workers for actor {actor_id}")
            for idx in range(num_to_add):
                # send num_to_add messages to add 1 worker so that messages are spread across multiple
                # spawners.
                worker_id = Worker.request_worker(tenant=g.request_tenant_id,
                                                  actor_id=dbid)
                logger.info(f"New worker id: {worker_id[0]}")
                ch = CommandChannel(name=actor.queue)
                ch.put_cmd(actor_id=actor.db_id,
                           worker_id=worker_id,
                           image=actor.image,
                           revision=actor.revision,
                           tenant=g.request_tenant_id,
                           site_id=site(),
                           stop_existing=False)
            ch.close()
            logger.info(f"Message put on command channel for new worker ids: {worker_id}")
            return ok(result=None, msg=f"Scheduled {num_to_add} new worker(s) to start. Previously, there were {current_number_workers} workers.")
        else:
            return ok(result=None, msg=f"Actor {actor_id} already had {num} worker(s).")


class WorkerResource(Resource):
    def get(self, actor_id, worker_id):
        logger.debug(f"top of GET /actors/{actor_id}/workers/{worker_id}.")
        id = g.db_id
        try:
            worker = Worker.get_worker(id, worker_id)
        except WorkerException as e:
            logger.debug(f"Did not find worker: {worker_id}. actor: {actor_id}.")
            raise ResourceError(e.msg, 404)
        # worker is an honest python dictionary with a single key, the id of the worker. need to
        # convert it to a Worker object
        worker.update({'id': worker_id})
        w = Worker(**worker)
        return ok(result=w.display(), msg="Worker retrieved successfully.")

    def delete(self, actor_id, worker_id):
        logger.debug(f"top of DELETE /actors/{actor_id}/workers/{worker_id}.")
        id = g.db_id
        try:
            worker = Worker.get_worker(id, worker_id)
        except WorkerException as e:
            logger.debug(f"Did not find worker: {worker_id}. actor: {actor_id}.")
            raise ResourceError(e.msg, 404)
        # if the worker is in requested status, we shouldn't try to shut it down because it doesn't exist yet;
        # we just need to remove the worker record from the workers_store[site()].
        # TODO - if worker.status == 'REQUESTED' ....
        logger.info(f"calling shutdown_worker(). worker: {worker_id}. actor: {actor_id}.")
        shutdown_worker(id, worker['id'], delete_actor_ch=False)
        logger.info(f"shutdown_worker() called for worker: {worker_id}. actor: {actor_id}.")
        return ok(result=None, msg="Worker scheduled to be stopped.")


class PermissionsResource(Resource):
    """This class handles permissions endpoints for all objects that need permissions.
    The `identifier` is the human-readable id (e.g., actor_id, alias).
    The code uses the request rule to determine which object is being referenced.
    """
    def get(self, identifier):
        is_config = False
        if 'actors/aliases/' in request.url_rule.rule:
            logger.debug(f"top of GET /actors/aliases/{identifier}/permissions.")
            id = Alias.generate_alias_id(g.request_tenant_id, identifier)
        elif 'actors/configs' in request.url_rule.rule:
            logger.debug(f"top of GET /actors/configs/{identifier}/permissions.")
            id = ActorConfig.get_config_db_key(g.request_tenant_id, identifier)
            is_config = True
        else:
            logger.debug(f"top of GET /actors/{identifier}/permissions.")
            id = g.db_id
        # config permissions are stored in a separate store --
        if is_config:
            try:
                permissions = get_config_permissions(id)
            except PermissionsException as e:
                logger.debug(f"Did not find config permissions for config: {identifier}.")
                raise ResourceError(e.msg, 404)
        else:
            try:
                permissions = get_permissions(id)
            except PermissionsException as e:
                logger.debug(f"Did not find actor permissions for config: {identifier}.")
                raise ResourceError(e.msg, 404)
        return ok(result=permissions, msg="Permissions retrieved successfully.")

    def validate_post(self):
        parser = RequestParser()
        parser.add_argument('user', type=str, required=True, help="User owning the permission.")
        parser.add_argument('level', type=str, required=True,
                            help=f"Level of the permission: {PERMISSION_LEVELS}")
        try:
            args = parser.parse_args()
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            raise DAOError(f"Invalid permissions description: {msg}")

        if not args['level'] in PERMISSION_LEVELS:
            raise ResourceError(f"Invalid permission level: {args['level']}. The valid values are {PERMISSION_LEVELS}")
        return args

    def post(self, identifier):
        """Add new permissions for an object `identifier`."""
        is_confg = False
        if 'actors/aliases/' in request.url_rule.rule:
            logger.debug(f"top of POST /actors/aliases/{identifier}/permissions.")
            dbid = Alias.generate_alias_id(g.request_tenant_id, identifier)
        elif 'actors/configs/' in request.url_rule.rule:
            logger.debug(f"top of POST /actors/configs/{identifier}/permissions.")
            is_confg = True
            dbid = ActorConfig.get_config_db_key(g.request_tenant_id, identifier)
        else:
            logger.debug(f"top of POST /actors/{identifier}/permissions.")
            dbid = g.db_id
        args = self.validate_post()
        logger.debug(f"POST permissions body validated for identifier: {dbid}.")
        if is_confg:
            set_config_permission(args['user'], config_id=dbid, level=PermissionLevel(args['level']))
            permissions = get_config_permissions(config_id=dbid)
        else:
            set_permission(args['user'], dbid, PermissionLevel(args['level']))
            permissions = get_permissions(actor_id=dbid)
        logger.info(f"Permission added for user: {args['user']}; identifier: {dbid}; level: {args['level']}")

        return ok(result=permissions, msg="Permission added successfully.")

class AdaptersResource(Resource):

    def get(self):
        logger.debug("top of GET /adapters")
        if len(request.args) > 1 or (len(request.args) == 1 and not 'x-nonce' in request.args):
            args_given = request.args
            args_full = {}
            args_full.update(args_given)
            result = Search(args_full, 'adapters', g.request_tenant_id, g.username).search()
            return ok(result=result, msg="adapters search completed successfully.")
        else:
            adapters = []
            for adapter_info in adapters_store[site()].items():
                if adapter_info['tenant'] == g.request_tenant_id:
                    adapter = Adapter.from_db(adapter_info)
                    if check_permissions(g.username, adapter.db_id, READ):
                        adapters.append(adapter.display())
            logger.info("adapters retrieved.")
            return ok(result=adapters, msg="adapters retrieved successfully.")

    def validate_post(self):
        logger.debug("top of validate post in /adapters")
        parser = Adapter.request_parser()
        try:
            args = parser.parse_args()
            logger.debug(f"initial adapter args from parser: {args}")
            if args['queue']:
                valid_queues = conf.spawner_host_queues
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
                msg = f'{msg}: {e}'
            logger.debug(f"Validate post - invalid adapter description: {msg}")
            raise DAOError(f"Invalid adapter description: {msg}")
        return args

    def post(self):
        logger.info("top of POST to register a new adapter.")
        args = self.validate_post()

        logger.debug("validate_post() successful")
        args['tenant'] = g.request_tenant_id
        args['api_server'] = g.api_server
        args['revision'] = 1
        args['owner'] = g.username
        # token attribute - if the user specifies whether the adapter requires a token, we always use that.
        # otherwise, we determine the default setting based on configs.
        if 'token' in args and args.get('token') is not None:
            token = args.get('token')
            logger.debug(f"user specified token: {token}")
        else:
            token = get_token_default()
        args['token'] = token
         # Checking for 'log_ex' input arg.
        if conf.web_case == 'camel':
            if 'logEx' in args and args.get('logEx') is not None:
                log_ex = int(args.get('logEx'))
                logger.debug(f"Found log_ex in args; using: {log_ex}")
                args['logEx'] = log_ex
        else:
            if 'log_ex' in args and args.get('log_ex') is not None:
                log_ex = int(args.get('log_ex'))
                logger.debug(f"Found log_ex in args; using: {log_ex}")
                args['log_ex'] = log_ex


        args['mounts'] = get_all_mounts(args)
        logger.debug(f"create args: {args}")
        adapter = Adapter(**args)
        # Change function
        adapters_store[site()].add_if_empty([adapter.db_id], adapter)
        abaco_metrics_store[site()].full_update(
            {'_id': f'{datetime.date.today()}-stats'},
            {'$inc': {'adapter_total': 1},
             '$addToSet': {'adapter_dbids': adapter.db_id}},
             upsert=True)

        logger.debug(f"new adapter saved in db. id: {adapter.db_id}. image: {adapter.image}. tenant: {adapter.tenant}")
        adapter.ensure_one_server()
        logger.debug("ensure_one_server() called")
        set_adapter_permission(g.username, adapter.db_id, UPDATE)
        logger.debug(f"UPDATE permission added to user: {g.username}")
        return ok(result=adapter.display(), msg="adapter created successfully.", request=request)

class AdapterResource(Resource):
    def get(self, adapter_id):
        logger.debug(f"top of GET /adapters/{adapter_id}")
        try:
            adapter = Adapter.from_db(adapters_store[site()][g.db_id])
        except KeyError:
            logger.debug(f"did not find adapter with id: {adapter_id}")
            raise ResourceError(
                f"No adapter found with identifier: {adapter_id}.", 404)
        logger.debug(f"found adapter {adapter_id}")
        return ok(result=adapter.display(), msg="adapter retrieved successfully.")

    def delete(self, adapter_id):
        logger.debug(f"top of DELETE /adapters/{adapter_id}")
        id = g.db_id
        try:
            adapter = Adapter.from_db(adapters_store[site()][id])
        except KeyError:
            adapter = None

        if adapter:
            # first set adapter status to SHUTTING_DOWN
            adapter.set_status(id, 'SHUTDOWN_REQUESTED')
            try:
                servers_by_adapter = adapter_servers_store[site()].items({'adapter_id': id})
                for server in servers_by_adapter:
                    AdapterServer.update_status(id, server['id'],'SHUTDOWN_REQUESTED')
            except KeyError as e:
                logger.info(f"got KeyError {e} trying to retrieve adapter or servers with id {id}")
        msg = 'adapter deleted successfully.'
        return ok(result=None, msg=msg)
    def put(self, adapter_id):
        logger.debug(f"top of PUT /adapters/{adapter_id}")
        dbid = g.db_id
        try:
            adapter = Adapter.from_db(adapters_store[site()][dbid])
        except KeyError:
            logger.debug(f"did not find adapter {dbid} in store.")
            raise ResourceError(
                f"No adapter found with id: {adapter_id}.", 404)
        previous_image = adapter.image
        previous_status = adapter.status
        previous_owner = adapter.owner
        previous_revision = adapter.revision
        args = self.validate_put(adapter)
        logger.debug(f"PUT args validated successfully.")
        args['tenant'] = g.request_tenant_id
         # Checking for 'log_ex' input arg.
        if conf.web_case == 'camel':
            if 'logEx' in args and args.get('logEx') is not None:
                log_ex = int(args.get('logEx'))
                logger.debug(f"Found log_ex in args; using: {log_ex}")
                args['logEx'] = log_ex
        else:
            if 'log_ex' in args and args.get('log_ex') is not None:
                log_ex = int(args.get('log_ex'))
                logger.debug(f"Found log_ex in args; using: {log_ex}")
                args['log_ex'] = log_ex
        if args['queue']:
            valid_queues = conf.spawner_host_queues
            if args['queue'] not in valid_queues:
                raise BadRequest('Invalid queue name.')
        if args['link']:
            validate_link(args)
        # user can force an update by setting the force param:
        update_image = args.get('force')
        if not update_image and args['image'] == previous_image:
            logger.debug("new image is the same and force was false. not updating adapter.")
            logger.debug(f"Setting status to the adapter's previous status which is: {previous_status}")
            args['status'] = previous_status
            args['revision'] = previous_revision
        else:
            update_image = True
            args['status'] = SUBMITTED
            args['revision'] = previous_revision + 1
            logger.debug("new image is different. updating adapter.")
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
                 

        args['mounts'] = get_all_mounts(args)
        args['last_update_time'] = get_current_utc_time()
        logger.debug(f"update args: {args}")

        logger.info(f"updated adapter {adapter_id} stored in db.")
        if update_image:
            #prepare existing servers to shut down
            servers_by_adapter = AdapterServer.get_servers(dbid)
            for server in servers_by_adapter:
                AdapterServer.update_status(dbid, server['id'],'SHUTDOWN_REQUESTED')
            # wait up to 20 seconds for all servers to shutdown
            logger.debug(f"start deleting the old servers for: {adapter_id}.")
            
        adapter = Adapter(**args)
        adapters_store[site()][adapter.db_id] = adapter.to_db()
        adapter.ensure_one_server()
        # put could have been issued by a user with
        if not previous_owner == g.username:
            set_adapter_permission(g.username, adapter.db_id, UPDATE)
        return ok(result=adapter.display(),
                  msg="adapter updated successfully.")

    def validate_put(self, adapter):
        # inherit derived attributes from the original adapter, including id and db_id:
        parser = Adapter.request_parser()
        # remove since name is only required for POST, not PUT
        parser.remove_argument('name')
        parser.add_argument('force', type=bool, required=False, help="Whether to force an update of the adapter image", default=False)

        # if camel case, need to remove fields snake case versions of fields that can be updated
        if conf.web_case == 'camel':
            adapter.pop('default_environment')
            adapter.pop('mem_limit')
            adapter.pop('max_cpus')
            adapter.pop('log_ex')

        # this update overrides all required and optional attributes
        try:
            new_fields = parser.parse_args()
            logger.debug(f"new fields from adapter PUT: {new_fields}")
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            else:
                msg = f'{msg}: {e}'
            raise DAOError(f"Invalid adapter description: {msg}")
        if not adapter.stateless and new_fields.get('stateless'):
            raise DAOError("Invalid adapter description: an adapter that was not stateless cannot be updated to be stateless.")
        if not adapter.stateless and (new_fields.get('max_workers') or new_fields.get('maxWorkers')):
            raise DAOError("Invalid adapter description: stateful adapters can only have 1 worker.")
        if new_fields['hints']:
            for hint in new_fields['hints']:
                for bad_char in ['"', "'", '{', '}', '[', ']']:
                    if bad_char in hint:
                        raise BadRequest(f"Hints must be simple stings or numbers, no lists or dicts. Error character: {bad_char}")    
        adapter.update(new_fields)
        return adapter

class AdapterMessagesResource(Resource):
    def get(self, adapter_id):
        logger.debug(f"top of GET /adapters/{adapter_id}/data")
        # check that adapter exists
        id = g.db_id
        logger.debug(f"adapter: {id}.")
        try:
            adapter = Adapter.from_db(adapters_store[site()][id])
        except KeyError:
            logger.debug(f"did not find adapter: {adapter_id}.")
            raise ResourceError(f"No adapter found with id: {adapter_id}.", 404)
        server = adapter['servers']
        networkaddy = adapter_servers_store[site()][f'{id}_{server[0]}','address']
        try:
            logger.debug(f"address: {networkaddy}.")
            result = requests.get(networkaddy)
            result.raise_for_status()
        except Exception as e:
            logger.debug(f'The get request gave an error {e}')
            raise AdapterMessageError('Unable to communicate with the adapter server')
        response = result.content.decode('utf8')
        logger.debug(f"messages found for adapter: {id}. {response}")
        return ok(response)
    
    def validate_post(self):
        logger.debug("validating message payload.")
        parser = RequestParser()
        parser.add_argument('message', type=str, required=False, help="The message to send to the adapter.")
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
                if int(length) > conf.web_max_content_length:
                    raise ResourceError(f"Message exceeds max content length of: {conf.web_max_content_length}")
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
                    logger.debug(f"message POST body could not be serialized. args: {args}")
                    raise DAOError('message POST body could not be serialized. Pass JSON data or use the message attribute.')
                args['_abaco_Content_Type'] = 'str'
        else:
            # the special message object is a string
            logger.debug("POST body has a message field. Setting _abaco_Content_type to 'str'.")
            args['_abaco_Content_Type'] = 'str'
        return args

    def post(self, adapter_id):
        def get_hypermedia(adapter, ser):
            return {'_links': {'self': f'{adapter.api_server}/v3/adapters/{adapter.id}/server/{ser}',
                               'owner': f'{adapter.api_server}/v3/oauth2/profiles/{adapter.owner}',
                               'messages': f'{adapter.api_server}/v3/adapters/{adapter.id}/messages'}, }

        logger.debug(f"top of POST /adapters/{adapter_id}/data")
        dbid = g.db_id
        try:
            adapter = Adapter.from_db(adapters_store[site()][dbid])
        except KeyError:
            logger.debug(f"did not find adapter: {adapter_id}.")
            raise ResourceError(f"No adapter found with id: {adapter_id}.", 404)
        args = self.validate_post()
        d = {}
        # build a dictionary of k:v pairs from the query parameters, and pass a single
        # additional object 'message' from within the post payload. Note that 'message'
        # need not be JSON data.
        logger.debug(f"POST body validated. adapter: {adapter_id}.")
        for k, v in request.args.items():
            if k == 'message':
                continue
            d[k] = v
        logger.debug(f"extra fields added to message from query parameters: {d}.")
        if hasattr(g, 'username'):
            d['_abaco_username'] = g.username
            logger.debug(f"_abaco_username: {g.username} added to message.")
        if hasattr(g, 'jwt_header_name'):
            d['_abaco_jwt_header_name'] = g.jwt_header_name
            logger.debug(f"abaco_jwt_header_name: {g.jwt_header_name} added to message.")

        d['Content-Type'] = args.get('_abaco_Content_Type', '')
        d['_abaco_adapter_revision'] = adapter.revision
        logger.debug(f"Final message dictionary: {d}")
        server = adapter['server']
        networkaddy = adapter_servers_store[site()][f'{dbid}_{server[0]}','address']
        result = requests.post(networkaddy, headers=d, data=args['message'])
        
        return ok(result)

class AdapterPermissionsResource(Resource):
    """This class handles permissions endpoints for all objects that need permissions.
    The `identifier` is the human-readable id (e.g., actor_id, alias).
    The code uses the request rule to determine which object is being referenced.
    """
    def get(self, identifier):
        is_config = False
       
        logger.debug(f"top of GET /adapter/{identifier}/permissions.")
        id = g.db_id
        # config permissions are stored in a separate store --
        try:
            permissions = get_adapter_permissions(id)
        except PermissionsException as e:
            logger.debug(f"Did not find adapter permissions for config: {identifier}.")
            raise ResourceError(e.msg, 404)
        return ok(result=permissions, msg="Permissions retrieved successfully.")

    def validate_post(self):
        parser = RequestParser()
        parser.add_argument('user', type=str, required=True, help="User owning the permission.")
        parser.add_argument('level', type=str, required=True,
                            help=f"Level of the permission: {PERMISSION_LEVELS}")
        try:
            args = parser.parse_args()
        except BadRequest as e:
            msg = 'Unable to process the JSON description.'
            if hasattr(e, 'data'):
                msg = e.data.get('message')
            raise DAOError(f"Invalid permissions description: {msg}")

        if not args['level'] in PERMISSION_LEVELS:
            raise ResourceError(f"Invalid permission level: {args['level']}. The valid values are {PERMISSION_LEVELS}")
        return args

    def post(self, identifier):
        """Add new permissions for an object `identifier`."""
        logger.debug(f"top of POST /adapter/{identifier}/permissions.")
        dbid = g.db_id
        args = self.validate_post()
        logger.debug(f"POST permissions body validated for identifier: {dbid}.")
       
        set_adapter_permission(args['user'], dbid, PermissionLevel(args['level']))
        permissions = get_adapter_permissions(actor_id=dbid)
        logger.info(f"Permission added for user: {args['user']}; identifier: {dbid}; level: {args['level']}")

        return ok(result=permissions, msg="Permission added successfully.")

class ActorPermissionsResource(PermissionsResource):
    pass


class AliasPermissionsResource(PermissionsResource):
    pass


class ActorConfigsPermissionsResource(PermissionsResource):
    pass
