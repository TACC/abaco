from copy import deepcopy
import datetime
import json
import timeit
import uuid
import re
import time
import datetime

from flask import g
from flask_restful import inputs
from hashids import Hashids
from parse import parse
from dateutil.relativedelta import relativedelta
from werkzeug.exceptions import BadRequest

from common.utils import RequestParser
from channels import CommandChannel, EventsChannel
from codes import REQUESTED, READY, ERROR, SHUTDOWN_REQUESTED, SHUTTING_DOWN, SUBMITTED, EXECUTE, PermissionLevel, \
    SPAWNER_SETUP, PULLING_IMAGE, CREATING_CONTAINER, UPDATING_STORE, BUSY
from common.config import conf
import errors
from errors import DAOError, ResourceError, PermissionsException, WorkerException, ExecutionException

from stores import actors_store, alias_store, executions_store, logs_store, nonce_store, \
    permissions_store, workers_store, abaco_metrics_store, configs_permissions_store, configs_store

from common.logs import get_logger
logger = get_logger(__name__)


HASH_SALT = 'eJa5wZlEX4eWU'

# default max length for an actor execution log - 1MB
DEFAULT_MAX_LOG_LENGTH = 1000000

def site():
    try:
        return g.site_id
    except:
        return conf.get('site_id')

def is_hashid(identifier):
    """ 
    Determine if `identifier` is an Abaco Hashid (e.g., actor id, worker id, nonce id, etc.
    
    :param identifier (str) - The identifier to check 
    :return: (bool) - True if the identifier is an Abaco Hashid.
    """
    hashids = Hashids(salt=HASH_SALT)
    dec = hashids.decode(identifier)
    # the decode() method returns a non-empty tuple (containing the original uuid seed)
    # iff the identifier was produced from an encode() with the HASH_SALT; otherwise, it returns an
    # empty tuple. Therefore, the length of the produced tuple can be used to check whether the
    # identify was created with the Abaco HASH_SALT.
    if len(dec) > 0:
        return True
    else:
        return False

def under_to_camel(value):
    def camel_case():
        yield type(value).lower
        while True:
            yield type(value).capitalize
    c = camel_case()
    return "".join(c.__next__()(x) if x else '_' for x in value.split("_"))


def dict_to_camel(d):
    """Convert all keys in a dictionary to camel case."""
    d2 = {}
    for k,v in d.items():
        d2[under_to_camel(k)] = v
    return d2

def camel_to_under(value):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', value).lower()

def dict_to_under(d):
    """Convert all keys in a dictionary to camel case."""
    d2 = {}
    for k,v in d.items():
        k = k.split(".")
        k[0] = camel_to_under(k[0])
        k = ".".join(k)
        d2[k] = v
    return d2

def get_current_utc_time():
    """Return string representation of current time in UTC."""
    return datetime.datetime.utcnow()

def display_time(t):
    """ Convert a string representation of a UTC timestamp to a display string."""
    if not t:
        return "None"
    try:
        dt = t.isoformat().replace('000', 'Z')
    except AttributeError as e:
        logger.error(f"Did not receive datetime object. Received object of type {type(t)}. Object: {t}. Exception: {e}")
        raise errors.DAOError("Error retrieving time data.")
    except Exception as e:
        logger.error(f"Error in formatting display time. Exception: {e}")

        raise errors.DAOError("Error retrieving time data.")
    return dt


class Search():
    def __init__(self, args, search_type, tenant, user):
        self.args = args
        self.search_type = search_type.lower()
        self.tenant = tenant
        self.user = user

    def search(self):
        """
        Does a search on one of four selected Mongo databases. Workers,
        executions, actors, and logs. Uses the Mongo aggregation function
        to first perform a full-text search. Following that permissions are
        checked, variable matching is attempted, logical operators are attempted,
        and truncation is performed.
        """
        logger.info(f'Received a request to search. Search type: {self.search_type}')

        queried_store, security = self.get_db_specific_sections()
        search, query, skip, limit = self.arg_parser()

        # Pipeline initially made use of 'project', I've opted to instead
        # do all post processing in 'post_processing()' for simplicity.
        pipeline = search + query + security
        start = time.time()
        full_search_res = list(queried_store.aggregate(pipeline))
        logger.info(f'Got search response in {time.time() - start} seconds.'\
                    f'Pipeline: {pipeline} First two results: {full_search_res[0:1]}')
        final_result = self.post_processing(full_search_res, skip, limit)
        return final_result

    def get_db_specific_sections(self):
        """
        Takes in the search_type and gives the correct store to query.
        Also figures out the permission pipeline sections for the specified
        search_type. The actors_store[site()] is the only store that require a
        different permission type. Permissions are done by matching tenant,
        but also joining the permissions store based on actor dbid and matching
        user to allow for shared permissions.
        """
        store_dict = {'executions': executions_store[site()],
                      'workers': workers_store[site()],
                      'actors': actors_store[site()],
                      'logs': logs_store[site()]}
        try:
            queried_store = store_dict[self.search_type]
        except KeyError:
            raise KeyError(f"Inputted search_type is invalid, must"\
                            " be one of {list(store_dict.keys())}.")
        
        localField = 'actor_id'
        if self.search_type =='actors':
            localField = '_id'
        security = [{'$match': {'tenant': self.tenant}},
                    {'$lookup':
                        {'from' : 'permissions_store',
                        'localField' : localField,
                        'foreignField' : '_id',
                        'as' : 'permissions'}},
                    {'$unwind': '$permissions'},
                    {'$match': {'permissions.' + self.user: {'$exists': True}}}]

        return queried_store, security

    def arg_parser(self):
        """
        Arg parser parses the query parameters of the url. Allows for specified
        Mongo logical operators, also for fuzzy or exact full-text search.
        Skip and limit parameters are defined here. If the given parameter
        matches none of the above, the function attempts to match a key to
        a value. When trying to reach a nested value, the exact key must be
        specified. For example, '_links.owner'.
        """
        query = []
        # Always searches by tenant to increase speed of search
        search = f'"{self.tenant}"'
        # Initial paging settings
        skip_amo = 0
        limit_amo = 100

        # Converts everything to snake case as that's what Mongo holds.
        # This means that snake case input will always work, but not camel case.
        case = conf.web_case
        if case == 'camel':
            self.args = dict_to_under(self.args)

        # Args are passed in as a dictionary. A list of values if there are
        # multiple of the same key, one value if not.
        # Checks each key given by the requests query parameters.
        for key, val in self.args.items():
            # Ignores x-nonce and that for Abaco authentication, not search.
            if key == "x-nonce":
                pass
            # Adds vals matched to 'search' to the 'search' string which will
            # later be added to the pipeline
            elif key == "search":
                if isinstance(val, list):
                    joined_val = ' '.join(val)
                    search = search + f' {joined_val}'
                else:
                    search = search + f' {val}'
            # Same as 'search', but with double quotation around the value
            elif key == "exactsearch":
                if isinstance(val, list):
                    joined_val = '" "'.join(val)
                    search = search + f' "{joined_val}"'
                else:
                    search = search + f' "{val}"'
            # Checks for only one limit and skip, sets. Raises errors.
            elif key == "skip" or key == "limit":
                try:
                    val = int(val)
                except ValueError:
                    raise ValueError(f'Inputted "{key}" paramater must be an int. Received: {val}')
                if val < 0:
                    raise ValueError(f'Inputted "{key}" must be positive. Received: {val}')
                if key == "skip":
                    skip_amo = val
                if key == "limit":
                    limit_amo = val
            else:
                # Keys in the mongo db that should neccessitate datetime objects.
                time_keys = ['start_time', 'message_received_time', 'last_execution_time',
                            'last_update_time', 'StartedAt', 'FinishedAt', 'create_time',
                            'last_health_check_time']
                # Trying lots of types to parse inputted strings.
                # Boolean check
                if val.lower() == 'false':
                    val = False
                elif val.lower() == 'true':
                    val = True
                # None check
                elif val.lower() == 'none':
                    val = None
                # Datetime check - Checks for keys that correspond to datetime objects,
                # if there is a match, the value is converted to a datetime object.
                # '.between' gets special parsing due to the comma delimination.
                elif any(time_key in key for time_key in time_keys):
                    if '.between' in key:
                        if not ',' in val:
                            raise ValueError('Between must have two variables seperated by a comma. Ex. io.between=20,40')
                        time1, time2 = val.split(',')
                        time1 = self.broad_ISO_to_datetime(time1)
                        time2 = self.broad_ISO_to_datetime(time2)
                        val = [time1, time2]
                    else:
                        val = self.broad_ISO_to_datetime(val)
                else:
                    # Number check (floats are fine for all comparisons)
                    try: 
                        val = float(val)
                    except (ValueError, TypeError):
                        # List check/Other JSON check
                        try:
                            val = json.loads(val.replace("'", '"'))
                        except json.JSONDecodeError:
                            # Checks if list had the wrong quotes
                            try:
                                val = json.loads(val.replace('"', "'"))
                            except json.JSONDecodeError:
                                pass

                used_oper = False
                # Alias of mongo operators to expected operators to match TACC
                # schema of search operators.
                oper_aliases = {'.neq': '$ne', '.eq': '$eq', '.lte': '$lte', '.lt': '$lt',
                                '.gte': '$gte', '.gt': '$gt', '.nin': '$nin', '.in': '$in'}
                # Checks for search operators, if one is found
                for oper_alias, mongo_oper in oper_aliases.items():
                    if oper_alias in key:
                        key = key.split(oper_alias)[0]
                        query += [{'$match': {key: {mongo_oper: val}}}]
                        used_oper = True
                        break
                if not used_oper:
                    # Now we check for operators that require extra parsing
                    # '.between' needs to parse out the comma and check for datetime or float
                    if '.between' in key:
                        if isinstance(val, list):
                            if isinstance(val[0], datetime.datetime) and isinstance(val[1], datetime.datetime):
                                key = key.split('.between')[0]
                                query += [{'$match': {key: {'$gte': val[0], '$lte': val[1]}}}]
                            else:
                                raise ValueError("The values given to .between should be either float or ISO 8601 datetime.")
                        else:
                            if not ',' in val:
                                raise ValueError('Between must have two variables seperated by a comma. Ex. io.between=20,40')
                            key = key.split('.between')[0]
                            val1, val2 = val.split(',')
                            try:
                                val1 = float(val1)
                                val2 = float(val2)
                            except ValueError:
                                raise ValueError("The values given to .between should be either float or ISO 8601 datetime.")
                            query += [{'$match': {key: {'$gte': val1, '$lte': val2}}}]
                    # '.nlike' requires some regex magic to attempt parity with SQL "nlike"
                    elif '.nlike' in key:
                        key = key.split('.nlike')[0]
                        query += [{'$match': {key: {'$not': {'$regex': f"{val}"}}}}]
                    # '.like' requires some regex magic to attempt parity with SQL "like"
                    elif '.like' in key:
                        key = key.split('.like')[0]
                        query += [{'$match': {key: {'$regex': f"{val}"}}}]
                    # If no operators match, a simple equality search is done.
                    else:
                        query += [{'$match': {key: val}}]
        
        # As mentioned, search gets added to the stanza and sort is done by
        # textScore. Always is done as search always include tenant for speed.
        search = [{'$match': {'$text': {'$search': search}}},
                  {'$sort': {'score': {'$meta': 'textScore'}}}]
        return search, query, skip_amo, limit_amo

    def post_processing(self, search_list, skip, limit):
        """
        This function performs post processing on the results. Post processing
        entails fixing times to display_times, changing case, eliminated
        variables, adding '_links', and fixing specific fields. Processing type
        is dependent on the search_type performed.
        """
        logger.info(f'Starting post_processing for search with search_type: {self.search_type}')

        total_count = len(search_list)
        search_list = search_list[skip: skip + limit]

        # Does post processing on execution db searches.
        if self.search_type == 'executions':
            for i, result in enumerate(search_list):
                if result.get('tenant') and result.get('actor_id'):
                    aid = Actor.get_display_id(result['tenant'], result['actor_id'])
                    search_list[i]['actor_id'] = aid
                    try:
                        api_server = result['api_server']
                        id = result['id']
                        executor = result['executor']
                        search_list[i]['_links'] = {
                            'self': f'{api_server}/v3/actors/{aid}/executions/{id}',
                            'owner': f'{api_server}/v3/oauth2/profiles/{executor}',
                            'logs': f'{api_server}/v3/actors/{aid}/logs'}
                        search_list[i].pop('api_server')
                    except KeyError:
                        pass
                if result.get('start_time'):
                    search_list[i]['start_time'] = display_time(result['start_time'])
                if result.get('finish_time'):
                    search_list[i]['finish_time'] = display_time(result['finish_time'])
                if result.get('message_received_time'):
                    search_list[i]['message_received_time'] = display_time(result['message_received_time'])
                if result.get('final_state'):
                    if result['final_state'].get('StartedAt'):
                        search_list[i]['final_state']['StartedAt'] = display_time(result['final_state']['StartedAt'])
                    if result['final_state'].get('FinishedAt'):
                        search_list[i]['final_state']['FinishedAt'] = display_time(result['final_state']['FinishedAt'])
                search_list[i].pop('_id', None)
                search_list[i].pop('permissions', None)
                search_list[i].pop('tenant', None)

        # Does post processing on workers db searches.
        elif self.search_type == 'workers':
            for i, result in enumerate(search_list):
                if result.get('tenant') and result.get('actor_id'):
                    aid = Actor.get_display_id(result['tenant'], result['actor_id'])
                    search_list[i]['actor_id'] = aid
                if result.get('last_execution_time'):
                    search_list[i]['last_execution_time'] = display_time(result['last_execution_time'])
                if result.get('last_health_check_time'):
                    search_list[i]['last_health_check_time'] = display_time(result['last_health_check_time'])
                if result.get('create_time'):
                    search_list[i]['create_time'] = display_time(result['create_time'])
                search_list[i].pop('_id', None)
                search_list[i].pop('permissions', None)
                search_list[i].pop('tenant', None)

        # Does post processing on actor db searches.
        elif self.search_type == 'actors':
            for i, result in enumerate(search_list):
                try:
                    api_server = result['api_server']
                    owner = result['owner']
                    id = result['id']
                    search_list[i]['_links'] = {
                        'self': f'{api_server}/v3/actors/{id}',
                        'owner': f'{api_server}/v3/oauth2/profiles/{owner}',
                        'executions': f'{api_server}/v3/actors/{id}/executions'}
                    search_list[i].pop('api_server')
                except KeyError:
                    pass
                if result.get('create_time'):
                    search_list[i]['create_time'] = display_time(result['create_time'])
                if result.get('last_update_time'):
                    search_list[i]['last_update_time'] = display_time(result['last_update_time'])
                search_list[i].pop('_id', None)
                search_list[i].pop('permissions', None)
                search_list[i].pop('api_server', None)
                search_list[i].pop('executions', None)
                search_list[i].pop('tenant', None)
                search_list[i].pop('db_id', None)

        # Does post processing on logs db searches.
        elif self.search_type == 'logs':
            for i, result in enumerate(search_list):
                try:
                    actor_id = result['actor_id']
                    exec_id = result['_id']
                    actor = Actor.from_db(actors_store[site()][actor_id])
                    search_list[i]['_links'] = {
                        'self': f'{actor.api_server}/v3/actors/{actor.id}/executions/{exec_id}/logs',
                        'owner': f'{actor.api_server}/v3/oauth2/profiles/{actor.owner}',
                        'execution': f'{actor.api_server}/v3/actors/{actor.id}/executions/{exec_id}'}
                except KeyError:
                    pass
                search_list[i].pop('_id', None)
                search_list[i].pop('permissions', None)
                search_list[i].pop('exp', None)
                search_list[i].pop('actor_id', None)
                search_list[i].pop('tenant', None)

        # Adjusts case of the response to match expected case.
        case = conf.web_case
        logger.info(f'Adjusting search response case')
        case_corrected_list = []
        for dictionary in search_list:
            if case == 'camel':
                case_corrected_list.append(dict_to_camel(dictionary))
            else:
                case_corrected_list = search_list

        # Creating a metadata object with paging information
        # and then adjusting for case as well.
        metadata = {"total_count": total_count,
                    "records_skipped": skip,
                    "record_limit": limit,
                    "count_returned": len(search_list)}
        if case == 'camel':
            case_corrected_metadata = dict_to_camel(metadata)
        else:
            case_corrected_metadata = metadata

        # Returning the final result which is an object with
        # _metadata and search for the search results.
        final_result = {"_metadata": case_corrected_metadata,
                        "search": case_corrected_list}
        return final_result

    def broad_ISO_to_datetime(self, dt_str):
        """
        Parses a broadly conforming ISO 8601 string.
        """
        dt_str = dt_str.replace('Z', '')
        datetime_strs = ['%Y-%m-%dT%H:%M:%S.%f%z', #0 
                        '%Y-%m-%dT%H:%M:%S.%f',
                        '%Y-%m-%dT%H:%M:%S%z',
                        '%Y-%m-%dT%H:%M:%S', 
                        '%Y-%m-%dT%H:%M%z', #4
                        '%Y-%m-%dT%H:%M',
                        '%Y-%m-%dT%H%z', #6
                        '%Y-%m-%dT%H',
                        '%Y-%m-%dT%z',
                        '%Y-%m-%dT', #9
                        '%Y-%m-%d%z',
                        '%Y-%m-%d',
                        '%Y-%m%z',
                        '%Y-%m',
                        '%Y%z',
                        '%Y']
        # Try each parse for each datetime str
        for i, datetime_try in enumerate(datetime_strs):
            # Fixing the datetime_str input's timezone. ISO has ":" in the timezone,
            # the parser does not want that, so conditionally we delete tz ":".
            if i <= 4:
                dt_tz_ready = dt_str[:19] + dt_str[19:].replace(':', '')
            elif i <= 6:
                dt_tz_ready = dt_str[:16] + dt_str[16:].replace(':', '')
            elif i <= 9:
                dt_tz_ready = dt_str[:13] + dt_str[13:].replace(':', '')
            else:
                dt_tz_ready = dt_str.replace(':', '')
            try:
                dt = datetime.datetime.strptime(dt_tz_ready, datetime_try)
                return dt
            except:
                pass
        # If no parse was successful, error!
        raise ValueError("Inputted datetime was in an incorrect format."\
                        " Please refer to docs and follow ISO 8601 formatting."\
                        " e.g. '2020-05-01T14:45:41.591Z'")


class Event(object):
    """
    Base event class for all Abaco events.
    """

    def __init__(self, dbid: str, event_type: str, data: dict):
        """
        Create a new event object
        """
        self.db_id = dbid
        self.tenant_id = dbid.split('_')[0]
        self.actor_id = Actor.get_display_id(self.tenant_id, dbid)
        self.event_type = event_type
        data['tenant_id'] = self.tenant_id
        data['event_type'] = event_type
        data['event_time_utc'] = get_current_utc_time().isoformat()[:23] + 'Z'
        data['event_time_display'] = data['event_time_utc']
        data['actor_id'] = self.actor_id
        data['actor_dbid'] = dbid
        self._get_events_attrs()
        data['_abaco_link'] = self.link
        data['_abaco_webhook'] = self.webhook
        data['event_type'] = event_type
        self.data = data

    def _get_events_attrs(self) -> str:
        """
        Check if the event is for an actor that has an event property defined (like, webhook, socket), 
        and return the actor db_id if so. Otherwise, returns an empty string.
        :return: the db_id associated with the link, if it exists, or '' otherwise.
        """
        try:
            actor = Actor.from_db(actors_store[site()][self.db_id])
        except KeyError:
            logger.debug(f"did not find actor with id: {self.actor_id}")
            raise errors.ResourceError(f"No actor found with identifier: {self.actor_id}.", 404)
        # the link and webhook attributes were added in 1.2.0; actors registered before 1.2.0 will not have
        # have these attributed defined so we use the .get() method below --
        # if the webhook exists, we always try it.
        self.webhook = actor.get('webhook') or ''

        # the actor link might not exist
        link = actor.get('link') or ''
        # if the actor link exists, it could be an actor id (not a dbid) or an alias.
        # the following code resolves the link data to an actor dbid
        if link:
            try:
                link_id = Actor.get_actor_id(self.tenant_id, link)
                self.link = Actor.get_dbid(self.tenant_id, link_id)
            except Exception as e:
                self.link = ''
                logger.error(f"Got exception: {e} calling get_actor_id to set the event link.")
        else:
            self.link = ''

    def publish(self):
        """
        External API for publishing events; handles all logic associated with event publishing including
        checking for actor links and pushing messages to the EventsExchange.
        :return: None 
        """
        logger.debug(f"top of publish for event: {self.data}")
        logger.info(self.data)
        if not self.link and not self.webhook:
            logger.debug("No link or webhook supplied for this event. Not publishing to the EventsChannel.")
            return None
        ch = EventsChannel()
        ch.put_event(self.data)


class ActorEvent(Event):
    """
    Data access object class for creating and working with actor event objects.
    """
    event_types = ('ACTOR_READY',
                  'ACTOR_ERROR',
                  )

    def __init__(self, dbid: str, event_type: str, data: dict):
        """
        Create a new even object
        """
        super().__init__(dbid, event_type, data)
        if not event_type.upper() in ActorEvent.event_types:
            logger.error("Invalid actor event type passed to the ActorEvent constructor. "
                         "event type: {}".format(event_type))
            raise errors.DAOError(f"Invalid actor event type {event_type}.")


class ActorExecutionEvent(Event):
    """
    Data access object class for creating and working with actor execution event objects.
    """
    event_types = ('EXECUTION_STARTED',
                  'EXECUTION_COMPLETE',
                  )

    def __init__(self, dbid: str, execution_id: str, event_type: str, data: dict):
        """
        Create a new even object
        """
        super().__init__(dbid, event_type, data)
        if not event_type.upper() in ActorExecutionEvent.event_types:
            logger.error("Invalid actor event type passed to the ActorExecutionEvent constructor. "
                         "event type: {}".format(event_type))
            raise errors.DAOError(f"Invalid actor execution event type {event_type}.")

        self.execution_id = execution_id
        self.data['execution_id'] = execution_id


class DbDict(dict):
    """Class for persisting a Python dictionary."""

    def __getattr__(self, key):
        # returning an AttributeError is important for making deepcopy work. cf.,
        # http://stackoverflow.com/questions/25977996/supporting-the-deep-copy-operation-on-a-custom-class
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e)

    def __setattr__(self, key, value):
        self[key] = value

    def to_db(self):
        # json serialization now happening in the store class. @todo - should remove all uses of .to_db
        return self
        # return json.dumps(self)


class AbacoDAO(DbDict):
    """Base Data Access Object class for Abaco models."""

    # the parameters for the DAO
    # tuples of the form (param name, required/optional/provided/derived, attr_name, type, help, default)
    # should be defined in the subclass.
    #
    #
    # required: these fields are required in the post/put methods of the web app.
    # optional: these fields are optional in the post/put methods of the web app and have a default value.
    # provided: these fields are required to construct the DAO but are provided by the abaco client code, not the user
    #           and not the DAO class code.
    # derived: these fields are derived by the DAO class code and do not need to be passed.
    PARAMS = []

    @classmethod
    def request_parser(cls):
        """Return a flask RequestParser object that can be used in post/put processing."""
        logger.debug("Top of request_parser().")
        parser = RequestParser()
        config_case = conf.web_case
        if config_case == 'camel':
            logger.debug("request_parser() using 'camel' case.")
        else:
            logger.debug("request_parser() using 'snake' case.")
        for name, source, attr, typ, help, default in cls.PARAMS:
            if source == 'derived':
                continue
            required = source == 'required'
            if config_case == 'camel':
                param_name = under_to_camel(name)
            else:
                param_name = name
            if param_name == 'hints':
                action='append'
            else:
                action='store'
            parser.add_argument(param_name, type=typ, required=required, help=help, default=default, action=action)
        return parser

    @classmethod
    def from_db(cls, db_json):
        """Construct a DAO from a db serialization."""
        return cls(**db_json)

    def __init__(self, **kwargs):
        """Construct a DAO from **kwargs. Client can also create from a dictionary, d, using AbacoDAO(**d)"""
        for name, source, attr, typ, help, default in self.PARAMS:
            pname = name
            # When using camel case, it is possible a given argument will come in in camel
            # case, for instance when creating an object directly from parameters passed in
            # a POST request.
            if name not in kwargs and conf.web_case == 'camel':
                # derived attributes always create the attribute with underscores:
                if source == 'derived':
                    pname = name
                else:
                    pname = under_to_camel(name)
            if source == 'required':
                try:
                    value = kwargs[pname]
                except KeyError:
                    logger.debug(f"required missing field: {pname}. ")
                    raise errors.DAOError(f"Required field {pname} missing.")
            elif source == 'optional':
                try:
                    value = kwargs[pname]
                except KeyError:
                    value = kwargs.get(pname, default)
            elif source == 'provided':
                try:
                    value = kwargs[pname]
                except KeyError:
                    logger.debug(f"provided field missing: {pname}.")
                    raise errors.DAOError(f"Required field {pname} missing.")
            else:
                # derived value - check to see if already computed
                if hasattr(self, pname):
                    value = getattr(self, pname)
                else:
                    value = self.get_derived_value(pname, kwargs)
            setattr(self, attr, value)

    def get_uuid(self):
        """Generate a random uuid."""
        hashids = Hashids(salt=HASH_SALT)
        return hashids.encode(uuid.uuid1().int>>64)

    def get_derived_value(self, name, d):
        """Compute a derived value for the attribute `name` from the dictionary d of attributes provided."""
        raise NotImplementedError

    def case(self):
        """Convert to camel case, if required."""
        case = conf.web_case
        if not case == 'camel':
            return self
        # if camel case, convert all attributes
        for name, _, _, _, _, _ in self.PARAMS:
            val = self.pop(name, None)
            if val is not None:
                self.__setattr__(under_to_camel(name), val)
        return self

    def display(self):
        """A default display method, for those subclasses that do not define their own."""
        return self.case()


class Actor(AbacoDAO):
    """Basic data access object for working with Actors."""

    PARAMS = [
        # param_name, required/optional/provided/derived, attr_name, type, help, default
        ('name', 'optional', 'name', str, 'User defined name for this actor.', None),
        ('image', 'required', 'image', str, 'Reference to image on docker hub for this actor.', None),

        ('stateless', 'optional', 'stateless', inputs.boolean, 'Whether the actor stores private state.', True),
        ('type', 'optional', 'type', str, 'Return type (none, bin, json) for this actor. Default is none.', 'none'),
        ('link', 'optional', 'link', str, "Actor identifier of actor to link this actor's events too. May be an actor id or an alias. Cycles not permitted.", ''),
        ('token', 'optional', 'token', inputs.boolean, 'Whether this actor requires an OAuth access token.', None),
        ('webhook', 'optional', 'webhook', str, "URL to publish this actor's events to.", ''),
        ('hints', 'optional', 'hints', str, 'Hints for personal tagging or Abaco special hints', []),
        ('description', 'optional', 'description', str,  'Description of this actor', ''),
        ('privileged', 'optional', 'privileged', inputs.boolean, 'Whether this actor runs in privileged mode.', False),
        ('max_workers', 'optional', 'max_workers', int, 'How many workers this actor is allowed at the same time.', None),
        ('mem_limit', 'optional', 'mem_limit', str, 'maximum amount of memory this actor can use.', None),
        ('max_cpus', 'optional', 'max_cpus', int, 'Maximum number of CPUs (nanoCPUs) this actor will have available to it.', None),
        ('use_container_uid', 'optional', 'use_container_uid', inputs.boolean, 'Whether this actor runs as the UID set in the container image.', False),
        ('default_environment', 'optional', 'default_environment', dict, 'A dictionary of default environmental variables and values.', {}),
        ('status', 'optional', 'status', str, 'Current status of the actor.', SUBMITTED),
        ('status_message', 'optional', 'status_message', str, 'Explanation of status.', ''),
        ('executions', 'optional', 'executions', dict, 'Executions for this actor.', {}),
        ('state', 'optional', 'state', dict, "Current state for this actor.", {}),
        ('create_time', 'derived', 'create_time', str, "Time (UTC) that this actor was created.", {}),
        ('last_update_time', 'derived', 'last_update_time', str, "Time (UTC) that this actor was last updated.", {}),

        ('tenant', 'provided', 'tenant', str, 'The tenant that this actor belongs to.', None),
        ('api_server', 'provided', 'api_server', str, 'The base URL for the tenant that this actor belongs to.', None),
        ('owner', 'provided', 'owner', str, 'The user who created this actor.', None),
        ('mounts', 'provided', 'mounts', list, 'List of volume mounts to mount into each actor container.', []),
        ('revision', 'provided', 'revision', int, 'A monotonically increasing integer, incremented each time the actor is updated with new version of its image.', 1),
        ('tasdir', 'optional', 'tasdir', str, 'Absolute path to the TAS defined home directory associated with the owner of the actor', None),
        ('uid', 'optional', 'uid', str, 'The uid to run the container as. Only used if user_container_uid is false.', None),
        ('gid', 'optional', 'gid', str, 'The gid to run the container as. Only used if user_container_uid is false.', None),

        ('queue', 'optional', 'queue', str, 'The command channel that this actor uses.', 'default'),
        ('db_id', 'derived', 'db_id', str, 'Primary key in the database for this actor.', None),
        ('id', 'derived', 'id', str, 'Human readable id for this actor.', None),
        ('log_ex', 'optional', 'log_ex', int, 'Amount of time, in seconds, after which logs will expire', None),
        ('cron_on', 'optional', 'cron_on', inputs.boolean, 'Whether cron is on or off', False),
        ('cron_schedule', 'optional', 'cron_schedule', str, 'yyyy-mm-dd hh + <number> <unit of time>', None),
        ('cron_next_ex', 'optional', 'cron_next_ex', str, 'The next cron execution yyyy-mm-dd hh', None)
        ]

    SYNC_HINT = 'sync'

    def get_derived_value(self, name, d):
        """Compute a derived value for the attribute `name` from the dictionary d of attributes provided."""
        # first, see if the attribute is already in the object:
        if hasattr(self, 'id'):
            return
        # next, see if it was passed:
        try:
            return d[name]
        except KeyError:
            pass
        # if not, generate an id
        try:
            actor_id, db_id = self.generate_id(d['name'], d['tenant'])
        except KeyError:
            logger.debug(f"name or tenant missing from actor dict: {d}.")
            raise errors.DAOError("Required field name or tenant missing")
        # id fields:
        self.id = actor_id
        self.db_id = db_id

        # time fields
        time_str = get_current_utc_time()
        self.create_time = time_str
        self.last_update_time = time_str
        if name == 'id':
            return actor_id
        elif name == 'create_time' or name == 'last_update_time':
            return time_str
        else:
            return db_id
    
    @classmethod
    def set_next_ex(cls, actor, actor_id):
        # Parse cron into [datetime, number, unit of time]
        logger.debug("In set_next_ex")
        cron = actor['cron_schedule']
        cron_parsed = parse("{} + {} {}", cron)
        unit_time = cron_parsed.fixed[2]
        time_length = int(cron_parsed.fixed[1])
        # Parse the first element of cron_parsed into another list of the form [year, month, day, hour]
        cron_next_ex = actor['cron_next_ex']
        time = parse("{}-{}-{} {}", cron_next_ex)
        # Create a datetime object
        start = datetime.datetime(int(time[0]), int(time[1]), int(time[2]), int(time[3]))
        logger.debug(f"cron_parsed[1] is {time_length}")
        # Logic for incrementing the next execution, whether unit of time is months, weeks, days, or hours
        if unit_time == "month" or unit_time == "months":
            end = start + relativedelta(months=+time_length)
        elif unit_time == "week" or unit_time == "weeks":
            end = start + datetime.timedelta(weeks=time_length)
        elif unit_time == "day" or unit_time == "days":
            end = start + datetime.timedelta(days=time_length)
        elif unit_time == "hour" or unit_time == "hours":
            end = start + datetime.timedelta(hours=time_length)
        else:
            # The unit of time is not supported, turn off cron or else it will continue to execute
            logger.debug("This unit of time is not supported, please choose either hours, days, weeks, or months")
            actors_store[actor_id, 'cron_on'] = False
        time = f"{end.year}-{end.month}-{end.day} {end.hour}"
        logger.debug(f"returning {time}")
        return time  

    @classmethod
    def set_cron(cls, cron):
        # Method checks for the 'now' alias and also checks that the cron sent in has not passed yet
        logger.debug("in set_cron()")
        now = get_current_utc_time()
        now_datetime = datetime.datetime(now.year, now.month, now.day, now.hour)
        logger.debug(f"now_datetime is {now_datetime}")
        # parse r: if r is not in the correct format, parse() will return None
        r = parse("{} + {} {}", cron)
        logger.debug(f"r is {r}")
        if r is None:
            raise errors.DAOError(f"The cron is not in the correct format")
        # Check that the cron schedule hasn't already passed
        # Check for the 'now' alias and change the cron to now if 'now' is sent in
        cron_time = r.fixed[0]
        logger.debug(f"Cron time is {cron_time}")
        if cron_time.lower() == "now":
            cron_time_parsed = [now.year, now.month, now.day, now.hour]
            # Change r to the current UTC datetime
            logger.debug(f"cron_time_parsed when now is sent in is {cron_time_parsed}")
            r_temp = "{}-{}-{} {}".format(cron_time_parsed[0], cron_time_parsed[1], cron_time_parsed[2], cron_time_parsed[3])
            r = "{} + {} {}".format(r_temp, int(r.fixed[1]), r.fixed[2])
            # parse r so that, when it is returned, it is a parsed object
            r = parse("{} + {} {}", r)
            logger.debug(f"User sent now, new r is the current time: {r}")
        else:
            cron_time_parsed = parse("{}-{}-{} {}", cron_time)
            if cron_time_parsed is None:
                logger.debug(f'{r} is not in the correct format')
                raise errors.DAOError(f"The starting date {r.fixed[0]} is not in the correct format")
            else:
                # Create a datetime object out of cron_datetime
                schedule_execution = datetime.datetime(int(cron_time_parsed[0]), int(cron_time_parsed[1]), int(cron_time_parsed[2]), int(cron_time_parsed[3]))
                if schedule_execution < now_datetime:
                    logger.debug("User sent in old time, raise exception")
                    raise errors.DAOError(f'The starting datetime is old. The current UTC time is {now_datetime}')
        return r

    @classmethod
    def get_actor_id(cls, tenant, identifier):
        """
        Return the human readable actor_id associated with the identifier 
        :param identifier (str): either an actor_id or an alias. 
        :return: The actor_id; raises a KeyError if no actor exists. 
        """
        if is_hashid(identifier):
            return identifier
        # look for an alias with the identifier:
        alias_id = Alias.generate_alias_id(tenant, identifier)
        alias = Alias.retrieve_by_alias_id(alias_id)
        return alias.actor_id

    @classmethod
    def get_actor(cls, identifier, is_alias=False):
        """
        Return the actor object based on `identifier` which could be either a dbid or an alias.
        :param identifier (str): Unique identifier for an actor; either a dbid or an alias dbid.
        :param is_alias (bool): Caller can pass a hint, "is_alias=True", to avoid extra code checks. 
        
        :return: Actor dictionary; caller should instantiate an Actor object from it.  
        """
        if not is_alias:
            # check whether the identifier is an actor_id:
            if is_hashid(identifier):
                return actors_store[site()][identifier]
        # if we're here, either the caller set the is_alias=True hint or the is_hashid() returned False.
        # either way, we need to check the alias store
        alias = alias_store[site()][identifier]
        db_id = alias['db_id']
        return actors_store[site()][db_id]

    def get_uuid_code(self):
        """ Return the uuid code for this object.
        :return: str
        """
        return '059'

    def display(self):
        """Return a representation fit for display."""
        self.update(self.get_hypermedia())
        self.pop('db_id')
        self.pop('executions')
        self.pop('tenant')
        self.pop('api_server')
        c_time_str = self.pop('create_time')
        up_time_str = self.pop('last_update_time')
        self['create_time'] = display_time(c_time_str)
        self['last_update_time'] = display_time(up_time_str)
        return self.case()

    def get_hypermedia(self):
        return {'_links': { 'self': f'{self.api_server}/v3/actors/{self.id}',
                            'owner': f'{self.api_server}/v3/oauth2/profiles/{self.owner}',
                            'executions': f'{self.api_server}/v3/actors/{self.id}/executions'
        }}

    def generate_id(self, name, tenant):
        """Generate an id for a new actor."""
        id = self.get_uuid()
        return id, Actor.get_dbid(tenant, id)

    def ensure_one_worker(self, site_id=None):
        """This method will check the workers store for the actor and request a new worker if none exist."""
        logger.debug("top of Actor.ensure_one_worker().")
        site_id = site_id or site()
        worker_id = Worker.ensure_one_worker(self.db_id, self.tenant)
        logger.debug(f"Worker.ensure_one_worker returned worker_id: {worker_id}")
        if worker_id:
            logger.info("Actor.ensure_one_worker() putting message on command "
                        "channel for worker_id: {}".format(worker_id))
            ch = CommandChannel(name=self.queue)
            ch.put_cmd(actor_id=self.db_id,
                       worker_id=worker_id,
                       image=self.image,
                       revision=self.revision,
                       tenant=self.tenant,
                       site_id=site_id,
                       stop_existing=False)
            ch.close()
            return worker_id
        else:
            logger.debug("Actor.ensure_one_worker() returning None.")
            return None

    @classmethod
    def get_actor_log_ttl(cls, actor_id):
        """
        Returns the log time to live, looking at both the config file and logEx if passed
        Find the proper log expiry time, checking user log_ex, tenant, and global.
        """
        logger.debug("In get_actor_log_ttl")
        actor = Actor.from_db(actors_store[site()][actor_id])
        tenant = actor['tenant']
        # Gets the log_ex variables from the config.
        # Get user_log_ex
        user_log_ex = actor['log_ex']
        # Get tenant_log_ex and tenant limit
        tenant_obj = conf.get(f'{tenant}_tenant_object')
        if tenant_obj:
            tenant_log_ex = tenant_obj.get('log_ex')
            tenant_log_ex_limit = tenant_obj.get('log_ex_limit')
        else:
            tenant_log_ex = None
            tenant_log_ex_limit = None
        # Get global_log_ex and global limit (these are required fields in configschema)
        global_obj = conf.get('global_tenant_object')
        global_log_ex = global_obj.get('log_ex')
        global_log_ex_limit = global_obj.get('log_ex_limit')

        # Inspect vars to check they're within set bounds.
        if not user_log_ex:
            # Try to set tenant log_ex if it exists, default to global.
            log_ex = tenant_log_ex or global_log_ex
        else:
            # Restrict user_log_ex based on tenant or global config limit
            if tenant_log_ex_limit and user_log_ex > tenant_log_ex_limit:
                #raise DAOError(f"{user_log_ex} is larger than max tenant expiration {tenant_log_ex_limit}, will be set to this expiration")
                log_ex = tenant_log_ex_limit
            elif global_log_ex_limit and user_log_ex > global_log_ex_limit:
                #raise DAOError(f"{user_log_ex} is larger than max global expiration {global_log_ex_limit}, will be set to this expiration")
                log_ex = global_log_ex_limit
            else:
                log_ex = user_log_ex

        logger.debug(f"log_ex will be set to {log_ex}")
        return log_ex

    @classmethod
    def get_dbid(cls, tenant, id):
        """Return the key used in mongo from the "display_id" and tenant. """
        return str(f'{tenant}_{id}')

    @classmethod
    def get_display_id(cls, tenant, dbid):
        """Return the display id from the dbid."""
        if tenant + '_' in dbid:
            return dbid[len(tenant + '_'):]
        else:
            return dbid

    @classmethod
    def set_status(cls, actor_id, status, status_message=None, site_id=None):
        """
        Update the status of an actor.
        actor_id (str) should be the actor db_id.
        """
        site_id = site_id or site()
        logger.debug(f"top of set_status for status: {status}")
        actors_store[site_id][actor_id, 'status'] = status
        # we currently publish status change events for actors when the status is changing to ERROR or READY:
        if status == ERROR or status == READY:
            try:
                event_type = f'ACTOR_{status}'.upper()
                event = ActorEvent(actor_id, event_type, {'status_message': status_message})
                event.publish()
            except Exception as e:
                logger.error("Got exception trying to publish an actor status event. "
                             "actor_id: {}; status: {}; exception: {}".format(actor_id, status, e))
        if status_message:
            actors_store[site_id][actor_id, 'status_message'] = status_message


class Alias(AbacoDAO):
    """Data access object for working with Actor aliases."""

    PARAMS = [
        # param_name, required/optional/provided/derived, attr_name, type, help, default
        ('tenant', 'provided', 'tenant', str, 'The tenant that this alias belongs to.', None),
        ('alias_id', 'provided', 'alias_id', str, 'Primary key for alias for the actor and primary key to this store; must be globally unique.', None),
        ('alias', 'required', 'alias', str, 'Actual alias for the actor; must be unique within a tenant.', None),
        ('actor_id', 'required', 'actor_id', str, 'The human readable id for the actor associated with this alias.',
         None),
        ('db_id', 'provided', 'db_id', str, 'Primary key in the database for the actor associated with this alias.', None),
        ('owner', 'provided', 'owner', str, 'The user who created this alias.', None),
        ('api_server', 'provided', 'api_server', str, 'The base URL for the tenant that this alias belongs to.', None),
    ]

    # the following nouns cannot be used for an alias as they
    RESERVED_WORDS = ['executions', 'nonces', 'logs', 'messages', 'adapters', 'admin']
    FORBIDDEN_CHAR = ['\\', ' ', '"', ':', '/', '?', '#', '[', ']', '@', '!', '$', '&', "'", '(', ')', '*', '+', ',', ';', '=']


    @classmethod
    def generate_alias_id(cls, tenant, alias):
        """Generate the alias id from the alias name and tenant."""
        return f'{tenant}_{alias}'

    @classmethod
    def generate_alias_from_id(cls, alias_id):
        """Generate the alias id from the alias name and tenant."""
        # assumes the format of the alias_id is {tenant}_{alias} where {tenant} does NOT have an underscore (_) char
        # in it -
        return alias_id[alias_id.find('_')+1:]

    def check_reserved_words(self):
        if self.alias in Alias.RESERVED_WORDS:
            raise errors.DAOError("{} is a reserved word. "
                                  "The following reserved words cannot be used "
                                  "for an alias: {}.".format(self.alias, Alias.RESERVED_WORDS))

    def check_forbidden_char(self):
        for char in Alias.FORBIDDEN_CHAR:
            if char in self.alias:
                raise errors.DAOError("'{}' is a forbidden character. "
                                      "The following characters cannot be used "
                                      "for an alias: ['{}'].".format(char, "', '".join(Alias.FORBIDDEN_CHAR)))

    def check_and_create_alias(self):
        """Check to see if an alias is unique and create it if so. If not, raises a DAOError."""

        # first, make sure alias is not a reserved word:
        self.check_reserved_words()
        # second, make sure alias is not using a forbidden char:
        self.check_forbidden_char()
        # attempt to create the alias within a transaction
        obj = alias_store[site()].add_if_empty([self.alias_id], self)
        if not obj:
            raise errors.DAOError(f"Alias {self.alias} already exists.")
        return obj

    @classmethod
    def retrieve_by_alias_id(cls, alias_id):
        """ Returns the Alias object associate with the alias_id or raises a KeyError."""
        logger.debug(f"top of retrieve_by_alias_id; alias_id: {alias_id}")
        obj = alias_store[site()][alias_id]
        logger.debug(f"got alias obj: {obj}")
        return Alias(**obj)

    def get_hypermedia(self):
        return {'_links': { 'self': f'{self.api_server}/v3/actors/aliases/{self.alias}',
                            'owner': f'{self.api_server}/v3/oauth2/profiles/{self.owner}',
                            'actor': f'{self.api_server}/v3/actors/{self.actor_id}'
        }}

    def display(self):
        """Return a representation fit for display."""
        self.update(self.get_hypermedia())
        self.pop('db_id')
        self.pop('tenant')
        self.pop('alias_id')
        self.pop('api_server')
        return self.case()


class Nonce(AbacoDAO):
    """Basic data access object for working with actor nonces."""

    PARAMS = [
        # param_name, required/optional/provided/derived, attr_name, type, help, default
        ('tenant', 'provided', 'tenant', str, 'The tenant that this nonce belongs to.', None),
        ('db_id', 'provided', 'db_id', str, 'Primary key in the database for the actor associates with this nonce.', None),
        ('roles', 'provided', 'roles', list, 'Roles occupied by the user when creating this nonce.', []),
        ('owner', 'provided', 'owner', str, 'username associated with this nonce.', None),
        ('api_server', 'provided', 'api_server', str, 'api_server associated with this nonce.', None),

        ('level', 'optional', 'level', str,
         f'Permission level associated with this nonce. Default is {EXECUTE}.', EXECUTE.name),
        ('max_uses', 'optional', 'max_uses', int,
         'Maximum number of times this nonce can be redeemed. Default is unlimited.', -1),
        ('description', 'optional', 'description', str, 'Description of this nonce', ''),
      
        ('id', 'derived', 'id', str, 'Unique id for this nonce.', None),
        ('actor_id', 'derived', 'actor_id', str, 'The human readable id for the actor associated with this nonce.',
         None),
        ('alias', 'derived', 'alias', str, 'The alias id associated with this nonce.', None),
        ('create_time', 'derived', 'create_time', str, 'Time stamp (UTC) when this nonce was created.', None),
        ('last_use_time', 'derived', 'last_use_time', str, 'Time stamp (UTC) when thic nonce was last redeemed.', None),
        ('current_uses', 'derived', 'current_uses', int, 'Number of times this nonce has been redeemed.', 0),
        ('remaining_uses', 'derived', 'remaining_uses', int, 'Number of uses remaining for this nonce.', -1),
    ]

    def get_derived_value(self, name, d):
        """Compute a derived value for the attribute `name` from the dictionary d of attributes provided."""
        # first, see if the attribute is already in the object:
        if hasattr(self, name):
            return
        # next, see if it was passed:
        try:
            return d[name]
        except KeyError:
            pass

        # check for provided fields:
        try:
            self.tenant = d['tenant']
        except KeyError:
            logger.error("The nonce controller did not pass tenant to the Nonce model.")
            raise errors.DAOError("Could not instantiate nonce: tenant parameter missing.")
        try:
            self.api_server = d['api_server']
        except KeyError:
            logger.error("The nonce controller did not pass api_server to the Nonce model.")
            raise errors.DAOError("Could not instantiate nonce: api_server parameter missing.")
        # either an alias or a db_id must be passed, but not both -
        try:
            self.db_id = d['db_id']
            self.alias = None
        except KeyError:
            try:
                self.alias = d['alias']
                self.db_id = None
                self.actor_id = None
            except KeyError:
                logger.error("The nonce controller did not pass db_id or alias to the Nonce model.")
                raise errors.DAOError("Could not instantiate nonce: both db_id and alias parameters missing.")
        if not self.db_id:
            try:
                self.alias = d['alias']
                self.actor_id = None
            except KeyError:
                logger.error("The nonce controller did not pass db_id or alias to the Nonce model.")
                raise errors.DAOError("Could not instantiate nonce: both db_id and alias parameters missing.")
        if self.alias and self.db_id:
            raise errors.DAOError("Could not instantiate nonce: both db_id and alias parameters present.")
        try:
            self.owner = d['owner']
        except KeyError:
            logger.error("The nonce controller did not pass owner to the Nonce model.")
            raise errors.DAOError("Could not instantiate nonce: owner parameter missing.")
        try:
            self.roles = d['roles']
        except KeyError:
            logger.error("The nonce controller did not pass roles to the Nonce model.")
            raise errors.DAOError("Could not instantiate nonce: roles parameter missing.")

        # generate a nonce id:
        if not hasattr(self, 'id') or not self.id:
            self.id = self.get_nonce_id(self.tenant, self.get_uuid())

        # derive the actor_id from the db_id if this is an actor nonce:
        logger.debug(f"inside get_derived_value for nonce; name={name}; d={d}; self={self}")
        if self.db_id:
            self.actor_id = Actor.get_display_id(self.tenant, self.db_id)

        # time fields
        time_str = get_current_utc_time()
        self.create_time = time_str
        # initially there are no uses
        self.last_use_time = None

        # apply defaults to provided fields since those aren't applied in the constructor:
        self.current_uses = 0
        self.remaining_uses = self.max_uses

        # always return the requested attribute since the __init__ expects it.
        return getattr(self, name)

    def get_nonce_id(self, tenant, uuid):
        """Return the nonce id from the tenant and uuid."""
        return f'{tenant}_{uuid}'

    def get_hypermedia(self):
        return {'_links': { 'self': f'{self.api_server}/v3/actors/{self.actor_id}/nonces/{self.id}',
                            'owner': f'{self.api_server}/v3/oauth2/profiles/{self.owner}',
                            'actor': f'{self.api_server}/v3/actors/{self.actor_id}'
        }}

    def display(self):
        """Return a representation fit for display."""
        self.update(self.get_hypermedia())
        self.pop('db_id')
        self.pop('tenant')
        alias_id = self.pop('alias')
        if alias_id:
            alias_st = Alias.generate_alias_from_id(alias_id=alias_id)
            self['alias'] = alias_st
        time_str = self.pop('create_time')
        self['create_time'] = display_time(time_str)
        time_str = self.pop('last_use_time')
        self['last_use_time'] = display_time(time_str)
        return self.case()

    @classmethod
    def get_validate_nonce_key(cls, actor_id, alias):
        if not actor_id and not alias:
            raise errors.DAOError('add_nonce did not receive an alias or an actor_id')
        if actor_id and alias:
            raise errors.DAOError('add_nonce received both an alias and an actor_id')
        if actor_id:
            return actor_id
        return alias

    @classmethod
    def get_tenant_from_nonce_id(cls, nonce_id):
        """Returns the tenant from the nonce id."""
        # the nonce id has the form <tenant_id>_<uuid>, where uuid should contain no "_" characters.
        # so, we split from the right on '_' and stop after the first occurrence.
        return nonce_id.rsplit('_', 1)[0]

    @classmethod
    def get_nonces(cls, actor_id, alias):
        """Retrieve all nonces for an actor. Pass db_id as `actor_id` parameter."""
        nonce_key = Nonce.get_validate_nonce_key(actor_id, alias)
        try:
            nonces = nonce_store[site()][nonce_key]
        except KeyError:
            # return an empty Abaco dict if not found
            return AbacoDAO()
        return [Nonce(**nonce) for _, nonce in nonces.items()]

    @classmethod
    def get_nonce(cls, actor_id, alias, nonce_id):
        """Retrieve a nonce for an actor. Pass db_id as `actor_id` parameter."""
        nonce_key = Nonce.get_validate_nonce_key(actor_id, alias)
        try:
            nonce = nonce_store[site()][nonce_key][nonce_id]
            return Nonce(**nonce)
        except KeyError:
            raise errors.DAOError("Nonce not found.")

    @classmethod
    def add_nonce(cls, actor_id, alias, nonce):
        """
        Atomically append a new nonce to the nonce_store[site()] for an actor. 
        The actor_id parameter should be the db_id and the nonce parameter should be a nonce object
        created from the contructor.
        """
        nonce_key = Nonce.get_validate_nonce_key(actor_id, alias)
        try:
            nonce_store[site()][nonce_key, nonce.id] = nonce
            logger.debug(f"nonce {nonce.id} appended to nonces for actor/alias {nonce_key}")
        except KeyError:
            nonce_store[site()].add_if_empty([nonce_key, nonce.id], nonce)
            logger.debug(f"nonce {nonce.id} added for actor/alias {nonce_key}")

    @classmethod
    def delete_nonce(cls, actor_id, alias, nonce_id):
        """Delete a nonce from the nonce_store[site()]."""
        nonce_key = Nonce.get_validate_nonce_key(actor_id, alias)
        nonce_store[site()].pop_field([nonce_key, nonce_id])

    @classmethod
    def check_and_redeem_nonce(cls, actor_id, alias, nonce_id, level):
        """
        Atomically, check for the existence of a nonce for a given actor_id and redeem it if it
        has not expired. Otherwise, raises PermissionsError. 
        """
        # first, make sure the nonce exists for the nonce_key:
        nonce_key = Nonce.get_validate_nonce_key(actor_id, alias)
        try:
            nonce = nonce_store[site()][nonce_key][nonce_id]
        except KeyError:
            raise errors.PermissionsException("Nonce does not exist.")

        # check if the nonce level is sufficient
        try:
            if PermissionLevel(nonce['level']) < level:
                raise errors.PermissionsException("Nonce does not have sufficient permissions level.")
        except KeyError:
            raise errors.PermissionsException("Nonce did not have an associated level.")
        
        try:
            # Check for remaining uses equal to -1
            res = nonce_store[site()].full_update(
                {'_id': nonce_key, nonce_id + '.remaining_uses': {'$eq': -1}},
                {'$inc': {nonce_id + '.current_uses': 1},
                '$set': {nonce_id + '.last_use_time': get_current_utc_time()}})
            if res.raw_result['updatedExisting'] == True:
                logger.debug("nonce has infinite uses. updating nonce.")
                return

            # Check for remaining uses greater than 0
            res = nonce_store[site()].full_update(
                {'_id': nonce_key, nonce_id + '.remaining_uses': {'$gt': 0}},
                {'$inc': {nonce_id + '.current_uses': 1,
                        nonce_id + '.remaining_uses': -1},
                '$set': {nonce_id + '.last_use_time': get_current_utc_time()}})
            if res.raw_result['updatedExisting'] == True:
                logger.debug("nonce still has uses remaining. updating nonce.")
                return
            
            logger.debug("nonce did not have at least 1 use remaining.")
            raise errors.PermissionsException("No remaining uses left for this nonce.")
        except KeyError:
            logger.debug("nonce did not have a remaining_uses attribute.")
            raise errors.PermissionsException("No remaining uses left for this nonce.")

      
class Execution(AbacoDAO):
    """Basic data access object for working with actor executions."""

    PARAMS = [
        # param_name, required/optional/provided/derived, attr_name, type, help, default
        ('tenant', 'required', 'tenant', str, 'The tenant that this execution belongs to.', None),
        ('api_server', 'required', 'api_server', str, 'The base URL for the tenant that this actor belongs to.', None),
        ('actor_id', 'required', 'actor_id', str, 'The human readable id for the actor associated with this execution.', None),
        ('executor', 'required', 'executor', str, 'The user who triggered this execution.', None),
        ('worker_id', 'optional', 'worker_id', str, 'The worker who supervised this execution.', None),
        ('message_received_time', 'derived', 'message_received_time', str, 'Time (UTC) the message was received.', None),
        ('start_time', 'optional', 'start_time', str, 'Time (UTC) the execution started.', None),
        ('finish_time', 'optional', 'finish_time', str, 'Time (UTC) the execution finished.', None),
        ('runtime', 'required', 'runtime', str, 'Runtime, in milliseconds, of the execution.', None),
        ('cpu', 'required', 'cpu', str, 'CPU usage, in user jiffies, of the execution.', None),
        ('io', 'required', 'io', str,
         'Block I/O usage, in number of 512-byte sectors read from and written to, by the execution.', None),
        ('id', 'derived', 'id', str, 'Human readable id for this execution.', None),
        ('status', 'required', 'status', str, 'Status of the execution.', None),
        ('exit_code', 'optional', 'exit_code', str, 'The exit code of this execution.', None),
        ('final_state', 'optional', 'final_state', str, 'The final state of the execution.', None),
    ]

    def get_derived_value(self, name, d):
        """Compute a derived value for the attribute `name` from the dictionary d of attributes provided."""
        # first, see if the attribute is already in the object:
        try:
            if d[name]:
                return d[name]
        except KeyError:
            pass
        self.id = self.get_uuid()
        self.message_received_time = get_current_utc_time()
        if name == 'id':
            return self.id
        else:
            return self.message_received_time

    @classmethod
    def add_execution(cls, actor_id, ex):
        """
        Add an execution to an actor.
        :param actor_id: str; the dbid of the actor
        :param ex: dict describing the execution.
        :return:
        """
        logger.debug(f"top of add_execution for actor: {actor_id} and execution: {ex}.")
        actor = Actor.from_db(actors_store[site()][actor_id])
        ex.update({'actor_id': actor_id,
                   'tenant': actor.tenant,
                   'api_server': actor['api_server']
                   })
        execution = Execution(**ex)
        start_timer = timeit.default_timer()
        
        executions_store[site()][f'{actor_id}_{execution.id}'] = execution
        abaco_metrics_store[site()].full_update(
            {'_id': 'stats'},
            {'$inc': {'execution_total': 1},
             '$addToSet': {'execution_dbids': f'{actor_id}_{execution.id}'}},
             upsert=True)

        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"Execution.add_execution took {ms} to run for actor {actor_id}, execution: {execution}")
        logger.info(f"Execution: {ex} saved for actor: {actor_id}.")
        return execution.id

    @classmethod
    def add_worker_id(cls, actor_id, execution_id, worker_id):
        """
        :param actor_id: the id of the actor
        :param execution_id: the id of the execution
        :param worker_id: the id of the worker that executed this actor.
        :return:
        """
        logger.debug("top of add_worker_id() for actor: {} execution: {} worker: {}".format(
            actor_id, execution_id, worker_id))
        start_timer = timeit.default_timer()
        try:
            executions_store[site()][f'{actor_id}_{execution_id}', 'worker_id'] = worker_id
            logger.debug("worker added to execution: {} actor: {} worker: {}".format(
            execution_id, actor_id, worker_id))
        except KeyError as e:
            logger.error("Could not add an execution. KeyError: {}. actor: {}. ex: {}. worker: {}".format(
                e, actor_id, execution_id, worker_id))
            raise errors.ExecutionException(f"Execution {execution_id} not found.")
        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"Execution.add_worker_id took {ms} to run for actor {actor_id}, execution: {execution_id}, worker: {worker_id}")


    @classmethod
    def update_status(cls, actor_id, execution_id, status):
        """
        :param actor_id: the id of the actor
        :param execution_id: the id of the execution
        :param status: the new status of the execution.
        :return:
        """
        logger.debug("top of update_status() for actor: {} execution: {} status: {}".format(
            actor_id, execution_id, status))
        start_timer = timeit.default_timer()
        try:
            executions_store[site()][f'{actor_id}_{execution_id}', 'status'] = status
            logger.debug("status updated for execution: {} actor: {}. New status: {}".format(
            execution_id, actor_id, status))
        except KeyError as e:
            logger.error("Could not update status. KeyError: {}. actor: {}. ex: {}. status: {}".format(
                e, actor_id, execution_id, status))
            raise errors.ExecutionException(f"Execution {execution_id} not found.")
        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"Exection.update_status took {ms} to run for actor {actor_id}, "
                            f"execution: {execution_id}.")

    @classmethod
    def finalize_execution(cls, actor_id, execution_id, status, stats, final_state, exit_code, start_time):
        """
        Update an execution status and stats after the execution is complete or killed.
         `actor_id` should be the dbid of the actor.
         `execution_id` should be the id of the execution returned from a prior call to add_execution.
         `status` should be the final status of the execution.
         `stats` parameter should be a dictionary with io, cpu, and runtime.
         `final_state` parameter should be the `State` object returned from the docker inspect command.
         `exit_code` parameter should be the exit code of the container.
         `start_time` should be the start time (UTC string) of the execution. 
         """
        params_str = "actor: {}. ex: {}. status: {}. final_state: {}. exit_code: {}. stats: {}".format(
            actor_id, execution_id, status, final_state, exit_code, stats)
        logger.debug(f"top of finalize_execution. Params: {params_str}")
        if not 'io' in stats:
            logger.error(f"Could not finalize execution. io missing. Params: {params_str}")
            raise errors.ExecutionException("'io' parameter required to finalize execution.")
        if not 'cpu' in stats:
            logger.error(f"Could not finalize execution. cpu missing. Params: {params_str}")
            raise errors.ExecutionException("'cpu' parameter required to finalize execution.")
        if not 'runtime' in stats:
            logger.error(f"Could not finalize execution. runtime missing. Params: {params_str}")
            raise errors.ExecutionException("'runtime' parameter required to finalize execution.")
        start_timer = timeit.default_timer()
        try:
            executions_store[site()][f'{actor_id}_{execution_id}', 'status'] = status
            executions_store[site()][f'{actor_id}_{execution_id}', 'io'] = stats['io']
            executions_store[site()][f'{actor_id}_{execution_id}', 'cpu'] = stats['cpu']
            executions_store[site()][f'{actor_id}_{execution_id}', 'runtime'] = stats['runtime']
            executions_store[site()][f'{actor_id}_{execution_id}', 'final_state'] = final_state
            executions_store[site()][f'{actor_id}_{execution_id}', 'exit_code'] = exit_code
            executions_store[site()][f'{actor_id}_{execution_id}', 'start_time'] = start_time
        except KeyError:
            logger.error(f"Could not finalize execution. execution not found. Params: {params_str}")
            raise errors.ExecutionException(f"Execution {execution_id} not found.")

        try:
            finish_time = final_state.get('FinishedAt')
            # we rely completely on docker for the final_state object which includes the FinishedAt time stamp;
            # under heavy load, we have seen docker fail to set this time correctly and instead set it to 1/1/0001.
            # in that case, we should use the total_runtime to back into it.
            if finish_time == datetime.datetime.min:
                derived_finish_time = start_time + datetime.timedelta(seconds=stats['runtime'])
                executions_store[site()][f'{actor_id}_{execution_id}', 'finish_time'] = derived_finish_time
            else:
                executions_store[site()][f'{actor_id}_{execution_id}', 'finish_time'] = finish_time
        except Exception as e:
            logger.error(f"Could not finalize execution. Error: {e}")
            raise errors.ExecutionException(f"Could not finalize execution. Error: {e}")

        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"Execution.finalize_execution took {ms} to run for actor {actor_id}, "\
                            f"execution_id: {execution_id}")

        try:
            event_type = 'EXECUTION_COMPLETE'
            data = {'actor_id': actor_id,
                    'execution_id': execution_id,
                    'status': status,
                    'exit_code': exit_code
                    }
            event = ActorExecutionEvent(actor_id, execution_id, event_type, data)
            event.publish()
        except Exception as e:
            logger.error("Got exception trying to publish an actor execution event. "
                         "actor_id: {}; execution_id: {}; status: {}; "
                         "exception: {}".format(actor_id, execution_id, status, e))


    @classmethod
    def set_logs(cls, exc_id, logs, actor_id, tenant, worker_id, log_ex):
        """
        Set the logs for an execution.
        :param exc_id: the id of the execution (str)
        :param logs: dict describing the logs
        :return:
        """
        log_ex = log_ex or conf.global_tenant_object.get('log_ex')
        try:
            max_log_length = conf.web_max_log_length
        except:
            max_log_length = DEFAULT_MAX_LOG_LENGTH
        if len(logs) > DEFAULT_MAX_LOG_LENGTH:
            logger.info(f"truncating log for execution: {exc_id}")
            # in some environments, perhaps depending on OS or docker version, the logs object is of type bytes.
            # in that case we need to convert to be able to concatenate.
            if type(logs) == bytes:
                logs = logs.decode('utf-8')
            if not type(logs) == str:
                logger.info(f"Got unexpected type for logs object- could not convert to type str; type: {type(logs)}")
                logs = ''
            logs = logs[:max_log_length] + " LOG LIMIT EXCEEDED; this execution log was TRUNCATED!"
        start_timer = timeit.default_timer()
        if log_ex > 0:
            logger.info(f"Storing log with expiry. exc_id: {exc_id}")
            logs_store[site()].set_with_expiry([exc_id, 'logs'], logs, log_ex)
        else:
            logger.info(f"Storing log without expiry. exc_id: {exc_id}")
            logs_store[site()][exc_id, logs] = logs
        logs_store[site()][exc_id, 'actor_id'] = actor_id
        logs_store[site()][exc_id, 'tenant'] = tenant
        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"Execution.set_logs took {ms} to run for execution: {exc_id}.")

    def get_uuid_code(self):
        """ Return the uuid code for this object.
        :return: str
        """
        return '053'

    def get_hypermedia(self):
        aid = Actor.get_display_id(self.tenant, self.actor_id)
        return {'_links': { 'self': f'{self.api_server}/v3/actors/{aid}/executions/{self.id}',
                            'owner': f'{self.api_server}/v3/oauth2/profiles/{self.executor}',
                            'logs': f'{self.api_server}/v3/actors/{aid}/executions/{self.id}/logs'
        }}

    def display(self):
        """Return a display version of the execution."""
        self.update(self.get_hypermedia())
        tenant = self.pop('tenant')
        if self.get('start_time'):
            self['start_time'] = display_time(self['start_time'])
        if self.get('finish_time'):
            self['finish_time'] = display_time(self['finish_time'])
        if self.get('message_received_time'):
            self['message_received_time'] = display_time(self['message_received_time'])
        if self.get('final_state'):
            if self['final_state'].get('StartedAt'):
                self['final_state']['StartedAt'] = display_time(self['final_state']['StartedAt'])
            if self['final_state'].get('FinishedAt'):
                self['final_state']['FinishedAt'] = display_time(self['final_state']['FinishedAt'])
        self.actor_id = Actor.get_display_id(tenant, self.actor_id)
        return self.case()


class ExecutionsSummary(AbacoDAO):
    """ Summary information for all executions performed by an actor. """
    PARAMS = [
        # param_name, required/optional/provided/derived, attr_name, type, help, default
        ('db_id', 'required', 'db_id', str, 'Primary key in the database for associated actor.', None),
        ('api_server', 'derived', 'api_server', str, 'Base URL for the tenant that associated actor belongs to.', None),
        ('actor_id', 'derived', 'actor_id', str, 'id for the actor.', None),
        ('owner', 'provided', 'owner', str, 'The user who created the associated actor.', None),
        ('executions', 'derived', 'executions', list, 'List of all executions with summary fields.', None),
        ('total_executions', 'derived', 'total_executions', str, 'Total number of execution.', None),
        ('total_io', 'derived', 'total_io', str,
         'Block I/O usage, in number of 512-byte sectors read from and written to, by all executions.', None),
        ('total_runtime', 'derived', 'total_runtime', str, 'Runtime, in milliseconds, of all executions.', None),
        ('total_cpu', 'derived', 'total_cpu', str, 'CPU usage, in user jiffies, of all execution.', None),
        ]

    def compute_summary_stats(self, dbid):
        try:
            actor = actors_store[site()][dbid]
        except KeyError:
            raise errors.DAOError(f"actor not found: {dbid}'", 404)
        tot = {'api_server': actor['api_server'],
               'actor_id': actor['id'],
               'owner': actor['owner'],
               'total_executions': 0,
               'total_cpu': 0,
               'total_io': 0,
               'total_runtime': 0,
               'executions': []}
        executions = executions_store[site()].items({'actor_id': dbid})
        for val in executions:
            start_time = val.get('start_time')
            if start_time:
                start_time = display_time(start_time)
            finish_time = val.get('finish_time')
            if finish_time:
                finish_time = display_time(finish_time)
            message_received_time = val.get('message_received_time')
            if message_received_time:
                message_received_time = display_time(message_received_time)
            execution = {'id': val.get('id'),
                         'status': val.get('status'),
                         'start_time': val.get('start_time'),
                         'finish_time': val.get('finish_time'),
                         'message_received_time': val.get('message_received_time')}
            if conf.web_case == 'camel':
                execution = dict_to_camel(execution)
            tot['executions'].append(execution)
            tot['total_executions'] += 1
            tot['total_cpu'] += int(val['cpu'])
            tot['total_io'] += int(val['io'])
            tot['total_runtime'] += int(val['runtime'])
        return tot

    def get_derived_value(self, name, d):
        """Compute a derived value for the attribute `name` from the dictionary d of attributes provided."""
        # first, see if the attribute is already in the object:
        try:
            if d[name]:
                return d[name]
        except KeyError:
            pass
        # if not, compute and store all values, returning the one requested:
        try:
            dbid = d['db_id']
        except KeyError:
            logger.error(f"db_id missing from call to get_derived_value. d: {d}")
            raise errors.ExecutionException('db_id is required.')
        tot = self.compute_summary_stats(dbid)
        d.update(tot)
        return tot[name]

    def get_hypermedia(self):
        return {'_links': {'self': f'{self.api_server}/v3/actors/{self.actor_id}/executions',
                           'owner': f'{self.api_server}/v3/oauth2/profiles/{self.owner}',
        }}

    def display(self):
        self.update(self.get_hypermedia())
        self.pop('db_id')
        return self.case()


class Worker(AbacoDAO):
    """Basic data access object for working with Workers."""

    PARAMS = [
        # param_name, required/optional/provided/derived, attr_name, type, help, default
        ('tenant', 'required', 'tenant', str, 'The tenant that this worker belongs to.', None),
        ('id', 'required', 'id', str, 'The unique id for this worker.', None),
        ('status', 'required', 'status', str, 'Status of the worker.', None),
        # Initially, only `tenant, `id` and `status` are required by the time a client using the __init__ method for a worker object.
        # They should already have the `id` field having used request_worker first.
        ('ch_name', 'optional', 'ch_name', str, 'The name of the associated worker chanel.', None),
        ('image', 'optional', 'image', list, 'The list of images associated with this worker', None),
        ('location', 'optional', 'location', str, 'The location of the docker daemon used by this worker.', None),
        ('cid', 'optional', 'cid', str, 'The container ID of this worker.', None),
        ('host_id', 'optional', 'host_id', str, 'id of the host where worker is running.', None),
        ('host_ip', 'optional', 'host_ip', str, 'ip of the host where worker is running.', None),
        ('create_time', 'derived', 'create_time', str, "Time (UTC) that this actor was created.", {}),
        ('last_execution_time', 'optional', 'last_execution_time', str, 'Last time the worker executed an actor container.', None),
        ('last_health_check_time', 'optional', 'last_health_check_time', str, 'Last time the worker had a health check.', None),
        ]

    def get_derived_value(self, name, d):
        """Compute a derived value for the attribute `name` from the dictionary d of attributes provided."""
        # first, see if the attribute is already in the object:
        if hasattr(self, name):
            return
        # next, see if it was passed:
        try:
            return d[name]
        except KeyError:
            pass
        # time fields
        if name == 'create_time':
            time_str = get_current_utc_time()
            self.create_time = time_str 
            return time_str 
    
    @classmethod
    def get_uuid(cls):
        """Generate a random uuid."""
        hashids = Hashids(salt=HASH_SALT)
        return hashids.encode(uuid.uuid1().int>>64)


    @classmethod
    def get_workers(cls, actor_id):
        """Retrieve all workers for an actor. Pass db_id as `actor_id` parameter."""
        start_timer = timeit.default_timer()
        try:
            result = workers_store[site()].items({'actor_id': actor_id})            
        except:
            result = workers_store['tacc'].items({'actor_id': actor_id})            
        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"get_workers took {ms} to run for actor {actor_id}")
        return result

    @classmethod
    def get_worker(cls, actor_id, worker_id, site_id=None):
        """Retrieve a worker from the workers store. Pass db_id as `actor_id` parameter."""
        site_id = site_id or site()
        start_timer = timeit.default_timer()
        try:
            result = workers_store[site_id][f'{actor_id}_{worker_id}']
        except KeyError:
            raise errors.WorkerException("Worker not found.")
        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"get_worker took {ms} to run for actor {actor_id}, worker: {worker_id}")
        return result

    @classmethod
    def delete_worker(cls, actor_id, worker_id, site_id=None):
        """Deletes a worker from the worker store. Uses Redis optimistic locking to provide thread-safety since multiple
        clients could be attempting to delete workers at the same time. Pass db_id as `actor_id`
        parameter.
        """
        site_id = site_id or site()
        logger.debug(f"top of delete_worker(). actor_id: {actor_id}; worker_id: {worker_id}")
        try:
            wk = workers_store[site_id].pop_field([f'{actor_id}_{worker_id}'])
            logger.info(f"worker deleted. actor: {actor_id}. worker: {worker_id}.")
        except KeyError as e:
            logger.info(f"KeyError deleting worker. actor: {actor_id}. worker: {actor_id}. exception: {e}")
            raise errors.WorkerException("Worker not found.")

    @classmethod
    def ensure_one_worker(cls, actor_id, tenant, site_id=None):
        """
        Atomically ensure at least one worker exists in the database. If not, adds a worker to the database in
        requested status.
        This method returns an id for the worker if a new worker was added and otherwise returns none.
        """
        logger.debug("top of ensure_one_worker.")
        site_id = site_id or site()
        worker_id = Worker.get_uuid()
        worker = {'status': REQUESTED,
                  'id': worker_id,
                  'tenant': tenant,
                  'create_time': get_current_utc_time(),
                  'actor_id': actor_id}
        workers_for_actor = len(workers_store[site_id].items({'actor_id': actor_id}))
        if workers_for_actor:
            logger.debug(f"workers_for_actor was: {workers_for_actor}; returning None.")
            return None
        else:
            val = workers_store[site_id][f'{actor_id}_{worker_id}'] = worker
            abaco_metrics_store[site_id].full_update(
                {'_id': 'stats'},
                {'$inc': {'worker_total': 1},
                 '$addToSet': {'worker_dbids': f'{actor_id}_{worker_id}'}},
                upsert=True)
            logger.info(f"got worker: {val} from add_if_empty.")
            return worker_id

    @classmethod
    def request_worker(cls, tenant, actor_id):
        """
        Add a new worker to the database in requested status. This method returns an id for the worker and
        should be called before putting a message on the command queue.
        """
        logger.debug("top of request_worker().")
        worker_id = Worker.get_uuid()
        worker = {'status': REQUESTED,
                  'tenant': tenant,
                  'id': worker_id,
                  'actor_id': actor_id,
                  'create_time': get_current_utc_time()}
        # it's possible the actor_id is not in the workers_store yet (i.e., new actor with no workers)
        # In that case we need to catch a KeyError:
        try:
            # we know this worker_id is new since we just generated it, so we don't need to use the update
            # method.
            workers_store[site()][f'{actor_id}_{worker_id}'] = worker
            abaco_metrics_store[site()].full_update(
                {'_id': 'stats'},
                {'$inc': {'worker_total': 1},
                 '$addToSet': {'worker_dbids': f'{actor_id}_{worker_id}'}},
                upsert=True)
            logger.info(f"added additional worker with id: {worker_id} to workers_store[site()].")
        except KeyError:
            workers_store[site()].add_if_empty([f'{actor_id}_{worker_id}'], worker)
            abaco_metrics_store[site()].full_update(
                {'_id': 'stats'},
                {'$inc': {'worker_total': 1},
                 '$addToSet': {'worker_dbids': f'{actor_id}_{worker_id}'}},
                upsert=True)
            logger.info(f"added first worker with id: {worker_id} to workers_store[site()].")
        return worker_id

    @classmethod
    def add_worker(cls, actor_id, worker, site_id=None):
        """
        Add a running worker to an actor's collection of workers. The `worker` parameter should be a complete
        description of the worker, including the `id` field. The worker should have already been created
        in the database in 'REQUESTED' status using request_worker, and the `id` should be the same as that
        returned.
        """
        logger.debug("top of add_worker().")
        site_id = site_id or site()
        workers_store[site_id][f'{actor_id}_{worker["id"]}'] = worker
        logger.info(f"worker {worker} added to actor: {actor_id}")

    @classmethod
    def update_worker_execution_time(cls, actor_id, worker_id):
        """Pass db_id as `actor_id` parameter."""
        logger.debug("top of update_worker_execution_time().")
        now = get_current_utc_time()
        start_timer = timeit.default_timer()
        try:
            workers_store[site()][f'{actor_id}_{worker_id}', 'last_execution_time'] = now
        except KeyError as e:
            logger.error(f"Got KeyError; actor_id: {actor_id}; worker_id: {worker_id}; exception: {e}")
            raise e
        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"update_worker_execution_time took {ms} to run for actor {actor_id}, worker: {worker_id}")
        logger.info(f"worker execution time updated. worker_id: {worker_id}")

    @classmethod
    def update_worker_health_time(cls, actor_id, worker_id):
        """Pass db_id as `actor_id` parameter."""
        logger.debug("top of update_worker_health_time().")
        now = get_current_utc_time()
        workers_store[site()][f'{actor_id}_{worker_id}', 'last_health_check_time'] = now
        logger.info(f"worker last_health_check_time updated. worker_id: {worker_id}")

    @classmethod
    def update_worker_status(cls, actor_id, worker_id, status, site_id=None):
        """Pass db_id as `actor_id` parameter."""
        logger.debug("top of update_worker_status().")
        # The valid state transitions are as follows - set correct ERROR:
        # REQUESTED -> SPAWNER_SETUP
        # SPAWNER_SETUP -> PULLING_IMAGE
        # PULLING_IMAGE -> CREATING_CONTAINER
        # CREATING_CONTAINER -> UPDATING_STORE
        # UPDATING_STORE -> READY
        # READY -> BUSY -> READY ... etc
        
        valid_transitions = {
            SPAWNER_SETUP: [REQUESTED],
            PULLING_IMAGE: [SPAWNER_SETUP],
            CREATING_CONTAINER: [PULLING_IMAGE],
            UPDATING_STORE: [CREATING_CONTAINER],
            READY: [UPDATING_STORE, BUSY],
            BUSY: [READY]}
        
        site_id = site_id or site()
        start_timer = timeit.default_timer()
        try:
            # workers can transition to SHUTTING_DOWN from any status
            if status == SHUTTING_DOWN or status == SHUTDOWN_REQUESTED:
                workers_store[site_id][f'{actor_id}_{worker_id}', 'status'] = status
                
            elif status == ERROR:
                res = workers_store[site_id].full_update(
                    {'_id': f'{actor_id}_{worker_id}'},
                    [{'$set': {'status': {"$concat": [ERROR, " (PREVIOUS ", f"${worker_id}.status", ")"]}}}])
            # workers can always transition to an ERROR status from any status and from an ERROR status to
            # any status.
            else:
                try:
                    valid_status_1, valid_status_2 = valid_transitions[status]
                except ValueError:
                    valid_status_1 = valid_transitions[status][0]
                    valid_status_2 = None
                res = workers_store[site_id].full_update(
                    {'_id': f'{actor_id}_{worker_id}',
                    '$or': [{'status': valid_status_1},
                            {'status': valid_status_2}]},
                    {'$set': {'status': status}})
                # Checks if nothing was modified (1 if yes, 0 if no)
                if not res.raw_result['nModified']:
                    prev_status = workers_store[site_id][f'{actor_id}_{worker_id}', 'status']
                    if not (prev_status == "READY" and status == "READY"):
                        raise Exception(f"Invalid State Transition '{prev_status}' -> '{status}'")
        except Exception as e:
            logger.warning(f"Got exception trying to update worker {worker_id} subfield status to {status}; e: {e}; type(e): {type(e)}")

        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"update_worker_status took {ms} to run for actor {actor_id}, worker: {worker_id}")
        logger.info(f"worker status updated to: {status}. worker_id: {worker_id}")

    def get_uuid_code(self):
        """ Return the uuid code for this object.
        :return: str
        """
        return '058'

    def display(self):
        """Return a representation fit for display."""
        last_execution_time_str = self.pop('last_execution_time')
        last_health_check_time_str = self.pop('last_health_check_time')
        create_time_str = self.pop('create_time')
        self['last_execution_time'] = display_time(last_execution_time_str)
        self['last_health_check_time'] = display_time(last_health_check_time_str)
        self['create_time'] = display_time(create_time_str)
        return self.case()

def get_permissions(actor_id):
    """ Return all permissions for an actor
    :param actor_id:
    :return:
    """
    try:
        return permissions_store[site()][actor_id]
    except KeyError:
        logger.error(f"Actor {actor_id} does not have entries in the permissions store, returning []")
        return {}


def get_config_permissions(config_id):
    """ Return all permissions for a config_id
    :param config_id: The unique id of the config (as returned by models.ActorConfig.get_config_db_key()).
    :return:
    """
    logger.debug(f"top of get_config_permissions for config_id: {config_id}")
    try:
        return configs_permissions_store[site()][config_id]
    except KeyError:
        raise errors.PermissionsException(f"Config {config_id} does not exist")


def permission_process(permissions, user, level, item):
    """
    Internal routine to check a list of precalculated permission objects
    :param permissions: list of permission objects for an actor, execution, actor_config, etc.
    :param user: the user to check the permissions against
    :param level: the permission level to check the permissions against.
    :param item: the object being requested (e.g., an actor, execution, config, etc.). This is used to populate
    messages.
    :return: bool -- True if user is permitted, false otherqise.
    """
    # get all permissions for this actor -
    logger.debug(f"top of permission_process; permissions: {permissions}; user: {user}; level: {level}; item: {item}")
    WORLD_USER = 'ABACO_WORLD'
    for p_user, p_name in permissions.items():
        # if the item has been shared with the WORLD_USER anyone can use it
        if p_user == WORLD_USER:
            logger.info(f"Allowing request - {item} has been shared with the WORLD_USER.")
            return True
        # otherwise, check if the permission belongs to this user and has the necessary level
        if p_user == user:
            p_pem = codes.PermissionLevel(p_name)
            if p_pem >= level:
                logger.info(f"Allowing request - user has appropriate permission with {item}.")
                return True
            else:
                # we found the permission for the user but it was insufficient; return False right away
                logger.info(f"Found permission {level} for {item}, rejecting request.")
                return False
    return False


def set_permission(user, actor_id, level):
    """Set the permission for a user and level to an actor. Here, actor_id can be a dbid or an alias dbid."""
    logger.debug("top of set_permission().")
    if not isinstance(level, PermissionLevel):
        raise errors.DAOError("level must be a PermissionLevel object.")
    new = permissions_store[site()].add_if_empty([actor_id, user], str(level))
    if not new:
        permissions_store[site()][actor_id, user] = str(level)
    logger.info(f"Permission set for actor: {actor_id}; user: {user} at level: {level}")


def set_config_permission(user, config_id, level):
    """Set the permission for user `user` and level `level` to an actor config with id `config_id`."""
    logger.debug(f"top of set_config_permission; user: {user}; config_id: {config_id}; level: {level}.")
    if not isinstance(level, PermissionLevel):
        raise errors.DAOError("level must be a PermissionLevel object.")
    new = configs_permissions_store[site()].add_if_empty([config_id, user], str(level))
    if not new:
        configs_permissions_store[site()][config_id, user] = str(level)
    logger.info(f"Permission set for actor: {config_id}; user: {user} at level: {level}")


class ActorConfig(AbacoDAO):
    """Data access object for working with Actor configs."""

    PARAMS = [
        # param_name, required/optional/provided/derived, attr_name, type, help, default
        ('tenant', 'provided', 'tenant', str, 'The tenant that this alias belongs to.', None),
        ('name', 'required', 'name', str, 'Name of the config', None),
        ('value', 'required', 'value', str, 'The value of the config; JSON Serializable. Set as the ENV VAR value.', None),
        ('is_secret', 'required', 'is_secret', inputs.boolean, 'Whether the config should be encrypted at rest and not retrievable.', None),
        # need write access to actor
        ('actors', 'required', 'actors', str, 'List of actor IDs or aliases that should get this config/secret.', []),
    ] # take both ids and aliases and figure out which one it is
     # they need write access on actor or alias
    # delete of aliases/ids needs to delete from configs

    # the following nouns cannot be used for an alias as they
    RESERVED_WORDS = ['executions', 'nonces', 'logs', 'messages', 'adapters', 'admin', 'utilization']
    FORBIDDEN_CHAR = [':', '/', '?', '#', '[', ']', '@', '!', '$', '&', "'", '(', ')', '*', '+', ',', ';', '=', ' ']

    @classmethod
    def get_config_db_key(cls, tenant_id, name):
        """
        Returns the unique key used in the actor_configs store for a give config.
        :return: (str)
        """
        return f"{tenant_id}_{name}"

    def display(self):
        """Return a representation fit for display."""
        self.pop('tenant')
        return self.case()

    def check_reserved_words(self):
        if self.name in ActorConfig.RESERVED_WORDS:
            raise errors.DAOError(f"{self.name} is a reserved word. The following reserved words cannot be used for a "
                                  f"config: {ActorConfig.RESERVED_WORDS}.")

    def check_forbidden_char(self):
        for char in ActorConfig.FORBIDDEN_CHAR:
            if char in self.name:
                forboden_chars_str = "".join(ActorConfig.FORBIDDEN_CHAR)
                raise errors.DAOError(f"'{char}' is a forbidden character. The following characters cannot be used "
                                      f"for a config name: [{forboden_chars_str}].")

    def check_and_create_config(self):
        """Check to see if a config id is unique and create it if so. If not, raises a DAOError."""

        # first, make sure config name is not a reserved word:
        self.check_reserved_words()
        # second, make sure config name is not using a forbidden char:
        self.check_forbidden_char()
        # attempt to create the config within a transaction
        config_id = ActorConfig.get_config_db_key(tenant_id=self.tenant, name=self.name)
        obj = configs_store[site()].add_if_empty([config_id], self)
        if not obj:
            raise errors.DAOError(f"Config {self.name} already exists; please choose another name for your config")
        return obj
