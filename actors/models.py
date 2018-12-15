from copy import deepcopy
import datetime
import json
import time
import uuid

from flask_restful import inputs
from hashids import Hashids

from agaveflask.utils import RequestParser

from channels import CommandChannel
from codes import REQUESTED, SUBMITTED, EXECUTE, PermissionLevel
from config import Config
import errors

from stores import actors_store, clients_store, executions_store, logs_store, nonce_store, permissions_store, workers_store

from agaveflask.logs import get_logger
logger = get_logger(__name__)


HASH_SALT = 'eJa5wZlEX4eWU'

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

def get_current_utc_time():
    """Return string representation of current time in UTC."""
    utcnow = datetime.datetime.utcnow()
    return str(utcnow.timestamp())

def display_time(t):
    """ Convert a string representation of a UTC timestamp to a display string."""
    if not t:
        return "None"
    try:
        time_f = float(t)
        dt = datetime.datetime.fromtimestamp(time_f)
    except ValueError as e:
        logger.error("Invalid time data. Could not cast {} to float. Exception: {}".format(t, e))
        raise errors.DAOError("Error retrieving time data.")
    except TypeError as e:
        logger.error("Invalid time data. Could not convert float to datetime. t: {}. Exception: {}".format(t, e))
        raise errors.DAOError("Error retrieving time data.")
    return str(dt)


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
        parser = RequestParser()
        for name, source, attr, typ, help, default in cls.PARAMS:
            if source == 'derived':
                continue
            required = source == 'required'
            if Config.get('web', 'case') == 'camel':
                param_name = under_to_camel(name)
            else:
                param_name = name
            logger.warning('LOOK HERE 2 {}'.format(param_name))
            parser.add_argument(param_name, type=typ, required=required, help=help, default=default)
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
            if name not in kwargs and Config.get('web', 'case') == 'camel':
                # derived attributes always create the attribute with underscores:
                if source == 'derived':
                    pname = name
                else:
                    pname = under_to_camel(name)
            if source == 'required':
                try:
                    value = kwargs[pname]
                except KeyError:
                    logger.debug("required missing field: {}. ".format(pname))
                    raise errors.DAOError("Required field {} missing.".format(pname))
            elif source == 'optional':
                value = kwargs.get(pname, default)
            elif source == 'provided':
                try:
                    value = kwargs[pname]
                except KeyError:
                    logger.debug("provided field missing: {}.".format(pname))
                    raise errors.DAOError("Required field {} missing.".format(pname))
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
        case = Config.get('web', 'case')
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

        ('stateless', 'optional', 'stateless', inputs.boolean, 'Whether the actor stores private state.', False),
        ('type', 'optional', 'type', str, 'Return type (none, bin, json) for this actor. Default is none.', 'none'),
        ('description', 'optional', 'description', str,  'Description of this actor', ''),
        ('privileged', 'optional', 'privileged', inputs.boolean, 'Whether this actor runs in privileged mode.', False),
        ('max_workers', 'optional', 'max_workers', int, 'How many workers this actor is allowed at the same time.', None),
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
        ('tasdir', 'optional', 'tasdir', str, 'Absolute path to the TAS defined home directory associated with the owner of the actor', None),
        ('uid', 'optional', 'uid', str, 'The uid to run the container as. Only used if user_container_uid is false.', None),
        ('gid', 'optional', 'gid', str, 'The gid to run the container as. Only used if user_container_uid is false.', None),

        ('db_id', 'derived', 'db_id', str, 'Primary key in the database for this actor.', None),
        ('id', 'derived', 'id', str, 'Human readable id for this actor.', None),
        ]

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
            logger.debug("name or tenant missing from actor dict: {}.".format(d))
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

    def get_uuid_code(self):
        """ Return the Agave code for this object.
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
        return {'_links': { 'self': '{}/actors/v2/{}'.format(self.api_server, self.id),
                            'owner': '{}/profiles/v2/{}'.format(self.api_server, self.owner),
                            'executions': '{}/actors/v2/{}/executions'.format(self.api_server, self.id)
        }}

    def generate_id(self, name, tenant):
        """Generate an id for a new actor."""
        id = self.get_uuid()
        return id, Actor.get_dbid(tenant, id)

    def ensure_one_worker(self):
        """This method will check the workers store for the actor and request a new worker if none exist."""
        logger.debug("top of Actor.ensure_one_worker().")
        worker_id = Worker.ensure_one_worker(self.db_id, self.tenant)
        logger.debug("Worker.ensure_one_worker returned worker_id: {}".format(worker_id))
        if worker_id:
            worker_ids = [worker_id]
            logger.info("Actor.ensure_one_worker() putting message on command channel for worker_id: {}".format(
                worker_id))
            ch = CommandChannel()
            ch.put_cmd(actor_id=self.db_id,
                       worker_ids=worker_ids,
                       image=self.image,
                       tenant=self.tenant,
                       num=1,
                       stop_existing=False)
            ch.close()
            return worker_ids
        else:
            logger.debug("Actor.ensure_one_worker() returning None.")
            return None

    @classmethod
    def get_dbid(cls, tenant, id):
        """Return the key used in redis from the "display_id" and tenant. """
        return str('{}_{}'.format(tenant, id))

    @classmethod
    def get_display_id(cls, tenant, dbid):
        """Return the display id from the dbid."""
        if tenant + '_' in dbid:
            return dbid[len(tenant + '_'):]
        else:
            return dbid

    @classmethod
    def set_status(cls, actor_id, status, status_message=None):
        """Update the status of an actor"""
        logger.debug("top of set_status for status: {}".format(status))
        actors_store.update(actor_id, 'status', status)
        if status_message:
            actors_store.update(actor_id, 'status_message', status_message)


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
         'Permission level associated with this nonce. Default is {}.'.format(EXECUTE), EXECUTE.name),
        ('max_uses', 'optional', 'max_uses', int,
         'Maximum number of times this nonce can be redeemed. Default is unlimited.', -1),

        ('id', 'derived', 'id', str, 'Unique id for this nonce.', None),
        ('actor_id', 'derived', 'actor_id', str, 'The human readable id for the actor associated with this nonce.',
         None),
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
        try:
            self.db_id = d['db_id']
        except KeyError:
            logger.error("The nonce controller did not pass db_id to the Nonce model.")
            raise errors.DAOError("Could not instantiate nonce: db_id parameter missing.")
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
        self.id = self.get_nonce_id(self.tenant, self.get_uuid())

        # derive the actor_id from the db_id
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
        return '{}_{}'.format(tenant, uuid)

    def get_hypermedia(self):
        return {'_links': { 'self': '{}/actors/v2/{}/nonces/{}'.format(self.api_server, self.actor_id, self.id),
                            'owner': '{}/profiles/v2/{}'.format(self.api_server, self.owner),
                            'actor': '{}/actors/v2/{}'.format(self.api_server, self.actor_id)
        }}

    def display(self):
        """Return a representation fit for display."""
        self.update(self.get_hypermedia())
        self.pop('db_id')
        self.pop('tenant')
        time_str = self.pop('create_time')
        self['create_time'] = display_time(time_str)
        time_str = self.pop('last_use_time')
        self['last_use_time'] = display_time(time_str)
        return self.case()

    @classmethod
    def get_tenant_from_nonce_id(cls, nonce_id):
        """Returns the tenant from the nonce id."""
        # the nonce id has the form <tenant_id>_<uuid>, where uuid should contain no "_" characters.
        # so, we split from the right on '_' and stop after the first occurrence.
        return nonce_id.rsplit('_', 1)[0]

    @classmethod
    def get_nonces(cls, actor_id):
        """Retrieve all nonces for an actor. Pass db_id as `actor_id` parameter."""
        try:
            nonces = nonce_store[actor_id]
        except KeyError:
            # return an empty Abaco dict if not found
            return AbacoDAO()
        return [Nonce(**nonce) for _, nonce in nonces.items()]

    @classmethod
    def get_nonce(cls, actor_id, nonce_id):
        """Retrieve a nonce for an actor. Pass db_id as `actor_id` parameter."""
        try:
            nonce = nonce_store[actor_id][nonce_id]
            return Nonce(**nonce)
        except KeyError:
            raise errors.DAOError("Nonce not found.")

    @classmethod
    def add_nonce(cls, actor_id, nonce):
        """
        Atomically append a new nonce to the nonce_store for an actor. 
        The actor_id parameter should be the db_id and the nonce parameter should be a nonce object
        created from the contructor.
        """
        try:
            nonce_store.update(actor_id, nonce.id, nonce)
            logger.debug("nonce {} appended to nonces for actor {}".format(nonce.id, actor_id))
        except KeyError:
            nonce_store[actor_id] = {nonce.id: nonce}
            logger.debug("nonce {} added for actor {}".format(nonce.id, actor_id))

    @classmethod
    def delete_nonce(cls, actor_id, nonce_id):
        """Delete a nonce from the nonce_store."""
        nonce_store.pop_field(actor_id, nonce_id)

    @classmethod
    def check_and_redeem_nonce(cls, actor_id, nonce_id, level):
        """
        Atomically, check for the existence of a nonce for a given actor_id and redeem it if it
        has not expired. Otherwise, raises PermissionsError. 
        """
        def _transaction(nonces):
            """
            This function can be passed to nonce_store.within_transaction() to atomically check 
            whether a nonce is expired and, if not, redeem a use. The parameter, nonces, should
            be the value under the key `actor_id` associated with the nonce.
            """
            # first pull the nonce from the nonces parameter
            try:
                nonce = nonces[nonce_id]
            except KeyError:
                raise errors.PermissionsException("Nonce does not exist.")
            # check if the nonce level is sufficient
            try:
                if PermissionLevel(nonce['level']) < level:
                    raise errors.PermissionsException("Nonce does not have sufficient permissions level.")
            except KeyError:
                raise errors.PermissionsException("Nonce did not have an associated level.")

            # check if there are remaining uses
            try:
                if nonce['remaining_uses'] == -1:
                    logger.debug("nonce has infinite uses. updating nonce.")
                    nonce['current_uses'] += 1
                    nonce['last_use_time'] = get_current_utc_time()
                    nonce_store.update(actor_id, nonce_id, nonce)
                elif nonce['remaining_uses'] > 0:
                    logger.debug("nonce still has uses remaining. updating nonce.")
                    nonce['current_uses'] += 1
                    nonce['remaining_uses'] -= 1
                    nonce_store.update(actor_id, nonce_id, nonce)
                else:
                    logger.debug("nonce did not have at least 1 use remaining.")
                    raise errors.PermissionsException("No remaining uses left for this nonce.")
            except KeyError:
                logger.debug("nonce did not have a remaining_uses attribute.")
                raise errors.PermissionsException("No remaining uses left for this nonce.")

        # first, make sure the nonce exists for the actor id:
        try:
            nonce_store[actor_id][nonce_id]
        except KeyError:
            raise errors.PermissionsException("Nonce does not exist.")
        # atomically, check if the nonce is still valid and add a use if so:
        nonce_store.within_transaction(_transaction, actor_id)


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
        ('runtime', 'required', 'runtime', str, 'Runtime, in milliseconds, of the execution.', None),
        ('cpu', 'required', 'cpu', str, 'CPU usage, in user jiffies, of the execution.', None),
        ('io', 'required', 'io', str,
         'Block I/O usage, in number of 512-byte sectors read from and written to, by the execution.', None),
        ('id', 'derived', 'id', str, 'Human readable id for this execution.', None),
        ('status', 'required', 'status', str, 'Status of the execution.', None),\
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
        logger.debug("top of add_execution for actor: {} and execution: {}.".format(actor_id, ex))
        actor = Actor.from_db(actors_store[actor_id])
        ex.update({'actor_id': actor_id,
                   'tenant': actor.tenant,
                   'api_server': actor['api_server']
                   })
        execution = Execution(**ex)

        try:
            executions_store.update(actor_id, execution.id, execution)
        except KeyError:
            # if actor has no executions, a KeyError will be thrown
            executions_store[actor_id] = {execution.id: execution}
        logger.info("Execution: {} saved for actor: {}.".format(ex, actor_id))
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
        try:
            executions_store.update_subfield(actor_id, execution_id, 'worker_id', worker_id)
            logger.debug("worker added to execution: {} actor: {} worker: {}".format(
            execution_id, actor_id, worker_id))
        except KeyError as e:
            logger.error("Could not add an execution. KeyError: {}. actor: {}. ex: {}. worker: {}".format(
                e, actor_id, execution_id, worker_id))
            raise errors.ExecutionException("Execution {} not found.".format(execution_id))

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
        logger.debug("top of finalize_execution. Params: {}".format(params_str))
        if not 'io' in stats:
            logger.error("Could not finalize execution. io missing. Params: {}".format(params_str))
            raise errors.ExecutionException("'io' parameter required to finalize execution.")
        if not 'cpu' in stats:
            logger.error("Could not finalize execution. cpu missing. Params: {}".format(params_str))
            raise errors.ExecutionException("'cpu' parameter required to finalize execution.")
        if not 'runtime' in stats:
            logger.error("Could not finalize execution. runtime missing. Params: {}".format(params_str))
            raise errors.ExecutionException("'runtime' parameter required to finalize execution.")
        try:
            executions_store.update_subfield(actor_id, execution_id, 'status', status)
            executions_store.update_subfield(actor_id, execution_id, 'io', stats['io'])
            executions_store.update_subfield(actor_id, execution_id, 'cpu', stats['cpu'])
            executions_store.update_subfield(actor_id, execution_id, 'runtime', stats['runtime'])
            executions_store.update_subfield(actor_id, execution_id, 'final_state', final_state)
            executions_store.update_subfield(actor_id, execution_id, 'exit_code', exit_code)
            executions_store.update_subfield(actor_id, execution_id, 'start_time', start_time)
        except KeyError:
            logger.error("Could not finalize execution. execution not found. Params: {}".format(params_str))
            raise errors.ExecutionException("Execution {} not found.".format(execution_id))

    @classmethod
    def set_logs(cls, exc_id, logs):
        """
        Set the logs for an execution.
        :param exc_id: the id of the execution (str)
        :param logs: dict describing the logs
        :return:
        """
        log_ex = Config.get('web', 'log_ex')
        try:
            log_ex = int(log_ex)
        except ValueError:
            log_ex = -1
        if log_ex > 0:
            logger.info("Storing log with expiry. exc_id: {}".format(exc_id))
            logs_store.set_with_expiry(exc_id, logs)
        else:
            logger.info("Storing log without expiry. exc_id: {}".format(exc_id))
            logs_store[exc_id] = logs

    def get_uuid_code(self):
        """ Return the Agave code for this object.
        :return: str
        """
        return '053'

    def get_hypermedia(self):
        return {'_links': { 'self': '{}/actors/v2/{}/executions/{}'.format(self.api_server, self.actor_id, self.id),
                            'owner': '{}/profiles/v2/{}'.format(self.api_server, self.executor),
                            'logs': '{}/actors/v2/{}/executions/{}/logs'.format(self.api_server, self.actor_id, self.id)
        }}

    def display(self):
        """Return a display version of the execution."""
        self.update(self.get_hypermedia())
        tenant = self.pop('tenant')
        start_time_str = self.pop('start_time')
        received_time_str = self.pop('message_received_time')
        self['start_time'] = display_time(start_time_str)
        self['message_received_time'] = display_time(received_time_str)
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
            actor = actors_store[dbid]
        except KeyError:
            raise errors.DAOError(
                "actor not found: {}'".format(dbid), 404)
        tot = {'api_server': actor['api_server'],
               'actor_id': actor['id'],
               'owner': actor['owner'],
               'total_executions': 0,
               'total_cpu': 0,
               'total_io': 0,
               'total_runtime': 0,
               'executions': []}
        try:
            executions = executions_store[dbid]
        except KeyError:
            executions = {}
        for id, val in executions.items():
            execution = {'id': id,
                         'status': val.get('status'),
                         'start_time': val.get('start_time'),
                         'message_received_time': val.get('message_received_time')}
            if val.get('final_state'):
                execution['finish_time'] = val.get('final_state').get('FinishedAt')
                # we rely completely on docker for the final_state object which includes the FinishedAt time stamp;
                # under heavy load, we have seen docker fail to set this time correctly and instead set it to 1/1/0001.
                # in that case, we should use the total_runtime to back into it.
                if execution['finish_time'].startswith('0001-01-01'):
                    finish_time = float(val.get('start_time')) + float(val['runtime'])
                    execution['finish_time'] = display_time(str(finish_time))
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
            logger.error("db_id missing from call to get_derived_value. d: {}".format(d))
            raise errors.ExecutionException('db_id is required.')
        tot = self.compute_summary_stats(dbid)
        d.update(tot)
        return tot[name]

    def get_hypermedia(self):
        return {'_links': {'self': '{}/actors/v2/{}/executions'.format(self.api_server, self.actor_id),
                           'owner': '{}/profiles/v2/{}'.format(self.api_server, self.owner),
        }}

    def display(self):
        self.update(self.get_hypermedia())
        self.pop('db_id')
        for e in self['executions']:
            if e.get('start_time'):
                start_time_str = e.pop('start_time')
                e['start_time'] = display_time(start_time_str)
            if e.get('message_received_time'):
                message_received_time_str = e.pop('message_received_time')
                e['message_received_time'] = display_time(message_received_time_str)
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
        ('last_health_check_time', 'optional', 'last_health_check_time', str, 'Last time the worker had a health check.',
         None),
        ]

    def get_derived_value(self, name, d):
        """Compute a derived value for the attribute `name` from the dictionary d of attributes provided."""
        # first, see if the attribute is already in the object:
        if hasattr(self, name):
            return
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
        try:
            return workers_store[actor_id]
        except KeyError:
            return {}

    @classmethod
    def get_worker(cls, actor_id, worker_id):
        """Retrieve a worker from the workers store. Pass db_id as `actor_id` parameter."""
        try:
            return workers_store[actor_id][worker_id]
        except KeyError:
            raise errors.WorkerException("Worker not found.")

    @classmethod
    def delete_worker(cls, actor_id, worker_id):
        """Deletes a worker from the worker store. Uses Redis optimistic locking to provide thread-safety since multiple
        clients could be attempting to delete workers at the same time. Pass db_id as `actor_id`
        parameter.
        """
        logger.debug("top of delete_worker().")
        try:
            wk = workers_store.pop_field(actor_id, worker_id)
            logger.info("worker deleted. actor: {}. worker: {}.".format(actor_id, worker_id))
        except KeyError as e:
            logger.info("KeyError deleting worker. actor: {}. worker: {}. exception: {}".format(actor_id, worker_id, e))
            raise errors.WorkerException("Worker not found.")

    @classmethod
    def ensure_one_worker(cls, actor_id, tenant):
        """
        Atomically ensure at least one worker exists in the database. If not, adds a worker to the database in
        requested status.
        This method returns an id for the worker if a new worker was added and otherwise returns none.
        """
        logger.debug("top of ensure_one_worker.")
        worker_id = Worker.get_uuid()
        worker = {'status': REQUESTED, 'id': worker_id, 'tenant': tenant}
        val = workers_store.add_if_empty(actor_id, worker_id, worker)
        if val:
            logger.info("got worker: {} from add_if_empty.".format(val))
            return worker_id
        else:
            logger.debug("did not get worker from add_if_empty.")
            return None

    @classmethod
    def request_worker(cls, tenant, actor_id):
        """
        Add a new worker to the database in requested status. This method returns an id for the worker and
        should be called before putting a message on the command queue.
        """
        logger.debug("top of request_worker().")
        worker_id = Worker.get_uuid()
        worker = {'status': REQUESTED, 'tenant': tenant, 'id': worker_id}
        # it's possible the actor_id is not in the workers_store yet (i.e., new actor with no workers)
        # In that case we need to catch a KeyError:
        try:
            # we know this worker_id is new since we just generated it, so we don't need to use the update
            # method.
            workers_store[actor_id][worker_id] = worker
            logger.info("added additional worker with id: {} to workers_store.".format(worker_id))
        except KeyError:
            workers_store[actor_id] = {worker_id: worker}
            logger.info("added first worker with id: {} to workers_store.".format(worker_id))
        return worker_id

    @classmethod
    def add_worker(cls, actor_id, worker):
        """
        Add a running worker to an actor's collection of workers. The `worker` parameter should be a complete
        description of the worker, including the `id` field. The worker should have already been created
        in the database in 'REQUESTED' status using request_worker, and the `id` should be the same as that
        returned.
        """
        logger.debug("top of add_worker().")
        workers_store.update(actor_id, worker['id'], worker)
        logger.info("worker {} added to actor: {}".format(worker, actor_id))

    @classmethod
    def update_worker_execution_time(cls, actor_id, worker_id):
        """Pass db_id as `actor_id` parameter."""
        logger.debug("top of update_worker_execution_time().")
        now = get_current_utc_time()
        try:
            workers_store.update_subfield(actor_id, worker_id, 'last_execution_time', now)
        except KeyError as e:
            logger.error("Got KeyError; actor_id: {}; worker_id: {}; exception: {}".format(actor_id, worker_id, e))
            raise e
        logger.info("worker execution time updated. worker_id: {}".format(worker_id))

    @classmethod
    def update_worker_health_time(cls, actor_id, worker_id):
        """Pass db_id as `actor_id` parameter."""
        logger.debug("top of update_worker_health_time().")
        now = get_current_utc_time()
        workers_store.update_subfield(actor_id, worker_id, 'last_health_check_time', now)
        logger.info("worker last_health_check_time updated. worker_id: {}".format(worker_id))

    @classmethod
    def update_worker_status(cls, actor_id, worker_id, status):
        """Pass db_id as `actor_id` parameter."""
        logger.debug("top of update_worker_status().")
        try:
            workers_store.update_subfield(actor_id, worker_id, 'status', status)
        except Exception as e:
            logger.error("Got exception trying to update worker {} subfield status to {}; "
                         "e: {}; type(e)".format(worker_id, status, e, type(e)))
        logger.info("worker status updated to: {}. worker_id: {}".format(status, worker_id))

    def get_uuid_code(self):
        """ Return the Agave code for this object.
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

class Client(AbacoDAO):
    """
    Data access object for OAuth clients generated for workers.
    NOTE: this is not the public python API for interacting with clients. Managing clients should be
    done through the ClientsChannel/clientg actor, since client management involves managing these models but
    also objects in the third party systems (agave APIM).
    """

    PARAMS = [
        # param_name, required/optional/provided/derived, attr_name, type, help, default
        ('tenant', 'required', 'tenant', str, 'The tenant of the worker owning the client.', None),
        ('actor_id', 'required', 'actor_id', str, 'The actor id of the worker owning the client.', None),
        ('worker_id', 'required', 'worker_id', list, 'The id of the worker owning the client.', None),
        ('client_key', 'required', 'client_key', str, 'The key of the client.', None),
        ('client_name', 'required', 'client_name', str, 'The name of the client.', None),
        ('id', 'derived', 'id', str, 'Unique id in the database for this client', None)
        ]

    def get_derived_value(self, name, d):
        """Compute a derived value for the attribute `name` from the dictionary d of attributes provided."""
        # first, see if the id attribute is already in the object:
        try:
            if d[name]:
                return d[name]
        except KeyError:
            pass
        # combine the tenant_id and client_key to get the unique id
        return Client.get_client_id(d['tenant'], d['client_key'])

    @classmethod
    def get_client_id(cls, tenant, key):
        return '{}_{}'.format(tenant, key)

    @classmethod
    def get_client(cls, tenant, client_key):
        return Client(clients_store[Client.get_client_id(tenant, client_key)])

    @classmethod
    def delete_client(cls, tenant, client_key):
        del clients_store[Client.get_client_id(tenant, client_key)]

def get_permissions(actor_id):
    """ Return all permissions for an actor
    :param actor_id:
    :return:
    """
    try:
        return permissions_store[actor_id]
    except KeyError:
        raise errors.PermissionsException("Actor {} does not exist".format(actor_id))

def set_permission(user, actor_id, level):
    """Set the permission for a user and level to an actor."""
    logger.debug("top of set_permission().")
    if not isinstance(level, PermissionLevel):
        raise errors.DAOError("level must be a PermissionLevel object.")
    try:
        permissions_store.update(actor_id, user, level.name)
    except KeyError:
        # if actor has no permissions, a KeyError will be thrown
        permissions_store[actor_id] = {user: level.name}
    logger.info("Permission set for actor: {}; user: {} at level: {}".format(actor_id, user, level))