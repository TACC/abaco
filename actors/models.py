from copy import deepcopy
import datetime
import json
import timeit
import uuid

from flask_restful import inputs
from hashids import Hashids

from agaveflask.utils import RequestParser

from channels import CommandChannel, EventsChannel
from codes import REQUESTED, READY, ERROR, SHUTDOWN_REQUESTED, SHUTTING_DOWN, SUBMITTED, EXECUTE, PermissionLevel, \
    SPAWNER_SETUP, PULLING_IMAGE, CREATING_CONTAINER, UPDATING_STORE, BUSY
from config import Config
import errors

from stores import actors_store, alias_store, clients_store, executions_store, logs_store, nonce_store, \
    permissions_store, workers_store

from agaveflask.logs import get_logger
logger = get_logger(__name__)


HASH_SALT = 'eJa5wZlEX4eWU'

# default max length for an actor execution log - 1MB
DEFAULT_MAX_LOG_LENGTH = 1000000

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
        data['event_time_utc'] = get_current_utc_time()
        data['event_time_display'] = display_time(data['event_time_utc'])
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
            actor = Actor.from_db(actors_store[self.db_id])
        except KeyError:
            logger.debug("did not find actor with id: {}".format(self.actor_id))
            raise errors.ResourceError("No actor found with identifier: {}.".format(self.actor_id), 404)
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
                logger.error("Got exception: {} calling get_actor_id to set the event link.".format(e))
        else:
            self.link = ''

    def publish(self):
        """
        External API for publishing events; handles all logic associated with event publishing including
        checking for actor links and pushing messages to the EventsExchange.
        :return: None 
        """
        logger.debug("top of publish for event: {}".format(self.data))
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
            raise errors.DAOError("Invalid actor event type {}.".format(event_type))


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
            raise errors.DAOError("Invalid actor execution event type {}.".format(event_type))

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
        parser = RequestParser()
        for name, source, attr, typ, help, default in cls.PARAMS:
            if source == 'derived':
                continue
            required = source == 'required'
            if Config.get('web', 'case') == 'camel':
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
                try:
                    value = kwargs[pname]
                except KeyError:
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
        ('tasdir', 'optional', 'tasdir', str, 'Absolute path to the TAS defined home directory associated with the owner of the actor', None),
        ('uid', 'optional', 'uid', str, 'The uid to run the container as. Only used if user_container_uid is false.', None),
        ('gid', 'optional', 'gid', str, 'The gid to run the container as. Only used if user_container_uid is false.', None),

        ('queue', 'optional', 'queue', str, 'The command channel that this actor uses.', 'default'),
        ('db_id', 'derived', 'db_id', str, 'Primary key in the database for this actor.', None),
        ('id', 'derived', 'id', str, 'Human readable id for this actor.', None),
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
                return actors_store[identifier]
        # if we're here, either the caller set the is_alias=True hint or the is_hashid() returned False.
        # either way, we need to check the alias store
        alias = alias_store[identifier]
        db_id = alias['db_id']
        return actors_store[db_id]

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
            logger.info("Actor.ensure_one_worker() putting message on command "
                        "channel for worker_id: {}".format(worker_id))
            ch = CommandChannel(name=self.queue)
            ch.put_cmd(actor_id=self.db_id,
                       worker_id=worker_id,
                       image=self.image,
                       tenant=self.tenant,
                       stop_existing=False)
            ch.close()
            return worker_id
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
        """Update the status of an actor.
        actor_id (str) should be the actor db_id.
        """
        logger.debug("top of set_status for status: {}".format(status))
        actors_store.update(actor_id, 'status', status)
        # we currently publish status change events for actors when the status is changing to ERROR or READY:
        if status == ERROR or status == READY:
            try:
                event_type = 'ACTOR_{}'.format(status).upper()
                event = ActorEvent(actor_id, event_type, {'status_message': status_message})
                event.publish()
            except Exception as e:
                logger.error("Got exception trying to publish an actor status event. "
                             "actor_id: {}; status: {}; exception: {}".format(actor_id, status, e))
        if status_message:
            actors_store.update(actor_id, 'status_message', status_message)


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
    RESERVED_WORDS = ['executions', 'nonces', 'logs', 'messages', 'adapters', 'admin', 'utilization']
    FORBIDDEN_CHAR = [':', '/', '?', '#', '[', ']', '@', '!', '$', '&', "'", '(', ')', '*', '+', ',', ';', '=']


    @classmethod
    def generate_alias_id(cls, tenant, alias):
        """Generate the alias id from the alias name and tenant."""
        return '{}_{}'.format(tenant, alias)

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
        obj = alias_store.add_key_val_if_empty(self.alias_id, self)
        if not obj:
            raise errors.DAOError("Alias {} already exists.".format(self.alias))
        return obj

    @classmethod
    def retrieve_by_alias_id(cls, alias_id):
        """ Returns the Alias object associate with the alias_id or raises a KeyError."""
        logger.debug("top of retrieve_by_alias_id; alias_id: {}".format(alias_id))
        obj = alias_store[alias_id]
        logger.debug("got alias obj: {}".format(obj))
        return Alias(**obj)

    def get_hypermedia(self):
        return {'_links': { 'self': '{}/actors/v2/aliases/{}'.format(self.api_server, self.alias),
                            'owner': '{}/profiles/v2/{}'.format(self.api_server, self.owner),
                            'actor': '{}/actors/v2/{}'.format(self.api_server, self.actor_id)
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
         'Permission level associated with this nonce. Default is {}.'.format(EXECUTE), EXECUTE.name),
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
        logger.debug("inside get_derived_value for nonce; name={}; d={}; self={}".format(name, d, self))
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
            nonces = nonce_store[nonce_key]
        except KeyError:
            # return an empty Abaco dict if not found
            return AbacoDAO()
        return [Nonce(**nonce) for _, nonce in nonces.items()]

    @classmethod
    def get_nonce(cls, actor_id, alias, nonce_id):
        """Retrieve a nonce for an actor. Pass db_id as `actor_id` parameter."""
        nonce_key = Nonce.get_validate_nonce_key(actor_id, alias)
        try:
            nonce = nonce_store[nonce_key][nonce_id]
            return Nonce(**nonce)
        except KeyError:
            raise errors.DAOError("Nonce not found.")

    @classmethod
    def add_nonce(cls, actor_id, alias, nonce):
        """
        Atomically append a new nonce to the nonce_store for an actor. 
        The actor_id parameter should be the db_id and the nonce parameter should be a nonce object
        created from the contructor.
        """
        nonce_key = Nonce.get_validate_nonce_key(actor_id, alias)
        try:
            nonce_store.update(nonce_key, nonce.id, nonce)
            logger.debug("nonce {} appended to nonces for actor/alias {}".format(nonce.id, nonce_key))
        except KeyError:
            nonce_store[nonce_key] = {nonce.id: nonce}
            logger.debug("nonce {} added for actor/alias {}".format(nonce.id, nonce_key))

    @classmethod
    def delete_nonce(cls, actor_id, alias, nonce_id):
        """Delete a nonce from the nonce_store."""
        nonce_key = Nonce.get_validate_nonce_key(actor_id, alias)
        nonce_store.pop_field(nonce_key, nonce_id)

    @classmethod
    def check_and_redeem_nonce(cls, actor_id, alias, nonce_id, level):
        """
        Atomically, check for the existence of a nonce for a given actor_id and redeem it if it
        has not expired. Otherwise, raises PermissionsError. 
        """
        def _transaction(nonces):
            """
            This function can be passed to nonce_store.within_transaction() to atomically check 
            whether a nonce is expired and, if not, redeem a use. The parameter, nonces, should
            be the value under the key `nonce_key` associated with the nonce.
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
                    nonce_store.update(nonce_key, nonce_id, nonce)
                elif nonce['remaining_uses'] > 0:
                    logger.debug("nonce still has uses remaining. updating nonce.")
                    nonce['current_uses'] += 1
                    nonce['remaining_uses'] -= 1
                    nonce_store.update(nonce_key, nonce_id, nonce)
                else:
                    logger.debug("nonce did not have at least 1 use remaining.")
                    raise errors.PermissionsException("No remaining uses left for this nonce.")
            except KeyError:
                logger.debug("nonce did not have a remaining_uses attribute.")
                raise errors.PermissionsException("No remaining uses left for this nonce.")

        # first, make sure the nonce exists for the nonce_key:
        nonce_key = Nonce.get_validate_nonce_key(actor_id, alias)
        try:
            nonce_store[nonce_key][nonce_id]
        except KeyError:
            raise errors.PermissionsException("Nonce does not exist.")
        # atomically, check if the nonce is still valid and add a use if so:
        nonce_store.within_transaction(_transaction, nonce_key)


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
        start_timer = timeit.default_timer()
        try:
            executions_store.update(actor_id, execution.id, execution)
        except KeyError:
            # if actor has no executions, a KeyError will be thrown
            executions_store[actor_id] = {execution.id: execution}
        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"Execution.add_execution took {ms} to run for actor {actor_id}, execution: {execution}")
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
        start_timer = timeit.default_timer()
        try:
            executions_store.update_subfield(actor_id, execution_id, 'worker_id', worker_id)
            logger.debug("worker added to execution: {} actor: {} worker: {}".format(
            execution_id, actor_id, worker_id))
        except KeyError as e:
            logger.error("Could not add an execution. KeyError: {}. actor: {}. ex: {}. worker: {}".format(
                e, actor_id, execution_id, worker_id))
            raise errors.ExecutionException("Execution {} not found.".format(execution_id))
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
            executions_store.update_subfield(actor_id, execution_id, 'status', status)
            logger.debug("status updated for execution: {} actor: {}. New status: {}".format(
            execution_id, actor_id, status))
        except KeyError as e:
            logger.error("Could not update status. KeyError: {}. actor: {}. ex: {}. status: {}".format(
                e, actor_id, execution_id, status))
            raise errors.ExecutionException("Execution {} not found.".format(execution_id))
        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"Exection.update_status took {ms} to run for actor {actor_id}, "
                            f"execution: {execution_id}. worker: {worker_id}")

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
        start_timer = timeit.default_timer()
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
        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"Execution.finalize_execution took {ms} to run for actor {actor_id}, "
                            f"execution_id: {execution_id}, worker: {worker_id}")

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
        try:
            max_log_length = int(Config.get('web', 'max_log_length'))
        except:
            max_log_length = DEFAULT_MAX_LOG_LENGTH
        if len(logs) > DEFAULT_MAX_LOG_LENGTH:
            logger.info("truncating log for execution: {}".format(exc_id))
            logs = logs[:max_log_length] + " LOG LIMIT EXCEEDED; this execution log was TRUNCATED!"
        start_timer = timeit.default_timer()
        if log_ex > 0:
            logger.info("Storing log with expiry. exc_id: {}".format(exc_id))
            logs_store.set_with_expiry(exc_id, logs)
        else:
            logger.info("Storing log without expiry. exc_id: {}".format(exc_id))
            logs_store[exc_id] = logs
        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"Execution.set_logs took {ms} to run for actor {actor_id}, "
                            f"execution: {exc_id}, worker: {worker_id}")


    def get_uuid_code(self):
        """ Return the Agave code for this object.
        :return: str
        """
        return '053'

    def get_hypermedia(self):
        aid = Actor.get_display_id(self.tenant, self.actor_id)
        return {'_links': { 'self': '{}/actors/v2/{}/executions/{}'.format(self.api_server, aid, self.id),
                            'owner': '{}/profiles/v2/{}'.format(self.api_server, self.executor),
                            'logs': '{}/actors/v2/{}/executions/{}/logs'.format(self.api_server, aid, self.id)
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
            result = workers_store[actor_id]
        except KeyError:
            result = {}
        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"get_workers took {ms} to run for actor {actor_id}")
        return result

    @classmethod
    def get_worker(cls, actor_id, worker_id):
        """Retrieve a worker from the workers store. Pass db_id as `actor_id` parameter."""
        start_timer = timeit.default_timer()
        try:
            result = workers_store[actor_id][worker_id]
        except KeyError:
            raise errors.WorkerException("Worker not found.")
        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"get_worker took {ms} to run for actor {actor_id}, worker: {worker_id}")
        return result

    @classmethod
    def delete_worker(cls, actor_id, worker_id):
        """Deletes a worker from the worker store. Uses Redis optimistic locking to provide thread-safety since multiple
        clients could be attempting to delete workers at the same time. Pass db_id as `actor_id`
        parameter.
        """
        logger.debug("top of delete_worker(). actor_id: {}; worker_id: {}".format(actor_id, worker_id))
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
            workers_store.update(actor_id, worker_id, worker)
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
        start_timer = timeit.default_timer()
        try:
            workers_store.update_subfield(actor_id, worker_id, 'last_execution_time', now)
        except KeyError as e:
            logger.error("Got KeyError; actor_id: {}; worker_id: {}; exception: {}".format(actor_id, worker_id, e))
            raise e
        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"update_worker_execution_time took {ms} to run for actor {actor_id}, worker: {worker_id}")
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
        # The valid state transitions are as follows - set correct ERROR:
        # REQUESTED -> SPAWNER_SETUP
        # SPAWNER_SETUP -> PULLING_IMAGE
        # PULLING_IMAGE -> CREATING_CONTAINER
        # CREATING_CONTAINER -> UPDATING_STORE
        # UPDATING_STORE -> READY
        # READY -> BUSY -> READY ... etc

        prev_status = workers_store[actor_id][worker_id]['status']

        # workers can transition to SHUTTING_DOWN from any status
        if status == SHUTTING_DOWN or status == SHUTDOWN_REQUESTED:
            pass
        # workers can always transition to an ERROR status from any status and from an ERROR status to
        # any status.
        elif status != ERROR and ERROR not in prev_status:
            if prev_status == REQUESTED and status != SPAWNER_SETUP:
                raise Exception(f"Invalid State Transition f{prev_status} -> f{status}")
            elif prev_status == SPAWNER_SETUP and status != PULLING_IMAGE:
                raise Exception(f"Invalid State Transition f{prev_status} -> f{status}")
            elif prev_status == PULLING_IMAGE and status != CREATING_CONTAINER:
                raise Exception(f"Invalid State Transition f{prev_status} -> f{status}")
            elif prev_status == CREATING_CONTAINER and status != UPDATING_STORE:
                raise Exception(f"Invalid State Transition f{prev_status} -> f{status}")
            elif prev_status == UPDATING_STORE and status != READY:
                raise Exception(f"Invalid State Transition f{prev_status} -> f{status}")

        if status == ERROR:
            status = ERROR + f" (PREVIOUS {prev_status})"

        logger.info(f"worker status will be changed from {prev_status} to {status}")

        # get worker's current status and then do V this separately
        # this is not threadsafe
        start_timer = timeit.default_timer()
        try:
            workers_store.update_subfield(actor_id, worker_id, 'status', status)
        except Exception as e:
            logger.error("Got exception trying to update worker {} subfield status to {}; "
                         "e: {}; type(e): {}".format(worker_id, status, e, type(e)))
        stop_timer = timeit.default_timer()
        ms = (stop_timer - start_timer) * 1000
        if ms > 2500:
            logger.critical(f"update_worker_status took {ms} to run for actor {actor_id}, worker: {worker_id}")
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

class PregenClient(AbacoDAO):
    """
    Data access object for pregenerated OAuth clients for worker. Use of these clients requires an initial
    load script to populate the pregen_clients store with clients available for use.

    Each client object
    """
    pass

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
