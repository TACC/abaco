# Status codes for actor objects
from config import Config
from agaveflask.logs import get_logger
logger = get_logger(__name__)

REQUESTED = 'REQUESTED'
COMPLETE = 'COMPLETE'
SUBMITTED = 'SUBMITTED'
READY = 'READY'
ERROR = 'ERROR'
BUSY = 'BUSY'

class PermissionLevel(object):

    def __init__(self, name, level=None):
        self.name = name
        if level:
            self.level = level
        elif name == 'NONE':
            self.level = 0
        elif name == 'READ':
            self.level = 1
        elif name == 'EXECUTE':
            self.level = 2
        elif name == 'UPDATE':
            self.level = 3

    def __lt__(self, other):
        if isinstance(other, PermissionLevel):
            return self.level.__lt__(other.level)
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, PermissionLevel):
            return self.level.__le__(other.level)
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, PermissionLevel):
            return self.level.__gt__(other.level)
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, PermissionLevel):
            return self.level.__ge__(other.level)
        return NotImplemented

    def __repr__(self):
        return self.name


NONE = PermissionLevel('NONE')
READ = PermissionLevel('READ')
EXECUTE = PermissionLevel('EXECUTE')
UPDATE = PermissionLevel('UPDATE')


PERMISSION_LEVELS = (NONE.name, READ.name, EXECUTE.name, UPDATE.name)

# role set by agaveflask in case the access_control_type is none
ALL_ROLE = 'ALL'

# roles - only used when Agave's JWT Auth is activated.
# the admin role allows users full access to Abaco, including modifying workers assigned to actors.
ADMIN_ROLE = 'Internal/abaco-admin'

# the privileged role allows users to create privileged actors.
PRIVILEGED_ROLE = 'Internal/abaco-privileged'

# the base user role in Abaco. This role isn't authorized to create privileged containers or add workers but is not
# throttled in the number of requests they can make.
try:
    USER_ROLE = Config.get('web', 'user_role')
except Exception:
    USER_ROLE = 'Internal/abaco-user'

logger.debug("using USER_ROLE: {}".format(USER_ROLE))

# a role with limited (throttled) access -- must be implemented in the Agave APIM tenant.
LIMITED_ROLE = 'Internal/abaco-limited'

roles = [ALL_ROLE, ADMIN_ROLE, PRIVILEGED_ROLE, USER_ROLE, LIMITED_ROLE]