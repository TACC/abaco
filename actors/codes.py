# Status codes for actor objects

REQUESTED = 'REQUESTED'
SPAWNER_SETUP = 'SPAWNER SETUP'
PULLING_IMAGE = 'PULLING IMAGE'
CREATING_CONTAINER = 'CREATING CONTAINER'
UPDATING_STORE = 'UPDATING STORE'
#TODO: error include prior state ie ERROR previous STATE
#TODO: comment about order of states
COMPLETE = 'COMPLETE'
SUBMITTED = 'SUBMITTED'
RUNNING = 'RUNNING'
READY = 'READY'
SHUTDOWN_REQUESTED = 'SHUTDOWN_REQUESTED'
SHUTTING_DOWN = 'SHUTTING_DOWN'
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

ALIAS_NONCE_PERMISSION_LEVELS = (NONE.name, READ.name, EXECUTE.name)

# role set by flaskbase in case the access_control_type is none
ALL_ROLE = 'ALL'

# roles - only used when Tapis's JWT Auth is activated.
# the admin role allows users full access to Abaco, including modifying workers assigned to actors.
ADMIN_ROLE = 'abaco_admin'

# the privileged role allows users to create privileged actors.
PRIVILEGED_ROLE = 'abaco_privileged'

roles = [ALL_ROLE, ADMIN_ROLE, PRIVILEGED_ROLE]