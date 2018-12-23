"""Read config files from:
- /service.conf (added by user)
- /etc/service.conf (an example is placed here by the Dockerfile)

"""

from configparser import ConfigParser
import os

HERE = os.path.dirname(os.path.abspath(__file__))


def read_config():
    parser = ConfigParser()
    places = ['/service.conf',
              '/etc/service.conf']
    place = places[0]
    for p in places:
        if os.path.exists(p):
            place = p
            break
    if not parser.read(place):
        raise RuntimeError("couldn't read config file; tried these places: {0}"
                           .format(', '.join(places)))
    return parser

# Config = read_config()

class AbacoConfig(ConfigParser):

    def __init__(self):
        self._config_parser = read_config()
        super().__init__()

    def get(self, section, option, **kwargs):
        # first, check for config attribute in env var:
        var = '{}_{}'.format(section, option)
        if var in os.environ.keys():
            return os.environ.get(var)
        else:
            return self._config_parser.get(section, option, **kwargs)

Config = AbacoConfig()