"""Set up the loggers for the system."""

import logging
import sys
from .config import conf

def get_module_log_level(name: str) -> str:
    """
    Get the log level to use for this module.
    """
    # look for a log level configuration with name equal to the current module name. if one does not exist, that's fine
    # we just fall back on the "global" service log level:
    try:
        return getattr(conf, f'{name}_log_level')
    except AttributeError:
        return conf.log_level

def get_logger(name: str) -> logging.Logger:
    """
    Returns a properly configured logger.
         name (str) should be the module name.
    """
    logger = logging.getLogger(name)
    level = get_module_log_level(name)
    logger.setLevel(level)
    if not logger.hasHandlers():
        if conf.log_filing_strategy == 'combined':
            log_file = conf.get('log_file')
        else:
            log_file = conf.get(f'{name}_log_file') or conf.get('log_file')
        if log_file:
            handler = logging.FileHandler(log_file)
        else:
            handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s '
            '[in %(pathname)s:%(lineno)d]'
        ))
        handler.setLevel(level)
        logger.addHandler(handler)
    logger.info("returning a logger set to level: {} for module: {}".format(level, name))
    return logger