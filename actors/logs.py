"""Set up the loggers for the Abaco system."""

import logging

from common.config import conf

def get_log_file_strategy():
    """
    Returns the strategy for writing logs to files based on the config.
    The string "combined" means the logs should be written to a single file, abaco.log, while the string
    "split" means the logs should be split to different files, depending on the Abaco agent.
    """
    strategy = conf.log_filing_strategy
    return strategy.lower()

def get_module_log_level(name):
    """Reads config file for a log level set for this module."""
    log_level = conf.get(f"log_level_{name}") or conf.log_level
    return log_level

def get_log_file(name):
    """
    Reads config file for a log file to record logs for this module.
    If a file isn't specified for the module, looks for a global config. Otherwise, returns the
    default.

    Note: These paths refer to container paths, and the files must already exist. Since separate host files can be
    mounted to the container, it it likely that this configuration is not needed.
    """
    log_file = conf.get(f"log_file_{name}") or conf.log_file
    return log_file


def get_logger(name):
    """
    Returns a properly configured logger.
         name (str) should be the module name.
    """
    logger = logging.getLogger(name)
    level = get_module_log_level(name)
    logger.setLevel(level)
    handler = logging.FileHandler(get_log_file(name))
    handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s '
        '[in %(pathname)s:%(lineno)d]'
    ))
    handler.setLevel(level)
    logger.addHandler(handler)
    logger.info("returning a logger set to level: {} for module: {}".format(level, name))
    return logger
