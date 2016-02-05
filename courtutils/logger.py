from logentries import LogentriesHandler
import logging
import os

def get_logger():
    FORMAT = str(os.getpid()) + ': %(levelname)s, %(message)s'
    log = logging.getLogger('logentries')
    log.setLevel(logging.INFO)
    log.addHandler(LogentriesHandler(os.environ['LOG_ENTRIES'], \
                    format=logging.Formatter(FORMAT)))
    log.addHandler(logging.StreamHandler())
    return log
