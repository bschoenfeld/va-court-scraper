import logging
import os

if 'LOG_ENTRIES' in os.environ:
    from logentries import LogentriesHandler

def get_logger():
    FORMAT = str(os.getpid()) + ': %(levelname)s, %(message)s'
    log = logging.getLogger('logentries')
    log.setLevel(logging.INFO)
    if 'LOG_ENTRIES' in os.environ:
        log.addHandler(LogentriesHandler(os.environ['LOG_ENTRIES'],
                                         format=logging.Formatter(FORMAT)))
    log.addHandler(logging.StreamHandler())
    return log
