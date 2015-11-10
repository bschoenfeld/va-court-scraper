from courtreader import readers
from courtutils.courtlogger import get_logger
import datetime
import pymongo
import os
import time

# configure logging
log = get_logger()
log.info('Worker running')

def get_db_connection():
    return pymongo.MongoClient(os.environ['MONGO_DB'])['temp-15-11-08']

def get_circuit_court_reader():
    reader = readers.CircuitCourtReader()
    reader.connect()
    log.info('Worker connected to court site')
    return reader

# Fill in cases
circuit_court = None
db = get_db_connection()
while True:
    task = db.tasks.find_one_and_delete({})
    if task is not None:
        log.info(task)
        if circuit_court is None:
            circuit_court = get_circuit_court_reader()
        cases = circuit_court.get_cases_by_name(task['court_fips'], task['term'])
        if len(cases) > 0:
            db.searches.insert_many(cases)
        log.info('Found ' + str(len(cases)) + ' cases')
    elif circuit_court is not None:
        circuit_court.log_off()
        circuit_court = None
        log.info('Worker disconnected from court site')
    time.sleep(2)
