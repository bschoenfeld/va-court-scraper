from courtreader import readers
from courtutils.courtlogger import get_logger
import datetime
import pymongo
import os
import sys
import time

# configure logging
log = get_logger()
log.info('Worker running')

court = sys.argv[1]

def get_db_connection():
    return pymongo.MongoClient(os.environ['MONGO_DB'])['va_court_search']

def get_court_reader():
    reader = None
    if court == 'c': reader = readers.CircuitCourtReader()
    if court == 'd': reader = readers.DistrictCourtReader()
    reader.connect()
    log.info('Worker connected to court')
    return reader

def get_next_task(db):
    if court == 'c': return db.circuit_court_tasks.find_one_and_delete({})
    if court == 'd': return db.district_court_tasks.find_one_and_delete({})

# Fill in cases
court_reader = None
db = get_db_connection()
while True:
    task = get_next_task(db)
    if task is not None:
        if court == 'c': task['court_type'] = 'circuit'
        if court == 'd': task['court_type'] = 'district'
        log.info(task)
        if court_reader is None:
            court_reader = get_court_reader()
        cases = court_reader.get_cases_by_name(task['court_fips'], task['term'])
        if len(cases) > 0:
            for case in cases:
                case['fips_code'] = task['court_fips']
                case['court_type'] = task['court_type']
            db.searches.insert_many(cases)
        log_msg = 'Found ' + str(len(cases)) + ' cases'
        log.info(log_msg)
        print log_msg
    elif court_reader is not None:
        court_reader.log_off()
        court_reader = None
        log.info('Worker disconnected from court site')
    time.sleep(2)
