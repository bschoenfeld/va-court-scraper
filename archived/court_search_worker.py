from courtreader import readers
from courtutils.logger import get_logger
from datetime import datetime
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

# Fill in cases
court_reader = None
current_court_fips = None
db = get_db_connection()

def get_court_reader():
    reader = None
    if court == 'c': reader = readers.CircuitCourtReader()
    if court == 'd': reader = readers.DistrictCourtReader()
    reader.connect()
    log.info('Worker connected to court')
    return reader

def get_completed_task_collection(task):
    return db[task['court_type'] + '_court_completed_tasks']

def get_next_task(court_fips):
    collection = db.circuit_court_tasks if court == 'c' \
                    else db.district_court_tasks
    task = None
    if court_fips is not None:
        print 'Looking for task in', court_fips
        task = collection.find_one_and_delete({'court_fips': court_fips}, {'_id': False})
    if task is None:
        task = collection.find_one_and_delete({}, {'_id': False})
    if task is not None:
        col = get_completed_task_collection(task)
        task['_id'] = col.insert_one(task).inserted_id
    return task

def get_previous_task(task):
    col = get_completed_task_collection(task)
    return col.find_one({
        'court_fips': task['court_fips'],
        'court_type': task['court_type'],
        'case_type': task['case_type'],
        'term': task['term'],
        'completed': {'$exists': True}
    })

def previous_search_needs_details(task, prev_task):
    return task['case_details'] == True and \
           'completed_details' not in prev_task and \
           prev_task['cases_found'] > 0

def get_previous_search_cases(task):
    return db.cases.find({
        'fips_code': task['court_fips'],
        'court_type': task['court_type'],
        'case_type': task['case_type'],
        'search_term': task['term']
    })

def get_case_details(task, cases, update_db=False):
    for case in cases:
        log.info('Getting details ' + case['case_number'])
        case['details'] = court_reader \
            .get_case_details_by_number(task['court_fips'], \
                                        task['case_type'], \
                                        case['case_number'])
        if update_db == True:
            db.cases.find_one_and_update({'_id': case['_id']}, {
                '$set': {
                    'details': case['details'],
                    'details_fetched': datetime.utcnow()
                }
            })
        time.sleep(1)
    if update_db == True:
        col = get_completed_task_collection(task)
        col.find_one_and_update({'_id': task['_id']}, {
            '$set': {'completed_details': datetime.utcnow()}
        })

while True:
    task = get_next_task(current_court_fips)
    if task is not None:
        log.info(task)

        # We've got a task to do, make sure the reader is ready
        if court_reader is None:
            court_reader = get_court_reader()

        # Set the court we are working on, the next time we get a task
        # we'll try to stay in the same court
        current_court_fips = task['court_fips']

        # Check to see if task has already been done
        previous_task = get_previous_task(task)
        if previous_task is not None:
            log.info('Previous search found')
            task['previous_search'] = previous_task['search_id']
            # If we didn't need case details the first time, get them now
            if previous_search_needs_details(task, previous_task):
                cases = get_previous_search_cases(previous_task)
                get_case_details(task, cases, True)
        else:
            cases = court_reader.get_cases_by_name(task['court_fips'], \
                                                   task['case_type'], \
                                                   task['term'])
            if len(cases) > 0:
                for case in cases:
                    case['fips_code'] = task['court_fips']
                    case['court_type'] = task['court_type']
                    case['case_type'] = task['case_type']
                    case['search_term'] = task['term']
                    case['search_id'] = task['search_id']
                    case['fetched'] = datetime.utcnow()
                    if task['case_details'] == True:
                        print 'Getting details', case['case_number']
                        case['details'] = court_reader \
                            .get_case_details_by_number(task['court_fips'], \
                                                        task['case_type'], \
                                                        case['case_number'])
                        case['details_fetched'] = datetime.utcnow()
                        time.sleep(1)
                db.cases.insert_many(cases)
            task['completed'] = datetime.utcnow()
            if task['case_details'] == True:
                task['completed_details'] = datetime.utcnow()
            task['cases_found'] = len(cases)
            log_msg = 'Found ' + str(len(cases)) + ' cases'
            log.info(log_msg)
        col = get_completed_task_collection(task)
        col.find_one_and_replace({'_id': task['_id']}, task)
    elif court_reader is not None:
        court_reader.log_off()
        court_reader = None
        log.info('Worker disconnected from court site')
    time.sleep(1)
