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

def get_court_reader():
    reader = None
    if court == 'c': reader = readers.CircuitCourtReader()
    if court == 'd': reader = readers.DistrictCourtReader()
    reader.connect()
    log.info('Worker connected to court')
    return reader

def get_next_task(db, court_fips):
    collection = db.circuit_court_tasks if court == 'c' \
                    else db.district_court_tasks
    task = None
    if court_fips is not None:
        print 'Looking for task in', court_fips
        task = collection.find_one_and_delete({'court_fips': court_fips})
    if task is not None:
        return task
    return collection.find_one_and_delete({})

# Fill in cases
court_reader = None
current_court_fips = None
db = get_db_connection()
while True:
    task = get_next_task(db, current_court_fips)
    if task is not None:
        if court_reader is None:
            court_reader = get_court_reader()
        current_court_fips = task['court_fips']
        if court == 'c': task['court_type'] = 'circuit'
        if court == 'd': task['court_type'] = 'district'
        completed_task_col = task['court_type'] + '_court_completed_tasks'
        log.info(task)
        print 'SEARCH', task['term'], task['court_fips']
        previously_completed_task = db[completed_task_col].find_one({
            'court_fips': task['court_fips'],
            'court_type': task['court_type'],
            'case_type': task['case_type'],
            'term': task['term'],
            'completed': {'$exists': True}
        })
        if previously_completed_task is not None:
            log.info('PREV SEARCH FOUND')
            print 'PREV SEARCH FOUND'
            task['previous_search'] = previously_completed_task['search_id']
            if 'completed_details' not in previously_completed_task:
                cases = db.cases.find({
                    'fips_code': task['court_fips'],
                    'court_type': task['court_type'],
                    'case_type': task['case_type'],
                    'search_term': task['term'],
                    'details': {'$exists': False}
                })
                for case in cases:
                    print 'Getting details', case['case_number']
                    case_details = court_reader \
                        .get_case_details_by_number(task['court_fips'], \
                                                    task['case_type'], \
                                                    case['case_number'])
                    db.cases.find_one_and_update({
                        '_id': case['_id']
                    }, {
                        '$set': {
                            'details': case_details,
                            'details_fetched': datetime.utcnow()
                        }
                    })
                db[completed_task_col].find_one_and_update({
                    '_id': previously_completed_task['_id']
                }, {
                    '$set': {'completed_details': datetime.utcnow()}
                })
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
                db.cases.insert_many(cases)
            task['completed'] = datetime.utcnow()
            if task['case_details'] == True:
                task['completed_details'] = datetime.utcnow()
            task['cases_found'] = len(cases)
            log_msg = 'Found ' + str(len(cases)) + ' cases'
            log.info(log_msg)
            print log_msg
        db[completed_task_col].insert_one(task)
    elif court_reader is not None:
        court_reader.log_off()
        court_reader = None
        log.info('Worker disconnected from court site')
    time.sleep(1)
