from courtreader import readers
from courtutils.logger import get_logger
from datetime import datetime, timedelta
from time import sleep
import pymongo
import os
import sys
import time
import traceback

MONGO = False
POSTGRES = True

if MONGO: from courtutils.databases.mongo import MongoDatabase
if POSTGRES: from courtutils.databases.postgres import PostgresDatabase

# configure logging
log = get_logger()
log.info('Worker running')

COURT_TYPE = sys.argv[1]
if COURT_TYPE != 'circuit' and COURT_TYPE != 'district':
    raise ValueError('Unknown court type')

def get_db_connection():
    if MONGO:
        return MongoDatabase('va_court_search', COURT_TYPE)
    if POSTGRES:
        return PostgresDatabase(COURT_TYPE)
    return None

def get_cases_on_date(db, reader, fips, case_type, date, dateStr):
    log.info('Getting cases on ' + dateStr)
    sleep(1)
    cases = reader.get_cases_by_date(fips, case_type, dateStr)
    for case in cases:
        case['details_fetched_for_hearing_date'] = date
        case['fips'] = fips
        case_details = db.get_more_recent_case_details(case, case_type, date)
        if case_details != None:
            last_date = case_details['details_fetched_for_hearing_date'].strftime('%m/%d/%Y')
            log.info(case['case_number'] + ' details collected for hearing on ' + last_date)
            continue
        case['details'] = reader.get_case_details_by_number( \
                            fips, \
                            case_type, \
                            case['case_number'],
                            case['details_url'] if 'details_url' in case else None)
        case['details_fetched'] = datetime.utcnow()
        if 'error' in case['details']:
            log.warn('Could not collect case details for ' + \
                case['case_number'] + ' in ' + case['fips'])
        else:
            log.info(case['case_number'] + ' ' + \
                        case['defendant'])
            keys = case['details'].keys()
            for key in keys:
                if not case['details'][key]:
                    del case['details'][key]
            if 'details_url' in case:
                del case['details_url']
        db.replace_case_details(case, case_type)

def run_collector(reader):
    db = get_db_connection()

    task = db.get_and_delete_date_task()
    if task is None:
        log.info('Nothing to do. Sleeping for 30 seconds.')
        sleep(30)
        return

    try:
        reader.connect()

        fips = task['fips']
        start_date = task['start_date']
        end_date = task['end_date']
        case_type = task['case_type']

        log.info('Start %s %s %s-%s',
                 fips,
                 case_type,
                 start_date.strftime('%m/%d/%Y'),
                 end_date.strftime('%m/%d/%Y'))
        date = start_date

        while date >= end_date:
            date_search = {
                'fips': fips,
                'case_type': case_type,
                'date': date
            }
            date_str = date.strftime('%m/%d/%Y')
            if db.get_date_search(date_search) != None:
                log.info(date_str + ' already searched')
            else:
                get_cases_on_date(db, reader, fips, case_type, date, date_str)
                db.add_date_search(date_search)
            date += timedelta(days=-1)

        reader.log_off()
    except Exception, err:
        log.error(traceback.format_exc())
        log.warn('Putting task back')
        db.rollback()
        db.add_date_task(task)
        db.disconnect()
        raise
    except KeyboardInterrupt:
        log.warn('Putting task back')
        db.rollback()
        db.add_date_task(task)
        db.disconnect()
        raise

    db.disconnect()

def get_reader():
    return readers.CircuitCourtReader() if 'circuit' in COURT_TYPE else \
            readers.DistrictCourtReader()

def run():
    reader = None
    while True:
        try:
            if reader is None:
                reader = get_reader()
            run_collector(reader)
        except Exception, err:
            try:
                reader.log_off()
            except:
                pass
            reader = None
            log.error(traceback.format_exc())
            log.info('Unexpect error. Sleeping for 1 minute')
            sleep(60)
        log.info('Sleeping for 10 seconds')
        sleep(10)

run()
