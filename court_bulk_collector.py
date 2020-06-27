from courtreader import readers
from courtutils.logger import get_logger
from datetime import datetime, timedelta
from time import sleep
import os
import sys
import time
import traceback

MONGO = False
POSTGRES = True

if MONGO:
    import pymongo
    from courtutils.databases.mongo import MongoDatabase
if POSTGRES:
    from courtutils.databases.postgres import PostgresDatabase

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
        case['collected'] = datetime.now()

        # If the hearing is in the future, add to the docket table - don't get details
        if date > datetime.now().date():
            log.info('Docket %s %s', case['case_number'], case['defendant'])
            case['CaseNumber'] = case['case_number']
            case['Defendant'] = case['defendant']
            if case_type == 'civil':
                case['CaseType'] = case['civil_case_type']
                case['Plaintiff'] = case['plaintiff']
            db.add_case_to_docket(case, case_type)
            continue
        
        case_details = db.get_more_recent_case_details(case, case_type, date)
        if case_details != None:
            last_date = case_details['details_fetched_for_hearing_date'].strftime('%m/%d/%Y')
            collected_date = case_details['collected'].strftime('%m/%d/%Y')
            if case_details['details_fetched_for_hearing_date'] < case_details['collected']:
                log.info('%s details collected for hearing on %s', case['case_number'], last_date)
                continue
            else:
                log.info('%s details were collected on %s before hearing date on %s - updating now', case['case_number'], collected_date, last_date)
        if '--' in case['case_number']:
            if case_type == 'civil':
                case['details'] = {
                    'CaseNumber': case['case_number']
                }
            elif 'defendant' in case:
                case['details'] = {
                    'CaseNumber': case['case_number'],
                    'Defendant': case['defendant']
                }
        else:
            if len(case['case_number']) < 13:
                log.warn('[%s] is an invalid case number', case['case_number'])
                continue
            case['details'] = reader.get_case_details_by_number(
                fips, case_type, case['case_number'],
                case['details_url'] if 'details_url' in case else None)
        if 'error' in case['details']:
            log.warn('Could not collect case details for %s in %s',
                     case['case_number'], case['fips'])
        else:
            log.info('%s %s', case['case_number'], case['defendant'])
            db.replace_case_details(case, case_type)

def run_collector(reader, last_task):
    db = get_db_connection()

    task = db.get_and_delete_date_task(last_task)
    if task is None:
        log.info('Nothing to do. Sleeping for 30 seconds.')
        sleep(30)
        return

    try:
        reader_connected = False

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
                if not reader_connected:
                    reader.connect()
                    reader_connected = True
                get_cases_on_date(db, reader, fips, case_type, date, date_str)
                db.add_date_search(date_search)
            date += timedelta(days=-1)

        if reader_connected:
            reader.log_off()
    except Exception, err:
        log.error(traceback.format_exc())
        log.warn('Putting task back')
        db.rollback()
        db.add_date_task(task, True)
        db.disconnect()
        try:
            reader.log_off()
        except:
            pass
        raise
    except KeyboardInterrupt:
        log.warn('Putting task back')
        db.rollback()
        db.add_date_task(task, True)
        db.disconnect()
        try:
            reader.log_off()
        except:
            pass
        raise

    db.disconnect()
    return task

def get_reader():
    return readers.CircuitCourtReader() if 'circuit' in COURT_TYPE else \
            readers.DistrictCourtReader()

def run():
    reader = None
    finished_task = None
    while True:
        try:
            if reader is None:
                reader = get_reader()
            finished_task = run_collector(reader, finished_task)
        except Exception, err:
            try:
                reader.log_off()
            except:
                pass
            reader = None
            log.error(traceback.format_exc())
            log.info('Unexpect error. Sleeping for 10 minute')
            sleep(600)
run()
