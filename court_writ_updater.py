from courtreader import readers
from courtutils.logger import get_logger
from datetime import datetime, timedelta, date
from time import sleep
import os
import sys
import time
import traceback

from courtutils.databases.postgres import PostgresDatabase

# configure logging
log = get_logger()
log.info('Worker running')

fips = sys.argv[1]
stop_at_fips = int(fips) + 100

def next_fips(all_fips, cur_fips):
    for f in all_fips:
        if int(f) >= stop_at_fips:
            return None
        if int(f) > int(cur_fips):
            return f
    return None

def update_case(reader, db, cur_fips, case):
    log.info('%s %s WritIssued:%s Fetched:%s Collected:%s', 
        case['fips'], case['case_number'], case['WritIssuedDate'], 
        case['details_fetched_for_hearing_date'], case['collected']
    )

    case['details'] = reader.get_case_details_by_number(cur_fips, 'civil', case['case_number'], None)
    if 'error' in case['details']:
        log.warn('Could not collect case details')
        return

    case['collected'] = datetime.now()

    if 'WritIssuedDate' in case['details']:
        print case['details']['WritIssuedDate']
    else:
        print 'No Writ Issued'

    db.replace_case_details(case, 'civil')

def run(cur_fips):
    reader = None

    db = PostgresDatabase('district')
    courts = db.get_courts()
    all_fips = sorted([c['fips'] for c in courts])

    while True:
        cur_fips = next_fips(all_fips, cur_fips)
        if cur_fips is None:
            break

        log.info('FIPS %s', cur_fips)

        while True:
            case = db.get_next_unlawful_detainer_to_update(cur_fips, date(2019, 12, 1))
            if case is None:
                break

            if reader is None:
                reader = readers.DistrictCourtReader()
                reader.connect()

            update_case(reader, db, cur_fips, case)
            time.sleep(0.25)
         
    db.disconnect()
    if reader is not None:
        reader.log_off()

run(fips)
