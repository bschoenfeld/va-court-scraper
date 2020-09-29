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

start_date = datetime.strptime(sys.argv[1], '%m/%d/%Y')
end_date = datetime.strptime(sys.argv[2], '%m/%d/%Y')

def run():
    reader = None

    db = PostgresDatabase('district')
    courts = db.get_courts()
    all_fips = sorted([c['fips'] for c in courts])

    if reader is None:
        reader = readers.DistrictCourtReader()
        reader.connect()

    for fips in all_fips:
        case_numbers = db.get_hearings_without_result(fips, start_date, end_date)
        for case_number in case_numbers:
            sleep(1)
            log.info('Updating %s %s', fips, case_number[0])
            case = {
                'fips': fips,
                'collected': datetime.now(),
                'case_number': case_number[0],
                'details_fetched_for_hearing_date': case_number[1]
            }
            try:
                case['details'] = reader.get_case_details_by_number(
                    fips, 'civil', case_number[0], None)
                db.replace_case_details(case, 'civil')
            except:
                log.info("Error updating case")
                log.error(traceback.format_exc())
                continue

    log.info('Disconnecting')

    db.disconnect()
    if reader is not None:
        reader.log_off()

run()
