from __future__ import absolute_import
from courtreader import readers
from datetime import datetime, timedelta, date
from time import sleep
import os
import sys
import time
import traceback

from courtutils.databases.postgres import PostgresDatabase

# configure logging
print('Worker running')

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
            print('Updating %s %s' % (fips, case_number[0]))
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
                print("Error updating case")
                print(traceback.format_exc())
                continue

    print('Disconnecting')

    db.disconnect()
    if reader is not None:
        reader.log_off()

run()
