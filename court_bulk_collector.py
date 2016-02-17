from courtreader import readers
from courtutils.logger import get_logger
from datetime import datetime, timedelta
import pymongo
import os
import sys
import time

# configure logging
log = get_logger()
log.info('Worker running')

def get_db_connection():
    return pymongo.MongoClient(os.environ['MONGO_DB'])['va_court_search']

# Fill in cases
court_reader = None
current_court_fips = None
db = get_db_connection()

court_fips = '013'
case_type = 'R'
year = 2015

reader = readers.CircuitCourtReader()
reader.connect()

date = datetime(year, 12, 31)
while date.year == year:
    dateStr = date.strftime('%m/%d/%Y')
    log.info('Getting cases on ' + dateStr)
    cases = reader.get_cases_by_date(court_fips, case_type, dateStr)
    for case in cases:
        case['details'] = reader.get_case_details_by_number( \
                            court_fips, \
                            case_type, \
                            case['case_number'])
        case['details_fetched'] = datetime.utcnow()
        print case['case_number'], case['defendant'], case['details']['Filed']
        break
    date += timedelta(days=-1)

reader.log_off()
