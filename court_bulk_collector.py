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

def get_db_connection():
    return pymongo.MongoClient(os.environ['MONGO_DB'])['va_court_search']

# Fill in cases
court_reader = None
current_court_fips = None
db = get_db_connection()

reader = readers.CircuitCourtReader()
reader.connect()
cases = reader.get_cases_by_date('013', 'R', '02/16/2016')
for case in cases:
    print case['case_number'], case['name']

reader.log_off()
