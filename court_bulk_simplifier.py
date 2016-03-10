from courtreader import readers
from courtutils.database import Database
from courtutils.logger import get_logger
from datetime import datetime, timedelta
import csv
import pymongo
import os
import sys
import time

def get_db_connection():
    return pymongo.MongoClient(os.environ['MONGO_DB'])['va_court_search']

db = get_db_connection()
cases = db.circuit_court_detailed_cases.find({})
for case in cases:
    print case['court_fips'], case['case_number']
    needs_replacing = False
    keys = case['details'].keys()
    for key in keys:
        if not case['details'][key]:
            del case['details'][key]
            needs_replacing = True
    if needs_replacing:
        print 'Replacing'
        db.circuit_court_detailed_cases.find_one_and_replace({
            'court_fips': case['court_fips'],
            'case_number': case['case_number']
        }, case)
