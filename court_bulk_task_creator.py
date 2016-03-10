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

def get_courts(court_type):
    if court_type == 'circuit':
        return list(Database.get_circuit_courts())
    else:
        return list(Database.get_district_courts())

def add_tasks(tasks, court_type):
    db = get_db_connection()
    if court_type == 'circuit':
        db.circuit_court_date_tasks.insert_many(tasks)
    else:
        db.district_court_date_tasks.insert_many(tasks)
    print 'Created', len(tasks), 'tasks'

start_date = datetime.strptime(sys.argv[1],'%m/%d/%Y')
end_date = datetime.strptime(sys.argv[2],'%m/%d/%Y')
if start_date < end_date:
    raise ValueError('Start Date must be after End Date so they decend')

court_type = sys.argv[3]
if court_type != 'circuit' and court_type != 'district':
    raise ValueError('Unknown court type')

courts = get_courts(court_type)
if len(sys.argv) > 4:
    courts = [court for court in courts if court['fips_code'] == sys.argv[4]]
tasks = []
for court in courts:
    tasks.append({
        'court_fips': court['fips_code'],
        'start_date': start_date,
        'end_date': end_date
    })

add_tasks(tasks, court_type)
