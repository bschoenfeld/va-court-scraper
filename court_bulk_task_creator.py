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

start_date = datetime.strptime(sys.argv[1],'%m/%d/%Y')
end_date = datetime.strptime(sys.argv[2],'%m/%d/%Y')
if start_date < end_date:
    raise ValueError('Start Date must be after End Date so they decend')

courts = list(Database.get_circuit_courts())
tasks = []
for court in courts:
    tasks.append({
        'court_fips': court['fips_code'],
        'start_date': start_date,
        'end_date': end_date
    })

db = get_db_connection()
db.circuit_court_date_tasks.insert_many(tasks)
