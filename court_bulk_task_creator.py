from courtreader import readers
from courtutils.logger import get_logger
from datetime import datetime, timedelta
import csv
import os
import sys
import time

MONGO = False
POSTGRES = True

if MONGO:
    import pymongo
    from courtutils.databases.mongo import MongoDatabase
if POSTGRES:
    from courtutils.databases.postgres import PostgresDatabase

# get command line args
start_date = datetime.strptime(sys.argv[1], '%m/%d/%Y')
end_date = datetime.strptime(sys.argv[2], '%m/%d/%Y')
if start_date < end_date:
    raise ValueError('Start Date must be after End Date so they decend')

court_type = sys.argv[3]
if court_type != 'circuit' and court_type != 'district':
    raise ValueError('Unknown court type')

case_type = sys.argv[4]
if case_type != 'criminal' and case_type != 'civil':
    raise ValueError('Unknown case type')

# connect to database
db = None
if MONGO: db = MongoDatabase('va_court_search', court_type)
if POSTGRES: db = PostgresDatabase(court_type)

# get the courts to create tasks for
# check command line args for a specific court
courts = list(db.get_courts())
if len(sys.argv) > 5:
    courts = [court for court in courts if court['fips'] == sys.argv[5]]

# create the tasks
tasks = []
for court in courts:
    tasks.append({
        'fips': court['fips'],
        'start_date': start_date,
        'end_date': end_date,
        'case_type': case_type
    })

# add the tasks to the database
db.add_date_tasks(tasks)
print 'Created', len(tasks), 'tasks'
