from courtreader import readers
from courtutils.databases.mongo import MongoDatabase
from courtutils.logger import get_logger
from datetime import datetime, timedelta
import csv
import pymongo
import os
import sys
import time

# get command line args
start_date = datetime.strptime(sys.argv[1],'%m/%d/%Y')
end_date = datetime.strptime(sys.argv[2],'%m/%d/%Y')
if start_date < end_date:
    raise ValueError('Start Date must be after End Date so they decend')

court_type = sys.argv[3]
if court_type != 'circuit' and court_type != 'district':
    raise ValueError('Unknown court type')

# connect to database
db = MongoDatabase('court-test-314', court_type)

# get the courts to create tasks for
# check command line args for a specific court
courts = list(db.get_courts())
if len(sys.argv) > 4:
    courts = [court for court in courts if court['fips_code'] == sys.argv[4]]

# create the tasks
tasks = []
for court in courts:
    tasks.append({
        'court_fips': court['fips_code'],
        'start_date': start_date,
        'end_date': end_date
    })

# add the tasks to the database
db.add_date_tasks(tasks)
print 'Created', len(tasks), 'tasks'
