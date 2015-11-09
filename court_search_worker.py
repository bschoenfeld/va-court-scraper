from courtreader import readers
from courtutils.courtlogger import get_logger
import datetime
import pymongo
import os
import time

# configure logging
log = get_logger()
log.info('Worker running')

# Connect to database
client = pymongo.MongoClient(os.environ['MONGO_DB'])
db = client['temp-15-11-08']

# Connect to District Court Reader
reader = readers.CircuitCourtReader()
reader.connect()
log.info('Worker connected to court site')

# Fill in cases
while True:
    task = db.tasks.find_one_and_delete({})
    if task is not None:
        log.info(task)
        cases = reader.get_cases_by_name(task['court_fips'], task['term'])
        if len(cases) > 0:
            db.searches.insert_many(cases)
        log.info('Found ' + str(len(cases)) + ' cases')
    time.sleep(2)
