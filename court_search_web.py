from courtreader import readers
from courtutils.courtlogger import get_logger
import datetime
import pymongo
import os

# configure logging
log = get_logger()
log.info('Web running')

# Connect to database
client = pymongo.MongoClient(os.environ['MONGO_DB'])
db = client['temp-15-11-08']

# Connect to District Court Reader
reader = readers.CircuitCourtReader()
courts = reader.connect()
log.info('Web connected to court site')

for court in courts:
    db.tasks.insert_one({'type': 'name', 'court_fips': court, 'term': 'GREENE'})
