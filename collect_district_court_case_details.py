import boto.utils
import datetime
import pymongo
import os
import sys
import time
import uuid
from courtreader import readers

# Connect to database
client = pymongo.MongoClient(os.environ['DISTRICT_DB'])
db = client.va_district_court_cases

# Connect to District Court Reader
reader = readers.DistrictCourtReader()
reader.connect()

# get some info about this process
process_id = str(uuid.uuid4())
cwd = os.getcwd()
ec2_id = None
try:
    ec2_id = boto.utils.get_instance_metadata(timeout=1, num_retries=1)['instance-id']
except:
    pass

# create db record for this process
db.scrapers.insert_one({
    'process_id': process_id,
    'cwd': cwd,
    'ec2_id': ec2_id
})

fips_code = sys.argv[1]

# Fill in cases
while True:
    case = db.cases.find_one({
        'FIPSCode': fips_code,
        'date_collected': {'$exists': False}
    })
    if case is None: break
    print case['CaseNumber']
    case_details = reader.get_case_details_by_number( \
        case['FIPSCode'], case['CaseNumber'])
    case_details['date_collected'] = datetime.datetime.utcnow()
    updated_case = dict(case.items() + case_details.items())
    db.cases.replace_one({'_id': case['_id']}, updated_case)
    db.scrapers.update_one({'process_id': process_id}, {
        '$set': {
            'fips_code':  fips_code,
            'last_update':  datetime.datetime.utcnow()
        }
    })
    time.sleep(2)
db.scrapers.remove({'process_id': process_id})
db['completed_courts'].replace_one({'fips_code': fips_code}, {
    'fips_code': fips_code,
    'completed_time': datetime.datetime.utcnow()
}, upsert=True)
print 'Finished'
