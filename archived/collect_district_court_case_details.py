import boto.utils
import datetime
import pymongo
import os
import random
import sys
import time
import uuid
from courtreader import readers

# Connect to database
client = pymongo.MongoClient(os.environ['DISTRICT_DB'])
db = client.va_district_court_cases

# Connect to District Court Reader
reader = readers.DistrictCourtReader()
courts = reader.connect()

def scraper_count(fips_code):
    ten_minutes_ago = datetime.datetime.utcnow() + datetime.timedelta(minutes=-10)
    return db.scrapers.count({
        'fips_code':  fips_code,
        'last_update': {'$gt': ten_minutes_ago}
    })

def get_random_fips_code():
    all_fips_codes = set(courts.keys())
    completed_fips_codes = set([x['fips_code'] for x in db['completed_courts'].find(projection={'fips_code': True})])
    remaining_fips_codes = list(all_fips_codes - completed_fips_codes)
    print str(len(remaining_fips_codes)), 'Courts Remaining'
    if len(remaining_fips_codes) == 0:
        return None
    return random.choice(list(remaining_fips_codes))

def collect_cases(fips_code):
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

def update_scraper_record(fips_code):
    db.scrapers.update_one({'process_id': process_id}, {
        '$set': {
            'fips_code':  fips_code,
            'last_update':  datetime.datetime.utcnow()
        }
    })

while True:
    try:
        fips_code = None
        while fips_code is None:
            fips_code = get_random_fips_code()
            if fips_code is None:
                db.scrapers.remove({'process_id': process_id})
                exit()
            print 'Checking FIPS Code', fips_code

            # see if its already being worked on
            if scraper_count(fips_code) > 0:
                fips_code = None
            else:
                # announce that we are going to work on it and wait
                update_scraper_record(fips_code)
                time.sleep(3)
                # make sure we are the only one that annouced our intent to work on this
                if scraper_count(fips_code) > 1:
                    fips_code = None
                    update_scraper_record(fips_code)

            # if we fail at any point, wait a random amount of time before trying again
            if fips_code is None:
                sleep_time = random.randint(10, 100)
                print 'Sleeping for', str(sleep_time)
                time.sleep(sleep_time)

        print 'Processing FIPS Code', fips_code
        collect_cases(fips_code)
    except Exception:
        print "Unexpected error:", sys.exc_info()[0]
        time.sleep(30)
