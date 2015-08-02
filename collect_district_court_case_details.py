import datetime
import pymongo
import os
from courtreader import readers

# Connect to database
client = pymongo.MongoClient(os.environ['MONGO_URI'])
db = client.va_district_court_cases

# Connect to District Court Reader
reader = readers.DistrictCourtReader()
reader.connect()

# Fill in cases
while True:
    case = db.cases.find_one({
        'FIPSCode': '013', \
        'date_collected': {'$exists': False} \
    })
    if case is None: break
    print case['CaseNumber']
    case_details = reader.get_case_details_by_number( \
        case['FIPSCode'], case['CaseNumber'])
    case_details['date_collected'] = datetime.datetime.utcnow()
    updated_case = dict(case.items() + case_details.items())
    db.cases.replace_one({'_id': case['_id']}, updated_case)
print 'Finished'
