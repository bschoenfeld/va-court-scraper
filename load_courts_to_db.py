import os
import pymongo
from courtreader import readers

# Connect to database
db = pymongo.MongoClient(os.environ['MONGO_DB'])['va_court_search']

print 'CIRCUIT COURT'
db.circuit_courts.drop()
reader = readers.CircuitCourtReader()
courts = reader.connect()
court_names = []
for fips_code, court in courts.iteritems():
    db.circuit_courts.insert_one({
        'name': court['name'],
        'fips_code': fips_code
    })
    court_names.append(court['name'] + ' ' + fips_code)
court_names.sort()
for court_name in court_names:
    print court_name

print 'DISTRICT COURT'
db.district_courts.drop()
reader = readers.DistrictCourtReader()
courts = reader.connect()
court_names = []
for fips_code, court in courts.iteritems():
    db.district_courts.insert_one({
        'name': court,
        'fips_code': fips_code
    })
    court_names.append(court + ' ' + fips_code)
court_names.sort()
for court_name in court_names:
    print court_name
