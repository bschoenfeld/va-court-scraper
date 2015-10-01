import os
import pymongo
from courtreader import readers

# Connect to database
district_db_client = pymongo.MongoClient(os.environ['DISTRICT_DB'])
district_db = district_db_client.va_district_court_cases
circuit_db_client = pymongo.MongoClient(os.environ['CIRCUIT_DB'])
circuit_db = circuit_db_client.va_circuit_court_cases

print 'CIRCUIT COURT'
circuit_db.courts.drop()
reader = readers.CircuitCourtReader()
courts = reader.connect()
court_names = []
for fips_code, court in courts.iteritems():
    circuit_db.courts.insert_one({
        'name': court['name'],
        'fips_code': fips_code
    })
    court_names.append(court['name'] + ' ' + fips_code)
court_names.sort()
for court_name in court_names:
    print court_name

print 'DISTRICT COURT'
district_db.courts.drop()
reader = readers.DistrictCourtReader()
courts = reader.connect()
court_names = []
for fips_code, court in courts.iteritems():
    district_db.courts.insert_one({
        'name': court,
        'fips_code': fips_code
    })
    court_names.append(court + ' ' + fips_code)
court_names.sort()
for court_name in court_names:
    print court_name
