import os
import pymongo
from geopy.geocoders import GoogleV3
from courtreader import readers

MONGO = False
POSTGRES = True

if MONGO: from courtutils.databases.mongo import MongoDatabase
if POSTGRES: from courtutils.databases.postgres import PostgresDatabase

geolocator = GoogleV3(api_key=os.environ['GOOGLE_API_KEY'])

print 'CIRCUIT COURT'
circuit_db = None
if MONGO: circuit_db = MongoDatabase('va_court_search', 'circuit')
if POSTGRES: circuit_db = PostgresDatabase('circuit')
circuit_db.drop_courts()
reader = readers.CircuitCourtReader()
courts = reader.connect()
court_names = []
for fips, court in courts.iteritems():
    print court['name']
    court_locality = court['name'].replace(' Circuit Court', '')
    location = geolocator.geocode(court_locality + ', Virginia, USA')
    circuit_db.add_court(court['name'], fips, location)
    court_names.append(court['name'] + ' ' + fips)
circuit_db.add_court_location_index()
circuit_db.commit()

'''
court_names.sort()
for court_name in court_names:
    print court_name
'''

print 'DISTRICT COURT'
district_db = None
if MONGO: district_db = MongoDatabase('va_court_search', 'district')
if POSTGRES: district_db = PostgresDatabase('district')
district_db.drop_courts()
reader = readers.DistrictCourtReader()
courts = reader.connect()
court_names = []
for fips, court in courts.iteritems():
    print court
    court_locality = court.replace(' General District Court', '')\
                          .replace('-Criminal', '')\
                          .replace('-Civil', '')\
                          .replace('-Traffic', '')
    location = geolocator.geocode(court_locality + ', Virginia, USA')
    district_db.add_court(court, fips, location)
    court_names.append(court + ' ' + fips)
district_db.add_court_location_index()
district_db.commit()

'''
court_names.sort()
for court_name in court_names:
    print court_name
'''
