import os
import pymongo

# Connect to database
circuit_db_client = pymongo.MongoClient(os.environ['CIRCUIT_DB'])
circuit_db = circuit_db_client.va_circuit_court_cases
district_db_client = pymongo.MongoClient(os.environ['DISTRICT_DB'])
district_db = district_db_client.va_district_court_cases

print 'DISTRICT COURT'
for court in district_db.courts.find():
    print court['name']
    court['total_count'] = district_db.cases.count({
        'FIPSCode': court['fips_code']
    })
    court['collected_count'] = district_db.cases.count({
        'FIPSCode': court['fips_code'],
        'date_collected': {'$exists': True}
    })
    district_db.courts.replace_one({
        '_id': court['_id']
    }, court)

print 'CIRCUIT COURT'
for court in circuit_db.courts.find():
    print court['name']
    court['total_count'] = circuit_db.cases.count({
        'FIPSCode': court['fips_code'],
        'CaseNumber': {'$regex': '^CR1[0-4].*'}
    })
    court['collected_count'] = circuit_db.cases.count({
        'FIPSCode': court['fips_code'],
        'CaseNumber': {'$regex': '^CR1[0-4].*'},
        'date_collected': {'$exists': True}
    })
    circuit_db.courts.replace_one({
        '_id': court['_id']
    }, court)
