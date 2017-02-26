import os
import pymongo
import sys

cases = []
file_path = sys.argv[1]
with open(file_path, 'r') as data_file:
    for line in data_file:
        case_data = line.split()
        case = {
            'FIPSCode': case_data[0],
            'CaseNumber': case_data[1]
        }
        if not case['FIPSCode'].isdigit(): continue
        cases.append(case)
print len(cases)

client = pymongo.MongoClient(os.environ['MONGO_URI'])
db = client.va_district_court_cases
db.cases.insert_many(cases)
