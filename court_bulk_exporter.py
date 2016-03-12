from courtreader import readers
from courtutils.database import Database
from courtutils.logger import get_logger
from datetime import datetime, timedelta
import boto3
import csv
import json
import pymongo
import os
import sys
import time
import zipfile

fieldnames = [
    'court_fips',
    'Court',
    'CaseNumber',
    'Locality',
    'Commencedby',
    'Filed',
    'Defendant',
    'AKA',
    'AKA2',
    'DOB',
    'Sex',
    'Race',
    'Address',
    'OffenseDate',
    'ArrestDate',
    'Class',
    'ConcludedBy',
    'Charge',
    'ChargeType',
    'AmendedCharge',
    'AmendedChargeType',
    'CodeSection',
    'AmendedCodeSection',
    'DispositionCode',
    'DispositionDate',
    'LifeDeath',
    'SentenceTime',
    'SentenceSuspended',
    'ConcurrentConsecutive',
    'JailPenitentiary',
    'ProbationTime',
    'ProbationType',
    'ProbationStarts',
    'RestitutionAmount',
    'RestitutionPaid',
    'FineAmount',
    'Costs',
    'FinesCostPaid',
    'TrafficFatality',
    'DriverImprovementClinic',
    'CourtDMVSurrender',
    'OperatorLicenseSuspensionTime',
    'DrivingRestrictions',
    'RestrictionStartDate',
    'RestrictionEndDate',
    'VAAlcoholSafetyAction',
    'ProgramType',
    'Military'
]

excluded_fields = [
    '_id', 'details', 'details_fetched', 'case_number',
    'details_fetched_for_hearing_date', 'Hearings', 'defendant'
]

def get_db_connection():
    return pymongo.MongoClient(os.environ['MONGO_DB'])['va_court_search']

courts = list(Database.get_circuit_courts())
courts_by_fips = {court['fips_code']:court for court in courts}

s3 = boto3.resource('s3')
db = get_db_connection()

def write_cases_to_file(cases, filename, metadata):
    metadata['total'] = 0
    metadata['byCourt'] = {}
    with open('./' + filename + '.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldnames))
        writer.writeheader()
        for case in cases:
            if 'error' in case['details']:
                print 'Error getting case details', case['case_number']
                continue
            case['Court'] = courts_by_fips[case['court_fips']]['name']
            if case['Court'] not in metadata['byCourt']:
                metadata['byCourt'][case['Court']] = 0
            metadata['byCourt'][case['Court']] += 1
            metadata['total'] += 1
            for detail in case['details']:
                new_key = detail.replace(' ', '')
                case[new_key] = case['details'][detail]
            for field in excluded_fields:
                if field in case:
                    del case[field]
            writer.writerow(case)

def upload_cases(filename, metadata):
    with open('./' + filename + '_metadata.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['Court', 'Cases'])
        writer.writeheader()
        for court in sorted(metadata['byCourt'].keys()):
            writer.writerow({
                'Court': court,
                'Cases': metadata['byCourt'][court]
            })

    zipped_file = zipfile.ZipFile(filename + '.zip', 'w')
    zipped_file.write(filename + '.csv', filename + '.csv', zipfile.ZIP_DEFLATED)
    zipped_file.write(filename + '_metadata.csv', filename + '_metadata.csv', zipfile.ZIP_DEFLATED)
    zipped_file.close()

    metadata['filesize'] = os.path.getsize(filename + '.zip')

    s3.Object('virginia-court-data', filename + '.zip')\
      .put(Body=open(filename + '.zip', 'rb'), ACL='public-read', ContentType='application/zip')

    os.remove(filename + '.zip')
    os.remove(filename + '.csv')
    os.remove(filename + '_metadata.csv')

def export_and_upload_data_by_court(court_fips):
    print courts_by_fips[court_fips]['name']

    cases = db.circuit_court_detailed_cases.find({
        'court_fips': court_fips
    })

    filename = 'criminal_circuit_court_cases_' + court_fips
    write_cases_to_file(cases, filename)
    upload_cases(filename)

def export_and_upload_data_by_year(year):
    start = datetime(year, 1, 1)
    end = datetime(year + 1, 1, 1)
    print 'From', start, 'to', end

    cases = db.circuit_court_detailed_cases.find({
        'details_fetched_for_hearing_date': {'$gte': start, '$lt': end}
    })

    filename = 'criminal_circuit_court_cases_' + str(year)
    metadata = {}
    write_cases_to_file(cases, filename, metadata)
    upload_cases(filename, metadata)
    return metadata

if len(sys.argv) > 2:
    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    complete_metadata = []
    for year in range(start_year, end_year+1):
        print 'Export', year
        metadata = export_and_upload_data_by_year(year)
        complete_metadata.append({
            'year': year,
            'downloadLink': 'https://s3.amazonaws.com/virginia-court-data/criminal_circuit_court_cases_' + str(year) + '.zip',
            'cases': metadata['total'],
            'filesize': metadata['filesize'],
            'byCourt': metadata['byCourt']
        })
    with open('./criminalCircuitCourtCaseExport.js', 'w') as jsonfile:
        jsonfile.write('var virginiaCourtCaseDataSets = ')
        jsonfile.write(json.dumps(complete_metadata))
else:
    for court in courts:
        export_and_upload_data_by_court(court['fips_code'])
