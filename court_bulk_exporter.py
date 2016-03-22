from courtreader import readers
from courtutils.databases.mongo import MongoDatabase
from courtutils.logger import get_logger
from datetime import datetime, timedelta
import boto3
import csv
import hashlib
import json
import pymongo
import os
import sys
import time
import zipfile

fieldnames = {
    'circuit': [
        'court_fips',
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
        'SentenceTimeDays',
        'SentenceSuspendedDays',
        'ConcurrentConsecutive',
        'JailPenitentiary',
        'ProbationTimeDays',
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
        'OperatorLicenseSuspensionTimeDays',
        'DrivingRestrictions',
        'RestrictionStartDate',
        'RestrictionEndDate',
        'VAAlcoholSafetyAction',
        'ProgramType',
        'Military'
    ],
    'district': [
        "court_fips",
        "CaseNumber",
        "CaseStatus",
        "FiledDate",
        "Locality",
        "Name",
        "Status",
        "DefenseAttorney",
        "Address",
        "AKA1",
        "AKA2",
        "Gender",
        "Race",
        "DOB",
        "Charge",
        "CodeSection",
        "CaseType",
        "Class",
        "OffenseDate",
        "ArrestDate",
        "Complainant",
        "AmendedCharge",
        "AmendedCode",
        "AmendedCaseType",
        "FinalDisposition",
        "SentenceTimeDays",
        "SentenceSuspendedTimeDays",
        "ProbationType",
        "ProbationTimeDays",
        "ProbationStarts",
        "OperatorLicenseSuspensionTimeDays",
        "RestrictionStartDate",
        "RestrictionEndDate",
        "OperatorLicenseRestrictionCodes",
        "Fine",
        "Costs",
        "FineCostsDue",
        "FineCostsPaid",
        "FineCostsPaidDate",
        "VASAP"
    ]
}

excluded_fields = [
    '_id', 'details', 'details_fetched', 'case_number',
    'details_fetched_for_hearing_date', 'Hearings', 'defendant',
    'Court', 'CourtName', 'status'
]

time_fields = [
    'SentenceTime', 'SentenceSuspended', 'SentenceSuspendedTime',
    'OperatorLicenseSuspensionTime', 'ProbationTime'
]

fields_to_anonomize = [
    'Defendant', 'Name', 'AKA1', 'AKA2'
]

court_type = sys.argv[1]
if court_type != 'circuit' and court_type != 'district':
    raise ValueError('Unknown court type')

# connect to database
db = MongoDatabase('va_court_search', court_type)

courts = list(db.get_courts())
courts_by_fips = {court['fips_code']:court for court in courts}

s3 = boto3.resource('s3')

def simplify_time(time_string):
    time_string= time_string.replace(' Year(s)', 'Years ')\
                            .replace(' Month(s)', 'Months ')\
                            .replace(' Day(s)', 'Days ')
    days = 0
    string_parts = time_string.split(' ')
    for string_part in string_parts:
        if 'Years' in string_part:
            days += int(string_part.replace('Years','')) * 365
        elif string_part == '12Months':
            days += 365
        elif 'Months' in string_part:
            days += int(string_part.replace('Months','')) * 30
        elif 'Days' in string_part:
            days += int(string_part.replace('Days',''))
        elif 'Hours' in string_part:
            hours = int(string_part.replace('Hours',''))
            if hours > 0: days += 1
    simplified_time = str(days) if days > 0 else ''
    return simplified_time

def write_cases_to_file(cases, court_type, filename, metadata):
    if 'total' not in metadata: metadata['total'] = 0
    if 'byCourt' not in metadata: metadata['byCourt'] = {}
    with open('./' + filename + '.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldnames[court_type]))
        writer.writeheader()
        for case in cases:
            if 'error' in case['details']:
                print 'Error getting case details', case['case_number']
                continue
            case['Court'] = courts_by_fips[case['court_fips']]['name']
            if case['Court'] not in metadata['byCourt']:
                metadata['byCourt'][case['Court']] = 0
            if 'status' in case:
                case['CaseStatus'] = case['status']
            metadata['byCourt'][case['Court']] += 1
            metadata['total'] += 1
            print (str(metadata['total']) + '\r'),
            for detail in case['details']:
                new_key = detail.replace(' ', '')
                if new_key in time_fields:
                    new_key += 'Days'
                    case[new_key] = simplify_time(case['details'][detail])
                else:
                    case[new_key] = case['details'][detail]
                if 'Month' in case[new_key]:
                    print new_key, case[new_key]
                if new_key in fields_to_anonomize:
                    hash = hashlib.md5()
                    hash.update(os.environ['ANON_DATA_SALT'])
                    hash.update(case[new_key])
                    case[new_key] = hash.hexdigest()
            for field in excluded_fields:
                if field in case:
                    del case[field]
            writer.writerow(case)

def upload_cases(filename):
    s3.Object('virginia-court-data', filename + '.zip')\
      .put(Body=open(filename + '.zip', 'rb'), ACL='public-read', ContentType='application/zip')
    os.remove(filename + '.zip')

def export_and_upload_data_by_year(year, court_type):
    year_filename = 'criminal_' + court_type + '_court_cases_' + str(year)
    year_zipped_file = zipfile.ZipFile(year_filename + '.zip', 'w')
    metadata = {'filesize': 0, 'filesizeUncompressed': 0}
    months = []
    for month in range(1, 13):
        months.append(datetime(year, month, 1))
    months.append(datetime(year + 1, 1, 1))
    for month in range(0, len(months)-1):
        start = months[month]
        end = months[month+1]
        print 'From', start, 'to', end
        cases = db.get_cases_by_hearing_date(start, end)

        month_filename = year_filename + '_' + str(month+1)
        write_cases_to_file(cases, court_type, month_filename, metadata)
        metadata['filesizeUncompressed'] += os.path.getsize(month_filename + '.csv')

        year_zipped_file.write(month_filename + '.csv', month_filename + '.csv', zipfile.ZIP_DEFLATED)
        os.remove(month_filename + '.csv')
    year_zipped_file.close()
    metadata['filesize'] = os.path.getsize(year_filename + '.zip')
    upload_cases(year_filename)
    return metadata

start_year = int(sys.argv[2])
end_year = int(sys.argv[3])
complete_metadata = []
for year in range(start_year, end_year+1):
    print 'Export', year
    metadata = export_and_upload_data_by_year(year, court_type)
    complete_metadata.append({
        'year': year,
        'downloadLink': 'https://s3.amazonaws.com/virginia-court-data/criminal_' + court_type + '_court_cases_' + str(year) + '.zip',
        'cases': metadata['total'],
        'filesize': metadata['filesize'],
        'filesizeUncompressed': metadata['filesizeUncompressed'],
        'byCourt': metadata['byCourt']
    })
with open('./criminal' + court_type[0].upper() + court_type[1:] + 'CourtCaseExport.js', 'w') as jsonfile:
    jsonfile.write('var ' + court_type + 'CourtCaseDataSets = ')
    jsonfile.write(json.dumps(complete_metadata))
