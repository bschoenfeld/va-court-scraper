from collections import Counter
from courtreader import readers
from courtutils.database import Database
from courtutils.logger import get_logger
from datetime import datetime, timedelta
import boto3
import csv
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
db = get_db_connection()

courts = list(Database.get_circuit_courts())
courts_by_fips = {court['fips_code']:court for court in courts}

cases_by_court = {}

def write_cases_to_file(cases, filename, details, exclude_cases):
    with open('./' + filename + '.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldnames))
        writer.writeheader()
        for case in cases:
            if case['case_number'] in exclude_cases:
                continue
            if 'error' in case['details']:
                print 'Error getting case details', case['case_number']
                continue
            case['Court'] = courts_by_fips[case['court_fips']]['name']
            if case['Court'] not in details:
                details[case['Court']] = 0
            details[case['Court']] += 1
            for detail in case['details']:
                new_key = detail.replace(' ', '')
                case[new_key] = case['details'][detail]
            for field in excluded_fields:
                if field in case:
                    del case[field]
            writer.writerow(case)

def export_data_by_year(year, court, exclude_cases):
    start = datetime(year, 1, 1)
    end = datetime(year + 1, 1, 1)
    print 'From', start, 'to', end

    cases = db.circuit_court_detailed_cases.find({
        'details_fetched_for_hearing_date': {'$gte': start, '$lt': end},
        'court_fips': court
    })

    filename = 'criminal_circuit_court_cases_' + court + '_' + str(year)
    details = {}
    write_cases_to_file(cases, filename, details, exclude_cases)

with open(sys.argv[1]) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        fips = row['CASE_NUM'][:3]
        case_number = row['CASE_NUM'][3:-2] + '-' + row['CASE_NUM'][-2:]
        if fips not in cases_by_court:
            cases_by_court[fips] = {}
        cases_by_court[fips][case_number] = row

for court in cases_by_court:
    print ''
    print '***', courts_by_fips[court]['name'], '***'
    print 'Cases in Pilot File', len(cases_by_court[court])
    all_cases = list(db.circuit_court_detailed_cases.find({
        'court_fips': court
    }, {
        'case_number': True,
        'details_fetched_for_hearing_date': True
    }))

    matched_cases = [case for case in all_cases if case['case_number'] in cases_by_court[court]]
    print 'Cases in db and Pilot file', len(matched_cases)

    all_case_numbers_in_db = [case['case_number'] for case in all_cases]
    cases_not_in_db = list(set(cases_by_court[court].keys()) - set(all_case_numbers_in_db))
    print 'Cases in Pilot file but not db', len(cases_not_in_db)

    dates = [case['details_fetched_for_hearing_date'].year for case in matched_cases]
    print 'Last hearing date for cases in VP file', Counter(dates)

    cases_2012 = [case for case in all_cases if case['details_fetched_for_hearing_date'].year == 2012]
    print '2012 cases in db', len(cases_2012)

    unmatched_cases = [case for case in cases_2012 if case['case_number'] not in cases_by_court[court]]
    print '2012 cases in db but not Pilot file', len(unmatched_cases)

    export_data_by_year(2012, court, cases_by_court[court].keys())
