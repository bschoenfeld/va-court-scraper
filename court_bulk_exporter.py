import calendar
import csv
import datetime
import os
import random
import string
import subprocess
import zipfile

import boto3
from firebase import firebase

from courtutils.databases.postgres import PostgresDatabase

REMOVE_FIELDS = [
    'collected', 'id', 'details_fetched_for_hearing_date'
]
ANON_FIELDS = [
    'CaseNumber', 'Name', 'Defendant', 'AKA', 'DOB', 'AKA1', 'AKA2',
    'Plaintiff1Name', 'Plaintiff2Name', 'Plaintiff3Name',
    'Defendant1Name', 'Defendant2Name', 'Defendant3Name'
]
PARTIES = [
    'Plaintiff', 'Defendant'
]

def get_party_temp_file(temp_filepath, party):
    return temp_filepath.replace('_temp.csv', '_{}_temp.csv'.format(party.lower()))

def export_table(table, court_type, case_type):
    db = PostgresDatabase(court_type)
    year = datetime.datetime.now().year
    metadata = []
    while True:
        year -= 1
        courts = db.count_courts()
        expected_count = (366 if calendar.isleap(year) else 365) * courts
        actual_count = db.count_dates_searched_for_year(case_type, year)
        if expected_count == actual_count:
            # Create filepaths
            filepath = '{}_{}_{}'.format(court_type, case_type, year)
            temp_filepath = filepath + '_temp.csv'

            # Download data from postgres into a temp file
            download_data(table, year, temp_filepath, case_type)

            # Create partitioned data files, on of which is anonymized
            metadata.append(create_data_files(filepath, temp_filepath, court_type, case_type))
            metadata[-1]['year'] = year

            # Zip data files
            data_zip_path = zip_data_files(filepath + '_complete', metadata[-1]['complete'])
            anon_data_zip_path = zip_data_files(filepath + '_anon', metadata[-1]['anon'])

            # Upload zip files
            delete_old_zip_files(filepath)
            upload_zip_file(data_zip_path)
            upload_zip_file(anon_data_zip_path)
        else:
            break
    return metadata

def download_data(table, year, outfile_path, case_type):
    copy_cmd = '\\copy (Select * From "{}" '.format(table)
    copy_cmd += 'where {0} >= \'{1}\' and {0} < \'{2}\' '.format(
        'details_fetched_for_hearing_date', '1/1/' + str(year), '1/1/' + str(year+1)
    )
    copy_cmd += 'order by id) To \'{}\' With CSV HEADER;'.format(outfile_path)

    psql_cmd = [
        'psql',
        '-c', copy_cmd
    ]
    print year, subprocess.check_output(psql_cmd)

    if case_type == 'civil':
        for party in PARTIES:
            download_party_data(
                table,
                table.replace('Case', party),
                year,
                get_party_temp_file(outfile_path, party)
            )

def download_party_data(table, party_table, year, outfile_path):
    copy_cmd = '\\copy (Select "{}".* From "{}" '.format(party_table, table)
    copy_cmd += 'inner join "{}" on "{}".case_id = "{}".id '.format(
        party_table, party_table, table
    )
    copy_cmd += 'where {0} >= \'{1}\' and {0} < \'{2}\' '.format(
        'details_fetched_for_hearing_date', '1/1/' + str(year), '1/1/' + str(year+1)
    )
    copy_cmd += 'order by "{}".case_id) To \'{}\' With CSV HEADER;'.format(
        party_table, outfile_path
    )

    psql_cmd = [
        'psql',
        '-c', copy_cmd
    ]
    print year, subprocess.check_output(psql_cmd)

CASES_PER_FILE = 250000
def create_data_files(filepath, temp_filepath, court_type, case_type):
    metadata = {
        'cases': 0,
        'complete': {
            'filepaths': []
        },
        'anon': {
            'filepaths': []
        }
    }
    with open(temp_filepath, 'r') as temp_file:
        data_file = None
        anon_data_file = None
        data_reader = csv.DictReader(temp_file)
        party_readers = []
        if case_type == 'civil':
            for party in PARTIES:
                party_filepath = get_party_temp_file(temp_filepath, party)
                party_file = open(party_filepath, 'r')
                party_readers.append({
                    'filepath': party_filepath, 'file': party_file,
                    'name': party, 'reader': csv.DictReader(party_file), 'lastRead': None
                })
        case_count = 0
        for case in data_reader:
            if case_count % CASES_PER_FILE == 0:
                fieldnames = list(data_reader.fieldnames)
                for party in party_readers:
                    fieldnames.extend(get_party_headers(party['name'], court_type))

                fieldnames = [field for field in fieldnames if field not in REMOVE_FIELDS]
                if data_file: data_file.close()
                metadata['complete']['filepaths'].append('{}_{}.csv'.format(
                    filepath, str(case_count/CASES_PER_FILE).zfill(2)
                ))
                data_file = open(metadata['complete']['filepaths'][-1], 'w')
                data_writer = csv.DictWriter(data_file, fieldnames=fieldnames)
                data_writer.writeheader()

                fieldnames = [field for field in fieldnames if field not in ANON_FIELDS]
                if anon_data_file: anon_data_file.close()
                metadata['anon']['filepaths'].append('{}_anon_{}.csv'.format(
                    filepath, str(case_count/CASES_PER_FILE).zfill(2)
                ))
                anon_data_file = open(metadata['anon']['filepaths'][-1], 'w')
                anon_data_writer = csv.DictWriter(anon_data_file, fieldnames=fieldnames)
                anon_data_writer.writeheader()

            for party in party_readers:
                party['curCaseParties'], party['lastRead'] = get_parties(
                    case['id'], party['reader'], party['lastRead']
                )
                add_parties_to_case(case, party['curCaseParties'], party['name'], court_type)

            for field in REMOVE_FIELDS:
                if field in case:
                    del case[field]
            data_writer.writerow(case)

            for field in ANON_FIELDS:
                if field in case:
                    del case[field]
            anon_data_writer.writerow(case)

            case_count += 1
    metadata['cases'] = case_count
    data_file.close()
    anon_data_file.close()
    os.remove(temp_filepath)
    for party in party_readers:
        party['file'].close()
        os.remove(party['filepath'])
    return metadata

def get_party_fields(court_type):
    if court_type.lower() == 'circuit':
        return ['Name', 'TradingAs', 'Attorney']
    return ['Name', 'Address', 'Attorney']

def get_party_headers(party_name, court_type):
    party_headers = []
    for i in range(0, 3):
        for field in get_party_fields(court_type):
            party_headers.append(party_name + str(i+1) + field)
    return party_headers

def add_parties_to_case(case, parties, party_name, court_type):
    for i in range(0, 3):
        if len(parties) > i:
            for field in get_party_fields(court_type):
                case[party_name + str(i+1) + field] = parties[i][field]

def get_parties(case_id, party_reader, last_party_read):
    parties = []
    party = last_party_read
    while True:
        if party is None:
            try:
                party = party_reader.next()
            except StopIteration:
                print 'EOF'
                return (parties, party)
        if party['case_id'] != case_id:
            return (parties, party)
        parties.append(party)
        party = None

def zip_data_files(filepath, metadata):
    metadata['filesizeUncompressed'] = 0
    data_zip_path = '{}_{}.zip'.format(filepath, id_generator())
    data_zip = zipfile.ZipFile(data_zip_path, 'w')
    for path in metadata['filepaths']:
        metadata['filesizeUncompressed'] += os.path.getsize(path)
        data_zip.write(path, path, zipfile.ZIP_DEFLATED)
        os.remove(path)
    data_zip.close()
    metadata['filepath'] = data_zip_path
    metadata['filesize'] = os.path.getsize(data_zip_path)
    return data_zip_path

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

S3 = boto3.client('s3')
S3_BUCKET = 'virginia-court-data'

def delete_old_zip_files(prefix):
    result = S3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    if 'Contents' in result:
        objects = [{'Key': obj['Key']} for obj in result['Contents']]
        S3.delete_objects(Bucket=S3_BUCKET, Delete={'Objects':objects})

def upload_zip_file(path):
    S3.upload_file(path, S3_BUCKET, path, ExtraArgs={
        'ACL': 'public-read',
        'ContentType':'application/zip'
    })
    os.remove(path)

FIREBASE_TOKEN = os.environ['FIREBASE_TOKEN']
def upload_metadata(metadata):
    authentication = firebase.FirebaseAuthentication(FIREBASE_TOKEN, None, extra={'uid': 'data-export-worker'})
    database = firebase.FirebaseApplication('https://virginiacourtdata.firebaseio.com', authentication)
    database.put('/', 'data', metadata)

def export_all():
    court_types = ['District', 'Circuit']
    case_types = ['Civil', 'Criminal']

    metadata = {
        'complete': {},
        'anon': {}
    }

    for court_type in court_types:
        for case_type in case_types:
            key = '{}-{}'.format(court_type.lower(), case_type.lower())
            metadata['complete'][key] = []
            metadata['anon'][key] = []

            table = '{}{}Case'.format(court_type, case_type)
            print 'Export', table
            cur_metadata = export_table(table, court_type.lower(), case_type.lower())

            for data in cur_metadata:
                copy_metadata(data, metadata['complete'][key], 'complete')
                copy_metadata(data, metadata['anon'][key], 'anon')
    print metadata
    upload_metadata(metadata)

def copy_metadata(source, dest, metadata_type):
    dest.append({
        'year': source['year'],
        'cases': source['cases'],
        'downloadLink': 'https://s3.amazonaws.com/virginia-court-data/{}'.format(
            source[metadata_type]['filepath']),
        'filesize': source[metadata_type]['filesize'],
        'filesizeUncompressed': source[metadata_type]['filesizeUncompressed']
    })

export_all()
