import calendar
import csv
import datetime
import os
import subprocess
import zipfile

import boto3

from courtutils.databases.postgres import PostgresDatabase

REMOVE_FIELDS = [
    'collected', 'id', 'details_fetched_for_hearing_date'
]
ANON_FIELDS = [
    'CaseNumber', 'Name', 'Defendant', 'AKA', 'DOB', 'AKA1', 'AKA2'
]

def export_table(table, court_type, case_type):
    db = PostgresDatabase(court_type)
    year = datetime.datetime.now().year
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
            download_data(table, year, temp_filepath)

            # Create partitioned data files, on of which is anonymized
            data_filepaths, anon_data_filepaths = create_data_files(filepath, temp_filepath)

            # Zip data files
            data_zip_path = zip_data_files(filepath + '_complete', data_filepaths)
            anon_data_zip_path = zip_data_files(filepath + '_anon', anon_data_filepaths)

            # Upload zip files
            upload_zip_file(data_zip_path)
            upload_zip_file(anon_data_zip_path)
            break
        else:
            break

def download_data(table, year, outfile_path):
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

CASES_PER_FILE = 250000
def create_data_files(filepath, temp_filepath):
    data_filepaths = []
    anon_data_filepaths = []
    with open(temp_filepath, 'r') as temp_file:
        data_file = None
        anon_data_file = None
        data_reader = csv.DictReader(temp_file)
        case_count = 0
        for case in data_reader:
            if case_count % CASES_PER_FILE == 0:
                fieldnames = data_reader.fieldnames

                fieldnames = [field for field in fieldnames if field not in REMOVE_FIELDS]
                if data_file: data_file.close()
                data_filepaths.append('{}_{}.csv'.format(
                    filepath, str(case_count/CASES_PER_FILE).zfill(2)
                ))
                data_file = open(data_filepaths[-1], 'w')
                data_writer = csv.DictWriter(data_file, fieldnames=fieldnames)
                data_writer.writeheader()

                fieldnames = [field for field in fieldnames if field not in ANON_FIELDS]
                if anon_data_file: anon_data_file.close()
                anon_data_filepaths.append('{}_anon_{}.csv'.format(
                    filepath, str(case_count/CASES_PER_FILE).zfill(2)
                ))
                anon_data_file = open(anon_data_filepaths[-1], 'w')
                anon_data_writer = csv.DictWriter(anon_data_file, fieldnames=fieldnames)
                anon_data_writer.writeheader()

            for field in REMOVE_FIELDS:
                if field in case:
                    del case[field]
            data_writer.writerow(case)

            for field in ANON_FIELDS:
                if field in case:
                    del case[field]
            anon_data_writer.writerow(case)

            case_count += 1
    data_file.close()
    anon_data_file.close()
    os.remove(temp_filepath)
    return (data_filepaths, anon_data_filepaths)

def zip_data_files(filepath, data_filepaths):
    data_zip_path = filepath + '.zip'
    data_zip = zipfile.ZipFile(data_zip_path, 'w')
    for path in data_filepaths:
        data_zip.write(path, path, zipfile.ZIP_DEFLATED)
        os.remove(path)
    data_zip.close()
    return data_zip_path

S3 = boto3.client('s3')
S3_BUCKET = 'virginia-court-data'
def upload_zip_file(path):
    S3.upload_file(path, S3_BUCKET, path, ExtraArgs={
        'ACL': 'public-read',
        'ContentType':'application/zip'
    })
    os.remove(path)

export_table('DistrictCriminalCase', 'district', 'criminal')
