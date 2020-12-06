import calendar
import csv
import datetime
import os
import random
import string
import subprocess
import sys
import zipfile

import boto3

from courtutils.databases.postgres import PostgresDatabase

DATA_ZIP_PATHS = []

def download_case_table(case_table, filed_field, start_year, end_year, outfile_path):
    copy_cmd = '\\copy (Select * From "{}" '.format(case_table)
    copy_cmd += 'where "{0}"."{1}" >= \'{2}\' and "{0}"."{1}" < \'{3}\' '.format(
        case_table, filed_field, '1/1/' + str(start_year), '1/1/' + str(end_year)
    )
    copy_cmd += 'limit 10) To \'{}\' With CSV HEADER;'.format(
        outfile_path
    )

    psql_cmd = [
        'psql',
        '-c', copy_cmd
    ]
    print start_year, subprocess.check_output(psql_cmd)

def download_child_table(case_table, child_table, filed_field, start_year, end_year, outfile_path):
    copy_cmd = '\\copy (Select "{}".* From "{}" '.format(child_table, case_table)
    copy_cmd += 'inner join "{}" on "{}".case_id = "{}".id '.format(
        child_table, child_table, case_table
    )
    copy_cmd += 'where "{0}"."{1}" >= \'{2}\' and "{0}"."{1}" < \'{3}\' '.format(
        case_table, filed_field, '1/1/' + str(start_year), '1/1/' + str(end_year)
    )
    copy_cmd += 'limit 10) To \'{}\' With CSV HEADER;'.format(
        outfile_path
    )

    psql_cmd = [
        'psql',
        '-c', copy_cmd
    ]
    print start_year, subprocess.check_output(psql_cmd)

def zip_data_files(zip_filename, filepaths):
    data_zip_path = 'DB_{}_{}.zip'.format(zip_filename, id_generator())
    data_zip = zipfile.ZipFile(data_zip_path, 'w')
    for path in filepaths:
        data_zip.write(path, path, zipfile.ZIP_DEFLATED)
        os.remove(path)
    data_zip.close()
    return data_zip_path

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

S3 = boto3.client('s3')
S3_BUCKET = 'virginia-court-data'

def upload_zip_file(path):
    S3.upload_file(path, S3_BUCKET, path, ExtraArgs={
        'ACL': 'public-read',
        'ContentType':'application/zip'
    })
    os.remove(path)

def export_data(table, start_year, end_year):
    zip_filename = '{}_{}'.format(table['prefix'], start_year)
    filepaths = []

    print "Downloading data for ", zip_filename

    # Download cases
    download_case_table(
        table['prefix'] + 'Case', 
        table['filedField'], 
        start_year,
        end_year, 
        'Cases.csv')
    filepaths.append('Cases.csv')
    
    # Download child tables
    for child_table in table['childTables']:
        print "Downloading", child_table
        download_child_table(
            table['prefix'] + 'Case', 
            table['prefix'] + child_table,
            table['filedField'], 
            start_year, 
            end_year, 
            child_table + 's.csv')
        filepaths.append(child_table + 's.csv')
    
    # Zip data files
    print "Creating ZIP file"
    data_zip_path = zip_data_files(zip_filename, filepaths)

    # Upload zip files
    print "Uploading ZIP file"
    upload_zip_file(data_zip_path)

    DATA_ZIP_PATHS.append(data_zip_path)

COURT_TABLES = [
    {
        'prefix': 'CircuitCivil',
        'filedField': 'Filed',
        'childTables': [
            'Plaintiff',
            'Defendant',
            'Hearing',
            'Pleading',
            'Service'
        ]
    },
    {
        'prefix': 'DistrictCivil',
        'filedField': 'FiledDate',
        'childTables': [
            'Plaintiff',
            'Defendant',
            'Hearing',
            'Report',
            'Service'
        ]
    },
    {
        'prefix': 'CircuitCriminal',
        'filedField': 'Filed',
        'childTables': [
            'Hearing',
            'Pleading',
            'Service'
        ]
    },
    {
        'prefix': 'DistrictCriminal',
        'filedField': 'FiledDate',
        'childTables': [
            'Hearing',
            'Service'
        ]
    }
]

year = datetime.datetime.now().year
while year >= 2010:
    for table in COURT_TABLES:
        export_data(table, year, year + 1)
    year -= 1

for table in COURT_TABLES:
    export_data(table, 2000, 2010)

print DATA_ZIP_PATHS
