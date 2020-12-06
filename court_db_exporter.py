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

def download_case_table(case_table, filed_field, start_year, end_year, outfile_path):
    copy_cmd = '\\copy (Select * From "{}" '.format(case_table)
    copy_cmd += 'where {0} >= \'{1}\' and {0} < \'{2}\' '.format(
        filed_field, '1/1/' + str(start_year), '1/1/' + str(end_year)
    )
    copy_cmd += 'order by id) To \'{}\' With CSV HEADER;'.format(
        outfile_path
    )

    psql_cmd = [
        'psql',
        '-c', copy_cmd
    ]
    print year, subprocess.check_output(psql_cmd)

def download_child_table(case_table, child_table, filed_field, start_year, end_year, outfile_path):
    copy_cmd = '\\copy (Select "{}".* From "{}" '.format(child_table, case_table)
    copy_cmd += 'inner join "{}" on "{}".case_id = "{}".id '.format(
        child_table, child_table, case_table
    )
    copy_cmd += 'where {0} >= \'{1}\' and {0} < \'{2}\' '.format(
        filed_field, '1/1/' + str(start_year), '1/1/' + str(end_year)
    )
    copy_cmd += 'order by "{}".case_id) To \'{}\' With CSV HEADER;'.format(
        child_table, outfile_path
    )

    psql_cmd = [
        'psql',
        '-c', copy_cmd
    ]
    print year, subprocess.check_output(psql_cmd)

def zip_data_files(zip_filename, filepaths):
    data_zip_path = '{}_{}.zip'.format(zip_filename, id_generator())
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
    zip_filename = '{}_{}'.format(table['prefix'], year)
    filepaths = []

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
        download_child_table(
            table['prefix'] + 'Case', 
            table['prefix'] + child_table,
            table['filedField'], 
            start_year, 
            end_year, 
            child_table + 's.csv')
        filepaths.append(child_table + 's.csv')
    
    # Zip data files
    data_zip_path = zip_data_files(zip_filename, filepaths)

    # Upload zip files
    upload_zip_file(data_zip_path)

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
        export_data(year, year + 1)
    year -= 1

export_data(1900, 2010)
