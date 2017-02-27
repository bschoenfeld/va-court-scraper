import calendar
import csv
import datetime
import os
import subprocess

from courtutils.databases.postgres import PostgresDatabase

REMOVE_FIELDS = [
    'collected', 'id', 'details_fetched_for_hearing_date'
]
ANON_FIELDS = [
    'CaseNumber', 'Defendant', 'AKA', 'DOB', 'AKA1', 'AKA2'
]

def export_table(table, court_type, case_type):
    db = PostgresDatabase(court_type)
    year = datetime.datetime.now().year
    while True:
        year -= 2
        courts = db.count_courts()
        expected_count = (366 if calendar.isleap(year) else 365) * courts
        actual_count = db.count_dates_searched_for_year(case_type, year)
        if expected_count == actual_count:
            filepath = '{}_{}_{}'.format(court_type, case_type, year)
            temp_filepath = filepath + '_temp.csv'
            download_data(table, year, temp_filepath)
            with open(temp_filepath, 'r') as temp_file:
                data_file = None
                anon_data_file = None
                data_reader = csv.DictReader(temp_file)
                case_count = 0
                for case in data_reader:
                    if case_count % 500000 == 0:
                        fieldnames = data_reader.fieldnames

                        fieldnames = [field for field in fieldnames if field not in REMOVE_FIELDS]
                        if data_file: data_file.close()
                        data_file = open(filepath + '_{}.csv'.format(
                            str(case_count/500000).zfill(2)), 'w')
                        data_writer = csv.DictWriter(data_file, fieldnames=fieldnames)
                        data_writer.writeheader()

                        fieldnames = [field for field in fieldnames if field not in ANON_FIELDS]
                        if anon_data_file: anon_data_file.close()
                        anon_data_file = open(filepath + '_anon_{}.csv'.format(
                            str(case_count/500000).zfill(2)), 'w')
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

export_table('DistrictCriminalCase', 'district', 'criminal')
