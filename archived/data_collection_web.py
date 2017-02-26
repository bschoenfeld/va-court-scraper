import StringIO
import csv
import datetime
import os
import pymongo
from flask import Flask, Response, render_template, jsonify

app = Flask(__name__)

@app.route("/")
def index():
    district_db_client = pymongo.MongoClient(os.environ['DISTRICT_DB'])
    district_db = district_db_client.va_district_court_cases
    courts = list(district_db.courts.find())
    ten_minutes_ago = datetime.datetime.utcnow() + datetime.timedelta(minutes=-10)
    one_day_ago = datetime.datetime.utcnow() + datetime.timedelta(days=-1)
    scrapers = list(district_db.scrapers.find({'last_update': {'$gt': one_day_ago}}))
    print courts
    return render_template('data_collection.html', courts=courts, scrapers=scrapers, ten_minutes_ago=ten_minutes_ago)

@app.route("/status/<fips_code>")
def status(fips_code):
    district_db_client = pymongo.MongoClient(os.environ['DISTRICT_DB'])
    district_db = district_db_client.va_district_court_cases
    court = {'fips_code': fips_code}
    court['total_count'] = district_db.cases.count({
        'FIPSCode': fips_code
    })
    court['collected_count'] = district_db.cases.count({
        'FIPSCode': fips_code,
        'date_collected': {'$exists': True}
    })
    return jsonify(**court)

@app.route('/export/<fips_code>/cases.csv')
def export_cases(fips_code):
    district_db_client = pymongo.MongoClient(os.environ['DISTRICT_DB'])
    district_db = district_db_client.va_district_court_cases
    cases = district_db.cases.find({
        'FIPSCode': fips_code,
        'date_collected': {'$exists': True}
    }, projection = {
        '_id': False,
        'error': False,
        'date_collected': False,
        'Hearings': False,
        'Services': False,
    })
    fieldnames = [
        'FIPSCode', 'CaseNumber', 'Locality', 'CourtName', 'FiledDate',
        'Name', 'AKA1', 'AKA2', 'DOB', 'Gender', 'Race', 'Address',
        'OffenseDate', 'ArrestDate', 'Class', 'Status', 'Complainant',
        'CaseType', 'Charge', 'CodeSection',
        'AmendedCaseType', 'AmendedCharge', 'AmendedCode',
        'FinalDisposition', 'DefenseAttorney',
        'SentenceTime', 'SentenceSuspendedTime',
        'ProbationType', 'ProbationTime', 'ProbationStarts',
        'Fine', 'Costs', 'FineCostsDue', 'FineCostsPaid', 'FineCostsPaidDate',
        'OperatorLicenseRestrictionCodes', 'OperatorLicenseSuspensionTime',
        'RestrictionStartDate', 'RestrictionEndDate', 'VASAP'
    ]
    output = StringIO.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(cases)
    return Response(output.getvalue(), mimetype='text/csv')

if __name__ == "__main__":
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.secret_key = 'doesnt-need-to-be-secret'
    app.run(host='0.0.0.0', port=port, debug=True)
