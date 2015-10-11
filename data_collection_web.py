import os
import pymongo
from flask import Flask, session, render_template, request, jsonify

app = Flask(__name__)

@app.route("/")
def index():
    district_db_client = pymongo.MongoClient(os.environ['DISTRICT_DB'])
    district_db = district_db_client.va_district_court_cases
    courts = list(district_db.courts.find())
    print courts
    return render_template('data_collection.html', courts=courts)

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

if __name__ == "__main__":
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.secret_key = 'doesnt-need-to-be-secret'
    app.run(host='0.0.0.0', port=port, debug=True)
