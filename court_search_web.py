from courtreader import readers
from courtutils.courtlogger import get_logger
from flask import Flask, render_template, request
import datetime
import pymongo
import os

app = Flask(__name__)

# configure logging
log = get_logger()
log.info('Web running')

# Connect to database
def get_db():
    return pymongo.MongoClient(os.environ['MONGO_DB'])['va_court_search']

def insert_tasks(courts, court_tasks, name):
    court_codes = [c['fips_code'] for c in courts.find()]
    for code in court_codes:
        court_tasks.insert_one({'type': 'name',
                                'court_fips': code,
                                'term': name})

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login')
def login():
    return render_template('login.html')

@app.route('/password')
def password():
    return render_template('password.html')

@app.route('/search')
def search():
    return render_template('search.html')

@app.route('/search/<name>')
def lookup_search_name(name):
    return render_template('search_results.html')

@app.route('/search/<name>', methods=['POST'])
def add_search_name_tasks(name):
    db = get_db()
    insert_tasks(db.circuit_courts, db.circuit_court_tasks, name.upper())
    insert_tasks(db.district_courts, db.district_court_tasks, name.upper())
    return ''

if __name__ == "__main__":
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.secret_key = 'doesnt-need-to-be-secret'
    app.run(host='0.0.0.0', port=port, debug=True)
