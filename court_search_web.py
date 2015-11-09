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
client = pymongo.MongoClient(os.environ['MONGO_DB'])
db = client['temp-15-11-08']

# Connect to District Court Reader
reader = readers.CircuitCourtReader()
courts = reader.connect()
log.info('Web connected to court site')

@app.route('/search')
def search():
    return render_template('search.html')

@app.route('/search/<name>', methods=['GET', 'POST'])
def search_name(name):
    if request.method == 'POST':
        for court in courts:
            db.tasks.insert_one({'type': 'name',
                                 'court_fips': court,
                                 'term': name.upper()})
        return ''
    return render_template('search_results.html')

if __name__ == "__main__":
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.secret_key = 'doesnt-need-to-be-secret'
    app.run(host='0.0.0.0', port=port, debug=True)
