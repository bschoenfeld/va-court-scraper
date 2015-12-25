from courtreader import readers
from courtutils.database import Database
from courtutils.email import send_welcome_email, verify_link
from courtutils.logger import get_logger
from courtutils.user import User
from flask import Flask, render_template, make_response, redirect, request, url_for
from flask.ext.login import LoginManager, login_required, login_user
import datetime
import os

app = Flask(__name__)

login_manager = LoginManager()
login_manager.init_app(app)

# configure logging
log = get_logger()
log.info('Web running')

@login_manager.user_loader
def load_user(user_id):
    print 'loading user'
    return User.get(user_id)

def user_registered(email):
    return email == 'ben.schoenfeld@gmail.com'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/home')
@login_required
def home():
    return render_template('home.html')

@app.route('/login')
def login():
    return render_template('login.html')

@app.route('/login', methods=['POST'])
def do_login():
    login_user(User.login(request.args.get('email')))
    return redirect(url_for('home'))

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))

@app.route('/password')
def password():
    email = request.args.get('email')
    expiration = request.args.get('expires')
    token = request.args.get('token')
    valid_request = verify_link('password', email, expiration, token)
    return render_template('password.html', email=email, valid_request=valid_request)

@app.route('/register', methods=['POST'])
def register():
    email = request.form['email']
    if user_registered(email):
        return 'Email address already registered', 409
    send_welcome_email(request.form['email'])
    return email

@app.route('/reset-password', methods=['POST'])
def reset_password():
    email = request.form['email']
    if not user_registered(email):
        return 'Email address is not registered', 409
    #send_welcome_email(request.form['email'])
    return email

@app.route('/search')
def search():
    return render_template('search.html')

@app.route('/search/<name>')
def lookup_search_name(name):
    return render_template('search_results.html')

@app.route('/search/<name>', methods=['POST'])
def add_search_name_tasks(name):
    Database.insert_tasks('circuit', name.upper())
    Database.insert_tasks('district', name.upper())
    return ''

if __name__ == "__main__":
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.secret_key = 'doesnt-need-to-be-secret'
    app.run(host='0.0.0.0', port=port, debug=True)
