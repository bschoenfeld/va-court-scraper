import datetime
import hashlib
import os
import sendwithus
import urllib

def unix_time_millis(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds())

def generate_token(data):
    hash = hashlib.sha512()
    hash.update(os.environ['EMAIL_TOKEN_SALT'])
    hash.update(data)
    return hash.hexdigest()

def create_link(email_address, return_link):
    expires = datetime.datetime.utcnow() + datetime.timedelta(days=1)
    uri = return_link
    uri += '?email=' + urllib.quote(email_address)
    uri += '&expires=' + str(unix_time_millis(datetime.datetime.utcnow()))
    uri += '&token=' + generate_token(uri)
    return uri

def send_welcome_email(email_address):
    api_key = os.environ['SEND_WITH_US']
    print create_link('ben.schoenfeld@gmail.com', 'password')
    #swu = sendwithus.api(api_key)
    #swu.send(
    #    email_id='tem_58MQPDcuQvGKoXG3aVp4Zb',
    #    recipient={'address': email_address},
    #    email_data={'setPasswordLink': 'password'})
