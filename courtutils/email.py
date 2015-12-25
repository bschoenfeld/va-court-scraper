from datetime import timedelta, datetime
import hashlib
import os
import sendwithus
import urllib

def unix_time_millis(dt):
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds())

def generate_token(data):
    hash = hashlib.sha256()
    hash.update(os.environ['EMAIL_TOKEN_SALT'])
    hash.update(data)
    return hash.hexdigest()

def generate_uri(route, email_address, expiration):
    uri = route
    uri += '?email=' + email_address
    uri += '&expires=' + expiration
    return uri

def create_link(email_address, route):
    expires = datetime.utcnow() + timedelta(days=1)
    uri = generate_uri(route,
        urllib.quote(email_address),
        str(unix_time_millis(expires)))
    uri += '&token=' + generate_token(uri)
    return uri

def verify_link(route, email_address, expiration, token):
    if datetime.fromtimestamp(float(expiration)) < datetime.utcnow(): 
        return False
    uri = generate_uri(route,
        urllib.quote(email_address),
        expiration)
    return token == generate_token(uri)

def send_welcome_email(email_address):
    api_key = os.environ['SEND_WITH_US']
    set_password_link = create_link(email_address, 'password')
    swu = sendwithus.api(api_key)
    swu.send(
        email_id='tem_58MQPDcuQvGKoXG3aVp4Zb',
        recipient={'address': email_address},
        email_data={'setPasswordLink': set_password_link})
