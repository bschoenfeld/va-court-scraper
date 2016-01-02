import hashlib
import os
from database import Database
from email import send_welcome_email
from flask.ext.login import UserMixin

def get_hash(data):
    hash = hashlib.sha256()
    hash.update(os.environ['PASSWORD_TOKEN_SALT'])
    hash.update(data)
    return hash.hexdigest()

class User(UserMixin):
    def __init__(self, user):
        self.id = user['email']
        self.user = user

    @classmethod
    def get(cls, id):
        return User(Database.get_user(id))

    @classmethod
    def registered(cls, email):
        return Database.get_user(email) != None

    @classmethod
    def create(cls, email):
        Database.add_user(email)
        send_welcome_email(email)

    @classmethod
    def update_password(cls, email, password):
        Database.set_user_password(email, get_hash(password))

    @classmethod
    def login(cls, email, password):
        user = Database.confirm_credentials(email, get_hash(password))
        return None if user is None else User(user)
