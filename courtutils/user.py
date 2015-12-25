from database import Database
from flask.ext.login import UserMixin

class User(UserMixin):
    def __init__(self, email):
        self.id = email

    @classmethod
    def get(cls, id):
        return User(id)
        #return Database.get_user(id)

    @classmethod
    def login(cls, email):
        #user = Database.get_user(email)
        return User(email)
