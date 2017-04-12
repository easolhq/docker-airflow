#!/usr/bin/env python
import sys
import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser

def create_user(username, email, password):
    user = PasswordUser(models.User())
    user.username = username
    user.email = email
    user.password = password
    session = settings.Session()
    session.add(user)
    session.commit()
    session.close()

def main(argv):
    """
    Creates a new user with access to the
    Airflow webserver.
    To run:
        $ AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://user:pass@host:port create_airflow_user.py USERNAME EMAIL PASSWORD
    """
    username = argv[0]
    email= argv[1]
    password = argv[2]
    create_user(username, email, password)

if __name__ == '__main__':
    main(sys.argv[1:])
