# db_connection
import mysql.connector
import os
from dotenv import load_dotenv
from os.path import split, abspath, join


def load_env():
    absdir = split(abspath(__file__))[0]
    env_file = (join(absdir, '../.env'))
    load_dotenv(dotenv_path=env_file)

def get_db_connection():
    load_env()
    user = os.environ.get('MYSQL_USER')
    passwd = os.environ.get('MYSQL_PASSWORD')
    host = os.environ.get('MYSQL_HOST')
    db = os.environ.get('MYSQL_DATABASE')
    port = os.environ.get('MYSQL_PORT')
    return mysql.connector.connect(host=host, user=user, password=passwd, port=port, database=db)