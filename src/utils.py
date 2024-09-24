from datetime import datetime
import pytz
import os
import psycopg2

#required to set Nepal TImeezone as aws doesnot have Nepal region
tz = pytz.timezone('Asia/Kathmandu')

from dotenv import load_dotenv

load_dotenv()


database = os.environ['DATABASE']
host = os.environ['HOST']
user = os.environ['DB_USER']
password = os.environ['PASSWORD']

#connect to postgres
def connect_postgres():
    conn = psycopg2.connect(database=database,
                            host=host,
                            user=user,
                            password=password,
                            port=5432
                        )
    return conn