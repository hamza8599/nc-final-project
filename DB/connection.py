from pg8000.native import Connection
import os
from dotenv import load_dotenv
load_dotenv()
def connect_db():
    user=os.environ['USER']
    pswd=os.environ['PASSWORD']
    db=os.environ['DATABASE']
    port=os.environ['PORT']
    host=os.environ['HOST']

    conn = Connection(user=user, password=pswd, database=db, host=host, port=port)
    return conn

def close_db(conn):
    conn.close()
