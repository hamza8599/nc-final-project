import pandas as pd
import boto3
import awswrangler as wr
from datetime import datetime,timedelta
import logging
from botocore.exceptions import ClientError
import json
import os
from awswrangler.exceptions import NoFilesFound
from pg8000.native import Connection
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extras import execute_values

PROCESSED_BUCKET = os.getenv('PROCESSED_BUCKET', 'team-501-dimensional-transformers-process-bucket')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def connect_db(secret_name,secrets_manager_client):
    """Establish connection to DB using creds stored in AWS Secrets Manager"""
    response = secrets_manager_client.get_secret_value(SecretId=secret_name)
    secret_dict = json.loads(response["SecretString"])
    user = secret_dict["username"]
    pswd = secret_dict["password"]
    db = secret_dict["dbname"]
    port = secret_dict["port"]
    host = secret_dict["host"]

    # db_url = f"postgresql://{user}:{pswd}@{host}:{port}/{db}"
    # engine=create_engine(db_url)
    # return engine

    conn = Connection(user=user, password=pswd, database=db, host=host, port=port)
    return conn

def create_engine_conn(secret_name,secrets_manager_client):
    response = secrets_manager_client.get_secret_value(SecretId=secret_name)
    secret_dict = json.loads(response["SecretString"])
    user = secret_dict["username"]
    pswd = secret_dict["password"]
    db = secret_dict["dbname"]
    port = secret_dict["port"]
    host = secret_dict["host"]

    db_url = f"postgresql://{user}:{pswd}@{host}:{port}/{db}"
    # engine=create_engine(db_url)
    return db_url



def close_db(conn):
    conn.close()



def load_data(filename):
    secrets_manager_client = boto3.client("secretsmanager")
    conn_string = create_engine_conn("data-warehouse-creds",secrets_manager_client)
    db = create_engine(conn_string)
    conn = db.connect()
    df  = pd.read_parquet(f"s3://{PROCESSED_BUCKET}/{filename}/{filename}.parquet")
    df.drop(columns=['staff_id'], inplace=True)
    df.columns = [c.lower() for c in df.columns]
    if filename=='sales_order':
        table_name=f'fact_{filename}'
    else:
        table_name=f'dim_{filename}'

    df.to_sql(table_name, con=conn, if_exists='append',index=False)

    c=connect_db("data-warehouse-creds",secrets_manager_client)
    print(c.run("SELECT * FROM dim_staff"))

def get_object_path(records):
    """Extracts bucket and object references from Records field of event."""
    return records[0]["s3"]["bucket"]["name"], records[0]["s3"]["object"]["key"]



def lambda_handler(event, context):
    """get bucket name and object name from event"""
    s3_bucket_name, s3_object_name = get_object_path(event["Records"])
    logger.info(f"Bucket is {s3_bucket_name}")
    logger.info(f"Object key is {s3_object_name}")
    
    tablename= s3_object_name.split('/', 3)[0]
    filename = s3_object_name.split('/', 3)[2]
    name, format = filename.split('.')
    print(f"==>> name: {name}")
    load_data(name)
 

# lambda_handler('staff','bhdyhd')