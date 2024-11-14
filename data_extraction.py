from datetime import datetime
import pyarrow.parquet as pq
import boto3
from io import BytesIO
import pandas as pd
import pyarrow as pa
from pg8000.native import Connection
import os
import json
from botocore.exceptions import ClientError
import logging


def connect_db(secret_name):
    secrets_manager_client = boto3.client('secretsmanager')
    response=secrets_manager_client.get_secret_value(SecretId=secret_name)
    secret_dict=json.loads(response['SecretString'])
    user=secret_dict['username']
    pswd=secret_dict['password']
    db=secret_dict['dbname']
    port=secret_dict['port']
    host=secret_dict['host']

    conn = Connection(user=user, password=pswd, database=db, host=host, port=port)
    return conn

def close_db(conn):
    conn.close()

def reset_secrets(sm_client,secret_name):
    try:
        response=sm_client.list_secrets(MaxResults=100)
        for resp in response['SecretList']:
            if secret_name==resp['Name']:
                response=sm_client.put_secret_value(
                SecretId=secret_name,
                SecretString='2020-11-14 09:41:09.839000'
                )

    except ClientError as e:
            print(e)

def existing_secret(sm_client,secret_name):
    try:
        response=sm_client.list_secrets(MaxResults=100)
        exist=False
        for resp in response['SecretList']:
            if secret_name==resp['Name']:
                exist=True
                logger.info(f"Secret updated value {'exists' if exist else 'does not exist'}: {secret_name}") 
                return exist
        return exist
            
    except ClientError as e:
            print(e)

def store_secret(table_name,last_updated):
     secrets_manager_client = boto3.client('secretsmanager')
     secret_exist=existing_secret(secrets_manager_client,table_name)
     if not secret_exist:
        try:
            response = secrets_manager_client.create_secret(
                    Name=table_name,  
                    SecretString=str(last_updated),  
                )
        except ClientError as e:
            logger.info("Secret Create Error")
     else:
         try:
            response=secrets_manager_client.put_secret_value(
                SecretId=table_name,
                SecretString=str(last_updated) 
                )
         except ClientError as e:
             logger.info("Secret Put Value Error")

def format_to_parquet(data, conn, table_name):
    columns = [col["name"] for col in conn.columns]
    df = pd.DataFrame(data, columns=columns)
    last_updated = df['last_updated'].max()
    store_secret(table_name,last_updated)
    table = pa.Table.from_pandas(df)
    return table

def write_table_to_parquet_buffer(pyarrow_table):
    parquet_buffer = BytesIO()
    pq.write_table(pyarrow_table, parquet_buffer)
    parquet_buffer.seek(0)
    return parquet_buffer

def get_created_date(data,conn):
     columns = [col["name"] for col in conn.columns]
     df = pd.DataFrame(data, columns=columns)
     created_at = df['created_at'].max()
     return created_at




logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):

    try:
        conn = connect_db('psql_creds')
        s3_client = boto3.client('s3')
        secret_manager_client = boto3.client('secretsmanager')

        
        table_names = conn.run("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name NOT LIKE '%prisma%';")
        
        for table in table_names:
            exist=existing_secret(secret_manager_client,table[0])

            if exist: 
                response = secret_manager_client.get_secret_value(SecretId=table[0])
                last_updated = response['SecretString']
                
                last_updated_obj = datetime.strptime(last_updated, '%Y-%m-%d %H:%M:%S.%f')
                   
                last_updated_str = last_updated_obj.strftime('%Y-%m-%d %H:%M:%S.%f')
            
                query = f"SELECT * FROM {table[0]} WHERE last_updated > '{last_updated_str}';"
                logger.info(f"Checking for New Data from {table[0]}")
            else:
                query = f"SELECT * FROM {table[0]};"
                logger.info(f"Checking for Data from {table[0]}")

            data = conn.run(query)
     
            if data:
                logger.info(f"Adding New Data in {table[0]}")
                formatted_data = format_to_parquet(data, conn, table[0])
                parquet_buffer = write_table_to_parquet_buffer(formatted_data)

                timestamp = table[0]+" "+datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
                created_at=get_created_date(data,conn)
                year=created_at.year
                month=created_at.strftime('%B')
                day=created_at.day
                s3_key=f'{table[0]}/{year}/{month}/{day}/{timestamp}'
    
                
                try:
                    s3_client.put_object(Bucket="dimensional-transformers-ingestion-bucket", Key=s3_key, Body=parquet_buffer)
                    logger.info(f"Putting New Data in s3 bucket for {table[0]}")
                except ClientError as e:
                    logger.info(f"Alert: Failed to write to s3: {str(e)}")
                   
            else:
                logger.info(f"No New data to add in {table[0]}")
        
        close_db(conn)
    except Exception as e:
        logger.info(f"Alert: Failed to connect to DB: {str(e)}")

# lambda_handler("s","s")
# resting secrets
# conn = connect_db('psql_creds')
# secret_manager_client = boto3.client('secretsmanager')
# table_names = conn.run("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name NOT LIKE '%prisma%';")
# for table in table_names:
#     reset_secrets(secret_manager_client,table[0])