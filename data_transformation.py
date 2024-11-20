import pandas as pd
import boto3
import awswrangler as wr
from datetime import datetime
import logging
from botocore.exceptions import ClientError
import json
import os

INGESTION_BUCKET = os.getenv('INGESTION_BUCKET', 'dwh-default-ingestion-bucket')
PROCESSED_BUCKET = os.getenv('PROCESSED_BUCKET', 'dwh-default-processed-bucket')
LAMBDA_BUCKET = os.getenv('LAMBDA_BUCKET', 'dwh-default-lambda-bucket')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
#ingestion_bucket = "team-500-dimensional-transformers-ingestion-bucket"   

def design(name, datestamp):
    try:
        design_df = wr.s3.read_parquet(f"s3://{INGESTION_BUCKET}/design/*/*/*/{name} {datestamp}", dataset=True)
        design_df = design_df.drop(columns=['created_at', 'last_updated'])
        return design_df
    except ClientError as e:
        logger.info(f"Alert: Failed in design function: {str(e)}")

def currency(name, datestamp):
    try: 
        currency_df = wr.s3.read_parquet(f"s3://{INGESTION_BUCKET}/currency/*/*/*/{name} {datestamp}", dataset=True)
        currency = {'GBP': 'Great British Pound', 'USD':'United States Dollar', 'EUR':'Euro'}
        currency_df = currency_df.drop(columns=["created_at", "last_updated"])
        currency_df["currency_name"] = currency_df["currency_code"].map(currency)
        return currency_df
    except ClientError as e:
        logger.info(f"Alert: Failed in currency function: {str(e)}")

def staff(name, datestamp):
    try:
        staff_df = wr.s3.read_parquet(f"s3://{INGESTION_BUCKET}/staff/*/*/*/{name} {datestamp}")
        department_df = wr.s3.read_parquet(f's3://{INGESTION_BUCKET}/department/*', dateset=True)
        departmental_staff = pd.merge(staff_df, department_df, on="department_id")
        return departmental_staff[["staff_id", "first_name", "last_name", "department_name", "location", "email_address"]]
    except ClientError as e:
        logger.info(f"Alert: Failed in staff function: {str(e)}")


def counterparty(name, datestamp):
    try:
        address_df = wr.s3.read_parquet(f's3://{INGESTION_BUCKET}/address/*')
        cp_df = wr.s3.read_parquet(f's3://{INGESTION_BUCKET}/cp/*/*/*/{name} {datestamp}', dataset=True)
        dim_counterparty_df = pd.merge(cp_df, address_df, left_on="legal_address_id", right_on="address_id", how="inner")
        return dim_counterparty_df[["counterparty_id",
        "counterparty_legal_name",
        "address_line_1",
        "address_line_2",
        "district",
        "city",
        "postal_code",
        "country",
        "phone"
    ]].rename(columns={
        "address_line_1": "counterparty_legal_address_line_1",
        "address_line_2": "counterparty_legal_address_line_2",
        "district": "counterparty_legal_district",
        "city": "counterparty_legal_city",
        "postal_code": "counterparty_legal_postal_code",
        "country": "counterparty_legal_country",
        "phone": "counterparty_legal_phone_number"
    })
    except ClientError as e:
        logger.info(f"Alert: Failed in counterparty function: {str(e)}")

def location(name, datestamp):
    try:
        sales_order_df = wr.s3.read_parquet(f's3://{INGESTION_BUCKET}/location/*/*/*/{name} {datestamp}', dataset=True)
        address_df = wr.s3.read_parquet(f's3://{INGESTION_BUCKET}/address/*')
        dims_location_df = pd.merge(sales_order_df, address_df, left_on="agreed_delivery_location_id", right_on="address_id", how="inner")
        return dims_location_df[["agreed_delivery_location_id", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"]].rename(columns={"agreed_delivery_location_id": "location_id"})
    except ClientError as e:
        logger.info(f"Alert: Failed in location function: {str(e)}")


def date():
    start_date = datetime(2020, 1, 1)
    end_date=datetime.today()
    date_range = pd.date_range(start=start_date, end = end_date, freq='D')
    dim_date_df = pd.DataFrame(date_range, columns=['date_id'])
    dim_date_df['year'] = dim_date_df['date_id'].dt.year
    dim_date_df['month'] = dim_date_df['date_id'].dt.month
    dim_date_df['day'] = dim_date_df['date_id'].dt.day
    dim_date_df['day_of_week'] = dim_date_df['date_id'].dt.weekday
    dim_date_df['day_name'] = dim_date_df['date_id'].dt.day_name()
    dim_date_df['month_name'] = dim_date_df['date_id'].dt.month_name()
    dim_date_df['quarter'] = dim_date_df['date_id'].dt.quarter



def sales_order(name, datestamp):
    try:
        sales_order_df = wr.s3.read_parquet(f"s3://{INGESTION_BUCKET}/sales_order/*/*/*/{name} {datestamp}", dataset=True)
        sales_order_df["created_date"] = pd.to_datetime(sales_order_df["created_at"]).dt.date
        sales_order_df["created_time"] = pd.to_datetime(sales_order_df["created_at"]).dt.time
        sales_order_df["updated_date"] = pd.to_datetime(sales_order_df["last_updated"]).dt.date
        sales_order_df["updated_time"] = pd.to_datetime(sales_order_df["last_updated"]).dt.time
        return sales_order_df.drop(columns=["created_at", "last_updated"])
    except ClientError as e:
        logger.info(f"Alert: Failed in sales_order function: {str(e)}")

def get_object_path(records):
    """Extracts bucket and object references from Records field of event."""
    return records[0]["s3"]["bucket"]["name"], records[0]["s3"]["object"]["key"]

class InvalidFileTypeError(Exception):
    """Traps error where file type is not txt."""

    pass

def lambda_handler(event, context):
    """get bucket name and object name from event"""
    s3_bucket_name, s3_object_name = get_object_path(event["Records"])
    logger.info(f"Bucket is {s3_bucket_name}")
    logger.info(f"Object key is {s3_object_name}")
    
    tablename= s3_object_name.split('/', 5)[0]
    filename = s3_object_name.split('/', 5)[4]
    name, datestamp = filename.split('+')
    # if s3_object_name[-7:] != "parquet":
    #         raise InvalidFileTypeError
    valid_objects = ["sales_order", "design", "currency", "counterparty", "staff", "location"]

    if tablename not in valid_objects:
        logger.info(f"Pass: no actions required for {tablename}")
        logger.info(json.dumps(event, indent=2))
        return {
        'statusCode': 200,
        'body': json.dumps(f'{tablename} processed successfully')
        }
    
    logger.info(f'working on {tablename}')
    try:
        func_name = f"{tablename}(name, datestamp)"
        updated_df = eval(func_name)
        wr.s3.to_parquet(
        df=updated_df,
        path=f's3://{PROCESSED_BUCKET}/{tablename}/',
        #mode="overwrite",
        dataset=True
        )
    except ClientError as e:
        logger.info(f"Alert: Failed to call the function: {str(e)}")
    logger.info(f"{tablename} updated on process bucket")


