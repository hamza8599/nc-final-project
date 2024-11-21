from data_transformation import lambda_handler,  design, dim_date, counterparty, currency, address, staff, sales_order, InvalidFileTypeError
from moto import mock_aws
from unittest import mock 
import pandas as pd
import boto3
import awswrangler as wr
import os
import pytest
from datetime import datetime


mock_design_2 = pd.DataFrame({
    "design_id": [1], 
    "design_name": ["abc"], 
    "file_location": ["path"], 
    "filename": ["path name"],
    "created_at": [datetime.now()], 
    "last_updated": [datetime.now()]
})

mock_design = pd.DataFrame({
    "design_id": [1], 
    "design_name": ["abc"], 
    "file_location": ["path"], 
    "filename": ["path name"]
})

mock_currency = pd.DataFrame({
    "currency_code": ['GBP', 'USD'], 
    "created_at": [datetime.now(), datetime.now()], 
    "last_updated": [datetime.now(), datetime.now()]
})

mock_staff = pd.DataFrame({
    "staff_id": [1, 2], 
    "first_name": ["will", "bill"], 
    "last_name": ["robb", "bobb"], 
    "department_id": [63, 36], 
    "location": ["London", "USA"], 
    "email_address": ["willrobb63@bmail.com", "billbobb@pmail.com"]
})

mock_address = pd.DataFrame({
    "address_id": [1, 2, 3], 
    "address_line_1": ["sesame street", "number 10", "bikini bottom"], 
    "city": ["muppets city", "London", "the sea"], 
    "postal_code": ["12345", "w1 4cd", "456Seaway"], 
    "country": ["USA", "UK", "belgium"], 
    "phone": ["123456789", "987654321", "123987543"]
})

mock_cp = pd.DataFrame({
    "counterparty_id": [1, 2], 
    "counterparty_legal_name": ["AMAZON", "NORTHCODERS"], 
    "legal_address_id": [1, 2], 
    "phone": ["123456789", "987654321"]
})

mock_sales_order = pd.DataFrame({
    "sales_order_id": [1, 2], 
    "created_at": [datetime.now(), datetime.now()], 
    "last_updated": [datetime.now(), datetime.now()],                                          # Variables folder??????????????????????
    "agreed_delivery_location_id": [1, 2]
})

mock_department = pd.DataFrame({
    "department_id": [63, 36], 
    "department_name": ["Northcoders", "CodersNorth"]
})

@pytest.fixture()
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
    os.environ['INGESTION_BUCKET'] = 'default-ingestion-bucket'  # Correct bucket name
    os.environ['PROCESSED_BUCKET'] = 'default-processed-bucket'


@pytest.fixture(autouse=True)
def s3_client(aws_creds):
    with mock_aws():
        # Create the required buckets in the mock S3 environment
        mock_s3 = boto3.client("s3")
        mock_s3.create_bucket(
            Bucket="default-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        mock_s3.create_bucket(
            Bucket="default-processed-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        
        # Now mock the upload of files
        yield mock_s3




def test_staff_merge(s3_client):
    # Upload mock data to S3
    wr.s3.to_parquet(df=mock_staff, path="s3://default-ingestion-bucket/staff/a/a/a/staff 2024-11-20", dataset=False)
    wr.s3.to_parquet(df=mock_department, path="s3://default-ingestion-bucket/department/a/a/a/department 2024-11-20", dataset=False)

    result = staff("staff", "2024-11-20")
  
    assert result["department_name"].iloc[0] == "Northcoders"
    assert result["first_name"].notnull().all(), "First name has null values"
    assert result["department_name"].nunique() == 2, "More than one department in the result"

    
    