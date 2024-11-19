from data_transformation import lambda_handler, read_parquet_files, writing_to_parquet, create_dim_design, create_dim_date, create_dim_counterparty, create_dim_currency, create_dim_location, create_dim_staff, create_facts_table
from moto import mock_aws
import pandas as pd
import boto3
import awswrangler as wr
import os
import pytest
from datetime import datetime

@pytest.fixture()
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture()
def s3_client(aws_creds):
    with mock_aws():
        mock_s3 = boto3.client("s3")
        mock_s3.create_bucket(
            Bucket="mock-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield mock_s3


mock_design = pd.DataFrame({"design_id": [1], "design_name": ["abc"], 
                            "file_location": ["path"], "filename": ["path name"]})

def test_writing_to_parquet(s3_client):
    writing_to_parquet(mock_design, "mock-bucket", "design_table")
    df = wr.s3.read_parquet("s3://mock-bucket/*", dataset=True)
    assert not df.empty
    # assert df.equals(mock_design)

def test_parquet_files(s3_client):
    key = "design_table"
    writing_to_parquet(mock_design, "mock-bucket", key)
    df = read_parquet_files("mock-bucket", key)
    assert not df.empty
    assert list(df.columns) == ["design_id", "design_name", "file_location", "filename"]

mock_design_2 = pd.DataFrame({"design_id": [1], "design_name": ["abc"], 
                            "file_location": ["path"], "filename": ["path name"],
                            "created_at": [datetime.now()], "last_updated": [datetime.now()]})

def test_dim_design_table(s3_client):
    writing_to_parquet(mock_design_2, "mock-bucket", "design_table")
    design = create_dim_design()
    assert list(design.columns) == ["design_id", "design_name", "file_location", "filename"]