from data_transformation import lambda_handler, design, date, counterparty, currency,location, staff, sales_order
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
# @pytest.mark.skip
# def test_writing_to_parquet(s3_client):
#     # writing_to_parquet(mock_design, "mock-bucket", "design_table")
#     df = wr.s3.read_parquet("s3://mock-bucket/*", dataset=True)
#     assert not df.empty
#     # assert df.equals(mock_design)
    
# @pytest.mark.skip
# def test_parquet_files(s3_client):
#     key = "design_table"
#     # writing_to_parquet(mock_design, "mock-bucket", key)
#     df = read_parquet_files("mock-bucket", key)
#     assert not df.empty
#     assert list(df.columns) == ["design_id", "design_name", "file_location", "filename"]

# mock_design_2 = pd.DataFrame({"design_id": [1], "design_name": ["abc"], 
#                             "file_location": ["path"], "filename": ["path name"],
#                             "created_at": [datetime.now()], "last_updated": [datetime.now()]})

# def test_dim_design_table(s3_client):
#     # writing_to_parquet(mock_design_2, "mock-bucket", "design_table")
#     design = design()
#     assert list(design.columns) == ["design_id", "design_name", "file_location", "filename"]



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
    "last_updated": [datetime.now(), datetime.now()],
    "agreed_delivery_location_id": [1, 2]
})
mock_department = pd.DataFrame({
    "department_id": [63, 36],
    "department_name": ["Northcoders", "CodersNorth"]
})

# def test_counter_party_merge(s3_client):
#     s3_client.
#     wr.s3.to_parquet(df=mock_address, path="s3://mock-bucket/address/", dataset=True)
#     wr.s3.to_parquet(df=mock_cp, path="s3://mock-bucket/cp/", dataset=True)
#     result = counterparty("mock_counterparty.parquet", "2024-11-20")
#     print(f"==>> result: {result}")
#     # assert "counterparty_id" in result.columns
#     # assert "counterparty_legal_name" in result.columns
#     # assert "counterparty_legal_address_line_1" in result.columns
#     # assert "counterparty_legal_address_line_2" in result.columns
#     # assert "counterparty_legal_district" in result.columns
#     # assert "counterparty_legal_city" in result.columns
#     # assert "counterparty_legal_postal_code" in result.columns
#     # assert "counterparty_legal_country" in result.columns
#     # assert "counterparty_legal_phone_number" in result.columns
#     # assert result["counterparty_legal_name"].iloc[0] == "AMAZON"
#     # assert result["counterparty_legal_city"].iloc[1] == "London"