from data_transformation import lambda_handler, read_parquet_files, writing_to_parquet, design, date, counterparty, currency, location, staff, sales_order, InvalidFileTypeError
from moto import mock_s3
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

@pytest.fixture()
def s3_client(aws_creds):
    with mock_s3():
        mock_s3 = boto3.client("s3", region_name="eu-west-2")
        mock_s3.create_bucket(Bucket="mock-bucket")
        yield mock_s3


@pytest.mark.skip
def test_parquet_files(s3_client):
    key = "design_table"
    writing_to_parquet(mock_design, "mock-bucket", key)
    df = read_parquet_files("mock-bucket", key)
    assert not df.empty
    assert list(df.columns) == ["design_id", "design_name", "file_location", "filename"]


def test_dim_design_table(s3_client):
    writing_to_parquet(mock_design_2, "mock-bucket", "design_table")
    design = design()
    assert list(design.columns) == ["design_id", "design_name", "file_location", "filename"]


def test_counter_party_merge(s3_client):

    wr.s3.to_parquet(df=mock_address, path="s3://mock-bucket/address/", dataset=True)
    wr.s3.to_parquet(df=mock_cp, path="s3://mock-bucket/cp/", dataset=True)
    
    result = counterparty("mock_counterparty.parquet", "2024-11-20")

    assert "counterparty_id" in result.columns
    assert "counterparty_legal_name" in result.columns
    assert "counterparty_legal_address_line_1" in result.columns
    assert "counterparty_legal_address_line_2" in result.columns
    assert "counterparty_legal_district" in result.columns
    assert "counterparty_legal_city" in result.columns
    assert "counterparty_legal_postal_code" in result.columns
    assert "counterparty_legal_country" in result.columns
    assert "counterparty_legal_phone_number" in result.columns

    assert result["counterparty_legal_name"].iloc[0] == "AMAZON"
    assert result["counterparty_legal_city"].iloc[1] == "London"


def test_staff_merge(s3_client):

    wr.s3.to_parquet(df=mock_staff, path="s3://mock-bucket/staff/", dataset=True)
    wr.s3.to_parquet(df=mock_department, path="s3://mock-bucket/department/", dataset=True)

    result = staff("mock_staff.parquet", "2024-11-20")

    assert result["department_name"].iloc[0] == "Northcoders"
    assert result["locatiun"].iloc[1] == "USA"

def test_location_merge(s3_client):

    wr.s3.to_parquet(df=mock_sales_order, path="s3://mock-bucket/location/", dataset=True)
    wr.s3.to_parquet(df=mock_address, path="s3://mock-bucket/address/", dataset=True)

    result = location("mock_location.parquet", "2024-11-20")

    assert result["location_id"].iloc[0] == 1
    assert result["city"].iloc[1] == "London"



def test_currency_mapping(s3_client):
    wr.s3.to_parquet(df=mock_currency, path="s3://mock-bucket/currency/", dataset=True)
    result = currency("mock_currency.parquet")
    assert "currency_name" in result.columns
    assert result["currency_name"].iloc[0] == "Great British Pound"
    assert result["currency_name"].iloc[1] == "United States Dollar"
    assert result["currency_name"].iloc[2] == "Euro"

def test_date_table():
    dim_date_df = date()
    assert "date_id" in dim_date_df.columns
    assert "year" in dim_date_df.columns
    assert "month" in dim_date_df.columns
    assert "day" in dim_date_df.columns
    assert "day_of_week" in dim_date_df.columns
    assert "day_name" in dim_date_df.columns
    assert "month_name" in dim_date_df.columns
    assert "quarter" in dim_date_df.columns

def test_missing_required_column(s3_client):   
    wrong_design_df = pd.DataFrame({
        "design_name": ["abc"], 
        "file_location": ["location"], 
        "filename": ["name"]
    })
    wr.s3.to_parquet(df=wrong_design_df, path="s3://mock-bucket/design/", dataset=True)

    with pytest.raises(KeyError):
        design("mock_invalid_design.parquet")



def test_lambda_handler(s3_client):
    mock_event = {
        "Records": [{
            "s3": {
                "bucket": {"name": "mock-bucket"},
                "object": {"key": "design design_file.parquet"}
            }
        }]
    }
    
    with mock.patch("data_transformation.design") as mock_design_func:
        response = lambda_handler(mock_event, {})

        mock_design_func.assert_called_once_with("design design_file.parquet")
        assert response["statusCode"] == 200
        assert "design processed successfully" in response["body"]


def test_lambda_handler_invalid_file_type(s3_client):
    """Test lambda_handler raises InvalidFileTypeError for invalid file type."""
    mock_event_invalid = {
        "Records": [{
            "s3": {
                "bucket": {"name": "mock-bucket"},
                "object": {"key": "design design_file.txt"}
            }
        }]
    }
    
    with pytest.raises(InvalidFileTypeError):
        lambda_handler(mock_event_invalid, {})



def test_lambda_handler_no_action_required(s3_client):
    mock_event_no_action = {
        "Records": [{
            "s3": {
                "bucket": {"name": "mock-bucket"},
                "object": {"key": "please_fail.parquet"}
            }
        }]
    }
    
    response = lambda_handler(mock_event_no_action, {})
    assert "Pass: no actions required" in response["body"]

mocked_now = datetime(2024, 11, 20)

@pytest.mark.parametrize("datetime_mocked", [mocked_now])
def test_date_table(datetime_mocked):
    with mock.patch('data_transformation.datetime') as mock_datetime:                  #
        mock_datetime.now.return_value = datetime_mocked  


        dim_date_df = date()  

        
        assert "date_id" in dim_date_df.columns
        assert "year" in dim_date_df.columns
        assert dim_date_df['year'].iloc[0] == 2024  
        assert dim_date_df['month'].iloc[0] == 11  
        assert dim_date_df['day'].iloc[0] == 20  



# def writing_to_parquet(df, bucket, key):kfgjbsskd
#     wr.s3.to_parquet(
#         df=df,
#         path=f"s3://{bucket}/{key}",
#         dataset=True
#     )
# def read_parquet_files(bucket, key):
#     return wr.s3.read_parquet(f"s3://{bucket}/{key}", dataset=True)
# @pytest.mark.skip
# def test_writing_to_parquet(s3_client):
#     writing_to_parquet(mock_design, "mock-bucket", "design_table")
#     df = wr.s3.read_parquet("s3://mock-bucket/*", dataset=True)
#     assert not df.empty
#     # assert df.equals(mock_design)