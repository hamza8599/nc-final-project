from data_extraction import store_secret,write_table_to_parquet_buffer,get_created_date,reset_secrets,existing_secret,lambda_handler,connect_db,close_db
from data_extraction import format_to_parquet
import pyarrow as pa
from moto import mock_aws
import boto3
import pytest
import os
from io import BytesIO
from unittest.mock import MagicMock,patch
import json


@pytest.fixture()
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture()
def sm_client(aws_creds):
    with mock_aws():
        mock_sm = boto3.client("secretsmanager")
        yield mock_sm

@pytest.fixture()
def s3_client(aws_creds):
    with mock_aws():
        mock_s3 = boto3.client("s3")
        yield mock_s3

def test_existing_secrets_false_if_no_secret_exists(sm_client):
    secret_name="sales_order"
    response=existing_secret(sm_client,secret_name)
    assert response==False


def test_existing_secrets_true_if_secret_exists(sm_client):
     sm_client.create_secret(
                    Name="northcoders",  
                    SecretString='pa55word',  
                )
     
     secret_name="northcoders"
     response=existing_secret(sm_client,secret_name)
     assert response==True

def test_rest_secrets_restes_values(sm_client):
    sm_client.create_secret(
                    Name="northcoders",  
                    SecretString='pa55word',  
                )
    reset_secrets(sm_client,'northcoders')
    response = sm_client.get_secret_value(SecretId='northcoders')
    assert response['SecretString']=='2020-11-14 09:41:09.839000'

def test_get_creted_date_returns_creted_at_date():
    data = [
        [1, '2020-11-14 09:41:09.839000', '2020-11-14 09:41:09.839000', 'test', 'test', 'test']
    ]
    columns=['design_id','created_at','last_updated','design_name','file_location','file_name']
    response=get_created_date(data,columns)
    assert response=='2020-11-14 09:41:09.839000'

def test_write_table_to_parquet_buffer_format():
      data = {
        "column1": [1, 2, 3],
        "column2": ["a", "b", "c"]
    }
      pyarrow_table=pa.table(data)
      response=write_table_to_parquet_buffer(pyarrow_table)
      assert isinstance(response,BytesIO)
      
def test_store_secrets_new_secret_if_not_exist(sm_client):
    secret_name='northcoders'
    last_updated='2020-11-14 09:41:09.839000'
    store_secret(secret_name,last_updated,sm_client)
    response = sm_client.get_secret_value(SecretId='northcoders')
    assert response['SecretString']=='2020-11-14 09:41:09.839000'

def test_store_secrets_update_secret_if_already_exist(sm_client):
    sm_client.create_secret(
                    Name="northcoders",  
                    SecretString='pa55word',  
                )
    secret_name='northcoders'
    last_updated='2020-11-14 09:41:09.839000'
    store_secret(secret_name,last_updated,sm_client)
    response = sm_client.get_secret_value(SecretId='northcoders')
    assert response['SecretString']=='2020-11-14 09:41:09.839000'


@patch("data_extraction.boto3.client")
def test_connect_db(mock_boto_client):
    mock_secrets_client = MagicMock()
    mock_boto_client.return_value = mock_secrets_client
    mock_secrets_client.get_secret_value.return_value = {
        "SecretString": json.dumps({
            "username": "testuser",
            "password": "testpass",
            "dbname": "testdb",
            "host": "localhost",
            "port": 5432
        })
    }

    class MockDBConnection:
        def __init__(self, user, password, database, host, port):
            self.user = user
            self.password = password
            self.database = database
            self.host = host
            self.port = port

    with patch("data_extraction.Connection", MockDBConnection):
        conn = connect_db("test_secret")
        assert conn.user == "testuser"
        assert conn.password == "testpass"
        assert conn.database == "testdb"
        assert conn.host == "localhost"
        assert conn.port == 5432

def test_close_db():
    mock_conn = MagicMock()
    close_db(mock_conn)
    mock_conn.close.assert_called_once()

    



# def test_format_to_parquet_returns_formated_data():
#     data=[[1],['2020-11-14 09:41:09.839000'],['2020-11-14 09:41:09.839000'],['test']]
#     formated_data=format_to_parquet(data,conn)
#     close_db(conn)
#     assert isinstance(formated_data,pyarrow.Table)

    


