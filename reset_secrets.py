import json
import boto3
from pg8000.native import Connection
from botocore.exceptions import ClientError

def reset_secrets(sm_client, secret_name):
    try:
        response = sm_client.list_secrets(MaxResults=100)
        for resp in response["SecretList"]:
            if secret_name == resp["Name"]:
                response = sm_client.put_secret_value(
                    SecretId=secret_name, SecretString="2020-11-14 09:41:09.839000"
                )

    except ClientError as e:
        print(e)

secrets_manager_client = boto3.client("secretsmanager", region_name = 'eu-west-2')
response = secrets_manager_client.get_secret_value(SecretId='psql-creds')
secret_dict = json.loads(response["SecretString"])
user = secret_dict["username"]
pswd = secret_dict["password"]
db = secret_dict["dbname"]
port = secret_dict["port"]
host = secret_dict["host"]
conn = Connection(user=user, password=pswd, database=db, host=host, port=port)
table_names = conn.run(
    "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name NOT LIKE '%prisma%';"
)
for table in table_names:
    reset_secrets(secrets_manager_client, table[0])
conn.close()

#secrets_manager_client = boto3.client("secretsmanager")
response = secrets_manager_client.get_secret_value(SecretId='data-warehouse-creds')
secret_dict = json.loads(response["SecretString"])
user = secret_dict["username"]
pswd = secret_dict["password"]
db = secret_dict["dbname"]
port = secret_dict["port"]
host = secret_dict["host"]
conn = Connection(user=user, password=pswd, database=db, host=host, port=port)

tables = [
    "fact_sales_order",
    "dim_date",
    "dim_design",
    "dim_location",
    "dim_counterparty",
    "dim_currency",
    "dim_staff",
]
for table in tables:
    conn.run(f"DELETE FROM {table};")
conn.close()
