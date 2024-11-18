from data_extraction import connect_db, reset_secrets
import boto3

conn = connect_db("psql_creds")
secret_manager_client = boto3.client("secretsmanager")
table_names = conn.run(
    "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name NOT LIKE '%prisma%';"
)
for table in table_names:
    reset_secrets(secret_manager_client, table[0])
