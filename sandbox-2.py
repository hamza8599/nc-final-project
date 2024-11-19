import awswrangler as wr
import pandas as pd
from datetime import datetime
import boto3

df = wr.s3.read_parquet("s3://team-12-dimensional-transformers-ingestion-bucket/sales_order/*", dataset=True)
print(df.to_string())



# split the Name column into two columns
df['created_date'] = pd.to_datetime(df['created_at']).dt.date
df['created_time'] = pd.to_datetime(df['created_at']).dt.time
df['updated_date'] = pd.to_datetime(df['last_updated']).dt.date
df['updated_time'] = pd.to_datetime(df['last_updated']).dt.time
new_df = df.drop(columns=['created_at', 'last_updated'])
s3= boto3.client('s3')
# view the updated DataFrame
print(new_df.to_string())
try:
    existing_df = wr.s3.read_parquet("s3://team-12-dimensional-transformers-process-bucket/facts_sales_orders.parquet", dataset=True)
    combined = existing_df.merge(new_df, on = 'sales_order_id')
except wr.exceptions.NoFilesFound:
    combined = new_df

wr.s3.to_parquet(
    df=combined,
    path='s3://team-12-dimensional-transformers-process-bucket/facts_sales_orders.parquet',
)