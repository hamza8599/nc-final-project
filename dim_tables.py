import pandas as pd
import boto3
import awswrangler as wr
from datetime import datetime


s3 = boto3.client('s3')
ingestion_bucket_name = ""
process_bucket_name = ""
table_name = ""
"""get bucket name and object name from event"""
# s3_bucket_name, s3_object_name = get_object_path(event["Records"])
# logger.info(f"Bucket is {s3_bucket_name}")
# logger.info(f"Object key is {s3_object_name}")

"""Read Parquet from ingestion bucket"""
df = wr.s3.read_parquet(f"s3://{ingestion_bucket_name}/{table_name}/*", dataset=True)

"""checks for existing data processed bucket"""
# try:
#     existing_df = df
#     combined = existing_df.merge(new_design_df, on='design_id')
# except wr.exceptions.NoFilesFound:
#     combined = new_design_df

"""Write processed data to process bucket"""
wr.s3.to_parquet(
    df=df,
    path=f's3://{process_bucket_name}/{table_name}/{table_name}.parquet'
    # mode="overwrite",
    # dataset=True
)

"""Create dim_design"""
design_df = df.drop(columns=['created_at', 'last_updated'])

"""Create dim_currency"""
currency_df = df.drop(columns=['created_at', 'last_updated'])
currency = {'GBP': 'Great British Pound', 'USD':'United States Dollar', 'EUR':'Euro'}
currency_df['currency_name'] = currency_df['currency_code'].map(currency)

"""Create dim_staff"""
staff_df = wr.s3.read_parquet(f's3://{ingestion_bucket_name}/staff/*', dataset=True)
dept_df = wr.s3.read_parquet(f's3://{ingestion_bucket_name}/department/*', dataset=True)
dim_staff_df = pd.merge(staff_df, dept_df, on="department_id")[["staff_id", "first_name", "last_name", "department_name", "location", "email_address"]]

"""Create dim_counterparty"""
address_df = wr.s3.read_parquet(f's3://{ingestion_bucket_name}/address/*', dataset=True)
cp_df = wr.s3.read_parquet(f's3://{ingestion_bucket_name}/counterparty/*', dataset=True)
dim_counterparty_df = pd.merge(cp_df, address_df, left_on="legal_address_id", right_on="address_id", how="inner")[[
    "counterparty_id",
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

"""Create dim_location"""
sales_order_df = wr.s3.read_parquet(f"s3://{ingestion_bucket_name}/sales_order/*", dataset=True)
dim_location_df = pd.merge(sales_order_df, address_df, left_on="agreed_delivery_location_id", right_on="address_id", how="inner")[[
    "agreed_delivery_location_id", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"
]].rename(columns={"agreed_delivery_location_id": "location_id"})

"""Create dim_date"""
start_date = datetime(2020, 1, 1)
end_date = datetime.today()
date_range = pd.date_range(start=start_date, end=end_date, freq='D')
dim_date_df = pd.DataFrame(date_range, columns=['date_id'])
dim_date_df['year'] = dim_date_df['date_id'].dt.year
dim_date_df['month'] = dim_date_df['date_id'].dt.month
dim_date_df['day'] = dim_date_df['date_id'].dt.day
dim_date_df['day_of_week'] = dim_date_df['date_id'].dt.weekday
dim_date_df['day_name'] = dim_date_df['date_id'].dt.day_name()
dim_date_df['month_name'] = dim_date_df['date_id'].dt.month_name()
dim_date_df['quarter'] = dim_date_df['date_id'].dt.quarter

"""Create fact_sales_order"""
sales_order_df['created_date'] = pd.to_datetime(sales_order_df['created_at']).dt.date
sales_order_df['created_time'] = pd.to_datetime(sales_order_df['created_at']).dt.time
sales_order_df['updated_date'] = pd.to_datetime(sales_order_df['last_updated']).dt.date
sales_order_df['updated_time'] = pd.to_datetime(sales_order_df['last_updated']).dt.time
fact_sales_order_df = sales_order_df.drop(columns=['created_at', 'last_updated'])
