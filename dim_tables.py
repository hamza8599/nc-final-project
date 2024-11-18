import pandas as pd
import boto3
import awswrangler as wr
from datetime import datetime

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!CHANGE BUCKET NAME!!!!!!

s3 = boto3.client('s3')


"""Create dim_design"""
design_df = wr.s3.read_parquet("s3://dorota-test-team10-dimensional-transformers-ingestion-bucket/design/*", dataset=True)
new_design_df = design_df.drop(columns=['created_at', 'last_updated'])


"""Create dim_currency"""
currency_df = wr.s3.read_parquet("s3://dorota-test-team10-dimensional-transformers-ingestion-bucket/currency/*", dataset=True)
new_currency_df = currency_df.drop(columns=['created_at', 'last_updated'])
currency = {'GBP': 'Great British Pound', 'USD':'United States Dollar', 'EUR':'Euro'}
new_currency_df['currency_name'] = new_currency_df['currency_code'].map(currency)


"""Create dim_staff"""
staff_df = wr.s3.read_parquet("s3://dorota-test-team10-dimensional-transformers-ingestion-bucket/staff/*", dataset=True)
dept_df = wr.s3.read_parquet("s3://dorota-test-team10-dimensional-transformers-ingestion-bucket/department/*", dataset=True)
dim_staff_df = pd.merge(staff_df, dept_df, on="department_id")[["staff_id", "first_name", "last_name", "department_name", "location", "email_address"]]
# print(dim_staff_df)


"""Create dim_counterparty"""
address_df = wr.s3.read_parquet("s3://dorota-test-team10-dimensional-transformers-ingestion-bucket/address/*", dataset=True)
cp_df = wr.s3.read_parquet("s3://dorota-test-team10-dimensional-transformers-ingestion-bucket/counterparty/*", dataset=True)
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
# print(dim_counterparty_df.to_string())


"""Create dim_location"""
sales_order_df = wr.s3.read_parquet("s3://dorota-test-team10-dimensional-transformers-ingestion-bucket/sales_order/*", dataset=True)
dim_location_df = pd.merge(sales_order_df, address_df, left_on="agreed_delivery_location_id", right_on="address_id", how="inner")[[
    "agreed_delivery_location_id", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"
]].rename(columns={"agreed_delivery_location_id": "location_id"})
# print(dim_location_df.to_string())


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
# print(dim_date_df)


"""Create fact_sales_order"""
sales_order_df['created_date'] = pd.to_datetime(sales_order_df['created_at']).dt.date
sales_order_df['created_time'] = pd.to_datetime(sales_order_df['created_at']).dt.time
sales_order_df['updated_date'] = pd.to_datetime(sales_order_df['last_updated']).dt.date
sales_order_df['updated_time'] = pd.to_datetime(sales_order_df['last_updated']).dt.time
fact_sales_order_df = sales_order_df.drop(columns=['created_at', 'last_updated'])

# """checks for existing data processed bucket"""
# try:
#     existing_df = wr.s3.read_parquet("s3://dorota-test-team10-dimensional-transformers-process-bucket/design/dim_design", dataset=True)
#     combined = existing_df.merge(new_design_df, on='design_id')
# except wr.exceptions.NoFilesFound:
#     combined = new_design_df

"""Write processed data to process bucket"""
wr.s3.to_parquet(
    df=fact_sales_order_df,
    path='s3://dorota-test-team10-dimensional-transformers-process-bucket/fact_sales_order/fact_sales_order.parquet'
    # mode="overwrite",
    # dataset=True
)