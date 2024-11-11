from DB.connection import connect_db,close_db
from DB.utils import format_to_parquet
import boto3


def data_extract():
    conn=connect_db()
    query='SELECT * FROM sales_order'
    data=conn.run(query)
    formated_data=format_to_parquet(data,conn)
    client=boto3.client('s3')

    return data

data_extract()