import boto3
import pyarrow.parquet as pa
import pandas as pd

#s3 = boto3.client('s3')

table = pa.read_table('facts_sales_orders.parquet') 
#print(table.shape)
df = table.to_pandas() 
# Taking tanspose so the printing dataset will easy. 
print(df.head().T)


df = pd.read_parquet('facts_sales_orders (1).parquet') 
print(df.to_string())