s3_object_name = "sales/12/15/10/sales-12350"
tablename = s3_object_name.split('/', 5)[0]

print(tablename)
import awswrangler as wr
import pandas as pd
df = pd.read_parquet("d9dda52ec67f4ef7bd6e8b2a30171575.snappy.parquet")
print(df.to_string())