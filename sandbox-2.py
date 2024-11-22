import pandas as pd
df = pd.read_parquet("staff.parquet")
print(df.to_string())