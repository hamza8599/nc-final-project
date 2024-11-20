import pandas as pd
df = pd.read_parquet("/Users/hamzashoukat/Desktop/new-northcoders/nc-final-project/date.parquet")
print(df.to_string())