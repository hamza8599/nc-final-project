import pandas as pd
import pyarrow as pa
def format_to_parquet(data,conn):
    columns = [col["name"] for col in conn.columns]
    df=pd.DataFrame(data,columns=columns)
    table=pa.Table.from_pandas(df)
    # table=df.to_parquet('output_data.parquet', engine='pyarrow')
    print(type(table))
    return table
    