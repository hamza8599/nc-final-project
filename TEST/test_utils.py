from DB.data_extraction import data_extract
from DB.utils import format_to_parquet
from DB.connection import connect_db,close_db
import pyarrow

def test_format_to_parquet_returns_formated_data():
    conn=connect_db()
    data=conn.run("SELECT * from sales_order;")
    formated_data=format_to_parquet(data,conn)
    close_db(conn)
    assert isinstance(formated_data,pyarrow.Table)




