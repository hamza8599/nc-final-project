from data_extraction import connect_db


conn = connect_db("dat-warehouse-creds")
tables = ["sales_record", "design", "date", "staff", "location", "counterparty", "currency"]
for table in tables:
    conn.run(f"ALTER SEQUENCE {table}_id_seq RESTART WITH 1")
