import pandas as pd
from datetime import datetime
from data_extraction import connect_db

conn = connect_db("psql_creds")


"""Create dim_design table"""

design_data = conn.run('''SELECT * FROM design;''')
columns = [col["name"] for col in conn.columns]
design_df = pd.DataFrame(design_data, columns=columns)
dim_design_df = design_df[["design_id", "design_name", "file_location", "file_name"]]
# print(dim_design_df)


"""Create dim_currency table"""

currency_data = conn.run('''SELECT * FROM currency;''')
columns = [col["name"] for col in conn.columns]
currency_df = pd.DataFrame(currency_data, columns=columns)
dim_currency_df = currency_df[["currency_id", "currency_code"]]
# print(dim_currency_df)


"""Create dim_staff table"""

staff_data = conn.run("SELECT * FROM staff;")
staff_columns = [col["name"] for col in conn.columns]
staff_df = pd.DataFrame(staff_data, columns=staff_columns)
dept_data = conn.run("SELECT * FROM department;")
dept_columns = [col["name"] for col in conn.columns]
dept_df = pd.DataFrame(dept_data, columns=dept_columns)
dim_staff_df = pd.merge(staff_df, dept_df, on="department_id")[["staff_id", "first_name", "last_name", "department_name", "location", "email_address"]]
# print(dim_staff_df)


"""Create dim_counterparty table"""

address_data = conn.run("SELECT * FROM address;")
address_columns = [col["name"] for col in conn.columns]
address_df = pd.DataFrame(address_data, columns=address_columns)
counterparty_data = conn.run("SELECT * FROM counterparty;")
cp_columns = [col["name"] for col in conn.columns]
cp_df = pd.DataFrame(counterparty_data, columns=cp_columns)
dim_counterparty_df = pd.merge(cp_df, address_df, left_on="legal_address_id",right_on="address_id", how="inner")[["counterparty_id", "counterparty_legal_name", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"]].rename(columns={"address_line_1": "counterparty_legal_address_line_1","address_line_2": "counterparty_legal_address_line_2", "district":"counterparty_legal_district", "city":"counterparty_legal_city", "postal_code":"counterparty_legal_postal_code", "country":"counterparty_legal_country", "phone":"counterparty_legal_phone_number"})
# print(dim_counterparty_df)


"""Create dim_location"""

sales_order_data = conn.run("SELECT * FROM sales_order;")
sales_order_cols = [col["name"] for col in conn.columns]
sales_order_df = pd.DataFrame(sales_order_data, columns=sales_order_cols)
dim_location_df = pd.merge(sales_order_df, address_df, left_on="agreed_delivery_location_id",right_on="address_id", how="inner")[["agreed_delivery_location_id", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"]].rename(columns={"agreed_delivery_location_id":"location_id"})
# print(dim_location_df)


"""Create dim_date table"""
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
print(dim_date_df)