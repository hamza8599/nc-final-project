s3_object_name = "sales/12/15/10/sales-12350"
tablename = s3_object_name.split('/', 5)[0]

print(tablename)