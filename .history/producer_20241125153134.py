import pymysql

connection = pymysql.connect(
    host="localhost",
    user="datalakeuser",
    password="Datalake101@",
    database="datalakedb"
)

with connection.cursor() as cursor:
    cursor.execute("SELECT * FROM transactions")
    results = cursor.fetchall()
print(results)