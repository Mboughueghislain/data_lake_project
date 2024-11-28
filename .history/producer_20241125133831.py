import pymysql

connection = pymysql.connect(
    host="localhost",
    user="your_username",
    password="your_password",
    database="your_database"
)

with connection.cursor() as cursor:
    cursor.execute("SELECT * FROM transactions")
    results = cursor.fetchall()
print(results)