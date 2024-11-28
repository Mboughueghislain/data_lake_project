import pymysql
from kafka import KafkaProducer
import json
import time
import os

batch_size = 2
connection = pymysql.connect(
    host="localhost",
    user="datalakedb",
    password="Datalake101@",
    database="datalakedb"
)

with connection.cursor() as cursor:
    cursor.execute("SELECT * FROM transactions")
    transactions = cursor.fetchall()
print(transactions)

def chunk_data(data, batch_size=2):
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]



print("All data sent to Kafka.")

