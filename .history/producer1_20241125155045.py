import pymysql
from kafka import KafkaProducer
import json
import time
import os

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

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

print(f"Batch {i // batch_size + 1} sent to Kafka")

print("All data sent to Kafka.")

