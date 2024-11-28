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

for transaction in transactions:
    producer.send("transactions", batch)
    print(f"Sent batch: {batch}")
    time.sleep(10)

print("All data sent to Kafka.")
producer.flush()
producer.close()

