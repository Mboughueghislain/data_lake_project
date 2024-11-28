import pymysql
from kafka import KafkaProducer
import json
import time
from decimal import Decimal

# Fonction de sérialisation pour convertir les objets Decimal en float
def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=decimal_to_float).encode('utf-8')
)

# Paramètres de la base de données
batch_size = 2
connection = pymysql.connect(
    host="localhost",
    user="datalakedb",
    password="Datalake", 
    database="datala"
)

# Fonction pour récupérer les données depuis MySQL et les envoyer en batches
def fetch_and_send_data():
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM transactions")
        transactions = cursor.fetchall()

    # Vous pouvez adapter les noms de colonnes à votre propre table
    column_names = [desc[0] for desc in cursor.description]  # Récupère les noms de colonnes

    # Envoie les données par batch
    for i in range(0, len(transactions), batch_size):
        batch = transactions[i:i + batch_size]

        for transaction in batch:
            # Crée un dictionnaire à partir de chaque ligne (tuple)
            transaction_dict = dict(zip(column_names, transaction))

            # Sérialisation et envoi de la transaction au topic Kafka
            producer.send("transactions", transaction_dict)
            print(f"Sent batch: {transaction_dict}")
        
        time.sleep(10)  # Attendre 10 secondes avant d'envoyer le prochain batch

    print("All data sent to Kafka.")

# Exécution de la fonction
fetch_and_send_data()

# Fermeture du producteur Kafka
producer.flush()
producer.close()

# Fermeture de la connexion MySQL
connection.close()