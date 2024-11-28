import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("FileProcessingAndVersioning") \
    .getOrCreate()

# Répertoires de travail
raw_dir = "hdfs://localhost:9000/transactions/raw"  # Répertoire des fichiers d'entrée
staging_dir = "hdfs://localhost:9000/transactions/staging"  # Répertoire de staging pour Parquet
result_dir = "hdfs://localhost:9000/transactions/result"  # Répertoire des résultats

# Fonction de traitement des fichiers dans raw
def process_files():
    # Liste des fichiers dans raw (par exemple JSON, CSV, TXT)
    files = os.listdir(raw_dir)

    for file in files:
        file_path = os.path.join(raw_dir, file)

        # Détecter le format du fichier par son extension
        if file.endswith('.json'):
            process_json(file_path)
        elif file.endswith('.csv'):
            process_csv(file_path)
        elif file.endswith('.txt'):
            process_txt(file_path)

def process_json(file_path):
    try:
        print(f"Processing JSON file: {file_path}")
        
        # Lire le fichier JSON
        df = spark.read.option("multiline", "true").json(file_path)
        
        # Ajouter un horodatage ou version (optionnel)
        df = df.withColumn("processed_time", current_timestamp())
        
        # Sauvegarder en Parquet dans le dossier staging
        output_parquet = os.path.join(staging_dir, "json", f"result_json_{int(time.time())}")
        df.write.mode("append").parquet(output_parquet)
        
        # Sauvegarder dans un fichier de résultats consolidé
        result_file = os.path.join(result_dir, "result_json")
        df.write.mode("append").parquet(result_file)
        
        print(f"JSON processed and saved to {output_parquet} and {result_file}")
    except Exception as e:
        print(f"Error processing JSON file {file_path}: {str(e)}")

def process_csv(file_path):
    try:
        print(f"Processing CSV file: {file_path}")
        
        # Lire le fichier CSV
        df = spark.read.option("header", "true").csv(file_path)
        
        # Ajouter un horodatage ou version (optionnel)
        df = df.withColumn("processed_time", current_timestamp())
        
        # Sauvegarder en Parquet dans le dossier staging
        output_parquet = os.path.join(staging_dir, "csv", f"result_csv_{int(time.time())}")
        df.write.mode("append").parquet(output_parquet)
        
        # Sauvegarder dans un fichier de résultats consolidé
        result_file = os.path.join(result_dir, "result_csv")
        df.write.mode("append").parquet(result_file)
        
        print(f"CSV processed and saved to {output_parquet} and {result_file}")
    except Exception as e:
        print(f"Error processing CSV file {file_path}: {str(e)}")

def process_txt(file_path):
    try:
        print(f"Processing TXT file: {file_path}")
        
        # Lire le fichier TXT
        df = spark.read.text(file_path)
        
        # Ajouter un horodatage ou version (optionnel)
        df = df.withColumn("processed_time", current_timestamp())
        
        # Sauvegarder en Parquet dans le dossier staging
        output_parquet = os.path.join(staging_dir, "txt", f"result_txt_{int(time.time())}")
        df.write.mode("append").parquet(output_parquet)
        
        # Sauvegarder dans un fichier de résultats consolidé
        result_file = os.path.join(result_dir, "result_txt")
        df.write.mode("append").parquet(result_file)
        
        print(f"TXT processed and saved to {output_parquet} and {result_file}")
    except Exception as e:
        print(f"Error processing TXT file {file_path}: {str(e)}")

# Fonction pour surveiller et traiter les fichiers
def monitor_and_process_files():
    while True:
        process_files()
        time.sleep(15)  # Vérifier toutes les 15 secondes

# Lancer le processus
monitor_and_process_files()
