from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import time
from datetime import datetime

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("Data Processing") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Fonction pour lire et traiter un fichier JSON
def process_json(file_path):
    hdfs_path = f"hdfs://localhost:9000{file_path}"  # Utilisation du chemin HDFS complet
    df = spark.read.json(hdfs_path)
    df = df.select(
        col("Post ID").alias("post_id"),
        col("Username").alias("username"),
        col("Platform").alias("platform"),
        col("Timestamp").alias("timestamp"),
        col("Content").alias("content"),
        col("Sentiment").alias("sentiment"),
        col("Likes").alias("likes"),
        col("Comments").alias("comments"),
        col("Shares").alias("shares"),
        col("Tags").alias("tags")
    )
    return df

# Fonction pour lire et traiter un fichier TXT (CSV)
def process_txt(file_path):
    hdfs_path = f"hdfs://localhost:9000{file_path}"  # Utilisation du chemin HDFS complet
    df = spark.read.option("header", "true").csv(hdfs_path)
    df = df.select(
        col("Post ID").alias("post_id"),
        col("Username").alias("username"),
        col("Platform").alias("platform"),
        col("Timestamp").alias("timestamp"),
        col("Content").alias("content"),
        col("Sentiment").alias("sentiment"),
        col("Likes").alias("likes"),
        col("Comments").alias("comments"),
        col("Shares").alias("shares"),
        col("Tags").alias("tags")
    )
    return df

# Fonction pour lire et traiter un fichier CSV
def process_csv(file_path):
    hdfs_path = f"hdfs://localhost:9092{file_path}"  # Utilisation du chemin HDFS complet
    df = spark.read.option("header", "true").csv(hdfs_path)
    df = df.select(
        col("transaction_id").alias("transaction_id"),
        col("customer_id").alias("customer_id"),
        col("customer_name").alias("customer_name"),
        col("email").alias("email"),
        col("phone").alias("phone"),
        col("address").alias("address"),
        col("transaction_status").alias("transaction_status"),
        col("payment_method").alias("payment_method"),
        col("total_amount").alias("total_amount"),
        col("currency").alias("currency"),
        col("shipping_fee").alias("shipping_fee"),
        col("discount").alias("discount"),
        col("transaction_details").alias("transaction_details")
    )
    return df

# Fonction de traitement des fichiers
def process_files(file_type):
    file_paths = []
    if file_type == "json":
        # Lister les fichiers JSON dans HDFS
        files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
            .listStatus(spark._jvm.org.apache.hadoop.fs.Path("hdfs://localhost:9092/transactions/raw/json"))
        file_paths = [file.getPath().toString() for file in files]
        process_func = process_json
    elif file_type == "txt":
        # Lister les fichiers TXT dans HDFS
        files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
            .listStatus(spark._jvm.org.apache.hadoop.fs.Path("hdfs://localhost:9092/transactions/raw/txt"))
        file_paths = [file.getPath().toString() for file in files]
        process_func = process_txt
    elif file_type == "csv":
        # Lister les fichiers CSV dans HDFS
        files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
            .listStatus(spark._jvm.org.apache.hadoop.fs.Path("hdfs://localhost:9092/transactions/raw/csv"))
        file_paths = [file.getPath().toString() for file in files]
        process_func = process_csv
    
    # Traiter chaque fichier
    for file_path in file_paths:
        print(f"Processing {file_path}...")
        try:
            df = process_func(file_path)
            print(f"DataFrame for {file_path} created successfully.")
            df.show(5)  # Affiche les 5 premières lignes du DataFrame pour vérifier
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

# Fonction principale pour surveiller les fichiers et les traiter toutes les 15 secondes pendant 5 minutes
def monitor_and_process_files():
    end_time = time.time() + 5 * 60  # 5 minutes à partir de maintenant
    while time.time() < end_time:
        print(f"Monitoring files at {datetime.now()}")
        
        # Traiter les fichiers JSON, TXT et CSV
        process_files("json")
        process_files("txt")
        process_files("csv")
        
        # Attendre 15 secondes avant de vérifier à nouveau
        time.sleep(15)

# Lancer le processus
monitor_and_process_files()
