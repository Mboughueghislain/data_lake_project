import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Initialiser la session Spark
spark = SparkSession.builder.appName("Process Raw Files").getOrCreate()

# Chemins des répertoires HDFS
raw_path_json = "/transactions/raw/json"
raw_path_txt = "/transactions/raw/txt"
raw_path_csv = "/transactions/raw/csv"
staging_path = "/transactions/stagging"

# Fonction pour lire et transformer un fichier JSON
def process_json(file_path):
    # Lire le fichier JSON
    df = spark.read.json(file_path)
    
    # Transformer les tags en une colonne séparée (chaque tag devient une ligne)
    df = df.withColumn("Tag", explode(col("Tags")))
    
    # Sélectionner les colonnes nécessaires
    df = df.select(
        "Post ID", "Username", "Platform", "Timestamp", "Content", "Sentiment",
        "Likes", "Comments", "Shares", "Tag"
    )
    
    return df

# Fonction pour lire et transformer un fichier TXT (au format CSV)
def process_txt(file_path):
    # Lire le fichier TXT avec le bon format CSV
    df = spark.read.option("delimiter", ",").csv(file_path, header=True, inferSchema=True)
    
    # Transformer les tags en une colonne séparée
    df = df.withColumn("Tag", explode(col("Tags").cast("string").split(",")))
    
    return df

# Fonction pour lire et transformer un fichier CSV
def process_csv(file_path):
    # Lire le fichier CSV
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    
    # Transformer les tags en une colonne séparée
    df = df.withColumn("Tag", explode(col("Tags").cast("string").split(",")))
    
    return df

# Fonction pour sauvegarder en format Delta
def save_to_delta(df, output_path):
    df.write.format("delta").mode("append").save(output_path)

# Fonction principale pour surveiller les répertoires et traiter les fichiers
def monitor_and_process_files():
    while True:
        # Récupérer les fichiers JSON, TXT et CSV depuis HDFS
        json_files = os.popen(f"hdfs dfs -ls {raw_path_json}").read().strip().split("\n")[1:]
        txt_files = os.popen(f"hdfs dfs -ls {raw_path_txt}").read().strip().split("\n")[1:]
        csv_files = os.popen(f"hdfs dfs -ls {raw_path_csv}").read().strip().split("\n")[1:]
        
        # Traiter les fichiers JSON
        for file in json_files:
            file_path = file.split()[-1]
            print(f"Processing JSON file: {file_path}")
            df = process_json(file_path)
            save_to_delta(df, staging_path)
        
        # Traiter les fichiers TXT
        for file in txt_files:
            file_path = file.split()[-1]
            print(f"Processing TXT file: {file_path}")
            df = process_txt(file_path)
            save_to_delta(df, staging_path)
        
        # Traiter les fichiers CSV
        for file in csv_files:
            file_path = file.split()[-1]
            print(f"Processing CSV file: {file_path}")
            df = process_csv(file_path)
            save_to_delta(df, staging_path)
        
        # Attendre 15 secondes avant de vérifier à nouveau
        time.sleep(15)

# Lancer la surveillance et le traitement des fichiers
monitor_and_process_files()
