import subprocess
from pyspark.sql import SparkSession
import os

# Configuration Spark avec Delta Lake
spark = SparkSession.builder \
    .appName("HDFS to Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Répertoires HDFS
HDFS_RAW_DIR = "/transactions/raw"
HDFS_STAGING_DIR = "/transactions/staging"

# Fonction pour charger un fichier dans un DataFrame selon son type
def load_file_as_dataframe(file_path, file_type):
    if file_type == "csv":
        df = spark.read.option("header", "true").csv(f"hdfs://{file_path}")
    elif file_type == "json":
        df = spark.read.json(f"hdfs://{file_path}")
    elif file_type == "txt":
        # On suppose que les fichiers TXT ont un format similaire au CSV
        df = spark.read.option("header", "true").csv(f"hdfs://{file_path}")
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    return df

# Fonction pour sauvegarder le DataFrame en format Delta
def save_as_delta(df, destination_path):
    df.write.format("delta").mode("overwrite").save(f"hdfs://{destination_path}")
    print(f"DataFrame sauvegardé en format Delta dans : {destination_path}")

# Fonction pour traiter les fichiers dans les répertoires de raw
def process_new_files():
    # Répertoires dans /transactions/raw (csv, json, txt, etc.)
    file_types = ['csv', 'json', 'txt']
    
    for file_type in file_types:
        raw_dir = os.path.join(HDFS_RAW_DIR, file_type)
        
        # Liste des fichiers dans le répertoire du type de fichier
        files = subprocess.check_output(["hdfs", "dfs", "-ls", raw_dir]).decode().splitlines()
        
        for file in files:
            file_path = file.split()[-1]  # Le dernier élément est le chemin du fichier
            print(f"Fichier détecté : {file_path} (Type : {file_type})")
            
            try:
                # Charger le fichier et le convertir en DataFrame
                df = load_file_as_dataframe(file_path, file_type)
                
                # Chemin de destination dans le répertoire staging
                destination_path = os.path.join(HDFS_STAGING_DIR, f"{file_type}_{os.path.basename(file_path).split('.')[0]}")
                
                # Sauvegarder en format Delta
                save_as_delta(df, destination_path)
                
            except Exception as e:
                print(f"Erreur lors du traitement du fichier {file_path}: {e}")

# Fonction principale qui peut être exécutée périodiquement
def main():
    process_new_files()

if __name__ == "__main__":
    main()
