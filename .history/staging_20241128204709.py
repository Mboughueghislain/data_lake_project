from pyspark.sql import SparkSession
import os

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Staging Files Processing") \
    .getOrCreate()

# Fonction pour traiter les fichiers JSON
def process_json(file_path):
    print(f"Processing JSON file: {file_path}")
    # Lire le fichier JSON dans un DataFrame
    df = spark.read.json(file_path)
    # Sauvegarder le DataFrame en format Parquet dans HDFS
    output_path = f"hdfs://localhost:9000/transactions/staging/json/{os.path.basename(file_path)}.parquet"
    df.write.parquet(output_path)
    print(f"Saved JSON to: {output_path}")

# Fonction pour traiter les fichiers CSV
def process_csv(file_path):
    print(f"Processing CSV file: {file_path}")
    # Lire le fichier CSV dans un DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    # Sauvegarder le DataFrame en format Parquet dans HDFS
    output_path = f"hdfs://localhost:9000/transactions/staging/csv/{os.path.basename(file_path)}.parquet"
    df.write.parquet(output_path)
    print(f"Saved CSV to: {output_path}")

# Fonction pour traiter les fichiers TXT
def process_txt(file_path):
    print(f"Processing TXT file: {file_path}")
    # Lire le fichier TXT dans un DataFrame
    df = spark.read.text(file_path)
    # Sauvegarder le DataFrame en format Parquet dans HDFS
    output_path = f"hdfs://localhost:9000/transactions/staging/txt/{os.path.basename(file_path)}.parquet"
    df.write.parquet(output_path)
    print(f"Saved TXT to: {output_path}")

# Fonction pour traiter tous les fichiers
def process_files():
    # Liste des répertoires dans /transactions/raw (csv, json, txt)
    raw_dirs = ['csv', 'json', 'txt']
    
    for raw_dir in raw_dirs:
        path = f"hdfs://localhost:9000/transactions/raw/{raw_dir}"
        
        # Utiliser HDFS pour lister les fichiers dans le sous-répertoire
        files = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \
            .get(spark._jsc.hadoopConfiguration()) \
            .listStatus(org.apache.hadoop.fs.Path(path))
        
        for file in files:
            file_path = file.getPath().toString()
            print(f"Found file: {file_path}")
            
            # Détecter le format du fichier par son extension et le traiter
            if file_path.endswith('.json'):
                process_json(file_path)
            elif file_path.endswith('.csv'):
                process_csv(file_path)
            elif file_path.endswith('.txt'):
                process_txt(file_path)

# Appel de la fonction principale pour traiter les fichiers
process_files()
