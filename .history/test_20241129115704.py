from flask import Flask, jsonify
from pyspark.sql import SparkSession

# Initialiser l'application Flask
app = Flask(__name__)

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Parquet API") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Chemin des fichiers Parquet
PARQUET_DIR = "hdfs://localhost:9000/staging/csv"

# Endpoint : Afficher les résultats de toutes les partitions concaténées
@app.route('/data/all', methods=['GET'])
def get_all_partitions():
    try:
        # Lire toutes les partitions dans un DataFrame
        df = spark.read.parquet(PARQUET_DIR)

        # Convertir les données en un format JSON lisible (limité aux 100 premières lignes)
        data = df.limit(100).toPandas().to_dict(orient="records")
        return jsonify({"data": data}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint : Afficher les résultats d'une partition spécifique
@app.route('/data/<partition_name>', methods=['GET'])
def get_partition(partition_name):
    try:
        # Construire le chemin de la partition
        partition_path = f"{PARQUET_DIR}/{partition_name}"

        # Lire les données de la partition
        df = spark.read.parquet(partition_path)

        # Convertir les données en un format JSON lisible (limité aux 100 premières lignes)
        data = df.limit(100).toPandas().to_dict(orient="records")
        return jsonify({"partition": partition_name, "data": data}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Lancer l'application Flask
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
