from flask import Flask, jsonify
from pyspark.sql import SparkSession

# Initialiser l'application Flask
app = Flask(__name__)

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Parquet API") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Chemin du répertoire contenant les fichiers Parquet
PARQUET_DIR = "hdfs://localhost:9000/staging/csv/result_csv"

# Endpoint : Afficher les données concaténées de tous les fichiers Parquet
@app.route('/data/all', methods=['GET'])
def get_all_data():
    try:
        # Lire tous les fichiers Parquet dans le répertoire
        df = spark.read.parquet(PARQUET_DIR)

        # Convertir en un format JSON lisible (limité aux 100 premières lignes)
        data = df.limit(100).toPandas().to_dict(orient="records")
        return jsonify({"data": data}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint : Afficher les données d'un seul fichier Parquet (partition spécifique)
@app.route('/data/<partition_name>', methods=['GET'])
def get_partition_data(partition_name):
    try:
        # Construire le chemin complet pour la partition
        partition_path = f"{PARQUET_DIR}/{partition_name}"

        # Lire la partition spécifique
        df = spark.read.parquet(partition_path)

        # Convertir en un format JSON lisible (limité aux 100 premières lignes)
        data = df.limit(100).toPandas().to_dict(orient="records")
        return jsonify({"partition": partition_name, "data": data}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Lancer l'application Flask
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
