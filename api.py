from flask import Flask, jsonify
from pyspark.sql import SparkSession

# Initialiser l'application Flask
app = Flask(__name__)

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Parquet API") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Chemins des répertoires contenant les fichiers Parquet
PARQUET_DIR_CSV = "hdfs://localhost:9000/transactions/staging/result_csv"
PARQUET_DIR_JSON = "hdfs://localhost:9000/transactions/staging/result_json"
PARQUET_DIR_TXT = "hdfs://localhost:9000/transactions/staging/result_txt"

# Fonction générique pour lire et retourner les données d'un répertoire Parquet
def read_parquet_dir(directory_path):
    try:
        # Lire tous les fichiers Parquet dans le répertoire
        df = spark.read.parquet(directory_path)

        # Convertir en un format JSON lisible (limité aux 100 premières lignes)
        data = df.limit(100).toPandas().to_dict(orient="records")
        return {"data": data}, 200
    except Exception as e:
        return {"error": str(e)}, 500

# Endpoint : Afficher les données concaténées de result_csv
@app.route('/data_csv/all', methods=['GET'])
def get_all_data_csv():
    response, status = read_parquet_dir(PARQUET_DIR_CSV)
    return jsonify(response), status

# Endpoint : Afficher les données concaténées de result_json
@app.route('/data_json/all', methods=['GET'])
def get_all_data_json():
    response, status = read_parquet_dir(PARQUET_DIR_JSON)
    return jsonify(response), status

# Endpoint : Afficher les données concaténées de result_txt
@app.route('/data_txt/all', methods=['GET'])
def get_all_data_txt():
    response, status = read_parquet_dir(PARQUET_DIR_TXT)
    return jsonify(response), status

# Lancer l'application Flask
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
