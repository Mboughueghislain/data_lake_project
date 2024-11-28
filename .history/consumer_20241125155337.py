from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, to_date

# Création de la session Spark
spark = (SparkSession.builder 
    .appName("Data_integretion_project") 
    .getOrCreate()
    )

# Définition des paramètres Kafka
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "hospital_trends"

# Lecture des données depuis Kafka
kafka_df = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) 
    .option("subscribe", kafka_topic) 
    .load()
    )

# Extraction du contenu des messages Kafka 
streaming_data = kafka_df.selectExpr("CAST(value AS STRING)").alias("value")

# Parse du CSV provenant du flux
schema = "Date STRING, Setting STRING, Category STRING, System STRING, `Facility Name` STRING, Count INT"
parsed_stream = streaming_data.selectExpr(f"from_csv(value, '{schema}') as data").select("data.*")


# Définition des chemins pour le HDFS
output_path = "hdfs://localhost:9000/hospital_data/final_output"
checkpoint_path = "hdfs://localhost:9000/hospital_data/checkpoints"

# Écriture du résultat final dans le HDFS 
final_output = (final_output.writeStream 
    .outputMode("append") 
    .format("parquet") 
    .option("path", output_path) 
    .option("checkpointLocation", checkpoint_path) 
    .start()
    )

# Attente la fin du streaming
final_output.awaitTermination() 