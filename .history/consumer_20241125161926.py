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


# Traitement des données 
final_output = parsed_stream.withColumnRenamed("Count", "Count1")

# Définition des chemins pour le HDFS
output_path = "hdfs://localhost:9000/transactions/final_output"
checkpoint_path = "hdfs://localhost:9000/transactions/checkpoints"

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