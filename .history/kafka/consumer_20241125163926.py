from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, to_date, coalesce, from_json, lit

# Création de la session Spark
spark = (SparkSession.builder 
    .appName("Data_integretion_project") 
    .getOrCreate()
    )

# Définition des paramètres Kafka
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "transactions_trends"

# Lecture des données depuis Kafka
kafka_df = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) 
    .option("subscribe", kafka_topic) 
    .load()
    )

# Définir le schéma pour les messages Kafka (en supposant que les messages sont en JSON)
message_schema = StructType([
    StructField("id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", StringType(), True),  # En tant que String pour conversion en Decimal
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), True),
    StructField("address", StringType(), True),
    StructField("discount_code", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("tax", StringType(), True),  # En tant que String pour conversion en Decimal
    StructField("shipping_cost", StringType(), True)  # En tant que String pour conversion en Decimal
])

# Parser les messages JSON
df_parsed = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), message_schema).alias("data"))

# Extraire les colonnes et effectuer la conversion en Decimal
df_transformed = df_parsed.select(
    col("data.id"),
    col("data.user_id"),
    col("data.amount").cast(DecimalType(10, 2)).alias("amount"),
    col("data.currency"),
    col("data.payment_method"),
    col("data.status"),
    col("data.address"),
    coalesce(col("data.discount_code"), lit("NULL")).alias("discount_code"),  # Si NULL, remplacer par 'NULL'
    col("data.transaction_type"),
    col("data.tax").cast(DecimalType(10, 2)).alias("tax"),
    col("data.shipping_cost").cast(DecimalType(10, 2)).alias("shipping_cost")
)
# Traitement des données 
final_output = df_transformed.withColumn("Count",lit("Count1"))

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