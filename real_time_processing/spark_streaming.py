from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Définition du schéma pour le JSON
schema = StructType([
    StructField("coin", StringType(), True),
    StructField("spot", FloatType(), True),
    StructField("buy", FloatType(), True),
    StructField("sell", FloatType(), True),
    StructField("spread", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# Création de la SparkSession avec configuration pour forcer l'utilisation de l'API V1 pour Kafka
spark = SparkSession.builder \
    .appName("KafkaSparkTableTest") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.sources.useV1SourceList", "kafka") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

# Lecture du flux depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_prices") \
    .load()

# Conversion du champ binaire "value" en chaîne, puis parsing du JSON selon le schéma
data = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*")

# Écriture du flux en mode "append" dans la console en affichant toutes les colonnes
query = data.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/kafka_spark_checkpoint") \
    .start()

query.awaitTermination()
