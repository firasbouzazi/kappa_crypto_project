from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# 1. Définition du schéma JSON
schema = StructType([
    StructField("coin", StringType(), True),
    StructField("spot", FloatType(), True),
    StructField("buy", FloatType(), True),
    StructField("sell", FloatType(), True),
    StructField("spread", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# 2. Création de la SparkSession avec configuration pour se connecter à Cassandra
spark = SparkSession.builder \
    .appName("CryptoSparkStreaming") \
    .master("spark://spark-master:7077") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 3. Lecture du stream depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parsing du JSON et conversion du champ timestamp
data = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))
def write_raw_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="crypto_keyspace", table="crypto_raw") \
        .mode("append") \
        .save()

raw_query = data.writeStream \
    .outputMode("update") \
    .foreachBatch(write_raw_to_cassandra) \
    .start()

# 5. Application d'un watermark pour gérer la latence
data_with_watermark = data.withWatermark("timestamp", "2 minutes")

# 6. Calcul de la volatilité : (spread / spot) * 100
data_with_volatility = data_with_watermark.withColumn(
    "volatility", (col("spread") / col("spot")) * 100
)

# 7. Agrégation sur une fenêtre de 30 secondes par 'coin'
aggregated_df = data_with_volatility.groupBy(
    col("coin"),
    window(col("timestamp"), "30 seconds").alias("window")
).agg(
    avg("spot").alias("avg_spot"),
    avg("spread").alias("avg_spread"),
    avg("volatility").alias("avg_volatility")
)

# 8. Ajout des indicateurs : pump, dump, spread_anomaly et EMA (simplifié ici comme avg_spot)
final_df_intermediate = aggregated_df.withColumn(
    "pump", when(col("avg_spot") > col("avg_spot") * 1.05, True).otherwise(False)
).withColumn(
    "dump", when(col("avg_spot") < col("avg_spot") * 0.95, True).otherwise(False)
).withColumn(
    "spread_anomaly", when(col("avg_spread") > col("avg_spread") * 5, True).otherwise(False)
).withColumn(
    "ema", col("avg_spot")
)

# 9. Extraction et renommage des champs window.start et window.end
final_df = final_df_intermediate.select(
    col("coin"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("avg_spot"),
    col("avg_spread"),
    col("avg_volatility"),
    col("pump"),
    col("dump"),
    col("spread_anomaly"),
    col("ema")
)

# 10. Fonction pour écrire chaque micro-batch dans Cassandra
def write_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="crypto_keyspace", table="crypto_streaming") \
        .mode("append") \
        .save()

# 11. Lancement du stream et écriture dans Cassandra via foreachBatch
query = final_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("update") \
    .start()
query.awaitTermination()
