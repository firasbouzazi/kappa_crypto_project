from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, when, corr, sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# -------- 1. Définition du Schéma JSON --------
schema = StructType([
    StructField("coin", StringType(), True),
    StructField("spot", FloatType(), True),
    StructField("buy", FloatType(), True),
    StructField("sell", FloatType(), True),
    StructField("spread", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# -------- 2. Initialisation de la Spark Session --------
spark = SparkSession.builder \
    .appName("CryptoSparkStreaming") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.sources.useV1SourceList", "kafka") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

# Désactiver les logs inutiles
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")


# -------- 3. Lecture des données depuis Kafka --------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "latest") \
    .load()

# -------- 4. Parsing JSON --------
data = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# -------- 5. Application du watermark --------
data_with_watermark = data.withWatermark("timestamp", "2 minutes")

# -------- 6. Calcul de la volatilité --------
data_with_volatility = data_with_watermark.withColumn("volatility", (col("spread") / col("spot")) * 100)

# -------- 7. Détection des pumps et dumps --------
pumps_dumps_df = data_with_volatility.groupBy("coin", window(col("timestamp"), "30 seconds")).agg(
    avg("spot").alias("avg_spot")
).na.fill(0).withColumn(
    "pump", when(col("avg_spot") > col("avg_spot") * 1.05, True).otherwise(False)
).withColumn(
    "dump", when(col("avg_spot") < col("avg_spot") * 0.95, True).otherwise(False)
)

# -------- 8. Détection des spreads anormaux --------
spread_anomalies_df = data_with_volatility.groupBy("coin", window("timestamp", "30 seconds")).agg(
    avg("spread").alias("avg_spread")
).na.fill(0).withColumn(
    "spread_anomaly", when(col("avg_spread") > col("avg_spread") * 5, True).otherwise(False)
)

# -------- 9. Corrélation entre cryptos --------
agg_data = data_with_watermark.groupBy(
    window(col("timestamp"), "30 seconds"), col("coin")
).agg(
    avg("spot").alias("avg_spot")
)

correlation_df = agg_data.groupBy("window").agg(
    sum("avg_spot").alias("total_avg_spot")  
).agg(
    corr("total_avg_spot", "total_avg_spot").alias("correlation")
)

# -------- 10. Calcul de la Moyenne Mobile Exponentielle (EMA) --------
ema_df = data_with_watermark.groupBy(
    window(col("timestamp"), "30 seconds"), col("coin")
).agg(
    avg("spot").alias("ema")  # Test avec moyenne simple avant d’ajouter le lissage EMA
)

# -------- 11. Écriture des résultats en Streaming --------
query_pumps_dumps = pumps_dumps_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query_spread_anomalies = spread_anomalies_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query_correlation = correlation_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query_ema = ema_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query_pumps_dumps.awaitTermination()
query_spread_anomalies.awaitTermination()
query_correlation.awaitTermination()
query_ema.awaitTermination()
