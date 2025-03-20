from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# 1. Définition du Schéma JSON
schema = StructType([
    StructField("coin", StringType(), True),
    StructField("spot", FloatType(), True),
    StructField("buy", FloatType(), True),
    StructField("sell", FloatType(), True),
    StructField("spread", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# 2. Création de la SparkSession
spark = SparkSession.builder \
    .appName("CryptoSparkStreaming") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.sources.useV1SourceList", "kafka") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 3. Lecture depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parsing JSON
data = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# 5. Watermark
data_with_watermark = data.withWatermark("timestamp", "2 minutes")

# 6. Calcul de la volatilité (ligne par ligne)
#    volatility = (spread / spot) * 100
data_with_volatility = data_with_watermark.withColumn(
    "volatility", (col("spread") / col("spot")) * 100
)

# 7. Agrégation unique : calcul de avg_spot, avg_spread, avg_volatility
aggregated_df = data_with_volatility.groupBy(
    col("coin"),
    window(col("timestamp"), "30 seconds")
).agg(
    avg("spot").alias("avg_spot"),
    avg("spread").alias("avg_spread"),
    avg("volatility").alias("avg_volatility")  # Ajout de la moyenne de la volatilité
)

# 8. Ajout des indicateurs (pump, dump, spread_anomaly, ema)
final_df = aggregated_df \
    .withColumn(
        "pump",
        when(col("avg_spot") > col("avg_spot") * 1.05, True).otherwise(False)
    ).withColumn(
        "dump",
        when(col("avg_spot") < col("avg_spot") * 0.95, True).otherwise(False)
    ).withColumn(
        "spread_anomaly",
        when(col("avg_spread") > col("avg_spread") * 5, True).otherwise(False)
    ).withColumn(
        "ema",  # Simplification : on prend avg_spot comme "EMA"
        col("avg_spot")
    )

# 9. Écriture du résultat final dans la console
query = final_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "true") \
    .start()

query.awaitTermination()
