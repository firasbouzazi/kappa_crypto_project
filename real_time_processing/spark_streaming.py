from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lag, window, avg, when, mean
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

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

# Ajout d'un cast du timestamp
crypto_df = data.withColumn("timestamp", col("timestamp").cast(TimestampType()))
time_window = Window.partitionBy("coin").orderBy("timestamp")

# 1. Calcul de la variation de prix
crypto_df = crypto_df.withColumn("price_change", col("spot") - lag("spot", 1).over(time_window))

# 2. Calcul de la volatilité
crypto_df = crypto_df.withColumn("volatility", (col("spread") / col("spot")) * 100)

# 3. Détection des opportunités d’arbitrage (simplifiée, nécessite des données multi-plateformes)
# Ici, nous supposons que chaque message concerne une plateforme distincte

# 4. Agrégation sur une fenêtre de 5 minutes
crypto_agg_5m = crypto_df.withWatermark("timestamp", "10 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "coin") \
    .agg(
        avg("spot").alias("avg_spot"),
        avg("volatility").alias("avg_volatility"),
        avg("spread").alias("avg_spread")
    )

# 5. Agrégation sur une fenêtre de 1 heure
crypto_agg_1h = crypto_df.withWatermark("timestamp", "2 hours") \
    .groupBy(window("timestamp", "1 hour"), "coin") \
    .agg(
        avg("spot").alias("avg_spot"),
        avg("volatility").alias("avg_volatility"),
        avg("spread").alias("avg_spread")
    )

# 6. Détection de montées rapides
crypto_df = crypto_df.withColumn("pump", when(col("spot") > lag("spot", 1).over(time_window) * 1.20, True).otherwise(False)) \
                     .withColumn("dump", when(col("spot") < lag("spot", 1).over(time_window) * 0.80, True).otherwise(False))

# 7. Détection d'un spread anormalement grand
crypto_df = crypto_df.withColumn("mean_spread", mean("spread").over(time_window)) \
                     .withColumn("spread_anomaly", when(col("spread") > 5 * col("mean_spread"), True).otherwise(False))


# Écriture du flux en mode "append" dans la console en affichant toutes les colonnes

print("Is streaming:", crypto_df.isStreaming)
query = crypto_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/kafka_spark_checkpoint") \
    .start()

query.awaitTermination()