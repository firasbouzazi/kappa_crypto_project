from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lag, avg, min, max, window, corr, stddev, count,
    abs, first, last
)
from pyspark.sql.window import Window

# Initialize Spark Session with Cassandra connection configuration
spark = SparkSession.builder \
    .appName("CryptoBatchAnalysis") \
    .config("spark.cassandra.connection.host", "cassandra1") \
    .getOrCreate()

# Load CSV into DataFrame (expects CSV at storage/output.csv, mounted from ../storage)
crypto_df = spark.read.csv("storage/output.csv", header=True, inferSchema=True)

# Convert timestamp column to TimestampType
crypto_df = crypto_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Window specification: partition by coin and order by timestamp
window_spec = Window.partitionBy("coin").orderBy("timestamp")

# --- Calculate Intra-row Percentage Change ---
crypto_df = crypto_df.withColumn("prev_price", lag("spot").over(window_spec))
crypto_df = crypto_df.withColumn("price_change", col("spot") - col("prev_price"))
crypto_df = crypto_df.withColumn("pct_change", ((col("spot") - col("prev_price")) / col("prev_price")) * 100)

# --- Aggregation over 30-minute windows ---
agg_df_30min = crypto_df.withWatermark("timestamp", "30 minutes") \
    .groupBy(window(col("timestamp"), "30 minutes"), col("coin")) \
    .agg(
        avg("spot").alias("avg_price"),
        min("spot").alias("min_price"),
        max("spot").alias("max_price"),
        stddev("spot").alias("std_dev"),
        count("*").alias("record_count")
    )

# Extract window start and end into separate columns, then drop the original window column
agg_df_30min = agg_df_30min.withColumn("window_start", col("window.start")) \
                           .withColumn("window_end", col("window.end")) \
                           .drop("window")

# --- Calculate Percentage Change Over 30-Minute Windows ---
pct_change_30min_df = crypto_df.withWatermark("timestamp", "30 minutes") \
    .groupBy(window(col("timestamp"), "30 minutes"), col("coin")) \
    .agg(
        first("spot").alias("start_price"),
        last("spot").alias("end_price")
    ) \
    .withColumn("pct_change_30min", ((col("end_price") - col("start_price")) / col("start_price")) * 100)

# --- Detect High-Variation Events (>|30%| change) ---
high_variation_df = crypto_df.filter(abs(col("pct_change")) > 30)

# --- Compute Correlation between Coin Pairs ---
# We join on timestamp and then group by coins.
# Aliasing the columns as coin_a and coin_b so they match the Cassandra table primary key.
raw_corr_df = crypto_df.alias("a") \
    .join(crypto_df.alias("b"), "timestamp") \
    .filter(col("a.coin") < col("b.coin")) \
    .groupBy(col("a.coin").alias("coin_a"), col("b.coin").alias("coin_b")) \
    .agg(corr("a.spot", "b.spot").alias("price_correlation"))

# Explicitly select the expected columns (ensuring coin_a and coin_b are present)
correlation_df = raw_corr_df.select("coin_a", "coin_b", "price_correlation")

# --- Debug Output ---
print("=== 30-Minute Aggregation ===")
agg_df_30min.show(truncate=False)

print("=== Percentage Change Over 30 Minutes ===")
pct_change_30min_df.select("window", "coin", "start_price", "end_price", "pct_change_30min").show(truncate=False)

print("=== Price Correlation Between Coin Pairs ===")
correlation_df.show(truncate=False)

print("=== Row-to-Row Percentage Change for All Coins ===")
crypto_df.select("timestamp", "coin", "spot", "prev_price", "pct_change").show(truncate=False)

print("=== Coins with >30% Intra-row Change ===")
high_variation_df.select("timestamp", "coin", "spot", "pct_change").show(truncate=False)

# --- Write Batch Aggregation Results to Cassandra ---
agg_df_30min.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(keyspace="crypto_keyspace", table="crypto_batch_aggregation") \
    .mode("append") \
    .save()

# --- Write Correlation Results to Cassandra ---
correlation_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(keyspace="crypto_keyspace", table="crypto_batch_correlation") \
    .mode("append") \
    .save()

print("Batch processing job completed and data written to Cassandra.")

spark.stop()
