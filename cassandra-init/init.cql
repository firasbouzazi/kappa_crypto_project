-- Create keyspace if it doesn't exist
CREATE KEYSPACE IF NOT EXISTS crypto_keyspace WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

-- Table for streaming data (existing)
CREATE TABLE IF NOT EXISTS crypto_keyspace.crypto_streaming (
  coin text,
  window_start timestamp,
  window_end timestamp,
  avg_spot float,
  avg_spread float,
  avg_volatility float,
  pump boolean,
  dump boolean,
  spread_anomaly boolean,
  ema float,
  PRIMARY KEY ((coin), window_start, window_end)
);

-- Table for raw streaming data (existing)
CREATE TABLE IF NOT EXISTS crypto_keyspace.crypto_raw (
  coin text,
  spot float,
  buy float,
  sell float,
  spread float,
  timestamp timestamp,
  PRIMARY KEY (coin, timestamp)
);

-- Table for batch aggregation results
CREATE TABLE IF NOT EXISTS crypto_keyspace.crypto_batch_aggregation (
  coin text,
  window_start timestamp,
  window_end timestamp,
  avg_price float,
  min_price float,
  max_price float,
  std_dev float,
  record_count int,
  PRIMARY KEY ((coin), window_start, window_end)
);

-- Table for batch correlation results
CREATE TABLE IF NOT EXISTS crypto_keyspace.crypto_batch_correlation (
  coin_a text,
  coin_b text,
  price_correlation float,
  PRIMARY KEY (coin_a, coin_b)
);


