CREATE KEYSPACE IF NOT EXISTS crypto_keyspace WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

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
