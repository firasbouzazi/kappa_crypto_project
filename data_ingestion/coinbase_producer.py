import time
from utils import get_kafka_producer, monitor_crypto_prices , send_to_kafka
if __name__ == "__main__":
    producer = get_kafka_producer()
    coins=["BTC","ETH","DOGE"]
    while True:
        crypto_data = monitor_crypto_prices(coins)
        send_to_kafka(producer, crypto_data)
        time.sleep(5)  # Fetch data every 5 seconds
