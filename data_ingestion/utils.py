import time
import json
import requests
from datetime import datetime
from kafka import KafkaProducer
from kafka_config import KAFKA_BROKER, KAFKA_TOPIC

# Initialize Kafka Producer
def get_kafka_producer():
    """
    Initializes and returns a Kafka producer with JSON serialization.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

# Fetch price data from Coinbase API
def get_price(coin, price_type):
    """
    Fetch the specified price (spot, buy, or sell) for a cryptocurrency.

    Args:
        coin (str): The cryptocurrency symbol (e.g., 'BTC').
        price_type (str): Type of price to fetch ('spot', 'buy', or 'sell').

    Returns:
        float: The price in USD, or None if an error occurs.
    """
    url = f"https://api.coinbase.com/v2/prices/{coin}-USD/{price_type}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise error for bad responses
        data = response.json()
        price_str = data.get("data", {}).get("amount")
        return float(price_str) if price_str else None
    except Exception as e:
        print(f"[ERROR] Fetching {price_type} price for {coin}: {e}")
        return None

# Get detailed price data for a cryptocurrency
def get_crypto_data(coin):
    """
    Get multiple KPIs for a given coin: spot, buy, and sell prices.
    Also calculates the spread between buy and sell prices.

    Args:
        coin (str): The cryptocurrency symbol.

    Returns:
        dict: A dictionary with keys 'spot', 'buy', 'sell', 'spread'.
    """
    spot = get_price(coin, "spot")
    buy = get_price(coin, "buy")
    sell = get_price(coin, "sell")
    spread = None

    if buy is not None and sell is not None:
        spread = buy - sell

    return {"spot": spot, "buy": buy, "sell": sell, "spread": spread}

# Monitor multiple cryptocurrencies
def monitor_crypto_prices(coins):
    """
    Fetch latest crypto prices (spot, buy, sell) for multiple coins.

    Args:
        coins (list): List of cryptocurrency symbols.

    Returns:
        dict: A dictionary with timestamp and prices.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    crypto_data = {"timestamp": timestamp, "prices": {}}

    for coin in coins:
        crypto_data["prices"][coin] = get_crypto_data(coin)

    return crypto_data

# Send crypto data to Kafka
def send_to_kafka(producer, data):
    """
    Sends cryptocurrency price data to a Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka producer instance.
        data (dict): The data to send to Kafka.
    """
    timestamp = data["timestamp"]
    prices = data["prices"]

    for coin, details in prices.items():
        message = {
            "coin": coin,
            "spot": details.get("spot"),
            "buy": details.get("buy"),
            "sell": details.get("sell"),
            "spread": details.get("spread"),
            "timestamp": timestamp,
        }
        try:
            producer.send(KAFKA_TOPIC, value=message)
            print(f"[INFO] Sent to Kafka: {message}")
        except Exception as e:
            print(f"[ERROR] Sending data to Kafka: {e}")

