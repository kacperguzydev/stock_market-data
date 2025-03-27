import os
from typing import Dict, Any

API_PROVIDER: str = os.getenv("API_PROVIDER", "COINGECKO")
BASE_URLS: Dict[str, str] = {
    "COINGECKO": os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
}
REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "10"))
RATE_LIMIT_PER_MINUTE: int = int(os.getenv("RATE_LIMIT_PER_MINUTE", "30"))

DB_SETTINGS: Dict[str, Any] = {
    "dbname": os.getenv("DB_NAME", "stock_market"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "1234"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "coin_prices_topic")

JDBC_URL: str = f"jdbc:postgresql://{DB_SETTINGS['host']}:{DB_SETTINGS['port']}/{DB_SETTINGS['dbname']}"
JDBC_PROPERTIES: Dict[str, str] = {
    "user": DB_SETTINGS["user"],
    "password": DB_SETTINGS["password"],
    "driver": "org.postgresql.Driver",
}
