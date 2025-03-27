import logging
import requests
import datetime
import time
import threading
from typing import Dict, List, Optional

import config
from postgre_connection import store_coin_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class TokenBucket:
    """
    Basic token bucket for rate limiting.
    """

    def __init__(self, rate: float, capacity: int) -> None:
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.timestamp = time.time()
        self.lock = threading.Lock()

    def consume(self, tokens: int = 1) -> bool:
        """
        Try to consume the given number of tokens. Returns True if successful.
        """
        with self.lock:
            now = time.time()
            elapsed = now - self.timestamp
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.timestamp = now
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def wait(self, tokens: int = 1) -> None:
        """
        Block until we have enough tokens.
        """
        while not self.consume(tokens):
            time.sleep(0.05)


# Configure rate limiting based on environment variables.
tokens_per_second = config.RATE_LIMIT_PER_MINUTE / 60.0
bucket = TokenBucket(rate=tokens_per_second, capacity=config.RATE_LIMIT_PER_MINUTE)


def get_top_cryptos(limit: int = 10) -> Optional[Dict[str, str]]:
    """
    Returns a dict of {crypto_id: symbol} for the top cryptos by market cap.
    """
    base_url = config.BASE_URLS["COINGECKO"]
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": limit, "page": 1}
    bucket.wait()
    try:
        response = requests.get(f"{base_url}/coins/markets", params=params, timeout=config.REQUEST_TIMEOUT)
    except Exception as exc:
        logger.error("Error fetching top cryptos: %s", exc)
        return None

    if response.status_code != 200:
        logger.error("Error fetching top cryptos: %s, response: %s", response.status_code, response.text)
        return None

    try:
        data = response.json()
        return {coin["id"]: coin["symbol"].upper() for coin in data}
    except Exception as exc:
        logger.error("Error parsing top cryptos: %s", exc)
        return None


def get_crypto_historical(crypto_id: str, days: int = 30) -> Optional[List[Dict[str, any]]]:
    """
    Fetches daily historical price data for a specific crypto over 'days' days.
    """
    base_url = config.BASE_URLS["COINGECKO"]
    params = {"vs_currency": "usd", "days": days, "interval": "daily"}
    wait_time = 5
    max_retries = 5

    for _ in range(max_retries):
        bucket.wait()
        try:
            response = requests.get(f"{base_url}/coins/{crypto_id}/market_chart", params=params, timeout=config.REQUEST_TIMEOUT)
        except Exception as exc:
            logger.error("Error fetching historical data for %s: %s", crypto_id, exc)
            return None

        if response.status_code == 200:
            try:
                data = response.json()
                out = []
                for entry in data["prices"]:
                    timestamp, price = entry
                    date_str = datetime.datetime.utcfromtimestamp(timestamp / 1000).strftime("%Y-%m-%d")
                    out.append({"date": date_str, "price": price})
                return out
            except Exception as exc:
                logger.error("Error parsing historical data for %s: %s", crypto_id, exc)
                return None
        elif response.status_code == 429:
            logger.warning("Rate limit for %s. Retrying in %s seconds...", crypto_id, wait_time)
            time.sleep(wait_time)
            wait_time *= 2
        else:
            logger.error("Error fetching data for %s: %s, response: %s", crypto_id, response.status_code, response.text)
            return None

    logger.error("Failed to fetch data for %s after %d attempts.", crypto_id, max_retries)
    return None


def update_coin_data() -> None:
    """
    Fetches the top cryptos, gets historical data, and stores it in PostgreSQL.
    """
    cryptos = get_top_cryptos(limit=10)
    if not cryptos:
        logger.error("No top cryptos found.")
        return

    for crypto_id, symbol in cryptos.items():
        logger.info("Processing %s (%s)", symbol, crypto_id)
        data_list = get_crypto_historical(crypto_id, days=30)
        if data_list:
            try:
                store_coin_data(symbol, data_list)
            except Exception as exc:
                logger.error("Error storing data for %s: %s", crypto_id, exc)
        else:
            logger.warning("No data for %s.", crypto_id)
        time.sleep(6)


if __name__ == "__main__":
    update_coin_data()
