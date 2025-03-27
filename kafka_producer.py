import json
import time
import logging
from typing import Any

from kafka import KafkaProducer
import config
from postgre_connection import fetch_all_coin_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def json_serializer(data: Any) -> bytes:
    return json.dumps(data).encode("utf-8")


def key_serializer(key: str) -> bytes:
    return key.encode("utf-8")


def produce_to_kafka() -> None:
    """
    Reads coin data from PostgreSQL and sends it to Kafka.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BROKER,
            value_serializer=json_serializer,
            key_serializer=key_serializer
        )
    except Exception as exc:
        logger.error("Error initializing Kafka producer: %s", exc)
        return

    coin_data = fetch_all_coin_data()
    if not coin_data:
        logger.warning("No coin data found in DB.")
        return

    logger.info("Fetched %d records from DB.", len(coin_data))
    for row in coin_data:
        coin_name, price_date, price = row
        message_key = f"{coin_name}_{price_date}"
        message_value = {"coin_name": coin_name, "price_date": str(price_date), "price": float(price)}
        try:
            producer.send(config.KAFKA_TOPIC, key=message_key, value=message_value)
            logger.info("Sent message: key=%s, value=%s", message_key, message_value)
        except Exception as exc:
            logger.error("Error sending message for %s: %s", message_key, exc)
        time.sleep(0.1)

    try:
        producer.flush()
        logger.info("All messages sent to Kafka.")
    except Exception as exc:
        logger.error("Error flushing Kafka producer: %s", exc)
