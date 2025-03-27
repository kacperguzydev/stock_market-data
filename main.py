import logging
from typing import Any

from postgre_connection import (
    create_table_if_not_exists,
    create_lowest_price_table,
    create_highest_price_table,
)
from api_client import update_coin_data
from kafka_producer import produce_to_kafka

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main() -> None:
    """
    Initializes the necessary tables, fetches coin data from the API,
    and sends messages to Kafka.
    """
    try:
        create_table_if_not_exists()
        create_lowest_price_table()
        create_highest_price_table()
    except Exception as exc:
        logging.error("Error creating tables: %s", exc)

    try:
        update_coin_data()
    except Exception as exc:
        logging.error("Error updating coin data: %s", exc)

    try:
        produce_to_kafka()
    except Exception as exc:
        logging.error("Error producing to Kafka: %s", exc)


if __name__ == "__main__":
    main()
