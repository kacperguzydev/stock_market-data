import logging
from typing import List, Tuple, Any

import psycopg2
import psycopg2.pool
import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Create a global connection pool for PostgreSQL
try:
    pool: psycopg2.pool.ThreadedConnectionPool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        **config.DB_SETTINGS
    )
    if pool:
        logger.info("PostgreSQL connection pool created.")
except Exception as exc:
    logger.error("Error creating PostgreSQL pool: %s", exc)
    pool = None


def get_connection() -> Any:
    """
    Retrieves a connection from the pool.
    """
    try:
        return pool.getconn()
    except Exception as exc:
        logger.error("Error getting connection: %s", exc)
        raise


def put_connection(conn: Any) -> None:
    """
    Returns a connection to the pool.
    """
    try:
        pool.putconn(conn)
    except Exception as exc:
        logger.error("Error returning connection: %s", exc)
        raise


def create_table_if_not_exists() -> None:
    query = """
    CREATE TABLE IF NOT EXISTS coin_prices (
        id SERIAL PRIMARY KEY,
        coin_name TEXT NOT NULL,
        price_date DATE NOT NULL,
        price NUMERIC(10,3) NOT NULL,
        UNIQUE (coin_name, price_date)
    );
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
        logger.info("coin_prices table is ready.")
    except Exception as exc:
        logger.error("Error creating coin_prices table: %s", exc)
        conn.rollback()
    finally:
        put_connection(conn)


def create_lowest_price_table() -> None:
    query = """
    CREATE TABLE IF NOT EXISTS lowest_price (
        coin_name TEXT PRIMARY KEY,
        lowest_price NUMERIC(10,3),
        lowest_price_date TEXT
    );
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
        logger.info("lowest_price table is ready.")
    except Exception as exc:
        logger.error("Error creating lowest_price table: %s", exc)
        conn.rollback()
    finally:
        put_connection(conn)


def create_highest_price_table() -> None:
    query = """
    CREATE TABLE IF NOT EXISTS highest_price (
        coin_name TEXT PRIMARY KEY,
        highest_price NUMERIC(10,3),
        highest_price_date TEXT
    );
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
        logger.info("highest_price table is ready.")
    except Exception as exc:
        logger.error("Error creating highest_price table: %s", exc)
        conn.rollback()
    finally:
        put_connection(conn)


def store_coin_data(coin_name: str, date_price_list: List[dict]) -> None:
    """
    Inserts rows into coin_prices (coin_name, price_date, price).
    """
    insert_query = """
    INSERT INTO coin_prices (coin_name, price_date, price)
    VALUES (%s, %s, %s)
    ON CONFLICT (coin_name, price_date) DO NOTHING;
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for row in date_price_list:
                cur.execute(insert_query, (coin_name, row["date"], row["price"]))
            conn.commit()
        logger.info("Inserted %d rows for %s.", len(date_price_list), coin_name)
    except Exception as exc:
        logger.error("Error inserting data for %s: %s", coin_name, exc)
        conn.rollback()
    finally:
        put_connection(conn)


def fetch_all_coin_data() -> List[Tuple[str, str, float]]:
    """
    Returns all records from coin_prices.
    """
    query = "SELECT coin_name, price_date, price FROM coin_prices ORDER BY coin_name, price_date;"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        return rows
    except Exception as exc:
        logger.error("Error fetching coin data: %s", exc)
        return []
    finally:
        put_connection(conn)
