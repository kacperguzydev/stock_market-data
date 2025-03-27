import os
import logging
from typing import Any

os.environ["HADOOP_HOME"] = "C:\\hadoop"  # Make sure winutils.exe is in C:\hadoop\bin

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def write_lowest_to_postgre(batch_df: DataFrame, batch_id: int) -> None:
    """
    Writes a batch of data to the lowest_price table in PostgreSQL.
    """
    try:
        batch_df.write.mode("overwrite").jdbc(
            url=config.JDBC_URL, table="lowest_price", properties=config.JDBC_PROPERTIES
        )
        logger.info("Wrote batch %s to lowest_price.", batch_id)
    except Exception as exc:
        logger.error("Error writing batch %s to lowest_price: %s", batch_id, exc)


def write_highest_to_postgre(batch_df: DataFrame, batch_id: int) -> None:
    """
    Writes a batch of data to the highest_price table in PostgreSQL.
    """
    try:
        batch_df.write.mode("overwrite").jdbc(
            url=config.JDBC_URL, table="highest_price", properties=config.JDBC_PROPERTIES
        )
        logger.info("Wrote batch %s to highest_price.", batch_id)
    except Exception as exc:
        logger.error("Error writing batch %s to highest_price: %s", batch_id, exc)


def main() -> None:
    """
    Launches a Spark job that reads data from Kafka, computes
    lowest/highest prices, and writes the results to PostgreSQL.
    """
    spark = (
        SparkSession.builder.appName("SparkToPostgre")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.2.18",
        )
        .getOrCreate()
    )

    schema = StructType(
        [
            StructField("coin_name", StringType(), True),
            StructField("price_date", StringType(), True),
            StructField("price", DoubleType(), True),
        ]
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BROKER)
        .option("subscribe", config.KAFKA_TOPIC)
        .load()
    )

    json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str")
    coin_df = json_df.select(F.from_json(F.col("json_str"), schema).alias("data")).select("data.*")

    lowest_df = coin_df.groupBy("coin_name").agg(
        F.min_by("price_date", "price").alias("lowest_date"),
        F.min("price").alias("lowest_price"),
    )
    highest_df = coin_df.groupBy("coin_name").agg(
        F.max_by("price_date", "price").alias("highest_date"),
        F.max("price").alias("highest_price"),
    )

    lowest_query = (
        lowest_df.writeStream.outputMode("complete")
        .foreachBatch(write_lowest_to_postgre)
        .option("checkpointLocation", "/tmp/checkpoint_lowest_new")
        .start()
    )

    highest_query = (
        highest_df.writeStream.outputMode("complete")
        .foreachBatch(write_highest_to_postgre)
        .option("checkpointLocation", "/tmp/checkpoint_highest_new")
        .start()
    )

    lowest_query.awaitTermination()
    highest_query.awaitTermination()


if __name__ == "__main__":
    main()
