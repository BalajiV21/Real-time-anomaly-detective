"""
Spark Structured Streaming consumer.

Reads raw trade ticks from Kafka, computes windowed aggregations, runs anomaly
detectors on each micro-batch, and publishes flagged anomalies to Kafka and
TimescaleDB.

    python -m streaming.consumer

Prerequisites:
    - JAVA_HOME set (JDK 11 or 17)
    - Docker stack running: docker-compose up -d
    - spark-sql-kafka JAR resolved automatically via spark.jars.packages
"""

from __future__ import annotations

import logging

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StringType

from config import settings
from detection.publisher import AnomalyPublisher
from streaming.aggregations import compute_windows, save_aggregations
from streaming.schemas import TRADE_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

KAFKA_PACKAGE    = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
CHECKPOINT_DIR   = "/tmp/anomaly_detective/checkpoint"
TRIGGER_INTERVAL = "10 seconds"

_publisher: AnomalyPublisher | None = None


def _get_publisher() -> AnomalyPublisher:
    global _publisher
    if _publisher is None:
        _publisher = AnomalyPublisher()
    return _publisher


def _build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FinancialAnomalyDetective")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    pdf: pd.DataFrame = batch_df.toPandas()
    symbols = pdf["symbol"].unique().tolist()
    logger.info("Batch %d: %d records | %s", batch_id, len(pdf), symbols)

    agg_rows = compute_windows(pdf)
    save_aggregations(agg_rows)

    publisher = _get_publisher()
    for symbol in symbols:
        symbol_trades = pdf[pdf["symbol"] == symbol]
        symbol_aggs   = [r for r in agg_rows if r["symbol"] == symbol]
        try:
            results = publisher.detect_and_publish(
                symbol=symbol,
                trades_df=symbol_trades,
                aggregations=symbol_aggs,
            )
            flagged = [r for r in results if r.is_anomaly]
            if flagged:
                logger.info("%s: %d anomaly/anomalies detected.", symbol, len(flagged))
        except Exception as exc:
            logger.error("Detection error for %s (batch %d): %s", symbol, batch_id, exc)


def run() -> None:
    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Consumer starting. Kafka=%s  topic=%s",
                settings.KAFKA_BOOTSTRAP_SERVERS, settings.KAFKA_TOPICS["TRADES"])

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe",               settings.KAFKA_TOPICS["TRADES"])
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .load()
    )

    parsed = (
        raw
        .select(col("value").cast(StringType()).alias("json_str"))
        .select(from_json(col("json_str"), TRADE_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("trade_timestamp_ts", to_timestamp("trade_timestamp"))
        .withWatermark("trade_timestamp_ts", "30 seconds")
    )

    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    logger.info("Streaming query active (trigger=%s).", TRIGGER_INTERVAL)
    query.awaitTermination()


if __name__ == "__main__":
    run()
