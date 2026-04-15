"""
Quote producer.

Polls Finnhub /quote for each configured symbol every POLL_INTERVALS['QUOTES']
seconds, publishes snapshots to Kafka, and persists them to TimescaleDB.

    python -m ingestion.quote_producer
"""

import json
import logging
import time

import finnhub
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings
from storage.connection import get_db
from storage.queries import insert_stock_quote

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

KAFKA_TOPIC    = settings.KAFKA_TOPICS["QUOTES"]
POLL_INTERVAL  = settings.POLL_INTERVALS["QUOTES"]

finnhub_client = finnhub.Client(api_key=settings.FINNHUB_API_KEY)

kafka_producer = KafkaProducer(
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    acks="all",
    retries=3,
)


def fetch_and_publish(symbol: str) -> None:
    try:
        quote = finnhub_client.quote(symbol)
    except Exception as exc:
        logger.error("Finnhub /quote error for %s: %s", symbol, exc)
        return

    if not quote or quote.get("c") is None:
        logger.warning("Empty quote for %s", symbol)
        return

    logger.info("Quote: %s = $%.4f (%+.2f%%)", symbol, quote["c"], quote.get("dp", 0))

    try:
        kafka_producer.send(KAFKA_TOPIC, key=symbol, value={"symbol": symbol, **quote})
    except KafkaError as exc:
        logger.error("Kafka publish failed for quote %s: %s", symbol, exc)

    try:
        with get_db() as session:
            insert_stock_quote(session, symbol=symbol, quote_dict=quote)
    except Exception as exc:
        logger.error("DB insert failed for quote %s: %s", symbol, exc)


def run() -> None:
    logger.info("Quote producer started. Polling %d symbols every %d s.", len(settings.SYMBOLS), POLL_INTERVAL)
    while True:
        for symbol in settings.SYMBOLS:
            fetch_and_publish(symbol)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
