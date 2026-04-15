"""
Sentiment producer.

Polls Finnhub /news-sentiment for each symbol every POLL_INTERVALS['SENTIMENT']
seconds, publishes payloads to Kafka, and persists to TimescaleDB.

    python -m ingestion.sentiment_producer
"""

import json
import logging
import time

import finnhub
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings
from storage.connection import get_db
from storage.queries import insert_sentiment

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

KAFKA_TOPIC   = settings.KAFKA_TOPICS["SENTIMENT"]
POLL_INTERVAL = settings.POLL_INTERVALS["SENTIMENT"]

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
        data = finnhub_client.news_sentiment(symbol)
    except Exception as exc:
        logger.error("Finnhub /news-sentiment error for %s: %s", symbol, exc)
        return

    if not data:
        logger.warning("Empty sentiment response for %s", symbol)
        return

    score = (data.get("sentiment") or {}).get("bullishPercent") or data.get("companyNewsScore", 0.0)
    logger.info("Sentiment: %s score=%.4f", symbol, score or 0.0)

    try:
        kafka_producer.send(KAFKA_TOPIC, key=symbol, value={"symbol": symbol, **data})
    except KafkaError as exc:
        logger.error("Kafka publish failed for sentiment %s: %s", symbol, exc)

    try:
        with get_db() as session:
            insert_sentiment(session, symbol=symbol, sentiment_dict=data)
    except Exception as exc:
        logger.error("DB insert failed for sentiment %s: %s", symbol, exc)


def run() -> None:
    logger.info("Sentiment producer started. Polling %d symbols every %d s.", len(settings.SYMBOLS), POLL_INTERVAL)
    while True:
        for symbol in settings.SYMBOLS:
            fetch_and_publish(symbol)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
