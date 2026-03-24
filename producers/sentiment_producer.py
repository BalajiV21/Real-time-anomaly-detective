"""
Sentiment producer for the Real-Time Financial Anomaly Detective.

Polls the Finnhub REST /news-sentiment endpoint for each configured equity
symbol every POLL_INTERVALS['SENTIMENT'] seconds (default 600 s), then:
  - Publishes the sentiment payload to the Kafka 'sentiment' topic.
  - Persists the record to the TimescaleDB sentiment_scores hypertable.

Run from the project root:
    python -m producers.sentiment_producer
"""

import json
import logging
import time

import finnhub
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings
from storage.db_connection import get_db
from storage.db_utils import insert_sentiment

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
KAFKA_TOPIC = settings.KAFKA_TOPICS["SENTIMENT"]
POLL_INTERVAL = settings.POLL_INTERVALS["SENTIMENT"]   # seconds

# ---------------------------------------------------------------------------
# Clients (module-level singletons)
# ---------------------------------------------------------------------------
finnhub_client = finnhub.Client(api_key=settings.FINNHUB_API_KEY)

kafka_producer = KafkaProducer(
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    acks="all",
    retries=3,
)


# ---------------------------------------------------------------------------
# Core polling logic
# ---------------------------------------------------------------------------

def fetch_and_publish_sentiment(symbol: str) -> None:
    """
    Fetch news-sentiment for *symbol*, publish to Kafka, and persist to DB.
    Errors are logged and swallowed so the polling loop continues.
    """
    try:
        sentiment_data = finnhub_client.news_sentiment(symbol)
    except Exception as exc:
        logger.error("Finnhub /news-sentiment error for %s: %s", symbol, exc)
        return

    if not sentiment_data:
        logger.warning("Empty sentiment response for %s", symbol)
        return

    # The overall bullish score is the primary signal we report
    sentiment_sub = sentiment_data.get("sentiment", {})
    score = sentiment_sub.get("bullishPercent") or sentiment_data.get("companyNewsScore", 0.0)

    logger.info("Sentiment: %s score=%.4f", symbol, score or 0.0)

    # ---- Publish to Kafka ---------------------------------------------------
    payload = {
        "symbol": symbol,
        **sentiment_data,
    }
    try:
        kafka_producer.send(KAFKA_TOPIC, key=symbol, value=payload)
    except KafkaError as exc:
        logger.error("Kafka publish failed for sentiment %s: %s", symbol, exc)

    # ---- Persist to TimescaleDB --------------------------------------------
    try:
        with get_db() as session:
            insert_sentiment(session, symbol=symbol, sentiment_dict=sentiment_data)
    except Exception as exc:
        logger.error("DB insert failed for sentiment %s: %s", symbol, exc)


def poll_forever() -> None:
    """
    Continuously poll sentiment for all configured equity symbols, sleeping
    POLL_INTERVAL seconds between full cycles.
    """
    logger.info(
        "Sentiment producer started. Polling %d symbols every %d s.",
        len(settings.SYMBOLS), POLL_INTERVAL,
    )
    while True:
        for symbol in settings.SYMBOLS:
            fetch_and_publish_sentiment(symbol)

        logger.info(
            "Sentiment cycle complete. Sleeping %d s before next poll…", POLL_INTERVAL
        )
        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    poll_forever()
