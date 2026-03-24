"""
Quote producer for the Real-Time Financial Anomaly Detective.

Polls the Finnhub REST /quote endpoint for each configured equity symbol every
POLL_INTERVALS['QUOTES'] seconds (default 60 s), then:
  - Publishes the quote snapshot to the Kafka 'stock-quotes' topic.
  - Persists the snapshot to the TimescaleDB stock_quotes hypertable.

Run from the project root:
    python -m producers.quote_producer
"""

import json
import logging
import time

import finnhub
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings
from storage.db_connection import get_db
from storage.db_utils import insert_stock_quote

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
KAFKA_TOPIC = settings.KAFKA_TOPICS["QUOTES"]
POLL_INTERVAL = settings.POLL_INTERVALS["QUOTES"]   # seconds

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

def fetch_and_publish_quote(symbol: str) -> None:
    """
    Fetch the latest quote for *symbol*, publish to Kafka, and persist to DB.
    Errors are logged and swallowed so the polling loop continues.
    """
    try:
        quote = finnhub_client.quote(symbol)
    except Exception as exc:
        logger.error("Finnhub /quote error for %s: %s", symbol, exc)
        return

    if not quote or quote.get("c") is None:
        logger.warning("Empty or null quote response for %s", symbol)
        return

    current_price: float = quote.get("c", 0.0)
    percent_change: float = quote.get("dp", 0.0)

    logger.info(
        "Quote: %s = $%.4f (change: %+.2f%%)",
        symbol, current_price, percent_change,
    )

    # ---- Publish to Kafka ---------------------------------------------------
    payload = {
        "symbol": symbol,
        "c": quote.get("c"),
        "d": quote.get("d"),
        "dp": quote.get("dp"),
        "h": quote.get("h"),
        "l": quote.get("l"),
        "o": quote.get("o"),
        "pc": quote.get("pc"),
    }
    try:
        kafka_producer.send(KAFKA_TOPIC, key=symbol, value=payload)
    except KafkaError as exc:
        logger.error("Kafka publish failed for quote %s: %s", symbol, exc)

    # ---- Persist to TimescaleDB --------------------------------------------
    try:
        with get_db() as session:
            insert_stock_quote(session, symbol=symbol, quote_dict=quote)
    except Exception as exc:
        logger.error("DB insert failed for quote %s: %s", symbol, exc)


def poll_forever() -> None:
    """
    Continuously poll all configured equity symbols in a round-robin cycle,
    sleeping POLL_INTERVAL seconds between full cycles.
    """
    logger.info(
        "Quote producer started. Polling %d symbols every %d s.",
        len(settings.SYMBOLS), POLL_INTERVAL,
    )
    while True:
        for symbol in settings.SYMBOLS:
            fetch_and_publish_quote(symbol)

        logger.info("Cycle complete. Sleeping %d s before next poll…", POLL_INTERVAL)
        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    poll_forever()
