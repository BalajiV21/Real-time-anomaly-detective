"""
WebSocket trade producer for the Real-Time Financial Anomaly Detective.

Connects to the Finnhub WebSocket feed, subscribes to equity and crypto symbols,
and for every incoming trade:
  - Publishes the raw trade message to the Kafka 'stock-trades' topic.
  - Persists the tick to the TimescaleDB raw_trades hypertable.

Run from the project root:
    python -m producers.websocket_trade_producer
"""

import json
import logging
import time
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings
from storage.db_connection import get_db
from storage.db_utils import insert_raw_trade

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
WS_URL = f"wss://ws.finnhub.io?token={settings.FINNHUB_API_KEY}"
KAFKA_TOPIC = settings.KAFKA_TOPICS["TRADES"]
ALL_SYMBOLS = settings.SYMBOLS + settings.CRYPTO_SYMBOLS

MAX_RETRIES = 5
BASE_BACKOFF = 5      # seconds
MAX_BACKOFF = 60      # seconds

# ---------------------------------------------------------------------------
# Kafka producer (module-level singleton)
# ---------------------------------------------------------------------------

def _create_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


kafka_producer: KafkaProducer | None = None


def _get_kafka_producer() -> KafkaProducer:
    global kafka_producer
    if kafka_producer is None:
        kafka_producer = _create_kafka_producer()
    return kafka_producer


# ---------------------------------------------------------------------------
# WebSocket handlers
# ---------------------------------------------------------------------------

def on_message(ws: websocket.WebSocketApp, raw: str) -> None:
    """Handle an incoming WebSocket frame."""
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("Could not parse WebSocket message: %s | error: %s", raw, exc)
        return

    msg_type = msg.get("type")

    if msg_type != "trade":
        logger.debug("Ignored message type=%s", msg_type)
        return

    trades = msg.get("data", [])
    producer = _get_kafka_producer()

    for trade in trades:
        symbol: str = trade.get("s", "UNKNOWN")
        price: float = trade.get("p", 0.0)
        volume: float = trade.get("v", 0.0)
        ts_ms: int = trade.get("t", 0)
        conditions: list = trade.get("c") or []

        trade_timestamp = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

        logger.info(
            "Trade: %s @ $%.4f, vol=%.2f, time=%s",
            symbol, price, volume, trade_timestamp.isoformat(),
        )

        # ---- Publish to Kafka -----------------------------------------------
        payload = {
            "symbol": symbol,
            "price": price,
            "volume": volume,
            "trade_timestamp": trade_timestamp.isoformat(),
            "conditions": conditions,
        }
        try:
            producer.send(KAFKA_TOPIC, key=symbol, value=payload)
        except KafkaError as exc:
            logger.error("Kafka publish failed for %s: %s", symbol, exc)

        # ---- Persist to TimescaleDB -----------------------------------------
        try:
            with get_db() as session:
                insert_raw_trade(
                    session,
                    symbol=symbol,
                    price=price,
                    volume=volume,
                    trade_timestamp=trade_timestamp,
                    conditions=conditions,
                )
        except Exception as exc:
            logger.error("DB insert failed for trade %s: %s", symbol, exc)


def on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
    logger.error("WebSocket error: %s", error)


def on_close(ws: websocket.WebSocketApp, close_status_code: int, close_msg: str) -> None:
    logger.warning(
        "WebSocket closed – status=%s, msg=%s",
        close_status_code, close_msg,
    )


def on_open(ws: websocket.WebSocketApp) -> None:
    """Subscribe to all configured symbols once the connection is established."""
    logger.info("WebSocket connection opened. Subscribing to %d symbols…", len(ALL_SYMBOLS))
    for symbol in ALL_SYMBOLS:
        sub_msg = json.dumps({"type": "subscribe", "symbol": symbol})
        ws.send(sub_msg)
        logger.info("Subscribed to: %s", symbol)


# ---------------------------------------------------------------------------
# Reconnect loop with exponential back-off
# ---------------------------------------------------------------------------

def run_with_backoff() -> None:
    """
    Run the WebSocket client with exponential back-off on failure.
    Retries up to MAX_RETRIES times before giving up.
    """
    attempt = 0
    backoff = BASE_BACKOFF

    while attempt <= MAX_RETRIES:
        logger.info("Connecting to Finnhub WebSocket (attempt %d/%d)…", attempt + 1, MAX_RETRIES + 1)
        ws_app = websocket.WebSocketApp(
            WS_URL,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )
        ws_app.run_forever(ping_interval=30, ping_timeout=10)

        # If we reach here the connection has been closed or errored out
        if attempt >= MAX_RETRIES:
            logger.critical("Exceeded maximum reconnect attempts (%d). Exiting.", MAX_RETRIES)
            break

        logger.info("Reconnecting in %d seconds…", backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)
        attempt += 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Starting WebSocket trade producer for symbols: %s", ALL_SYMBOLS)
    run_with_backoff()
