"""
WebSocket trade producer.

Connects to the Finnhub WebSocket feed, subscribes to equity and crypto symbols,
and for every incoming trade publishes to Kafka and persists to TimescaleDB.

    python -m ingestion.trade_producer
"""

import json
import logging
import time
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings
from storage.connection import get_db
from storage.queries import insert_raw_trade

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

WS_URL      = f"wss://ws.finnhub.io?token={settings.FINNHUB_API_KEY}"
KAFKA_TOPIC = settings.KAFKA_TOPICS["TRADES"]
ALL_SYMBOLS = settings.SYMBOLS + settings.CRYPTO_SYMBOLS

MAX_RETRIES  = 5
BASE_BACKOFF = 5
MAX_BACKOFF  = 60

_producer: KafkaProducer | None = None


def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
        )
    return _producer


def on_message(ws: websocket.WebSocketApp, raw: str) -> None:
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("Unparseable WebSocket message: %s | %s", raw, exc)
        return

    if msg.get("type") != "trade":
        return

    producer = _get_producer()
    for trade in msg.get("data", []):
        symbol    = trade.get("s", "UNKNOWN")
        price     = trade.get("p", 0.0)
        volume    = trade.get("v", 0.0)
        ts_ms     = trade.get("t", 0)
        conditions = trade.get("c") or []

        trade_ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        logger.info("Trade: %s @ $%.4f  vol=%.2f  time=%s", symbol, price, volume, trade_ts.isoformat())

        payload = {
            "symbol":          symbol,
            "price":           price,
            "volume":          volume,
            "trade_timestamp": trade_ts.isoformat(),
            "conditions":      conditions,
        }
        try:
            producer.send(KAFKA_TOPIC, key=symbol, value=payload)
        except KafkaError as exc:
            logger.error("Kafka publish failed for %s: %s", symbol, exc)

        try:
            with get_db() as session:
                insert_raw_trade(session, symbol=symbol, price=price, volume=volume,
                                 trade_timestamp=trade_ts, conditions=conditions)
        except Exception as exc:
            logger.error("DB insert failed for trade %s: %s", symbol, exc)


def on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
    logger.error("WebSocket error: %s", error)


def on_close(ws: websocket.WebSocketApp, code: int, msg: str) -> None:
    logger.warning("WebSocket closed - code=%s msg=%s", code, msg)


def on_open(ws: websocket.WebSocketApp) -> None:
    logger.info("WebSocket open. Subscribing to %d symbols.", len(ALL_SYMBOLS))
    for symbol in ALL_SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))


def run() -> None:
    attempt = 0
    backoff  = BASE_BACKOFF
    while attempt <= MAX_RETRIES:
        logger.info("Connecting to Finnhub WebSocket (attempt %d/%d).", attempt + 1, MAX_RETRIES + 1)
        ws_app = websocket.WebSocketApp(
            WS_URL,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )
        ws_app.run_forever(ping_interval=30, ping_timeout=10)

        if attempt >= MAX_RETRIES:
            logger.critical("Max reconnect attempts (%d) reached. Exiting.", MAX_RETRIES)
            break

        logger.info("Reconnecting in %d s.", backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)
        attempt += 1


if __name__ == "__main__":
    logger.info("Starting trade producer for symbols: %s", ALL_SYMBOLS)
    run()
