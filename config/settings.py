"""
Central configuration module for the Real-Time Financial Anomaly Detective.
Loads all settings from the .env file via python-dotenv.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Finnhub API
# ---------------------------------------------------------------------------
FINNHUB_API_KEY: str = os.getenv("FINNHUB_API_KEY", "")

# ---------------------------------------------------------------------------
# Database (TimescaleDB / PostgreSQL)
# ---------------------------------------------------------------------------
DB_HOST: str = os.getenv("DB_HOST", "localhost")
DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
DB_NAME: str = os.getenv("DB_NAME", "anomaly_detective")
DB_USER: str = os.getenv("DB_USER", "postgres")
DB_PASSWORD: str = os.getenv("DB_PASSWORD", "postgres")

DATABASE_URL: str = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# ---------------------------------------------------------------------------
# Symbols to track
# ---------------------------------------------------------------------------
SYMBOLS: list[str] = ["AAPL", "TSLA", "AMZN", "MSFT", "GOOGL"]

# Crypto symbol used for 24/7 testing (market never closes)
CRYPTO_SYMBOLS: list[str] = ["BINANCE:BTCUSDT"]

# ---------------------------------------------------------------------------
# Kafka topic names
# ---------------------------------------------------------------------------
KAFKA_TOPICS: dict[str, str] = {
    "TRADES": "stock-trades",
    "QUOTES": "stock-quotes",
    "NEWS": "market-news",
    "SENTIMENT": "sentiment",
    "ANOMALIES": "anomalies",
}

# ---------------------------------------------------------------------------
# Producer poll intervals (seconds)
# ---------------------------------------------------------------------------
POLL_INTERVALS: dict[str, int] = {
    "QUOTES": 60,
    "NEWS": 300,
    "SENTIMENT": 600,
}
