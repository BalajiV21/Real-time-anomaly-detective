# Real-Time Financial Anomaly Detective

A production-grade real-time streaming system that ingests live financial market data, detects anomalies using hybrid statistical and ML methods, and surfaces findings on an interactive Streamlit dashboard.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Data Ingestion Layer                        │
│  Finnhub WebSocket ──► trade_producer                               │
│  Finnhub REST API  ──► quote_producer / news_producer /             │
│                        sentiment_producer                           │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ Kafka topics
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Stream Processing Layer                        │
│  Spark Structured Streaming                                         │
│  ├── Windowed aggregations  (1m / 5m / 15m OHLCV + VWAP)           │
│  └── foreachBatch dispatch to anomaly detectors                     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       Detection Layer                               │
│  Z-Score · IQR · Volume Spike · Isolation Forest (ML)              │
│  └── Anomalies ──► Kafka "anomalies" topic + TimescaleDB           │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Storage Layer                               │
│  TimescaleDB  raw_trades / aggregated_metrics / stock_quotes /      │
│               sentiment_scores                                      │
│  PostgreSQL   anomaly_logs / root_cause_reports / agent_audit_trail │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       Dashboard Layer                               │
│  Streamlit  ──  Live Prices / Anomaly Timeline /                    │
│                 Root Cause Reports / System Health                  │
└─────────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Technology |
|---|---|
| Data source | [Finnhub API](https://finnhub.io) (WebSocket + REST) |
| Message bus | Apache Kafka (via Confluent Docker image) |
| Stream processing | Apache Spark Structured Streaming (PySpark) |
| Time-series storage | TimescaleDB (PostgreSQL 15 + TimescaleDB extension) |
| Anomaly detection | Z-Score, IQR, Volume Spike, Isolation Forest (scikit-learn) |
| Dashboard | Streamlit + Plotly |
| Infrastructure | Docker Compose |

## Tracked Symbols

`AAPL` · `TSLA` · `AMZN` · `MSFT` · `GOOGL` + `BINANCE:BTCUSDT` (24/7 crypto testing)

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.10+ | |
| Docker Desktop | Latest | Must be running |
| Java JDK | 11 or 17 | Required for Spark. Set `JAVA_HOME`. |
| Finnhub API key | — | Free tier at [finnhub.io](https://finnhub.io/register) |

---

## Quick Start

### 1. Clone and install dependencies

```bash
git clone https://github.com/your-username/real-time-anomaly-detective.git
cd real-time-anomaly-detective
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env and add your FINNHUB_API_KEY
```

### 3. Start infrastructure

```bash
docker-compose up -d
# Wait ~30 seconds for all containers to reach healthy state
docker-compose ps
```

### 4. Initialise the database schema (run once)

```bash
python -c "from storage.connection import init_db; init_db()"
```

### 5. Run the full pipeline (6 terminals from project root)

```bash
# Terminal 1 — WebSocket trade stream (US market hours; crypto 24/7)
python -m ingestion.trade_producer

# Terminal 2 — Quote poller (every 60 s, 24/7)
python -m ingestion.quote_producer

# Terminal 3 — News poller (every 5 min, 24/7)
python -m ingestion.news_producer

# Terminal 4 — Sentiment poller (every 10 min, 24/7)
python -m ingestion.sentiment_producer

# Terminal 5 — Spark streaming consumer + anomaly detection
python -m streaming.consumer

# Terminal 6 — Dashboard
streamlit run dashboard/app.py
```

Open **http://localhost:8501** for the dashboard and **http://localhost:8080** for Kafka UI.

---

## Configuration

All configuration is loaded from the `.env` file via `python-dotenv`. See `.env.example` for the full list of variables.

| Variable | Default | Description |
|---|---|---|
| `FINNHUB_API_KEY` | — | **Required.** Your Finnhub API key. |
| `DB_HOST` | `localhost` | TimescaleDB host |
| `DB_PORT` | `5432` | TimescaleDB port |
| `DB_NAME` | `anomaly_detective` | Database name |
| `DB_USER` | `postgres` | Database user |
| `DB_PASSWORD` | `postgres` | Database password |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |

---

## Detection Methods

| Method | Type | Threshold |
|---|---|---|
| Z-Score | Statistical | \|z\| > 3.0 |
| IQR (Tukey Fence) | Statistical | Outside Q1 − 1.5×IQR … Q3 + 1.5×IQR |
| Volume Spike | Threshold | Current volume > 5× rolling average |
| Isolation Forest | Machine Learning | Anomaly score > 0.55 (requires training) |

### Training the Isolation Forest

After collecting at least 20–30 minutes of data, run once:

```bash
python -c "from detection.ml import IsolationForestDetector; IsolationForestDetector().train_from_db()"
```

The fitted model is saved to `models/isolation_forest.pkl` and loaded automatically on subsequent runs.

---

## Project Structure

```
.
├── config/             Application settings loaded from .env
├── ingestion/          Finnhub data producers (WebSocket + REST pollers)
├── streaming/          Spark Structured Streaming consumer and aggregations
├── detection/          Anomaly detectors (statistical, ML) and Kafka publisher
├── storage/            SQLAlchemy connection pool, query helpers, SQL schema files
├── dashboard/          Streamlit app — pages and reusable chart components
├── scripts/            Standalone utility scripts
└── tests/              Pytest test suite
```

---

## Development

### Running tests

```bash
pytest tests/ -v
```

### Stopping the stack

```bash
# Stop all producers with Ctrl+C, then:
docker-compose down          # preserve data
docker-compose down -v       # wipe data volumes
```

### Makefile shortcuts

```bash
make up          # docker-compose up -d
make down        # docker-compose down
make init-db     # initialise database schema
make test        # run pytest
make dashboard   # launch Streamlit
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Containers not healthy | Wait 30 s, then `docker-compose restart` |
| `init_db()` connection refused | DB still starting — wait and retry |
| No trades from WebSocket | US market closed — `BINANCE:BTCUSDT` works 24/7 |
| Spark `JAVA_HOME` error | Install JDK 17 and set the `JAVA_HOME` environment variable |
| Spark JAR download slow | First-run only (~200 MB download); subsequent runs use cache |
| Port 9092 already in use | Stop conflicting process or change `KAFKA_BOOTSTRAP_SERVERS` in `.env` |

---

## License

MIT
