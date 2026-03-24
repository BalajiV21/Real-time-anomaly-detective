-- =============================================================================
-- TimescaleDB hypertable setup for Real-Time Financial Anomaly Detective
-- Run this file against the anomaly_detective database AFTER the TimescaleDB
-- extension has been enabled (the official Docker image enables it automatically).
-- =============================================================================

-- Enable the TimescaleDB extension (idempotent)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- -----------------------------------------------------------------------------
-- 1. raw_trades  –  tick-level trade data streamed from Finnhub WebSocket
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_trades (
    id               BIGSERIAL        PRIMARY KEY,
    symbol           VARCHAR(20),
    price            DECIMAL(12, 4),
    volume           DECIMAL(20, 4),
    trade_timestamp  TIMESTAMPTZ      NOT NULL,
    conditions       TEXT[],
    created_at       TIMESTAMPTZ      DEFAULT NOW()
);

SELECT create_hypertable(
    'raw_trades',
    'trade_timestamp',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_raw_trades_symbol_ts
    ON raw_trades (symbol, trade_timestamp DESC);

-- -----------------------------------------------------------------------------
-- 2. aggregated_metrics  –  pre-computed OHLCV / statistical windows
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aggregated_metrics (
    id            BIGSERIAL    PRIMARY KEY,
    symbol        VARCHAR(20),
    window_size   VARCHAR(10),
    window_start  TIMESTAMPTZ  NOT NULL,
    window_end    TIMESTAMPTZ  NOT NULL,
    avg_price     DECIMAL(12, 4),
    std_dev       DECIMAL(12, 6),
    total_volume  DECIMAL(20, 4),
    trade_count   INTEGER,
    vwap          DECIMAL(12, 4),
    min_price     DECIMAL(12, 4),
    max_price     DECIMAL(12, 4),
    created_at    TIMESTAMPTZ  DEFAULT NOW()
);

SELECT create_hypertable(
    'aggregated_metrics',
    'window_start',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_agg_metrics_symbol_window
    ON aggregated_metrics (symbol, window_size, window_start DESC);

-- -----------------------------------------------------------------------------
-- 3. stock_quotes  –  REST poll snapshots from /quote endpoint
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_quotes (
    id              BIGSERIAL    PRIMARY KEY,
    symbol          VARCHAR(20),
    current_price   DECIMAL(12, 4),
    change          DECIMAL(12, 4),
    percent_change  DECIMAL(8, 4),
    high_price      DECIMAL(12, 4),
    low_price       DECIMAL(12, 4),
    open_price      DECIMAL(12, 4),
    prev_close      DECIMAL(12, 4),
    quoted_at       TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

SELECT create_hypertable(
    'stock_quotes',
    'quoted_at',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_stock_quotes_symbol_ts
    ON stock_quotes (symbol, quoted_at DESC);

-- -----------------------------------------------------------------------------
-- 4. sentiment_scores  –  company-level sentiment from /news-sentiment
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sentiment_scores (
    id                       BIGSERIAL    PRIMARY KEY,
    symbol                   VARCHAR(20),
    buzz_articles_in_last_week INTEGER,
    buzz_weekly_average      DECIMAL(8, 4),
    company_news_score       DECIMAL(8, 6),
    sector_avg_bullish       DECIMAL(8, 6),
    sector_avg_bearish       DECIMAL(8, 6),
    sentiment_score          DECIMAL(8, 6),
    scored_at                TIMESTAMPTZ  NOT NULL,
    created_at               TIMESTAMPTZ  DEFAULT NOW()
);

SELECT create_hypertable(
    'sentiment_scores',
    'scored_at',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_sentiment_symbol_ts
    ON sentiment_scores (symbol, scored_at DESC);
