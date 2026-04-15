"""
Database query helpers — insert and select operations.

All functions accept a SQLAlchemy Session so they can participate in an
existing transaction managed by the caller via get_db().
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def insert_raw_trade(
    session: Session,
    symbol: str,
    price: float,
    volume: float,
    trade_timestamp: datetime,
    conditions: Optional[List[str]] = None,
) -> int:
    result = session.execute(
        text("""
            INSERT INTO raw_trades (symbol, price, volume, trade_timestamp, conditions)
            VALUES (:symbol, :price, :volume, :trade_timestamp, :conditions)
            RETURNING id
        """),
        {"symbol": symbol, "price": price, "volume": volume,
         "trade_timestamp": trade_timestamp, "conditions": conditions or []},
    )
    row_id: int = result.scalar_one()
    logger.debug("Inserted raw_trade id=%d for %s", row_id, symbol)
    return row_id


def insert_stock_quote(session: Session, symbol: str, quote_dict: Dict[str, Any]) -> int:
    result = session.execute(
        text("""
            INSERT INTO stock_quotes
                (symbol, current_price, change, percent_change,
                 high_price, low_price, open_price, prev_close, quoted_at)
            VALUES
                (:symbol, :current_price, :change, :percent_change,
                 :high_price, :low_price, :open_price, :prev_close, :quoted_at)
            RETURNING id
        """),
        {
            "symbol":        symbol,
            "current_price": quote_dict.get("c"),
            "change":        quote_dict.get("d"),
            "percent_change":quote_dict.get("dp"),
            "high_price":    quote_dict.get("h"),
            "low_price":     quote_dict.get("l"),
            "open_price":    quote_dict.get("o"),
            "prev_close":    quote_dict.get("pc"),
            "quoted_at":     datetime.now(timezone.utc),
        },
    )
    row_id: int = result.scalar_one()
    logger.debug("Inserted stock_quote id=%d for %s", row_id, symbol)
    return row_id


def insert_sentiment(session: Session, symbol: str, sentiment_dict: Dict[str, Any]) -> int:
    buzz            = sentiment_dict.get("buzz", {})
    sentiment_sub   = sentiment_dict.get("sentiment", {})
    score = sentiment_sub.get("bullishPercent") or sentiment_dict.get("companyNewsScore")

    result = session.execute(
        text("""
            INSERT INTO sentiment_scores
                (symbol, buzz_articles_in_last_week, buzz_weekly_average,
                 company_news_score, sector_avg_bullish, sector_avg_bearish,
                 sentiment_score, scored_at)
            VALUES
                (:symbol, :buzz_articles, :buzz_avg,
                 :company_score, :sector_bullish, :sector_bearish,
                 :score, :scored_at)
            RETURNING id
        """),
        {
            "symbol":        symbol,
            "buzz_articles": buzz.get("articlesInLastWeek"),
            "buzz_avg":      buzz.get("weeklyAverage"),
            "company_score": sentiment_dict.get("companyNewsScore"),
            "sector_bullish":sentiment_dict.get("sectorAverageBullishPercent"),
            "sector_bearish":sentiment_dict.get("sectorAverageBearishPercent"),
            "score":         score,
            "scored_at":     datetime.now(timezone.utc),
        },
    )
    row_id: int = result.scalar_one()
    logger.debug("Inserted sentiment id=%d for %s (score=%s)", row_id, symbol, score)
    return row_id


def insert_anomaly_log(
    session: Session,
    symbol: str,
    anomaly_type: str,
    severity: str,
    detection_method: str,
    anomaly_score: float,
    price: float,
    volume: float,
    context: Optional[Dict[str, Any]] = None,
) -> int:
    result = session.execute(
        text("""
            INSERT INTO anomaly_logs
                (symbol, anomaly_type, severity, detection_method,
                 anomaly_score, price_at_anomaly, volume_at_anomaly, context)
            VALUES
                (:symbol, :anomaly_type, :severity, :detection_method,
                 :anomaly_score, :price, :volume, :context)
            RETURNING id
        """),
        {
            "symbol":           symbol,
            "anomaly_type":     anomaly_type,
            "severity":         severity,
            "detection_method": detection_method,
            "anomaly_score":    anomaly_score,
            "price":            price,
            "volume":           volume,
            "context":          json.dumps(context) if context else None,
        },
    )
    row_id: int = result.scalar_one()
    logger.info("Inserted anomaly_log id=%d  %s  %s  %s", row_id, symbol, anomaly_type, severity)
    return row_id


def get_recent_trades(session: Session, symbol: str, minutes: int = 60) -> List[Dict]:
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    rows = session.execute(
        text("""
            SELECT id, symbol, price, volume, trade_timestamp, conditions, created_at
            FROM   raw_trades
            WHERE  symbol = :symbol AND trade_timestamp >= :since
            ORDER  BY trade_timestamp ASC
        """),
        {"symbol": symbol, "since": since},
    ).mappings().all()
    return [dict(r) for r in rows]


def get_aggregated_metrics(
    session: Session, symbol: str, window_size: str, limit: int = 100
) -> List[Dict]:
    rows = session.execute(
        text("""
            SELECT id, symbol, window_size, window_start, window_end,
                   avg_price, std_dev, total_volume, trade_count,
                   vwap, min_price, max_price, created_at
            FROM   aggregated_metrics
            WHERE  symbol = :symbol AND window_size = :window_size
            ORDER  BY window_start DESC
            LIMIT  :limit
        """),
        {"symbol": symbol, "window_size": window_size, "limit": limit},
    ).mappings().all()
    return [dict(r) for r in rows]
