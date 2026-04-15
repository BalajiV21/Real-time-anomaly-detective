"""
Windowed aggregation helpers called inside Spark foreachBatch callbacks.

Computes 1-minute, 5-minute, and 15-minute rolling statistics (OHLCV, VWAP,
std dev) per symbol from a Pandas micro-batch, then persists to TimescaleDB.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd
from sqlalchemy import text

from storage.connection import get_db

logger = logging.getLogger(__name__)

WINDOW_CONFIGS: List[Dict] = [
    {"label": "1m",  "delta": timedelta(minutes=1)},
    {"label": "5m",  "delta": timedelta(minutes=5)},
    {"label": "15m", "delta": timedelta(minutes=15)},
]


def _vwap(prices: pd.Series, volumes: pd.Series) -> float:
    total_vol = volumes.sum()
    if total_vol < 1e-9:
        return float(prices.mean())
    return float((prices * volumes).sum() / total_vol)


def compute_windows(batch_df: pd.DataFrame) -> List[Dict]:
    """
    Compute all window aggregations over a micro-batch of raw trades.

    Parameters
    ----------
    batch_df : pd.DataFrame
        Required columns: symbol, price, volume, trade_timestamp (ISO-8601)

    Returns
    -------
    list of dicts matching the aggregated_metrics table schema
    """
    if batch_df.empty:
        return []

    df = batch_df.copy()
    df["event_time"] = pd.to_datetime(df["trade_timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["event_time"])
    if df.empty:
        return []

    batch_end = df["event_time"].max()
    results: List[Dict] = []

    for symbol, group in df.groupby("symbol"):
        for cfg in WINDOW_CONFIGS:
            window_start = batch_end - cfg["delta"]
            windowed     = group[group["event_time"] >= window_start]
            if windowed.empty:
                continue

            wp = windowed["price"].astype(float)
            wv = windowed["volume"].astype(float)
            n  = len(windowed)

            results.append({
                "symbol":       symbol,
                "window_size":  cfg["label"],
                "window_start": window_start.to_pydatetime(),
                "window_end":   batch_end.to_pydatetime(),
                "avg_price":    float(wp.mean()),
                "std_dev":      float(wp.std(ddof=1)) if n > 1 else 0.0,
                "total_volume": float(wv.sum()),
                "trade_count":  n,
                "vwap":         _vwap(wp, wv),
                "min_price":    float(wp.min()),
                "max_price":    float(wp.max()),
            })

    return results


def save_aggregations(rows: List[Dict]) -> None:
    """Persist computed window rows to the aggregated_metrics hypertable."""
    if not rows:
        return

    sql = text(
        """
        INSERT INTO aggregated_metrics
            (symbol, window_size, window_start, window_end,
             avg_price, std_dev, total_volume, trade_count,
             vwap, min_price, max_price)
        VALUES
            (:symbol, :window_size, :window_start, :window_end,
             :avg_price, :std_dev, :total_volume, :trade_count,
             :vwap, :min_price, :max_price)
        """
    )
    try:
        with get_db() as session:
            session.execute(sql, rows)
        logger.info("Saved %d aggregation rows.", len(rows))
    except Exception as exc:
        logger.error("Failed to save aggregations: %s", exc)
