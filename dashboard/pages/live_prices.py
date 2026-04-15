"""
Live Prices page.

Queries the latest stock_quotes rows from TimescaleDB for each tracked symbol,
renders per-symbol price charts, and auto-refreshes every 10 seconds.

Called from dashboard/app.py as show().
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import streamlit as st
from sqlalchemy import text

from config.settings import SYMBOLS
from dashboard.components.price_chart import build_multi_symbol_chart, build_price_chart
from dashboard.components.sentiment_gauge import build_sentiment_gauge, build_sentiment_history_chart
from storage.connection import get_db


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=10)
def _load_quotes(symbol: str, hours: int = 1) -> List[Dict]:
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    sql = text(
        """
        SELECT symbol, current_price, change, percent_change,
               high_price, low_price, open_price, prev_close, quoted_at
        FROM   stock_quotes
        WHERE  symbol   = :symbol
          AND  quoted_at >= :since
        ORDER  BY quoted_at ASC
        """
    )
    try:
        with get_db() as session:
            rows = session.execute(sql, {"symbol": symbol, "since": since}).mappings().all()
        return [dict(r) for r in rows]
    except Exception as exc:
        st.error(f"DB error loading quotes for {symbol}: {exc}")
        return []


@st.cache_data(ttl=10)
def _load_latest_quote(symbol: str) -> Dict:
    sql = text(
        """
        SELECT symbol, current_price, change, percent_change,
               high_price, low_price, quoted_at
        FROM   stock_quotes
        WHERE  symbol = :symbol
        ORDER  BY quoted_at DESC
        LIMIT  1
        """
    )
    try:
        with get_db() as session:
            row = session.execute(sql, {"symbol": symbol}).mappings().first()
        return dict(row) if row else {}
    except Exception:
        return {}


@st.cache_data(ttl=30)
def _load_latest_sentiment(symbol: str) -> Dict:
    sql = text(
        """
        SELECT symbol, sentiment_score, scored_at
        FROM   sentiment_scores
        WHERE  symbol = :symbol
        ORDER  BY scored_at DESC
        LIMIT  1
        """
    )
    try:
        with get_db() as session:
            row = session.execute(sql, {"symbol": symbol}).mappings().first()
        return dict(row) if row else {}
    except Exception:
        return {}


@st.cache_data(ttl=30)
def _load_sentiment_history(symbol: str) -> List[Dict]:
    since = datetime.now(timezone.utc) - timedelta(hours=6)
    sql = text(
        """
        SELECT symbol, sentiment_score, scored_at
        FROM   sentiment_scores
        WHERE  symbol = :symbol AND scored_at >= :since
        ORDER  BY scored_at ASC
        """
    )
    try:
        with get_db() as session:
            rows = session.execute(sql, {"symbol": symbol, "since": since}).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

def show() -> None:
    st.title("Live Prices")

    # Controls row
    col_refresh, col_hours, _ = st.columns([1, 1, 4])
    hours       = col_hours.selectbox("History window", [1, 2, 4, 8], index=0, key="lp_hours")
    auto_refresh = col_refresh.checkbox("Auto-refresh (10 s)", value=True, key="lp_refresh")

    st.markdown("---")

    # ---- Ticker header metrics -------------------------------------------
    st.subheader("Current Snapshot")
    metric_cols = st.columns(len(SYMBOLS))
    for i, symbol in enumerate(SYMBOLS):
        latest = _load_latest_quote(symbol)
        if latest:
            price = float(latest.get("current_price") or 0)
            pct   = float(latest.get("percent_change") or 0)
            metric_cols[i].metric(
                label=symbol,
                value=f"${price:.2f}",
                delta=f"{pct:+.2f}%",
            )
        else:
            metric_cols[i].metric(label=symbol, value="—", delta="no data")

    st.markdown("---")

    # ---- Multi-symbol normalised chart ----------------------------------
    st.subheader("Relative Performance (Normalised to 100)")
    all_records = {sym: _load_quotes(sym, hours) for sym in SYMBOLS}
    st.plotly_chart(
        build_multi_symbol_chart(all_records),
        use_container_width=True,
        key="multi_chart",
    )

    st.markdown("---")

    # ---- Per-symbol charts + sentiment -----------------------------------
    st.subheader("Per-Symbol Detail")
    for symbol in SYMBOLS:
        with st.expander(f"{symbol} — Price & Sentiment", expanded=True):
            records = all_records[symbol]
            c_chart, c_gauge = st.columns([3, 1])

            with c_chart:
                st.plotly_chart(
                    build_price_chart(records, symbol),
                    use_container_width=True,
                    key=f"chart_{symbol}",
                )

            with c_gauge:
                sentiment = _load_latest_sentiment(symbol)
                score     = float(sentiment.get("sentiment_score") or 0.5)
                st.plotly_chart(
                    build_sentiment_gauge(symbol, score),
                    use_container_width=True,
                    key=f"gauge_{symbol}",
                )

            # Sentiment history sparkline
            sent_hist = _load_sentiment_history(symbol)
            if sent_hist:
                st.plotly_chart(
                    build_sentiment_history_chart(sent_hist, symbol),
                    use_container_width=True,
                    key=f"sent_hist_{symbol}",
                )

    # ---- Auto-refresh ---------------------------------------------------
    if auto_refresh:
        time.sleep(10)
        st.rerun()
