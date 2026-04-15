"""
System Health page.

Shows real-time pipeline health: row counts per table, recent errors,
Kafka topic status indicators, and throughput rates.

Called from dashboard/app.py as show().
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Dict

import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

from storage.connection import get_db

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=15)
def _table_counts() -> Dict[str, int]:
    tables = [
        "raw_trades",
        "stock_quotes",
        "sentiment_scores",
        "aggregated_metrics",
        "anomaly_logs",
        "root_cause_reports",
        "agent_audit_trail",
    ]
    counts: Dict[str, int] = {}
    try:
        with get_db() as session:
            for tbl in tables:
                val = session.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
                counts[tbl] = int(val or 0)
    except Exception as exc:
        st.error(f"DB error: {exc}")
    return counts


@st.cache_data(ttl=15)
def _recent_throughput() -> Dict[str, int]:
    """Rows inserted in the last 60 seconds per streaming table."""
    since = datetime.now(timezone.utc) - timedelta(seconds=60)
    queries = {
        "raw_trades (60s)":          "SELECT COUNT(*) FROM raw_trades          WHERE created_at   >= :since",
        "stock_quotes (60s)":        "SELECT COUNT(*) FROM stock_quotes        WHERE created_at   >= :since",
        "aggregated_metrics (60s)":  "SELECT COUNT(*) FROM aggregated_metrics  WHERE created_at   >= :since",
        "anomaly_logs (60s)":        "SELECT COUNT(*) FROM anomaly_logs        WHERE created_at   >= :since",
    }
    result: Dict[str, int] = {}
    try:
        with get_db() as session:
            for label, sql in queries.items():
                val = session.execute(text(sql), {"since": since}).scalar()
                result[label] = int(val or 0)
    except Exception:
        pass
    return result


@st.cache_data(ttl=15)
def _recent_anomalies_by_severity() -> Dict[str, int]:
    since = datetime.now(timezone.utc) - timedelta(hours=1)
    sql = text(
        """
        SELECT severity, COUNT(*) AS cnt
        FROM   anomaly_logs
        WHERE  detected_at >= :since
        GROUP  BY severity
        """
    )
    try:
        with get_db() as session:
            rows = session.execute(sql, {"since": since}).mappings().all()
        return {r["severity"]: int(r["cnt"]) for r in rows}
    except Exception:
        return {}


@st.cache_data(ttl=15)
def _latest_timestamps() -> Dict[str, str]:
    queries = {
        "Last trade":      "SELECT MAX(trade_timestamp) FROM raw_trades",
        "Last quote":      "SELECT MAX(quoted_at)       FROM stock_quotes",
        "Last sentiment":  "SELECT MAX(scored_at)       FROM sentiment_scores",
        "Last anomaly":    "SELECT MAX(detected_at)     FROM anomaly_logs",
        "Last report":     "SELECT MAX(generated_at)    FROM root_cause_reports",
    }
    result: Dict[str, str] = {}
    try:
        with get_db() as session:
            for label, sql in queries.items():
                val = session.execute(text(sql)).scalar()
                result[label] = str(val) if val else "—"
    except Exception:
        pass
    return result


# ---------------------------------------------------------------------------
# Health indicator helpers
# ---------------------------------------------------------------------------

def _status_indicator(label: str, is_healthy: bool) -> None:
    icon  = "🟢" if is_healthy else "🔴"
    state = "RUNNING" if is_healthy else "NO DATA"
    st.markdown(f"{icon} **{label}** — `{state}`")


def _build_table_counts_bar(counts: Dict[str, int]) -> go.Figure:
    labels = list(counts.keys())
    values = list(counts.values())

    fig = go.Figure(
        go.Bar(
            x=labels,
            y=values,
            marker_color="#1f77b4",
            text=values,
            textposition="outside",
        )
    )
    fig.update_layout(
        title="Total Rows per Table",
        xaxis_title="Table",
        yaxis_title="Row Count",
        height=320,
        margin=dict(l=40, r=20, t=50, b=80),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis_tickangle=-30,
    )
    return fig


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

def show() -> None:
    st.title("System Health")

    auto_refresh = st.checkbox("Auto-refresh (15 s)", value=True, key="sh_refresh")

    # ---- Pipeline component status --------------------------------------
    st.subheader("Component Status")

    counts = _table_counts()
    timestamps = _latest_timestamps()

    recent_trade  = timestamps.get("Last trade",     "—")
    recent_quote  = timestamps.get("Last quote",     "—")
    recent_sent   = timestamps.get("Last sentiment", "—")
    recent_anomal = timestamps.get("Last anomaly",   "—")

    trade_healthy   = counts.get("raw_trades",     0) > 0
    quote_healthy   = counts.get("stock_quotes",   0) > 0
    agg_healthy     = counts.get("aggregated_metrics", 0) > 0
    anomaly_healthy = counts.get("anomaly_logs",   0) > 0
    report_healthy  = counts.get("root_cause_reports", 0) > 0

    c1, c2 = st.columns(2)
    with c1:
        _status_indicator("WebSocket Trade Producer",  trade_healthy)
        _status_indicator("Quote Producer (REST)",     quote_healthy)
        _status_indicator("Sentiment Producer (REST)", counts.get("sentiment_scores", 0) > 0)
    with c2:
        _status_indicator("Spark Stream Consumer",     agg_healthy)
        _status_indicator("Anomaly Detection Engine",  anomaly_healthy)
        _status_indicator("LangGraph Root Cause Agent",report_healthy)

    st.markdown("---")

    # ---- Latest activity timestamps ------------------------------------
    st.subheader("Latest Activity")
    ts_cols = st.columns(len(timestamps))
    for i, (label, ts) in enumerate(timestamps.items()):
        ts_cols[i].metric(label, ts[:19] if ts and ts != "—" else "—")

    st.markdown("---")

    # ---- Table row counts chart ----------------------------------------
    st.subheader("Database Record Counts")
    st.plotly_chart(
        _build_table_counts_bar(counts),
        use_container_width=True,
        key="counts_bar",
    )

    # ---- Throughput (last 60 s) ----------------------------------------
    st.subheader("Ingestion Throughput (last 60 seconds)")
    throughput = _recent_throughput()
    tp_cols    = st.columns(len(throughput))
    for i, (label, count) in enumerate(throughput.items()):
        tp_cols[i].metric(label, count)

    st.markdown("---")

    # ---- Anomaly severity breakdown (last hour) ------------------------
    st.subheader("Anomalies Detected — Last Hour")
    severity_counts = _recent_anomalies_by_severity()
    if severity_counts:
        sc_cols = st.columns(4)
        severity_order = ["critical", "high", "medium", "low"]
        colors         = ["#dc3545", "#fd7e14", "#ffc107", "#28a745"]
        for i, (sev, col) in enumerate(zip(severity_order, sc_cols)):
            col.metric(sev.capitalize(), severity_counts.get(sev, 0))
    else:
        st.info("No anomalies in the last hour.")

    # ---- Raw table counts display --------------------------------------
    st.markdown("---")
    st.subheader("Raw Table Counts")
    count_df_rows = [{"Table": k, "Rows": f"{v:,}"} for k, v in counts.items()]
    st.table(count_df_rows)

    # ---- Auto-refresh --------------------------------------------------
    if auto_refresh:
        time.sleep(15)
        st.rerun()
