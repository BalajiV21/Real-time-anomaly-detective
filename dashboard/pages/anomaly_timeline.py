"""
Anomaly Timeline page.

Queries anomaly_logs from PostgreSQL and renders a colour-coded scatter
plot on a timeline, with filter controls for symbol, severity, detection
method, and date range.

Called from dashboard/app.py as show().
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

from config.settings import SYMBOLS
from dashboard.components.anomaly_card import render_anomaly_card
from storage.connection import get_db

# Severity display order and colours
SEVERITY_ORDER  = ["low", "medium", "high", "critical"]
SEVERITY_COLORS = {
    "low":      "#28a745",
    "medium":   "#ffc107",
    "high":     "#fd7e14",
    "critical": "#dc3545",
}


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=15)
def _load_anomalies(
    symbol: Optional[str],
    severity: Optional[str],
    method: Optional[str],
    since: datetime,
    limit: int = 500,
) -> List[Dict]:
    conditions = ["detected_at >= :since"]
    params: Dict = {"since": since, "limit": limit}

    if symbol and symbol != "All":
        conditions.append("symbol = :symbol")
        params["symbol"] = symbol
    if severity and severity != "All":
        conditions.append("severity = :severity")
        params["severity"] = severity
    if method and method != "All":
        conditions.append("detection_method = :method")
        params["method"] = method

    where = " AND ".join(conditions)
    sql = text(
        f"""
        SELECT id, symbol, anomaly_type, severity, detection_method,
               anomaly_score, price_at_anomaly, volume_at_anomaly,
               context, detected_at
        FROM   anomaly_logs
        WHERE  {where}
        ORDER  BY detected_at DESC
        LIMIT  :limit
        """
    )
    try:
        with get_db() as session:
            rows = session.execute(sql, params).mappings().all()
        return [dict(r) for r in rows]
    except Exception as exc:
        st.error(f"DB error: {exc}")
        return []


@st.cache_data(ttl=15)
def _anomaly_counts_by_severity() -> Dict[str, int]:
    sql = text(
        """
        SELECT severity, COUNT(*) AS cnt
        FROM   anomaly_logs
        GROUP  BY severity
        """
    )
    try:
        with get_db() as session:
            rows = session.execute(sql).mappings().all()
        return {r["severity"]: int(r["cnt"]) for r in rows}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Chart builder
# ---------------------------------------------------------------------------

def _build_timeline_chart(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No anomalies found for the selected filters.",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color="gray"),
        )
        fig.update_layout(title="Anomaly Timeline", height=400)
        return fig

    fig = go.Figure()

    for severity in SEVERITY_ORDER:
        sub = df[df["severity"] == severity]
        if sub.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=sub["detected_at"],
                y=sub["symbol"],
                mode="markers",
                name=severity.capitalize(),
                marker=dict(
                    color=SEVERITY_COLORS.get(severity, "#888"),
                    size=sub["anomaly_score"].clip(upper=20) + 6,
                    symbol="circle",
                    line=dict(width=1, color="white"),
                    opacity=0.85,
                ),
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Time: %{x|%Y-%m-%d %H:%M:%S}<br>"
                    "Type: %{customdata[0]}<br>"
                    "Method: %{customdata[1]}<br>"
                    "Score: %{customdata[2]:.4f}"
                    "<extra></extra>"
                ),
                customdata=sub[["anomaly_type", "detection_method", "anomaly_score"]].values,
            )
        )

    fig.update_layout(
        title="Anomaly Timeline (bubble size ∝ anomaly score)",
        xaxis_title="Detected At",
        yaxis_title="Symbol",
        height=max(300, len(df["symbol"].unique()) * 60 + 100),
        margin=dict(l=60, r=20, t=50, b=40),
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="closest",
    )
    return fig


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

def show() -> None:
    st.title("Anomaly Timeline")

    # ---- Summary metrics row -------------------------------------------
    counts = _anomaly_counts_by_severity()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Critical", counts.get("critical", 0), delta_color="inverse")
    c2.metric("High",     counts.get("high",     0), delta_color="inverse")
    c3.metric("Medium",   counts.get("medium",   0))
    c4.metric("Low",      counts.get("low",      0))

    st.markdown("---")

    # ---- Filters --------------------------------------------------------
    st.subheader("Filters")
    fc1, fc2, fc3, fc4 = st.columns(4)

    symbol_options   = ["All"] + SYMBOLS
    severity_options = ["All"] + SEVERITY_ORDER
    method_options   = ["All", "zscore", "iqr", "volume_spike", "isolation_forest"]
    lookback_options = {"Last 1 hour": 1, "Last 6 hours": 6, "Last 24 hours": 24, "Last 7 days": 168}

    sel_symbol   = fc1.selectbox("Symbol",           symbol_options,   key="at_symbol")
    sel_severity = fc2.selectbox("Severity",          severity_options, key="at_severity")
    sel_method   = fc3.selectbox("Detection Method",  method_options,   key="at_method")
    sel_lookback = fc4.selectbox("Time Window",       list(lookback_options.keys()), key="at_lookback")

    since_hours = lookback_options[sel_lookback]
    since_dt    = datetime.now(timezone.utc) - timedelta(hours=since_hours)

    # ---- Load data ------------------------------------------------------
    rows = _load_anomalies(
        symbol=sel_symbol   if sel_symbol   != "All" else None,
        severity=sel_severity if sel_severity != "All" else None,
        method=sel_method   if sel_method   != "All" else None,
        since=since_dt,
    )

    st.caption(f"{len(rows)} anomalies found.")

    if rows:
        df = pd.DataFrame(rows)
        df["detected_at"]   = pd.to_datetime(df["detected_at"], utc=True, errors="coerce")
        df["anomaly_score"] = df["anomaly_score"].astype(float)

        # ---- Timeline scatter chart ------------------------------------
        st.plotly_chart(
            _build_timeline_chart(df),
            use_container_width=True,
            key="timeline_chart",
        )

        # ---- Breakdown bar chart ---------------------------------------
        st.markdown("---")
        st.subheader("Breakdown by Symbol × Detection Method")
        pivot = df.groupby(["symbol", "detection_method"]).size().reset_index(name="count")
        bar_fig = px.bar(
            pivot,
            x="symbol",
            y="count",
            color="detection_method",
            barmode="stack",
            title="Anomaly Count by Symbol and Method",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        bar_fig.update_layout(
            height=300,
            margin=dict(l=40, r=20, t=40, b=30),
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        st.plotly_chart(bar_fig, use_container_width=True, key="breakdown_bar")

        # ---- Detail cards ----------------------------------------------
        st.markdown("---")
        st.subheader("Anomaly Details")
        for row in rows[:50]:   # cap at 50 cards to keep page fast
            render_anomaly_card(row)
    else:
        st.info("No anomalies detected yet for the selected filters. The pipeline may still be warming up.")
