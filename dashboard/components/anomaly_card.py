"""
Reusable anomaly card component.

Renders a single anomaly_logs row as a styled Streamlit card using
st.container() + st.markdown().
"""

from __future__ import annotations

import json
from typing import Any, Dict

import streamlit as st

# Severity → colour mapping (Streamlit uses CSS via st.markdown unsafe_allow_html)
SEVERITY_COLOURS: Dict[str, str] = {
    "low":      "#28a745",   # green
    "medium":   "#ffc107",   # amber
    "high":     "#fd7e14",   # orange
    "critical": "#dc3545",   # red
}

SEVERITY_ICONS: Dict[str, str] = {
    "low":      "●",
    "medium":   "▲",
    "high":     "!!",
    "critical": "★★",
}


def render_anomaly_card(row: Dict[str, Any]) -> None:
    """
    Render one anomaly_logs record as an expander card.

    Parameters
    ----------
    row : dict with keys matching anomaly_logs columns
    """
    severity      = (row.get("severity") or "low").lower()
    colour        = SEVERITY_COLOURS.get(severity, "#6c757d")
    icon          = SEVERITY_ICONS.get(severity, "●")
    symbol        = row.get("symbol", "?")
    anomaly_type  = row.get("anomaly_type", "unknown")
    method        = row.get("detection_method", "—")
    score         = row.get("anomaly_score")
    price         = row.get("price_at_anomaly")
    volume        = row.get("volume_at_anomaly")
    detected_at   = row.get("detected_at")
    context       = row.get("context")

    score_str  = f"{float(score):.4f}"  if score  is not None else "—"
    price_str  = f"${float(price):.4f}" if price  is not None else "—"
    volume_str = f"{float(volume):.2f}" if volume is not None else "—"

    header = (
        f"{icon} **{symbol}** — {anomaly_type.replace('_', ' ').title()} "
        f"| Severity: :{colour}[**{severity.upper()}**] "
        f"| {detected_at}"
    )

    with st.expander(header, expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Symbol",    symbol)
        col2.metric("Price",     price_str)
        col3.metric("Volume",    volume_str)
        col4.metric("Score",     score_str)

        st.markdown(
            f"**Detection method:** `{method}`  |  "
            f"**Anomaly type:** `{anomaly_type}`"
        )

        if context:
            st.markdown("**Context:**")
            ctx = context if isinstance(context, dict) else json.loads(context)
            st.json(ctx)


def render_severity_badge(severity: str) -> str:
    """Return an HTML badge string for inline markdown rendering."""
    colour = SEVERITY_COLOURS.get(severity.lower(), "#6c757d")
    return (
        f'<span style="background-color:{colour};color:white;'
        f'padding:2px 8px;border-radius:4px;font-size:0.8em;'
        f'font-weight:bold;">{severity.upper()}</span>'
    )
