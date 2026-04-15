"""
Root Cause Reports page.

Displays AI-generated root cause reports from the root_cause_reports table,
including confidence score badges and the agent audit trail.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from config.settings import SYMBOLS
from storage.connection import get_db


@st.cache_data(ttl=30)
def _load_reports(symbol: Optional[str] = None, limit: int = 100) -> List[Dict]:
    params: Dict = {"limit": limit}
    where = "1=1"
    if symbol and symbol != "All":
        where = "r.symbol = :symbol"
        params["symbol"] = symbol

    sql = text(
        f"""
        SELECT r.id, r.anomaly_log_id, r.symbol, r.summary,
               r.full_report, r.confidence_score, r.root_cause_type,
               r.evidence, r.recommendations, r.agent_model,
               r.tokens_used, r.generated_at,
               a.severity, a.anomaly_type, a.detection_method,
               a.price_at_anomaly, a.detected_at AS anomaly_detected_at
        FROM   root_cause_reports r
        LEFT JOIN anomaly_logs a ON r.anomaly_log_id = a.id
        WHERE  {where}
        ORDER  BY r.generated_at DESC
        LIMIT  :limit
        """
    )
    try:
        with get_db() as session:
            rows = session.execute(sql, params).mappings().all()
        return [dict(r) for r in rows]
    except Exception as exc:
        st.error(f"DB error loading reports: {exc}")
        return []


@st.cache_data(ttl=30)
def _load_audit_trail(report_id: int) -> List[Dict]:
    sql = text(
        """
        SELECT step_name, step_input, step_output, tool_calls, duration_ms, executed_at
        FROM   agent_audit_trail
        WHERE  report_id = :report_id
        ORDER  BY executed_at ASC
        """
    )
    try:
        with get_db() as session:
            rows = session.execute(sql, {"report_id": report_id}).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        return []


@st.cache_data(ttl=30)
def _summary_stats() -> Dict:
    sql = text(
        """
        SELECT COUNT(*)               AS total_reports,
               AVG(confidence_score)  AS avg_confidence,
               AVG(tokens_used)       AS avg_tokens,
               COUNT(DISTINCT symbol) AS symbols_investigated
        FROM root_cause_reports
        """
    )
    try:
        with get_db() as session:
            row = session.execute(sql).mappings().first()
        return dict(row) if row else {}
    except Exception:
        return {}


def _confidence_label(score: Optional[float]) -> tuple[str, str]:
    if score is None:
        return "—", "#888"
    if score >= 0.7:
        return f"{score:.0%} (High)",   "#28a745"
    if score >= 0.4:
        return f"{score:.0%} (Medium)", "#ffc107"
    return f"{score:.0%} (Low)",        "#dc3545"


def show() -> None:
    st.title("Root Cause Reports")
    st.caption("AI-generated investigation reports produced by the root cause agent.")

    stats = _summary_stats()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Reports",        int(stats.get("total_reports",        0) or 0))
    c2.metric("Avg Confidence",       f"{float(stats.get('avg_confidence',  0) or 0):.0%}")
    c3.metric("Avg Tokens Used",      int(stats.get("avg_tokens",           0) or 0))
    c4.metric("Symbols Investigated", int(stats.get("symbols_investigated", 0) or 0))

    st.markdown("---")

    fc1, fc2, _ = st.columns([1, 1, 3])
    sel_symbol = fc1.selectbox("Symbol", ["All"] + SYMBOLS, key="rcr_symbol")
    sel_limit  = fc2.selectbox("Show last N", [25, 50, 100], key="rcr_limit")

    reports = _load_reports(
        symbol=sel_symbol if sel_symbol != "All" else None,
        limit=sel_limit,
    )

    if not reports:
        st.info("No root cause reports found. Start the root cause agent to generate them.")
        return

    st.caption(f"{len(reports)} report(s).")

    df = pd.DataFrame(reports)
    df["confidence_score"] = pd.to_numeric(df["confidence_score"], errors="coerce")

    if not df["confidence_score"].isna().all():
        st.subheader("Confidence Distribution")
        hist_fig = px.histogram(
            df.dropna(subset=["confidence_score"]),
            x="confidence_score",
            nbins=20,
            title="Report Confidence Scores",
            color_discrete_sequence=["#1f77b4"],
        )
        hist_fig.update_layout(height=250, margin=dict(l=40, r=20, t=40, b=30),
                               plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(hist_fig, use_container_width=True, key="conf_hist")
        st.markdown("---")

    st.subheader("Reports")
    for report in reports:
        report_id       = report.get("id")
        symbol          = report.get("symbol", "?")
        summary         = report.get("summary") or "(no summary)"
        full_report     = report.get("full_report") or ""
        root_cause_type = report.get("root_cause_type") or "—"
        generated_at    = report.get("generated_at")
        conf_score      = report.get("confidence_score")
        conf_label, _   = _confidence_label(conf_score)

        with st.expander(f"**{symbol}** | {root_cause_type} | Confidence: {conf_label} | {generated_at}"):
            m1, m2, m3, m4 = st.columns(4)
            m1.markdown(f"**Symbol**\n\n{symbol}")
            m2.markdown(f"**Anomaly Type**\n\n{report.get('anomaly_type') or '—'}")
            m3.markdown(f"**Severity**\n\n{(report.get('severity') or '—').upper()}")
            m4.markdown(f"**Model**\n\n{report.get('agent_model') or '—'}")
            st.markdown("---")
            st.markdown("**Summary**")
            st.info(summary)
            if full_report:
                st.markdown("**Full Report**")
                st.markdown(full_report)
            recs = report.get("recommendations")
            if recs:
                st.markdown("**Recommendations**")
                for rec in (recs if isinstance(recs, list) else [recs]):
                    st.markdown(f"- {rec}")
            evidence = report.get("evidence")
            if evidence:
                st.markdown("**Evidence**")
                st.json(evidence)
            if report_id:
                audit = _load_audit_trail(report_id)
                if audit:
                    st.markdown("**Investigation Steps**")
                    for step in audit:
                        dur = step.get("duration_ms")
                        label = f"Step: {step.get('step_name', 'step')}" + (f" ({dur} ms)" if dur else "")
                        with st.expander(label):
                            col_in, col_out = st.columns(2)
                            col_in.markdown("**Input**");  col_in.json(step.get("step_input") or {})
                            col_out.markdown("**Output**"); col_out.json(step.get("step_output") or {})
