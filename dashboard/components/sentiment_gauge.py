"""
Reusable sentiment gauge component.

Returns a Plotly indicator figure that can be rendered with st.plotly_chart().
"""

from __future__ import annotations

from typing import Dict, List, Optional

import plotly.graph_objects as go


def build_sentiment_gauge(
    symbol: str,
    score: float,
    title: Optional[str] = None,
) -> go.Figure:
    """
    Build a half-donut gauge for a sentiment score in [0, 1].

    0   = fully bearish (red)
    0.5 = neutral       (yellow)
    1   = fully bullish (green)

    Parameters
    ----------
    symbol : ticker label shown in the gauge title
    score  : sentiment score, expected in [0, 1]
    title  : optional override for the figure title
    """
    score = max(0.0, min(1.0, float(score)))
    pct   = score * 100

    if pct >= 60:
        bar_color = "#2ca02c"   # green
        label     = "Bullish"
    elif pct >= 40:
        bar_color = "#ffc107"   # amber
        label     = "Neutral"
    else:
        bar_color = "#d62728"   # red
        label     = "Bearish"

    fig = go.Figure(
        go.Indicator(
            mode="gauge+number+delta",
            value=pct,
            number={"suffix": "%", "font": {"size": 24}},
            title={"text": title or f"{symbol} Sentiment", "font": {"size": 14}},
            gauge={
                "axis": {
                    "range": [0, 100],
                    "tickwidth": 1,
                    "tickcolor": "#888",
                },
                "bar":  {"color": bar_color},
                "bgcolor": "white",
                "borderwidth": 2,
                "bordercolor": "#ddd",
                "steps": [
                    {"range": [0,  40],  "color": "#ffe6e6"},
                    {"range": [40, 60],  "color": "#fff9e6"},
                    {"range": [60, 100], "color": "#e6f4e6"},
                ],
                "threshold": {
                    "line": {"color": "#333", "width": 3},
                    "thickness": 0.75,
                    "value": pct,
                },
            },
            delta={"reference": 50, "relative": False},
        )
    )

    fig.update_layout(
        height=200,
        margin=dict(l=20, r=20, t=50, b=10),
        annotations=[
            dict(
                text=label,
                x=0.5, y=0.15,
                xref="paper", yref="paper",
                showarrow=False,
                font=dict(size=13, color=bar_color, family="sans-serif"),
            )
        ],
    )
    return fig


def build_sentiment_history_chart(
    records: List[Dict],
    symbol: str,
) -> go.Figure:
    """
    Line chart of sentiment_score over time for a single symbol.

    Parameters
    ----------
    records : list of dicts with keys: scored_at, sentiment_score
    symbol  : used in chart title
    """
    import pandas as pd

    fig = go.Figure()

    if not records:
        fig.add_annotation(
            text=f"No sentiment data for {symbol}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=13, color="gray"),
        )
        fig.update_layout(title=f"{symbol} Sentiment History", height=200)
        return fig

    df = pd.DataFrame(records)
    df["scored_at"]       = pd.to_datetime(df["scored_at"], utc=True, errors="coerce")
    df["sentiment_score"] = df["sentiment_score"].astype(float)
    df = df.sort_values("scored_at")

    fig.add_trace(
        go.Scatter(
            x=df["scored_at"],
            y=df["sentiment_score"],
            mode="lines+markers",
            name="Sentiment",
            line=dict(color="#9467bd", width=2),
            marker=dict(size=5),
            fill="tozeroy",
            fillcolor="rgba(148,103,189,0.1)",
            hovertemplate="<b>%{x|%H:%M}</b><br>Score: %{y:.4f}<extra></extra>",
        )
    )

    # Reference lines
    fig.add_hline(y=0.6, line_dash="dot", line_color="#2ca02c", annotation_text="Bullish", annotation_position="right")
    fig.add_hline(y=0.4, line_dash="dot", line_color="#d62728", annotation_text="Bearish", annotation_position="right")

    fig.update_layout(
        title=f"{symbol} — Sentiment Score Over Time",
        xaxis_title="Time",
        yaxis_title="Sentiment Score",
        yaxis=dict(range=[0, 1]),
        height=250,
        margin=dict(l=50, r=60, t=40, b=40),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    return fig
