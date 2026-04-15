"""
Reusable Plotly price chart component.

Returns a go.Figure that can be rendered with st.plotly_chart().
"""

from __future__ import annotations

from typing import Dict, List

import pandas as pd
import plotly.graph_objects as go


def build_price_chart(
    records: List[Dict],
    symbol: str,
    title: str = "",
) -> go.Figure:
    """
    Build a line chart of price over time for a single symbol.

    Parameters
    ----------
    records : list of dicts with keys: quoted_at (datetime), current_price (float)
    symbol  : ticker string used in the chart title
    title   : optional override for chart title
    """
    if not records:
        fig = go.Figure()
        fig.add_annotation(
            text=f"No data available for {symbol}",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=14, color="gray"),
        )
        fig.update_layout(
            title=title or f"{symbol} — No Data",
            height=300,
            margin=dict(l=40, r=20, t=40, b=30),
        )
        return fig

    df = pd.DataFrame(records)
    df["quoted_at"] = pd.to_datetime(df["quoted_at"], utc=True, errors="coerce")
    df = df.sort_values("quoted_at")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["quoted_at"],
            y=df["current_price"].astype(float),
            mode="lines+markers",
            name=symbol,
            line=dict(width=2, color="#1f77b4"),
            marker=dict(size=4),
            hovertemplate=(
                "<b>%{x|%H:%M:%S}</b><br>"
                "Price: $%{y:.2f}<extra></extra>"
            ),
        )
    )

    # Add percent change annotation on the latest point
    if len(df) > 1:
        first_price = float(df["current_price"].iloc[0])
        last_price  = float(df["current_price"].iloc[-1])
        pct_change  = ((last_price - first_price) / first_price) * 100 if first_price else 0
        color = "#2ca02c" if pct_change >= 0 else "#d62728"
        fig.add_annotation(
            x=df["quoted_at"].iloc[-1],
            y=last_price,
            text=f"{pct_change:+.2f}%",
            showarrow=True,
            arrowhead=2,
            font=dict(color=color, size=12, family="monospace"),
            arrowcolor=color,
        )

    fig.update_layout(
        title=title or f"{symbol} — Price (Last Hour)",
        xaxis_title="Time",
        yaxis_title="Price (USD)",
        height=300,
        margin=dict(l=50, r=20, t=40, b=40),
        xaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
        yaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        hovermode="x unified",
    )
    return fig


def build_multi_symbol_chart(
    symbol_records: Dict[str, List[Dict]],
) -> go.Figure:
    """
    Overlay multiple symbols' price series on one chart (normalised to 100).
    """
    fig = go.Figure()

    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"]

    for i, (symbol, records) in enumerate(symbol_records.items()):
        if not records:
            continue
        df = pd.DataFrame(records)
        df["quoted_at"]     = pd.to_datetime(df["quoted_at"], utc=True, errors="coerce")
        df["current_price"] = df["current_price"].astype(float)
        df = df.sort_values("quoted_at")

        # Normalise: first price = 100
        base = df["current_price"].iloc[0]
        if base and base > 0:
            normalised = (df["current_price"] / base) * 100
        else:
            normalised = df["current_price"]

        fig.add_trace(
            go.Scatter(
                x=df["quoted_at"],
                y=normalised,
                mode="lines",
                name=symbol,
                line=dict(width=2, color=colors[i % len(colors)]),
                hovertemplate=f"<b>{symbol}</b> %{{y:.2f}}<extra></extra>",
            )
        )

    fig.update_layout(
        title="All Symbols — Normalised Price (Base = 100)",
        xaxis_title="Time",
        yaxis_title="Normalised Price",
        height=400,
        margin=dict(l=50, r=20, t=50, b=40),
        plot_bgcolor="white",
        paper_bgcolor="white",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig
