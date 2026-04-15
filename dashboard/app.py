"""
Streamlit dashboard entry point.

    streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
from sqlalchemy import text

from storage.connection import get_db

st.set_page_config(
    page_title="Financial Anomaly Detective",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

PAGES = {
    "Home":               None,
    "Live Prices":        "live_prices",
    "Anomaly Timeline":   "anomaly_timeline",
    "Root Cause Reports": "root_cause_reports",
    "System Health":      "system_health",
}

with st.sidebar:
    st.markdown("## Financial Anomaly Detective")
    st.markdown("---")
    selection = st.radio("Navigate", list(PAGES.keys()), label_visibility="collapsed")
    st.markdown("---")
    st.caption(
        "**Symbols:** AAPL · TSLA · AMZN · MSFT · GOOGL\n\n"
        "**Detection:** Z-Score · IQR · Volume Spike · Isolation Forest"
    )

if selection == "Home":
    st.title("Real-Time Financial Anomaly Detective")
    st.markdown(
        """
        A real-time streaming system that ingests live Finnhub market data,
        detects price and volume anomalies using statistical and ML methods,
        and presents findings on this dashboard.

        ---

        ### Architecture

        ```
        Finnhub WebSocket  -->  Kafka (stock-trades)  -->  Spark Structured Streaming
        Finnhub REST API   -->  Kafka (quotes/news/    -->  Windowed Aggregations
                                sentiment/anomalies)   -->  Anomaly Detectors
                                                       -->  TimescaleDB / PostgreSQL
                                                       -->  Dashboard
        ```

        ### Detection Methods

        | Method | Type | Threshold |
        |--------|------|-----------|
        | Z-Score | Statistical | \\|z\\| > 3.0 |
        | IQR Fence | Statistical | Outside Q1 - 1.5*IQR ... Q3 + 1.5*IQR |
        | Volume Spike | Threshold | > 5x rolling average |
        | Isolation Forest | ML | Score > 0.55 |

        ---

        ### Quick Start

        ```bash
        docker-compose up -d
        python -c "from storage.connection import init_db; init_db()"
        python -m ingestion.trade_producer       # terminal 1
        python -m ingestion.quote_producer       # terminal 2
        python -m ingestion.news_producer        # terminal 3
        python -m ingestion.sentiment_producer   # terminal 4
        python -m streaming.consumer             # terminal 5
        streamlit run dashboard/app.py           # terminal 6
        ```
        """
    )

    st.markdown("---")
    st.subheader("Pipeline Stats")
    try:
        with get_db() as session:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Trades Ingested",    f'{session.execute(text("SELECT COUNT(*) FROM raw_trades")).scalar() or 0:,}')
            c2.metric("Anomalies Detected", f'{session.execute(text("SELECT COUNT(*) FROM anomaly_logs")).scalar() or 0:,}')
            c3.metric("Root Cause Reports", f'{session.execute(text("SELECT COUNT(*) FROM root_cause_reports")).scalar() or 0:,}')
            c4.metric("Symbols Active",     f'{session.execute(text("SELECT COUNT(DISTINCT symbol) FROM stock_quotes")).scalar() or 0}')
    except Exception as exc:
        st.error(f"Cannot connect to database: {exc}")
        st.info("Ensure the Docker stack is running: `docker-compose up -d`")

elif selection == "Live Prices":
    from dashboard.pages.live_prices import show
    show()

elif selection == "Anomaly Timeline":
    from dashboard.pages.anomaly_timeline import show
    show()

elif selection == "Root Cause Reports":
    from dashboard.pages.root_cause_reports import show
    show()

elif selection == "System Health":
    from dashboard.pages.system_health import show
    show()
