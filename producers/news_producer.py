"""
News producer for the Real-Time Financial Anomaly Detective.

Polls the Finnhub REST /news endpoint (category=general) every
POLL_INTERVALS['NEWS'] seconds (default 300 s) and publishes each
new article to the Kafka 'market-news' topic.

Duplicate detection is handled in-memory via a set of seen article IDs.
This is sufficient for a single-process producer; for distributed deployments
consider persisting seen IDs in Redis or a database.

Run from the project root:
    python -m producers.news_producer
"""

import json
import logging
import time
from typing import Set

import finnhub
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
KAFKA_TOPIC = settings.KAFKA_TOPICS["NEWS"]
POLL_INTERVAL = settings.POLL_INTERVALS["NEWS"]   # seconds

# ---------------------------------------------------------------------------
# Clients (module-level singletons)
# ---------------------------------------------------------------------------
finnhub_client = finnhub.Client(api_key=settings.FINNHUB_API_KEY)

kafka_producer = KafkaProducer(
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3,
)

# In-memory duplicate-detection set
_published_ids: Set[int] = set()


# ---------------------------------------------------------------------------
# Core polling logic
# ---------------------------------------------------------------------------

def fetch_and_publish_news() -> None:
    """
    Fetch the latest general-market news articles, publish any that have not
    been seen before to the Kafka market-news topic.
    """
    try:
        articles = finnhub_client.general_news("general", min_id=0)
    except Exception as exc:
        logger.error("Finnhub /news error: %s", exc)
        return

    if not articles:
        logger.info("No news articles returned this cycle.")
        return

    new_count = 0
    for article in articles:
        article_id = article.get("id")

        if article_id in _published_ids:
            continue

        headline: str = article.get("headline", "(no headline)")
        logger.info("News published: %s", headline)

        # ---- Publish to Kafka -----------------------------------------------
        try:
            kafka_producer.send(KAFKA_TOPIC, value=article)
            _published_ids.add(article_id)
            new_count += 1
        except KafkaError as exc:
            logger.error(
                "Kafka publish failed for news article id=%s: %s", article_id, exc
            )

    logger.info("Published %d new article(s) this cycle.", new_count)


def poll_forever() -> None:
    """
    Continuously poll Finnhub general news, sleeping POLL_INTERVAL seconds
    between each fetch cycle.
    """
    logger.info("News producer started. Polling every %d s.", POLL_INTERVAL)
    while True:
        fetch_and_publish_news()
        logger.info("Sleeping %d s before next news poll…", POLL_INTERVAL)
        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    poll_forever()
