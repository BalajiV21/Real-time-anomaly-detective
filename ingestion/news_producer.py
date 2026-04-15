"""
News producer.

Polls Finnhub /news (category=general) every POLL_INTERVALS['NEWS'] seconds
and publishes new articles to Kafka. Duplicate detection is handled in-memory
via a set of seen article IDs; suitable for a single-process deployment.

    python -m ingestion.news_producer
"""

import json
import logging
import time
from typing import Set

import finnhub
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

KAFKA_TOPIC   = settings.KAFKA_TOPICS["NEWS"]
POLL_INTERVAL = settings.POLL_INTERVALS["NEWS"]

finnhub_client = finnhub.Client(api_key=settings.FINNHUB_API_KEY)

kafka_producer = KafkaProducer(
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3,
)

_published_ids: Set[int] = set()


def fetch_and_publish() -> None:
    try:
        articles = finnhub_client.general_news("general", min_id=0)
    except Exception as exc:
        logger.error("Finnhub /news error: %s", exc)
        return

    if not articles:
        return

    new_count = 0
    for article in articles:
        article_id = article.get("id")
        if article_id in _published_ids:
            continue
        try:
            kafka_producer.send(KAFKA_TOPIC, value=article)
            _published_ids.add(article_id)
            new_count += 1
            logger.info("News: %s", article.get("headline", "(no headline)"))
        except KafkaError as exc:
            logger.error("Kafka publish failed for article id=%s: %s", article_id, exc)

    logger.info("Published %d new article(s).", new_count)


def run() -> None:
    logger.info("News producer started. Polling every %d s.", POLL_INTERVAL)
    while True:
        fetch_and_publish()
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
