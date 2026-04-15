"""
AnomalyPublisher — runs all detectors and publishes results.

For each detected anomaly:
  1. Sends a message to the Kafka 'anomalies' topic.
  2. Inserts a row into the anomaly_logs table.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings
from detection.ml import IsolationForestDetector
from detection.statistical import AnomalyResult, IQRDetector, ZScoreDetector
from detection.volume import VolumeSpikeDetector
from storage.connection import get_db
from storage.queries import insert_anomaly_log

logger = logging.getLogger(__name__)

KAFKA_TOPIC = settings.KAFKA_TOPICS["ANOMALIES"]


class AnomalyPublisher:

    def __init__(self) -> None:
        self._zscore   = ZScoreDetector(threshold=3.0)
        self._iqr      = IQRDetector(k=1.5)
        self._volume   = VolumeSpikeDetector(threshold=5.0)
        self._ml       = IsolationForestDetector()
        self._producer = self._init_producer()

    def _init_producer(self) -> Optional[KafkaProducer]:
        try:
            return KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
        except Exception as exc:
            logger.error("Kafka producer unavailable: %s. Anomalies will be DB-only.", exc)
            return None

    def detect_and_publish(
        self,
        symbol: str,
        trades_df: pd.DataFrame,
        aggregations: Optional[List[Dict]] = None,
    ) -> List[AnomalyResult]:
        """
        Run all detectors against a micro-batch for a single symbol.

        Returns all AnomalyResult objects (including non-anomalies). Anomalies
        are automatically published to Kafka and persisted to the database.
        """
        results: List[AnomalyResult] = []
        if trades_df.empty:
            return results

        prices  = trades_df["price"].astype(float).tolist()
        volumes = trades_df["volume"].astype(float).tolist()

        if len(prices) > 1:
            results.append(self._zscore.detect(prices[:-1], prices[-1], "price_spike"))
            results.append(self._iqr.detect(prices[:-1],    prices[-1], "price_outlier"))

        if len(volumes) > 1:
            results.append(self._volume.detect(volumes[:-1], volumes[-1]))

        if aggregations:
            for agg in [a for a in aggregations if a.get("window_size") == "5m"]:
                results.append(self._ml.detect(agg))

        avg_price  = float(pd.Series(prices).mean())  if prices  else 0.0
        avg_volume = float(pd.Series(volumes).mean()) if volumes else 0.0

        for r in results:
            if r.is_anomaly:
                self._publish(symbol, r, avg_price, avg_volume)

        return results

    def _publish(self, symbol: str, result: AnomalyResult, price: float, volume: float) -> None:
        payload: Dict = {
            "symbol":           symbol,
            "anomaly_type":     result.anomaly_type,
            "severity":         result.severity,
            "detection_method": result.detection_method,
            "anomaly_score":    round(result.anomaly_score, 6),
            "price":            round(price, 4),
            "volume":           round(volume, 4),
            "detected_at":      datetime.now(timezone.utc).isoformat(),
            "context":          result.context,
        }

        if self._producer:
            try:
                self._producer.send(KAFKA_TOPIC, key=symbol, value=payload)
                logger.info("Anomaly -> Kafka | %s | %s | %s | score=%.4f",
                            symbol, result.anomaly_type, result.severity, result.anomaly_score)
            except KafkaError as exc:
                logger.error("Kafka publish failed for %s: %s", symbol, exc)

        try:
            with get_db() as session:
                insert_anomaly_log(
                    session,
                    symbol=symbol,
                    anomaly_type=result.anomaly_type,
                    severity=result.severity,
                    detection_method=result.detection_method,
                    anomaly_score=result.anomaly_score,
                    price=price,
                    volume=volume,
                    context=result.context,
                )
        except Exception as exc:
            logger.error("DB insert failed for anomaly %s: %s", symbol, exc)
