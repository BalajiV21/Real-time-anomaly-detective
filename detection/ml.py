"""
Isolation Forest ML anomaly detector.

Trains on historical aggregated_metrics rows from TimescaleDB and scores
incoming windows for anomalousness. The fitted model and its scaler are
persisted together to models/isolation_forest.pkl.

Features: [avg_price, total_volume, std_dev, vwap, trade_count]

Typical usage:
    detector = IsolationForestDetector()
    detector.train_from_db()     # once, after collecting warm-up data
    result = detector.detect(agg_row)
"""

from __future__ import annotations

import logging
import pickle
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

from detection.statistical import AnomalyResult

logger = logging.getLogger(__name__)

MODEL_PATH    = Path(__file__).parent.parent / "models" / "isolation_forest.pkl"
N_ESTIMATORS  = 100
CONTAMINATION = 0.05


class IsolationForestDetector:

    METHOD = "isolation_forest"

    def __init__(self) -> None:
        self._bundle: Optional[Dict[str, Any]] = None
        self._try_load()

    def _try_load(self) -> None:
        if not MODEL_PATH.exists():
            logger.info("No saved model at %s. Call train_from_db() to train.", MODEL_PATH)
            return
        try:
            with open(MODEL_PATH, "rb") as fh:
                self._bundle = pickle.load(fh)
            logger.info("Isolation Forest loaded from %s.", MODEL_PATH)
        except Exception as exc:
            logger.warning("Could not load model: %s", exc)

    def _save(self) -> None:
        MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(MODEL_PATH, "wb") as fh:
            pickle.dump(self._bundle, fh)
        logger.info("Model saved to %s.", MODEL_PATH)

    @staticmethod
    def _to_matrix(records: List[Dict]) -> np.ndarray:
        return np.array([
            [float(r.get("avg_price",    0) or 0),
             float(r.get("total_volume", 0) or 0),
             float(r.get("std_dev",      0) or 0),
             float(r.get("vwap",         0) or 0),
             float(r.get("trade_count",  0) or 0)]
            for r in records
        ], dtype=float)

    def train(self, records: List[Dict]) -> None:
        if len(records) < 20:
            logger.warning("Need >= 20 training records, got %d. Skipping.", len(records))
            return

        from sklearn.ensemble import IsolationForest
        from sklearn.preprocessing import StandardScaler

        X      = self._to_matrix(records)
        scaler = StandardScaler()
        X_sc   = scaler.fit_transform(X)
        model  = IsolationForest(n_estimators=N_ESTIMATORS, contamination=CONTAMINATION,
                                 random_state=42, n_jobs=-1)
        model.fit(X_sc)
        self._bundle = {"model": model, "scaler": scaler}
        self._save()
        logger.info("Isolation Forest trained on %d samples.", len(records))

    def train_from_db(
        self,
        symbol: Optional[str] = None,
        window_size: str = "5m",
        limit_per_symbol: int = 500,
    ) -> None:
        """Load aggregated_metrics from TimescaleDB and fit the model."""
        from config.settings import SYMBOLS
        from storage.connection import get_db
        from storage.queries import get_aggregated_metrics

        targets = [symbol] if symbol else SYMBOLS
        all_records: List[Dict] = []
        with get_db() as session:
            for sym in targets:
                all_records.extend(
                    get_aggregated_metrics(session, symbol=sym,
                                          window_size=window_size, limit=limit_per_symbol)
                )

        if not all_records:
            logger.warning("No training data found for window_size=%s.", window_size)
            return

        logger.info("Training on %d rows.", len(all_records))
        self.train(all_records)

    def detect(self, record: Dict) -> AnomalyResult:
        ref_value = float(record.get("avg_price", 0) or 0)
        if self._bundle is None:
            return AnomalyResult.no_anomaly(self.METHOD, ref_value)

        try:
            model  = self._bundle["model"]
            scaler = self._bundle["scaler"]
            X_sc   = scaler.transform(self._to_matrix([record]))
            pred   = int(model.predict(X_sc)[0])
            raw    = float(model.score_samples(X_sc)[0])
            # score_samples returns values in roughly [-0.5, 0.5]; normalise to [0, 1]
            score  = float(max(0.0, min(1.0, 1.0 - (raw + 0.5))))
        except Exception as exc:
            logger.error("Isolation Forest inference failed: %s", exc)
            return AnomalyResult.no_anomaly(self.METHOD, ref_value)

        if pred != -1:
            return AnomalyResult.no_anomaly(self.METHOD, ref_value)

        if score >= 0.85:   severity = "critical"
        elif score >= 0.70: severity = "high"
        elif score >= 0.55: severity = "medium"
        else:               severity = "low"

        logger.info("ML anomaly: window=%s  score=%.4f  severity=%s",
                    record.get("window_size", "?"), score, severity)
        return AnomalyResult(
            is_anomaly=True,
            anomaly_type="ml_anomaly",
            severity=severity,
            detection_method=self.METHOD,
            anomaly_score=score,
            value=ref_value,
            context={
                "raw_isolation_score": round(raw, 6),
                "window_size": record.get("window_size", ""),
                "features": {k: record.get(k) for k in
                             ("avg_price", "total_volume", "std_dev", "vwap", "trade_count")},
            },
        )
