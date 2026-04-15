"""
Statistical anomaly detectors: Z-Score and IQR.

Both detectors accept a history list plus a current value and return an
AnomalyResult describing whether an anomaly was detected and its severity.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class AnomalyResult:
    is_anomaly:       bool
    anomaly_type:     str
    severity:         str        # low | medium | high | critical
    detection_method: str
    anomaly_score:    float
    value:            float
    context:          Dict = field(default_factory=dict)

    @classmethod
    def no_anomaly(cls, detection_method: str, value: float) -> "AnomalyResult":
        return cls(
            is_anomaly=False,
            anomaly_type="none",
            severity="low",
            detection_method=detection_method,
            anomaly_score=0.0,
            value=value,
        )


def _severity_from_zscore(z: float) -> str:
    abs_z = abs(z)
    if abs_z >= 5.0: return "critical"
    if abs_z >= 4.0: return "high"
    if abs_z >= 3.0: return "medium"
    return "low"


def _severity_from_ratio(ratio: float, baseline: float = 3.0) -> str:
    r = ratio / baseline
    if r >= 3.0: return "critical"
    if r >= 2.0: return "high"
    if r >= 1.0: return "medium"
    return "low"


class ZScoreDetector:
    """
    Flags values where |z-score| > threshold (default 3.0).

    Requires at least 5 history points to produce a meaningful standard deviation.
    """

    METHOD = "zscore"

    def __init__(self, threshold: float = 3.0) -> None:
        self.threshold = threshold

    def detect(
        self,
        history: List[float],
        current_value: float,
        anomaly_type: str = "price_spike",
    ) -> AnomalyResult:
        if len(history) < 5:
            return AnomalyResult.no_anomaly(self.METHOD, current_value)

        arr  = np.array(history, dtype=float)
        mean = float(arr.mean())
        std  = float(arr.std(ddof=1))

        if std < 1e-9:
            return AnomalyResult.no_anomaly(self.METHOD, current_value)

        z = (current_value - mean) / std
        if abs(z) < self.threshold:
            return AnomalyResult.no_anomaly(self.METHOD, current_value)

        logger.info("Z-Score anomaly: %s  z=%.3f  mean=%.4f  std=%.4f", anomaly_type, z, mean, std)
        return AnomalyResult(
            is_anomaly=True,
            anomaly_type=anomaly_type,
            severity=_severity_from_zscore(z),
            detection_method=self.METHOD,
            anomaly_score=abs(z),
            value=current_value,
            context={"z_score": round(z, 4), "mean": round(mean, 4),
                     "std": round(std, 4), "threshold": self.threshold},
        )


class IQRDetector:
    """
    Flags values outside the Tukey fence: [Q1 - k*IQR, Q3 + k*IQR].

    Default k=1.5 is the standard inter-quartile range fence.
    """

    METHOD = "iqr"

    def __init__(self, k: float = 1.5) -> None:
        self.k = k

    def detect(
        self,
        history: List[float],
        current_value: float,
        anomaly_type: str = "price_outlier",
    ) -> AnomalyResult:
        if len(history) < 5:
            return AnomalyResult.no_anomaly(self.METHOD, current_value)

        arr = np.array(history, dtype=float)
        q1  = float(np.percentile(arr, 25))
        q3  = float(np.percentile(arr, 75))
        iqr = q3 - q1

        if iqr < 1e-9:
            return AnomalyResult.no_anomaly(self.METHOD, current_value)

        lower_fence = q1 - self.k * iqr
        upper_fence = q3 + self.k * iqr

        if lower_fence <= current_value <= upper_fence:
            return AnomalyResult.no_anomaly(self.METHOD, current_value)

        score = ((current_value - upper_fence) if current_value > upper_fence
                 else (lower_fence - current_value)) / iqr

        logger.info("IQR anomaly: value=%.4f  fence=[%.4f, %.4f]  score=%.3f",
                    current_value, lower_fence, upper_fence, score)
        return AnomalyResult(
            is_anomaly=True,
            anomaly_type=anomaly_type,
            severity=_severity_from_ratio(score),
            detection_method=self.METHOD,
            anomaly_score=float(score),
            value=current_value,
            context={"q1": round(q1, 4), "q3": round(q3, 4), "iqr": round(iqr, 4),
                     "lower_fence": round(lower_fence, 4), "upper_fence": round(upper_fence, 4),
                     "k": self.k},
        )
