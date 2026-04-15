"""
Volume spike detector.

Flags trades where current volume exceeds threshold * rolling average volume.
"""

from __future__ import annotations

import logging
from typing import List

import numpy as np

from detection.statistical import AnomalyResult

logger = logging.getLogger(__name__)


class VolumeSpikeDetector:
    """
    Raises an anomaly when current_volume > threshold * avg(volume_history).

    Default threshold is 5.0 (five times the rolling average).
    """

    METHOD = "volume_spike"

    def __init__(self, threshold: float = 5.0) -> None:
        self.threshold = threshold

    def detect(self, volume_history: List[float], current_volume: float) -> AnomalyResult:
        if len(volume_history) < 3:
            return AnomalyResult.no_anomaly(self.METHOD, current_volume)

        avg_volume = float(np.array(volume_history, dtype=float).mean())
        if avg_volume < 1e-9:
            return AnomalyResult.no_anomaly(self.METHOD, current_volume)

        ratio = current_volume / avg_volume
        if ratio < self.threshold:
            return AnomalyResult.no_anomaly(self.METHOD, current_volume)

        if ratio >= self.threshold * 4:   severity = "critical"
        elif ratio >= self.threshold * 2: severity = "high"
        elif ratio >= self.threshold * 1.5: severity = "medium"
        else: severity = "low"

        logger.info("Volume spike: current=%.2f  avg=%.2f  ratio=%.2fx  severity=%s",
                    current_volume, avg_volume, ratio, severity)
        return AnomalyResult(
            is_anomaly=True,
            anomaly_type="volume_spike",
            severity=severity,
            detection_method=self.METHOD,
            anomaly_score=float(ratio),
            value=current_volume,
            context={"current_volume": round(current_volume, 4),
                     "avg_volume": round(avg_volume, 4),
                     "ratio": round(ratio, 4),
                     "threshold": self.threshold},
        )
