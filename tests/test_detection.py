"""
Unit tests for the anomaly detection module.

    pytest tests/ -v
"""

import numpy as np
import pytest

from detection.statistical import AnomalyResult, IQRDetector, ZScoreDetector
from detection.volume import VolumeSpikeDetector


class TestAnomalyResult:
    def test_no_anomaly_factory(self):
        r = AnomalyResult.no_anomaly("zscore", 100.0)
        assert r.is_anomaly is False
        assert r.anomaly_type == "none"
        assert r.anomaly_score == 0.0
        assert r.value == 100.0

    def test_dataclass_fields(self):
        r = AnomalyResult(
            is_anomaly=True,
            anomaly_type="price_spike",
            severity="high",
            detection_method="zscore",
            anomaly_score=4.2,
            value=250.0,
            context={"z_score": 4.2},
        )
        assert r.severity == "high"
        assert r.context["z_score"] == 4.2


class TestZScoreDetector:
    def setup_method(self):
        self.detector = ZScoreDetector(threshold=3.0)

    def test_normal_price_not_flagged(self):
        history = [100.0, 101.0, 99.5, 100.5, 100.2, 99.8, 101.5, 100.1]
        assert self.detector.detect(history, 100.4).is_anomaly is False

    def test_within_2_sigma_not_flagged(self):
        history = [100.0] * 20
        assert self.detector.detect(history, 100.05).is_anomaly is False

    def test_extreme_high_price_flagged(self):
        history = [100.0, 100.1, 99.9, 100.2, 100.0, 100.1, 99.8, 100.3]
        result  = self.detector.detect(history, 200.0)
        assert result.is_anomaly is True
        assert result.anomaly_type == "price_spike"

    def test_extreme_low_price_flagged(self):
        history = [100.0, 100.1, 99.9, 100.2, 100.0, 100.1, 99.8, 100.3]
        assert self.detector.detect(history, 50.0).is_anomaly is True

    def test_severity_critical_at_5sigma(self):
        history = [100.0] * 8 + [101.0, 99.0]
        assert self.detector.detect(history, 500.0).severity == "critical"

    def test_insufficient_history(self):
        assert self.detector.detect([100.0, 101.0], 200.0).is_anomaly is False

    def test_zero_std_returns_no_anomaly(self):
        assert self.detector.detect([100.0] * 10, 100.0).is_anomaly is False

    def test_custom_threshold_respected(self):
        detector = ZScoreDetector(threshold=2.0)
        history  = [99.0, 100.0, 101.0, 100.0, 99.5, 100.5, 100.2, 99.8]
        assert detector.detect(history, 103.0).is_anomaly is True

    def test_result_contains_context(self):
        history = [100.0, 100.1, 99.9, 100.2, 100.0, 100.1, 99.8, 100.3]
        result  = self.detector.detect(history, 200.0)
        assert all(k in result.context for k in ("z_score", "mean", "std", "threshold"))


class TestIQRDetector:
    def setup_method(self):
        self.detector = IQRDetector(k=1.5)

    def test_normal_value_not_flagged(self):
        assert self.detector.detect(list(range(50, 150)), 99).is_anomaly is False

    def test_value_just_inside_fence_not_flagged(self):
        history = [10.0, 12.0, 11.0, 10.5, 11.5, 12.5, 9.5, 10.8]
        # actual Q1~10.375, Q3~11.625, IQR~1.25, upper fence~13.5
        assert self.detector.detect(history, 13.0).is_anomaly is False

    def test_extreme_high_flagged(self):
        assert self.detector.detect(list(range(50, 150)), 500).is_anomaly is True

    def test_extreme_low_flagged(self):
        assert self.detector.detect(list(range(50, 150)), -100).is_anomaly is True

    def test_insufficient_history(self):
        assert self.detector.detect([10.0, 11.0], 500.0).is_anomaly is False

    def test_zero_iqr_returns_no_anomaly(self):
        assert self.detector.detect([100.0] * 10, 999.0).is_anomaly is False

    def test_context_contains_fence_values(self):
        result = self.detector.detect(list(range(50, 150)), 500)
        assert all(k in result.context for k in ("lower_fence", "upper_fence", "iqr"))

    def test_detection_method_label(self):
        assert self.detector.detect(list(range(50, 150)), 500).detection_method == "iqr"


class TestVolumeSpikeDetector:
    def setup_method(self):
        self.detector = VolumeSpikeDetector(threshold=5.0)

    def test_normal_volume_not_flagged(self):
        history = [100.0, 110.0, 90.0, 105.0, 95.0, 100.0, 108.0]
        assert self.detector.detect(history, 106.0).is_anomaly is False

    def test_4x_average_not_flagged(self):
        assert self.detector.detect([100.0] * 7, 400.0).is_anomaly is False

    def test_5x_average_flagged(self):
        result = self.detector.detect([100.0] * 7, 510.0)
        assert result.is_anomaly is True
        assert result.anomaly_type == "volume_spike"

    def test_20x_average_is_critical(self):
        assert self.detector.detect([100.0] * 7, 2100.0).severity == "critical"

    def test_insufficient_history(self):
        assert self.detector.detect([100.0, 200.0], 10000.0).is_anomaly is False

    def test_zero_average_volume(self):
        assert self.detector.detect([0.0] * 7, 100.0).is_anomaly is False

    def test_context_contains_ratio(self):
        result = self.detector.detect([100.0] * 7, 1000.0)
        assert result.is_anomaly is True
        assert result.context["ratio"] == pytest.approx(10.0, abs=0.1)

    def test_custom_threshold(self):
        assert VolumeSpikeDetector(threshold=2.0).detect([100.0] * 7, 250.0).is_anomaly is True


class TestDetectorAgreement:
    def test_obvious_spike_flagged_by_all_statistical(self):
        history = [100.0, 100.1, 99.9, 100.2, 100.0, 100.1, 99.8, 100.3]
        assert ZScoreDetector(3.0).detect(history, 500.0).is_anomaly is True
        assert IQRDetector(1.5).detect(history, 500.0).is_anomaly is True

    def test_anomaly_score_is_float(self):
        history = [100.0, 100.5, 101.0, 99.5, 100.0, 101.0, 100.0, 100.5, 99.5, 100.0]
        result  = ZScoreDetector(3.0).detect(history, 105.0)
        assert isinstance(result.anomaly_score, float)
