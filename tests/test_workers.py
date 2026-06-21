"""Tests for task implementations (no Ray required)."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.backend.workers import _simulate, _short_circuit, _bond_price


def test_short_circuit():
    result = _short_circuit({})
    assert result["value"] == 0
    assert result["calc_time"] == 0.0


def test_simulate_range():
    import time
    for _ in range(5):
        t0 = time.perf_counter()
        result = _simulate({"max_calc_time": 0.1})
        elapsed = time.perf_counter() - t0
        assert 0.0 <= result["calc_time"] <= 0.11
        assert elapsed < 0.15


def test_bond_price_basic():
    # Par bond: coupon_rate == ytm → price ≈ face
    result = _bond_price({
        "face_value": 1000,
        "coupon_rate": 0.05,
        "ytm": 0.05,
        "periods": 10,
        "freq": 1,
        "max_calc_time": 0,   # skip sleep
    })
    assert abs(result["price"] - 1000.0) < 0.01
    assert result["modified_duration"] > 0


def test_bond_price_premium():
    # Lower yield → price above face
    result = _bond_price({
        "face_value": 1000,
        "coupon_rate": 0.05,
        "ytm": 0.03,
        "periods": 10,
        "freq": 1,
        "max_calc_time": 0,
    })
    assert result["price"] > 1000.0


def test_bond_price_discount():
    # Higher yield → price below face
    result = _bond_price({
        "face_value": 1000,
        "coupon_rate": 0.05,
        "ytm": 0.08,
        "periods": 10,
        "freq": 1,
        "max_calc_time": 0,
    })
    assert result["price"] < 1000.0
